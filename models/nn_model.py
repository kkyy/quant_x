"""
PyTorch MLP alpha model with Apple MPS (M-series GPU) support.

Registered name: "mlp"

Requires: pip install torch
On Apple Silicon M1/M2/M3, PyTorch automatically uses the MPS backend
(Metal Performance Shaders) for GPU acceleration when available.

Install:
    pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu
    # or for latest nightly with better MPS support:
    pip install --pre torch --index-url https://download.pytorch.org/whl/nightly/cpu
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from .base import BaseAlphaModel, ModelRegistry

logger = logging.getLogger(__name__)

_DEFAULT_PARAMS: Dict[str, Any] = {
    "hidden_dims": [256, 128, 64],
    "dropout": 0.2,
    "batch_norm": True,
    "lr": 1e-3,
    "weight_decay": 1e-4,
    "batch_size": 2048,
    "max_epochs": 100,
    "patience": 10,          # early stopping patience (epochs)
    "seed": 42,
    "device": "auto",        # "auto" → mps > cuda > cpu
}


def _resolve_device(preference: str = "auto") -> "torch.device":
    import torch
    if preference == "auto":
        if torch.backends.mps.is_available():
            device = torch.device("mps")
            logger.info("MLPAlphaModel: using Apple MPS (M-series GPU)")
        elif torch.cuda.is_available():
            device = torch.device("cuda")
            logger.info("MLPAlphaModel: using CUDA GPU")
        else:
            device = torch.device("cpu")
            logger.info("MLPAlphaModel: using CPU")
    else:
        device = torch.device(preference)
        logger.info("MLPAlphaModel: using device=%s (explicit)", device)
    return device


def _build_mlp(input_dim: int, hidden_dims: List[int], dropout: float, batch_norm: bool):
    """Return an nn.Sequential MLP (no output activation; predicts raw scores)."""
    import torch.nn as nn
    layers: list = []
    in_dim = input_dim
    for h in hidden_dims:
        layers.append(nn.Linear(in_dim, h))
        if batch_norm:
            layers.append(nn.BatchNorm1d(h))
        layers.append(nn.ReLU())
        if dropout > 0:
            layers.append(nn.Dropout(dropout))
        in_dim = h
    layers.append(nn.Linear(in_dim, 1))
    return nn.Sequential(*layers)


@ModelRegistry.register("mlp")
class MLPAlphaModel(BaseAlphaModel):
    """
    Feed-forward MLP alpha model backed by PyTorch.

    On Apple Silicon (M1/M2/M3), automatically uses the MPS backend for
    GPU-accelerated training and inference.  Falls back to CUDA or CPU if
    MPS is unavailable.

    Supports extra factor columns (sector / custom factors) merged into the
    feature matrix, identical to LGBMAlphaModel.

    Parameters
    ----------
    mlp_params    : override any _DEFAULT_PARAMS keys
    extra_factors : pre-computed extra factor DataFrame merged into features
    """

    def __init__(
        self,
        mlp_params: Optional[Dict[str, Any]] = None,
        extra_factors: Optional[pd.DataFrame] = None,
    ):
        try:
            import torch  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "PyTorch is required for MLPAlphaModel. "
                "Install with: pip install torch"
            ) from exc

        self.params = {**_DEFAULT_PARAMS, **(mlp_params or {})}
        self.extra_factors = extra_factors
        self.model = None
        self.feature_names_: Optional[List[str]] = None
        self._input_dim: Optional[int] = None
        self._feature_mean: Optional[np.ndarray] = None
        self._feature_std: Optional[np.ndarray] = None

    # ── fit ──────────────────────────────────────────────────────────────────

    def fit(self, dataset, **kwargs) -> "MLPAlphaModel":
        import torch
        import torch.nn as nn
        from torch.utils.data import DataLoader, TensorDataset

        seed = int(self.params["seed"])
        torch.manual_seed(seed)
        np.random.seed(seed)

        device = _resolve_device(self.params["device"])

        df_tr = dataset.prepare("train", col_set=["feature", "label"], data_key="learn")
        df_va = dataset.prepare("valid", col_set=["feature", "label"], data_key="learn")
        X_tr, y_tr = df_tr["feature"], df_tr["label"].squeeze()
        X_va, y_va = df_va["feature"], df_va["label"].squeeze()

        X_tr = self._merge_extra(X_tr, self.extra_factors)
        X_va = self._merge_extra(X_va, self.extra_factors)
        self.feature_names_ = X_tr.columns.tolist()

        # Impute and normalise
        X_tr_np = X_tr.values.astype(np.float32)
        X_va_np = X_va.values.astype(np.float32)
        self._feature_mean = np.nanmean(X_tr_np, axis=0)
        self._feature_std = np.nanstd(X_tr_np, axis=0) + 1e-8

        X_tr_np = np.nan_to_num((X_tr_np - self._feature_mean) / self._feature_std)
        X_va_np = np.nan_to_num((X_va_np - self._feature_mean) / self._feature_std)

        y_tr_np = y_tr.fillna(0).values.astype(np.float32)
        y_va_np = y_va.fillna(0).values.astype(np.float32)

        self._input_dim = X_tr_np.shape[1]
        self.model = _build_mlp(
            self._input_dim,
            self.params["hidden_dims"],
            self.params["dropout"],
            self.params["batch_norm"],
        ).to(device)

        optimizer = torch.optim.Adam(
            self.model.parameters(),
            lr=float(self.params["lr"]),
            weight_decay=float(self.params["weight_decay"]),
        )
        loss_fn = nn.MSELoss()

        tr_loader = DataLoader(
            TensorDataset(
                torch.from_numpy(X_tr_np),
                torch.from_numpy(y_tr_np).unsqueeze(1),
            ),
            batch_size=int(self.params["batch_size"]),
            shuffle=True,
        )

        X_va_t = torch.from_numpy(X_va_np).to(device)
        y_va_t = torch.from_numpy(y_va_np).unsqueeze(1).to(device)

        best_val_loss = float("inf")
        patience_counter = 0
        best_state = None

        for epoch in range(int(self.params["max_epochs"])):
            self.model.train()
            for X_batch, y_batch in tr_loader:
                X_batch, y_batch = X_batch.to(device), y_batch.to(device)
                optimizer.zero_grad()
                loss = loss_fn(self.model(X_batch), y_batch)
                loss.backward()
                optimizer.step()

            self.model.eval()
            with torch.no_grad():
                val_loss = loss_fn(self.model(X_va_t), y_va_t).item()

            if (epoch + 1) % 10 == 0:
                logger.info("  epoch %d/%d  val_loss=%.6f", epoch + 1, self.params["max_epochs"], val_loss)

            if val_loss < best_val_loss:
                best_val_loss = val_loss
                best_state = {k: v.cpu().clone() for k, v in self.model.state_dict().items()}
                patience_counter = 0
            else:
                patience_counter += 1
                if patience_counter >= int(self.params["patience"]):
                    logger.info("  Early stopping at epoch %d (patience=%d)", epoch + 1, self.params["patience"])
                    break

        if best_state is not None:
            self.model.load_state_dict(best_state)

        logger.info(
            "MLPAlphaModel trained: features=%d  best_val_loss=%.6f  device=%s",
            self._input_dim, best_val_loss, device,
        )
        return self

    # ── predict ──────────────────────────────────────────────────────────────

    def predict(self, dataset, segment: str = "test") -> pd.Series:
        import torch

        if self.model is None:
            raise RuntimeError("MLPAlphaModel: call fit() before predict()")

        device = _resolve_device(self.params["device"])
        X = dataset.prepare(segment, col_set="feature", data_key="infer")
        X = self._merge_extra(X, self.extra_factors).reindex(
            columns=self.feature_names_, fill_value=np.nan
        )
        X_np = X.values.astype(np.float32)
        X_np = np.nan_to_num((X_np - self._feature_mean) / self._feature_std)

        self.model.eval()
        self.model.to(device)
        with torch.no_grad():
            scores = (
                self.model(torch.from_numpy(X_np).to(device))
                .squeeze(1)
                .cpu()
                .numpy()
            )
        return pd.Series(scores, index=X.index, name="score")
