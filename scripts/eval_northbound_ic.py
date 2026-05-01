"""Evaluate IC/ICIR of individual northbound features against forward 5-day returns.

Computes daily Rank IC (Spearman) per cross-section, then aggregates
mean IC, IC std, ICIR, and valid day count for each northbound feature column.
"""
import sys
import os

# Add project root to sys.path so quant_ex imports work
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import qlib
import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from features.northbound_factor import NorthboundFactor


def main():
    # 1. Initialize qlib
    qlib_bin = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "qlib_data", "qlib_bin")
    qlib.init(provider_uri=qlib_bin, region="cn")

    # 2. Load csi1000 instruments and price data
    from qlib.data import D

    instruments = D.instruments("csi1000")
    fields = ["$close", "$volume"]
    print("Loading price data for csi1000 (2020-01-01 to 2026-04-29)...")
    price_data = D.features(
        instruments, fields, start_time="2020-01-01", end_time="2026-04-29"
    )
    print(f"  Price data shape: {price_data.shape}")
    print(f"  Instruments: {price_data.index.get_level_values(0).nunique()}")
    print(f"  Date range: {price_data.index.get_level_values(1).min().date()} to "
          f"{price_data.index.get_level_values(1).max().date()}")

    # 3. Instantiate NorthboundFactor with default config
    factor = NorthboundFactor(
        windows=[5, 10, 20, 60],
        include_raw=True,
        include_change=True,
    )

    # 4. Compute northbound features
    print("\nComputing northbound features...")
    nb_df = factor.compute(price_data)
    if nb_df is None:
        print("ERROR: NorthboundFactor returned None (no cache data).")
        sys.exit(1)
    print(f"  Northbound features shape: {nb_df.shape}")
    print(f"  Feature columns: {list(nb_df.columns)}")

    # 5. Compute forward 5-day returns
    print("\nComputing forward 5-day returns...")
    fwd_ret = price_data["$close"].groupby(level=0).pct_change(5).shift(-5)
    fwd_ret.name = "fwd_ret_5d"

    # Align indices
    common_idx = nb_df.index.intersection(fwd_ret.dropna().index)
    fwd_ret_aligned = fwd_ret.reindex(common_idx)
    nb_df_aligned = nb_df.reindex(common_idx)
    print(f"  Common index size: {len(common_idx)}")

    # 6. Compute Rank IC per feature per day
    print("\nComputing Rank IC for each northbound feature...")
    dates = common_idx.get_level_values(1).unique().sort_values()
    results = []

    for col in nb_df_aligned.columns:
        feature_series = nb_df_aligned[col]
        ic_values = []
        valid_days = 0

        for dt in dates:
            day_mask = common_idx.get_level_values(1) == dt
            feat_day = feature_series[day_mask].dropna()
            ret_day = fwd_ret_aligned[day_mask].dropna()

            # Align on common instruments for this day
            common_inst = feat_day.index.get_level_values(0).intersection(
                ret_day.index.get_level_values(0)
            )
            if len(common_inst) < 10:
                continue

            feat_vals = feat_day.loc[common_inst].values
            ret_vals = ret_day.loc[common_inst].values

            # Skip if feature has zero variance
            if np.nanstd(feat_vals) < 1e-12:
                continue

            corr, _ = spearmanr(feat_vals, ret_vals, nan_policy="omit")
            if not np.isnan(corr):
                ic_values.append(corr)
                valid_days += 1

        if valid_days == 0:
            results.append({
                "feature": col,
                "mean_ic": np.nan,
                "ic_std": np.nan,
                "icir": np.nan,
                "valid_days": 0,
            })
        else:
            ic_arr = np.array(ic_values)
            mean_ic = ic_arr.mean()
            ic_std = ic_arr.std()
            icir = mean_ic / ic_std if ic_std > 1e-12 else np.nan
            results.append({
                "feature": col,
                "mean_ic": mean_ic,
                "ic_std": ic_std,
                "icir": icir,
                "abs_icir": abs(icir) if not np.isnan(icir) else 0.0,
                "valid_days": valid_days,
            })

    # 7. Print table sorted by |ICIR| descending
    results_df = pd.DataFrame(results).sort_values("abs_icir", ascending=False)
    results_df = results_df.drop(columns=["abs_icir"])

    print("\n" + "=" * 80)
    print("Northbound Feature IC Evaluation (vs Forward 5-Day Returns)")
    print(f"Universe: csi1000 | Period: 2020-01-01 to 2026-04-29 | Forward: 5d")
    print("=" * 80)
    print(results_df.to_string(index=False, float_format="%.4f"))
    print("=" * 80)

    # Summary
    significant = results_df[results_df["icir"].notna() & (results_df["valid_days"] > 0)]
    if len(significant) > 0:
        print(f"\nFeatures with |ICIR| > 0.3:")
        strong = significant[significant["icir"].abs() > 0.3]
        if len(strong) > 0:
            print(strong[["feature", "mean_ic", "icir", "valid_days"]].to_string(index=False, float_format="%.4f"))
        else:
            print("  None")

        print(f"\nFeatures with |ICIR| > 0.2:")
        moderate = significant[significant["icir"].abs() > 0.2]
        if len(moderate) > 0:
            print(moderate[["feature", "mean_ic", "icir", "valid_days"]].to_string(index=False, float_format="%.4f"))
        else:
            print("  None")


if __name__ == "__main__":
    main()
