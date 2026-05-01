#!/usr/bin/env python3
"""Export cached stock financial data into one CSV with sector metadata.

Default inputs:
  - cache/financial/*.csv
  - cache/sector_map.json
  - crawler/data/sector_stocks.json

Output is a denormalized row-level table suitable for pivot analysis.
"""
from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT.parent))

from quant_ex.data.utils import code_to_qlib_instrument, load_stock_names, normalize_qlib_instrument

logger = logging.getLogger("export_financial_sector_csv")

_COLUMN_LABELS = {
    "instrument": "股票代码",
    "stock_name": "股票名称",
    "industry_sector": "行业板块",
    "concept_sector": "概念板块",
    "sw1_industry": "申万一级行业",
    "datetime": "报告日期",
    "source_file": "来源文件",
    "revenue_growth": "营收增速",
    "eps": "每股收益",
    "roe_weighted": "加权净资产收益率",
    "roa_alt": "总资产收益率_备用",
    "net_margin": "销售净利率",
    "profit_growth": "净利润增速",
    "gross_margin": "毛利率",
    "roa": "总资产收益率",
    "ocf_to_np": "经营现金流净额比净利润",
    "roe": "净资产收益率",
    "ar_turnover": "应收账款周转率",
    "ar_turn_days": "应收账款周转天数",
    "inventory_turnover": "存货周转率",
    "inventory_turn_days": "存货周转天数",
    "fixed_asset_turnover": "固定资产周转率",
    "asset_turnover": "总资产周转率",
    "asset_turn_days": "总资产周转天数",
    "current_asset_turnover": "流动资产周转率",
    "current_asset_turn_days": "流动资产周转天数",
    "equity_turnover": "股东权益周转率",
}

_CNINFO_LOOKUP_CODE = "\n".join(
    [
        "import sys",
        "import akshare as ak",
        "df = ak.stock_profile_cninfo(symbol=sys.argv[1])",
        "industry = ''",
        "if '所属行业' in df.columns and not df.empty:",
        "    value = df['所属行业'].iloc[0]",
        "    industry = '' if value is None else str(value).strip()",
        "print(industry)",
    ]
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--financial-dir",
        default=str(PROJECT_ROOT / "cache" / "financial"),
        help="Directory containing per-stock financial CSV files.",
    )
    parser.add_argument(
        "--sector-map",
        default=str(PROJECT_ROOT / "cache" / "sector_map.json"),
        help="JSON file mapping instrument to industry sector.",
    )
    parser.add_argument(
        "--sector-stocks",
        default=str(PROJECT_ROOT / "crawler" / "data" / "sector_stocks.json"),
        help="Offline crawler JSON used for concept and stock-name fallback.",
    )
    parser.add_argument(
        "--sw1-map",
        default=str(PROJECT_ROOT / "cache" / "sw1_industry_map.csv"),
        help="CSV file mapping instrument to SW Level-1 industry.",
    )
    parser.add_argument(
        "--output",
        default=str(PROJECT_ROOT / "cache" / "financial_sector_snapshot.csv"),
        help="Output CSV path.",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8-sig",
        help="CSV encoding. Default uses utf-8-sig for Excel compatibility.",
    )
    return parser


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _load_industry_map(sector_map_path: Path, sector_stocks_path: Path) -> Dict[str, str]:
    if sector_map_path.exists():
        data = _load_json(sector_map_path)
        if isinstance(data, dict) and data:
            return {
                normalize_qlib_instrument(str(k)): str(v)
                for k, v in data.items()
            }

    sector_stocks = _load_json(sector_stocks_path)
    industry = sector_stocks.get("industry", {})
    result: Dict[str, str] = {}
    for info in industry.values():
        sector_name = info.get("name", "")
        for stock in info.get("stocks", []):
            code = stock.get("code", "")
            if code:
                result[code_to_qlib_instrument(code)] = sector_name
    return result


def _lookup_industry_via_cninfo(symbol: str, timeout_sec: int = 20) -> str | None:
    try:
        result = subprocess.run(
            [sys.executable, "-c", _CNINFO_LOOKUP_CODE, symbol],
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            cwd=str(PROJECT_ROOT),
        )
    except subprocess.TimeoutExpired:
        logger.warning("CNInfo industry lookup timed out for %s", symbol)
        return None

    if result.returncode != 0:
        stderr = result.stderr.strip()
        logger.warning("CNInfo industry lookup failed for %s: %s", symbol, stderr or result.returncode)
        return None

    industry = result.stdout.strip()
    if not industry or industry.lower() == "nan":
        return None
    return industry


def _backfill_industry_map(
    instruments: List[str],
    industry_map: Dict[str, str],
    sector_map_path: Path,
) -> Dict[str, str]:
    missing = sorted(
        {
            normalize_qlib_instrument(str(instrument))
            for instrument in instruments
            if instrument and normalize_qlib_instrument(str(instrument)) not in industry_map
        }
    )
    if not missing:
        return industry_map

    logger.info("CNInfo backfill for %d instruments missing industry", len(missing))
    updates = 0
    for instrument in missing:
        industry = _lookup_industry_via_cninfo(instrument[2:])
        if industry:
            industry_map[instrument] = industry
            updates += 1
        time.sleep(0.2)

    if updates:
        sector_map_path.parent.mkdir(parents=True, exist_ok=True)
        sector_map_path.write_text(
            json.dumps(dict(sorted(industry_map.items())), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info("CNInfo backfill recovered %d industries and updated %s", updates, sector_map_path)
    else:
        logger.info("CNInfo backfill did not recover any industries")
    return industry_map


def _load_concept_map(sector_stocks_path: Path) -> Dict[str, str]:
    sector_stocks = _load_json(sector_stocks_path)
    concept = sector_stocks.get("concept", {})
    result: Dict[str, str] = {}
    for info in concept.values():
        concept_name = info.get("name", "")
        for stock in info.get("stocks", []):
            code = stock.get("code", "")
            instrument = code_to_qlib_instrument(code)
            if instrument and instrument not in result:
                result[instrument] = concept_name
    return result


def _read_financial_files(financial_dir: Path) -> pd.DataFrame:
    files = sorted(financial_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No financial CSV files found in {financial_dir}")

    frames: List[pd.DataFrame] = []
    skipped: List[str] = []
    for file_path in files:
        try:
            df = pd.read_csv(file_path)
        except Exception as exc:
            logger.warning("Skip unreadable file %s: %s", file_path.name, exc)
            skipped.append(file_path.name)
            continue

        if df.empty:
            continue

        if "instrument" not in df.columns:
            df.insert(0, "instrument", file_path.stem)
        else:
            df["instrument"] = df["instrument"].fillna(file_path.stem)

        if "datetime" in df.columns:
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")

        df["source_file"] = file_path.name
        frames.append(df)

    if not frames:
        raise RuntimeError("All financial CSV files were empty or unreadable.")

    merged = pd.concat(frames, ignore_index=True, sort=False)
    if skipped:
        logger.warning("Skipped %d files during merge", len(skipped))
    return merged


def _annotate_columns(columns: List[str]) -> List[str]:
    annotated = []
    for column in columns:
        label = _COLUMN_LABELS.get(column)
        if label:
            annotated.append(f"{column}[{label}]")
        else:
            annotated.append(column)
    return annotated


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _build_parser().parse_args()

    financial_dir = Path(args.financial_dir).expanduser().resolve()
    sector_map_path = Path(args.sector_map).expanduser().resolve()
    sector_stocks_path = Path(args.sector_stocks).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()

    financial_df = _read_financial_files(financial_dir)
    unique_instruments = financial_df["instrument"].nunique()
    stock_names = load_stock_names()
    industry_map = _load_industry_map(sector_map_path, sector_stocks_path)
    industry_map = _backfill_industry_map(
        financial_df["instrument"].dropna().astype(str).tolist(),
        industry_map,
        sector_map_path,
    )
    concept_map = _load_concept_map(sector_stocks_path)

    # SW Level-1 industry map
    sw1_map_path = Path(args.sw1_map).expanduser().resolve()
    sw1_map: Dict[str, str] = {}
    if sw1_map_path.exists():
        sw1_df = pd.read_csv(sw1_map_path, dtype=str)
        sw1_map = dict(zip(sw1_df["instrument"], sw1_df["sw1_name"]))
        logger.info("SW1 map loaded: %d stocks", len(sw1_map))
    else:
        logger.warning("SW1 map not found at %s; run: python run_fetch_data.py --type sw1_industry", sw1_map_path)

    financial_df["stock_name"] = financial_df["instrument"].map(stock_names)
    financial_df["industry_sector"] = financial_df["instrument"].map(industry_map)
    financial_df["concept_sector"] = financial_df["instrument"].map(concept_map)
    financial_df["sw1_industry"] = financial_df["instrument"].map(sw1_map)

    base_cols = [
        "instrument",
        "stock_name",
        "industry_sector",
        "concept_sector",
        "sw1_industry",
        "datetime",
        "source_file",
    ]
    ordered_cols = base_cols + [c for c in financial_df.columns if c not in base_cols]
    financial_df = financial_df[ordered_cols]
    financial_df.columns = _annotate_columns(financial_df.columns.tolist())

    financial_df = financial_df.sort_values([
        "instrument[股票代码]",
        "datetime[报告日期]",
    ], kind="stable")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    financial_df.to_csv(output_path, index=False, encoding=args.encoding)

    logger.info("Exported %d rows, %d columns", len(financial_df), len(financial_df.columns))
    logger.info("Unique instruments: %d", unique_instruments)
    logger.info("Output written to %s", output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())