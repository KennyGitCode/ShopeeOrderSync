# -*- coding: utf-8 -*-
"""
階段二：Google Sheets 讀取與模糊比對（thefuzz token_set_ratio）
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from typing import Any

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from gspread import Cell
from thefuzz import fuzz

from text_normalize import normalize_for_match

# Google Sheets 讀寫所需範圍
GSPREAD_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

DEFAULT_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1cNKJT4Cf4ghk2yL2n1rINxdZgc9HLlR3GZhM1Kn1uJg/edit?gid=2082271300#gid=2082271300"
)
DEFAULT_WORKSHEET_NAME = "預定(大陸現貨)"

# 依使用者提供之正確標題名稱（請勿改寫）
REQUIRED_SHEET_COLUMNS = (
    "品名",
    "款式細項",
    "平台",
    "買家",
    "賣場售價",
    "賣場手續費",
)

# ---------------------------------------------------------------------------
# 試算表列結構（1-based 列號說明）
# - 第 1 列：財務加總面板（不讀）
# - 第 2 列：真正標題列 Headers → all_values[1]
# - 第 3 列：與標題合併之區塊（不讀、不當資料）
# - 第 4 列起：資料列 → all_values[3:]，enumerate(..., start=4) = 真實 Sheet 列號
# ---------------------------------------------------------------------------
HEADER_ROW_IDX_0 = 1  # 第 2 列
DATA_ROWS_SLICE_START_IDX_0 = 3  # 第 4 列（含）起
SHEET_FIRST_DATA_ROW_1BASED = 4


def extract_spreadsheet_id(url: str) -> str | None:
    """從 Google Sheets 網址取出 spreadsheetId。"""
    if not url or not str(url).strip():
        return None
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", str(url).strip())
    return m.group(1) if m else None


def open_gspread_client(service_account_path: str) -> gspread.Client:
    """使用服務帳戶 JSON 建立 gspread Client。"""
    creds = Credentials.from_service_account_file(
        service_account_path,
        scopes=GSPREAD_SCOPES,
    )
    return gspread.authorize(creds)


def write_order_values_to_sheet_row(
    service_account_path: str,
    spreadsheet_id: str,
    worksheet_name: str,
    sheet_row: int,
    header_index_1based: dict[str, int],
    buyer_account: str,
    sale_price: float | int,
    fee_value: float | int,
) -> None:
    """
    以動態欄位索引將單筆訂單寫入指定列。

    寫入欄位：
    - 平台 -> 蝦皮
    - 買家 -> 買家帳號
    - 賣場售價 -> 商品原價
    - 賣場手續費 -> 單件實扣手續費
    """
    required = ("平台", "買家", "賣場售價", "賣場手續費")
    miss = [k for k in required if k not in header_index_1based]
    if miss:
        raise ValueError(f"欄位索引缺少：{', '.join(miss)}")

    gc = open_gspread_client(service_account_path)
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(worksheet_name)

    cells = [
        Cell(sheet_row, int(header_index_1based["平台"]), "蝦皮"),
        Cell(sheet_row, int(header_index_1based["買家"]), str(buyer_account or "")),
        Cell(sheet_row, int(header_index_1based["賣場售價"]), str(sale_price)),
        Cell(sheet_row, int(header_index_1based["賣場手續費"]), str(fee_value)),
    ]
    ws.update_cells(cells, value_input_option="USER_ENTERED")


def get_row_values_by_columns(
    service_account_path: str,
    spreadsheet_id: str,
    worksheet_name: str,
    sheet_row: int,
    header_index_1based: dict[str, int],
    columns: list[str],
) -> dict[str, str]:
    """讀取指定列、指定欄位目前值。"""
    gc = open_gspread_client(service_account_path)
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(worksheet_name)
    out: dict[str, str] = {}
    for col in columns:
        idx = int(header_index_1based[col])
        out[col] = str(ws.cell(int(sheet_row), idx).value or "")
    return out


def update_row_values_by_columns(
    service_account_path: str,
    spreadsheet_id: str,
    worksheet_name: str,
    sheet_row: int,
    header_index_1based: dict[str, int],
    values_by_col: dict[str, str | int | float],
) -> None:
    """依動態欄位索引批次更新指定列。"""
    gc = open_gspread_client(service_account_path)
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(worksheet_name)
    cells: list[Cell] = []
    for col, val in values_by_col.items():
        if col not in header_index_1based:
            continue
        cells.append(Cell(int(sheet_row), int(header_index_1based[col]), str(val)))
    if cells:
        ws.update_cells(cells, value_input_option="USER_ENTERED")


def fetch_worksheet_catalog(
    service_account_path: str,
    spreadsheet_id: str,
    worksheet_name: str,
) -> pd.DataFrame:
    """
    讀取工作表，回傳含以下欄位的 DataFrame：
    - _sheet_row: Google Sheet **真實列號**（1-based；**資料從第 4 列起**，與階段三 update 列號一致）
    - 品名, 款式細項, 平台, 買家, 賣場售價, 賣場手續費（及表上其他欄位；欄名來自**第 2 列**）
    - _雲端合併比對字串: 品名 + 空白 + 款式細項（**原始繁體／表上原文**，供 UI 摘要）
    - _雲端正規化簡體比對用: 與階段一相同的清洗 + OpenCC 繁轉簡，供 token_set_ratio
    """
    gc = open_gspread_client(service_account_path)
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(worksheet_name)
    all_values = ws.get_all_values()
    if not all_values:
        return pd.DataFrame()

    if len(all_values) <= HEADER_ROW_IDX_0:
        raise ValueError(
            "工作表列數不足：至少需要第 2 列作為標題列（第 1 列為財務面板可略過）。"
        )

    header = [str(h).strip() for h in all_values[HEADER_ROW_IDX_0]]
    header_index = {name: i for i, name in enumerate(header)}
    missing = [c for c in REQUIRED_SHEET_COLUMNS if c not in header_index]
    if missing:
        raise ValueError(
            f"工作表**第 2 列**（標題列）缺少欄位：{', '.join(missing)}；"
            f"目前第 2 列欄位：{', '.join(header)}"
        )

    # 第 4 列起為資料（略過第 1 列財務、第 2 列標題、第 3 列合併標題區）
    data_rows = (
        all_values[DATA_ROWS_SLICE_START_IDX_0:]
        if len(all_values) > DATA_ROWS_SLICE_START_IDX_0
        else []
    )

    rows: list[dict[str, Any]] = []
    ncols = len(header)
    for sheet_row, row in enumerate(data_rows, start=SHEET_FIRST_DATA_ROW_1BASED):
        cells = list(row) + [""] * (ncols - len(row))
        cells = cells[:ncols]
        rec = dict(zip(header, cells))
        # 供階段三更新用：保留欄位名稱對應到 1-based 欄索引（A=1, B=2...）
        rec["_header_index_1based"] = {
            k: (v + 1) for k, v in header_index.items() if k in REQUIRED_SHEET_COLUMNS
        }
        rec["_sheet_row"] = sheet_row
        pn = str(rec.get("品名", "") or "").strip()
        xi = str(rec.get("款式細項", "") or "").strip()
        raw_merged = f"{pn} {xi}".strip()
        rec["_雲端合併比對字串"] = raw_merged
        rec["_雲端正規化簡體比對用"] = normalize_for_match(raw_merged)
        rows.append(rec)

    return pd.DataFrame(rows)


def platform_passes_filter(platform_cell: Any, stock_tag: str) -> bool:
    """依訂單現貨/預定/未知篩選雲端「平台」欄。"""
    p = "" if pd.isna(platform_cell) else str(platform_cell).strip()
    if stock_tag == "現貨":
        return p == "預現貨"
    if stock_tag == "預定":
        return p == ""
    return True


def filter_catalog_for_stock_tag(catalog: pd.DataFrame, stock_tag: str) -> pd.DataFrame:
    """相容舊名稱：回傳此訂單可參與比對的候選資料池。"""
    return candidate_pool_for_stock_tag(catalog, stock_tag)


def candidate_pool_for_stock_tag(catalog: pd.DataFrame, stock_tag: str) -> pd.DataFrame:
    """模糊比對前的唯一候選池（Single Source of Truth）。"""
    if catalog.empty:
        return catalog
    mask = catalog["平台"].apply(lambda x: platform_passes_filter(x, stock_tag))
    return catalog.loc[mask].copy()


def get_catalog_row_by_sheet_row(
    catalog: pd.DataFrame, sheet_row: int
) -> pd.Series | None:
    """依真實列號取得該列 Series；無則 None。"""
    if catalog.empty or sheet_row < 1:
        return None
    m = catalog[catalog["_sheet_row"] == int(sheet_row)]
    if m.empty:
        return None
    return m.iloc[0]


def format_platform_buyer_status(row: pd.Series | None) -> str:
    """供 UI 顯示：平台／買家狀態（空白以「空白」表示）。"""
    if row is None:
        return "查無此列（請確認行號是否在資料區）"
    plat = str(row.get("平台", "") or "").strip() or "空白"
    buyer = str(row.get("買家", "") or "").strip() or "空白"
    return f"平台[{plat}], 買家[{buyer}]"


def row_has_order_like_data(row: pd.Series | None) -> bool:
    """
    判斷該列是否已有「訂單向」資料，需勾選強制覆蓋才可寫入。
    以 買家／賣場售價／賣場手續費 任一有非空白內容為準（避免僅有品名範本即擋死）。
    """
    if row is None:
        return False
    for col in ("買家", "賣場售價", "賣場手續費"):
        if str(row.get(col, "") or "").strip():
            return True
    return False


def fuzzy_top3_matches(
    query_simplified: str,
    catalog_filtered: pd.DataFrame,
) -> list[tuple[int, int, str]]:
    """
    模糊比對並「展開」同名庫存列：

    1. 每列以 `token_set_ratio`（蝦皮簡體 vs `_雲端正規化簡體比對用`）計分。
    2. 以 `_雲端合併比對字串`（原文合併）去重，每個合併字串保留**最高分**。
    3. 取分數最高的前 **3 個不同合併字串**。
    4. 將雲端表中合併字串**完全等於**這 3 者之一的所有列號一併回傳。

    回傳：(sheet_row, 該合併字串代表分數, 雲端合併顯示字串)，排序 (-score, sheet_row)。
    """
    if catalog_filtered.empty or not (query_simplified or "").strip():
        return []

    q = (query_simplified or "").strip()
    # 合併字串 -> [(sheet_row, score), ...]
    bundles: dict[str, list[tuple[int, int]]] = defaultdict(list)

    for _, row in catalog_filtered.iterrows():
        display = str(row.get("_雲端合併比對字串", "") or "")
        target_norm = str(row.get("_雲端正規化簡體比對用", "") or "")
        sheet_row = int(row["_sheet_row"])
        score = int(fuzz.token_set_ratio(q, target_norm))
        bundles[display].append((sheet_row, score))

    best_by_display: dict[str, int] = {
        disp: max(sc for _, sc in pairs) for disp, pairs in bundles.items()
    }
    if not best_by_display:
        return []

    top_displays = sorted(
        best_by_display.keys(),
        key=lambda d: (-best_by_display[d], d),
    )[:3]

    out: list[tuple[int, int, str]] = []
    for disp in top_displays:
        bscore = best_by_display[disp]
        for sheet_row, _ in sorted(bundles[disp], key=lambda x: x[0]):
            out.append((sheet_row, bscore, disp))

    out.sort(key=lambda x: (-x[1], x[0]))
    return out


def sheet_rows_with_duplicate_selection(
    effective_row_by_uid: dict[str, int | None],
) -> set[int]:
    """回傳被兩筆以上訂單選中的列號（略過 None 不計）。"""
    cnt = Counter(r for r in effective_row_by_uid.values() if r is not None)
    return {r for r, n in cnt.items() if n > 1}
