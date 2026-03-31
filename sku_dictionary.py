# -*- coding: utf-8 -*-
"""⚙️商品字典：Raw_Name ↔ Standard_Keyword 半自動學習（gspread）。"""

from __future__ import annotations

from typing import Any

from sheets_match import open_gspread_client

DICT_SHEET_NAME = "⚙️商品字典"
DICT_HEADERS = ["Raw_Name", "Standard_Keyword"]


def ensure_dictionary_worksheet(service_account_path: str, spreadsheet_id: str):
    gc = open_gspread_client(service_account_path)
    sh = gc.open_by_key(spreadsheet_id)
    try:
        ws = sh.worksheet(DICT_SHEET_NAME)
    except Exception:
        ws = sh.add_worksheet(title=DICT_SHEET_NAME, rows=5000, cols=4)
    first = ws.row_values(1)
    if [str(x).strip() for x in first[: len(DICT_HEADERS)]] != DICT_HEADERS:
        ws.update("A1:B1", [DICT_HEADERS], value_input_option="USER_ENTERED")
    _try_hide_worksheet(sh, ws)
    return ws


def _try_hide_worksheet(sh, ws) -> None:
    try:
        sh.batch_update(
            {
                "requests": [
                    {
                        "updateSheetProperties": {
                            "properties": {
                                "sheetId": ws.id,
                                "hidden": True,
                            },
                            "fields": "hidden",
                        }
                    }
                ]
            }
        )
    except Exception:
        pass


def read_dictionary_map(service_account_path: str, spreadsheet_id: str) -> dict[str, str]:
    """Raw_Name (strip) -> Standard_Keyword；若同鍵多列，保留最後一列。"""
    ws = ensure_dictionary_worksheet(service_account_path, spreadsheet_id)
    values = ws.get_all_values()
    if len(values) <= 1:
        return {}
    out: dict[str, str] = {}
    for row in values[1:]:
        cells = list(row) + ["", ""]
        raw = str(cells[0]).strip()
        std = str(cells[1]).strip()
        if raw:
            out[raw] = std
    return out


def learn_dictionary_entry(
    service_account_path: str,
    spreadsheet_id: str,
    *,
    raw_name: str,
    standard_keyword: str,
) -> None:
    raw = (raw_name or "").strip()
    std = (standard_keyword or "").strip()
    if not raw or not std:
        return
    ws = ensure_dictionary_worksheet(service_account_path, spreadsheet_id)
    values = ws.get_all_values()
    hit_row: int | None = None
    for i, row in enumerate(values[1:], start=2):
        cells = list(row) + ["", ""]
        if str(cells[0]).strip() == raw:
            hit_row = i
            break
    if hit_row is not None:
        ws.update(f"B{hit_row}", [[std]], value_input_option="USER_ENTERED")
    else:
        ws.append_row([raw, std], value_input_option="USER_ENTERED")


def batch_learn_dictionary_entries(
    service_account_path: str,
    spreadsheet_id: str,
    entries: list[tuple[str, str]],
) -> int:
    """一次學習多筆 Raw_Name -> Standard_Keyword；回傳實際處理筆數。"""
    norm: dict[str, str] = {}
    for raw, std in entries:
        rk = str(raw or "").strip()
        sv = str(std or "").strip()
        if rk and sv:
            norm[rk] = sv
    if not norm:
        return 0

    ws = ensure_dictionary_worksheet(service_account_path, spreadsheet_id)
    values = ws.get_all_values()
    hit_rows: dict[str, int] = {}
    for i, row in enumerate(values[1:], start=2):
        cells = list(row) + ["", ""]
        rk = str(cells[0]).strip()
        if rk and rk not in hit_rows:
            hit_rows[rk] = i

    updates: list[list[str]] = []
    appends: list[list[str]] = []
    for raw, std in norm.items():
        rn = hit_rows.get(raw)
        if rn is not None:
            updates.append([f"B{rn}", std])
        else:
            appends.append([raw, std])

    if updates:
        ws.batch_update(
            [{"range": rng, "values": [[val]]} for rng, val in updates],
            value_input_option="USER_ENTERED",
        )
    if appends:
        ws.append_rows(appends, value_input_option="USER_ENTERED")
    return len(norm)


def batch_forget_dictionary_entries(
    service_account_path: str,
    spreadsheet_id: str,
    raw_names: list[str],
) -> int:
    """
    一次移除多筆 Raw_Name（記憶體過濾後整批覆寫，並以 batchUpdate 刪除尾部多餘列）。
    回傳實際從字典表移除的資料列數。
    """
    forget = {str(r).strip() for r in raw_names if r and str(r).strip()}
    if not forget:
        return 0
    ws = ensure_dictionary_worksheet(service_account_path, spreadsheet_id)
    sh = ws.spreadsheet
    values = ws.get_all_values()
    if len(values) <= 1:
        return 0
    removed = 0
    body: list[list[str]] = []
    for row in values[1:]:
        cells = list(row) + ["", ""]
        raw = str(cells[0]).strip()
        if raw in forget:
            removed += 1
            continue
        body.append([str(cells[0]), str(cells[1])])
    full: list[list[str]] = [DICT_HEADERS] + body
    old_count = len(values)
    new_count = len(full)
    ws.update(
        f"A1:B{new_count}",
        full,
        value_input_option="USER_ENTERED",
    )
    if old_count > new_count:
        try:
            sh.batch_update(
                {
                    "requests": [
                        {
                            "deleteDimension": {
                                "range": {
                                    "sheetId": ws.id,
                                    "dimension": "ROWS",
                                    "startIndex": new_count,
                                    "endIndex": old_count,
                                }
                            }
                        }
                    ]
                }
            )
        except Exception:
            for _ in range(old_count - new_count):
                ws.delete_rows(new_count + 1)
    return removed


def forget_dictionary_by_raw_name(
    service_account_path: str,
    spreadsheet_id: str,
    raw_name: str,
) -> int:
    """刪除所有與 Raw_Name 相符的資料列（自底向上刪，避免列號位移）。回傳刪除列數。"""
    raw = (raw_name or "").strip()
    if not raw:
        return 0
    ws = ensure_dictionary_worksheet(service_account_path, spreadsheet_id)
    values = ws.get_all_values()
    if len(values) <= 1:
        return 0
    to_delete: list[int] = []
    for i, row in enumerate(values[1:], start=2):
        cells = list(row) + ["", ""]
        if str(cells[0]).strip() == raw:
            to_delete.append(i)
    for rn in sorted(set(to_delete), reverse=True):
        if rn >= 2:
            ws.delete_rows(rn)
    return len(to_delete)


def merged_standard_keyword_from_catalog_row(row: Any) -> str:
    """自 catalog Series / dict 取品名+款式細項，作為 Standard_Keyword。"""
    if row is None:
        return ""
    try:
        pn = str(row.get("品名", "") or "").strip()
        xi = str(row.get("款式細項", "") or "").strip()
    except Exception:
        return ""
    return f"{pn} {xi}".strip()
