# -*- coding: utf-8 -*-
"""雲端歷史紀錄表（⚙️系統歷史紀錄）：扁平化 Action 日誌與回溯。"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from gspread import Cell

from sheets_match import HEADER_ROW_IDX_0, open_gspread_client

HISTORY_SHEET_NAME = "⚙️系統歷史紀錄"
SETTINGS_SHEET_NAME = "⚙️系統設定"
HISTORY_HEADERS = [
    "Batch_ID",
    "Upload_Time",
    "Filename",
    "Order_UID",
    "Action_Type",
    "Target_Row",
    "Orig_Platform",
    "Orig_Buyer",
    "Orig_Price",
    "Orig_Fee",
    "Last_Hint",
    "Raw_Name",
    "Order_Created_At",
    "Stock_Tag",
    "Expected_Platform",
]
SETTINGS_HEADERS = ["Key", "Value"]


def _col_to_letter(n: int) -> str:
    """1-based 欄號轉 Excel 欄名。"""
    out = ""
    x = int(n)
    while x > 0:
        x, r = divmod(x - 1, 26)
        out = chr(ord("A") + r) + out
    return out


def _parse_time(s: str) -> datetime:
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.min


def _target_header_map(ws) -> dict[str, int]:
    values = ws.get_all_values()
    if len(values) <= HEADER_ROW_IDX_0:
        raise ValueError("目標工作表缺少第 2 列標題。")
    header = [str(h).strip() for h in values[HEADER_ROW_IDX_0]]
    return {k: i + 1 for i, k in enumerate(header)}


def ensure_history_worksheet(service_account_path: str, spreadsheet_id: str):
    gc = open_gspread_client(service_account_path)
    sh = gc.open_by_key(spreadsheet_id)
    try:
        ws = sh.worksheet(HISTORY_SHEET_NAME)
    except Exception:
        ws = sh.add_worksheet(title=HISTORY_SHEET_NAME, rows=2000, cols=20)
    first = ws.row_values(1)
    if [str(x).strip() for x in first[: len(HISTORY_HEADERS)]] != HISTORY_HEADERS:
        end_col = _col_to_letter(len(HISTORY_HEADERS))
        ws.update(f"A1:{end_col}1", [HISTORY_HEADERS], value_input_option="USER_ENTERED")
    return ws


def ensure_settings_worksheet(service_account_path: str, spreadsheet_id: str):
    gc = open_gspread_client(service_account_path)
    sh = gc.open_by_key(spreadsheet_id)
    try:
        ws = sh.worksheet(SETTINGS_SHEET_NAME)
    except Exception:
        ws = sh.add_worksheet(title=SETTINGS_SHEET_NAME, rows=200, cols=6)
    first = ws.row_values(1)
    if [str(x).strip() for x in first[: len(SETTINGS_HEADERS)]] != SETTINGS_HEADERS:
        ws.update("A1:B1", [SETTINGS_HEADERS], value_input_option="USER_ENTERED")
    return ws


def read_setting_value(
    service_account_path: str,
    spreadsheet_id: str,
    key: str,
) -> str:
    ws = ensure_settings_worksheet(service_account_path, spreadsheet_id)
    vals = ws.get_all_values()
    if len(vals) <= 1:
        return ""
    k = str(key or "").strip()
    if not k:
        return ""
    for row in vals[1:]:
        cells = list(row) + ["", ""]
        if str(cells[0]).strip() == k:
            return str(cells[1]).strip()
    return ""


def write_setting_value(
    service_account_path: str,
    spreadsheet_id: str,
    key: str,
    value: str,
) -> None:
    ws = ensure_settings_worksheet(service_account_path, spreadsheet_id)
    vals = ws.get_all_values()
    k = str(key or "").strip()
    v = str(value or "").strip()
    if not k:
        return
    hit_row: int | None = None
    for i, row in enumerate(vals[1:], start=2):
        cells = list(row) + ["", ""]
        if str(cells[0]).strip() == k:
            hit_row = i
            break
    if hit_row is None:
        ws.append_row([k, v], value_input_option="USER_ENTERED")
    else:
        ws.update(f"B{hit_row}", [[v]], value_input_option="USER_ENTERED")


def append_history_action(
    service_account_path: str,
    spreadsheet_id: str,
    *,
    batch_id: str,
    upload_time: str,
    filename: str,
    order_uid: str,
    action_type: str,  # write | skip
    target_row: int | None,
    original_data: dict[str, str] | None,
    last_hint: str,
    raw_name: str = "",
    order_created_at: str = "",
    stock_tag: str = "",
    expected_platform: str = "",
) -> None:
    ws = ensure_history_worksheet(service_account_path, spreadsheet_id)
    orig = original_data or {}
    ws.append_row(
        [
            batch_id,
            upload_time,
            filename,
            order_uid,
            action_type,
            "" if target_row is None else str(int(target_row)),
            str(orig.get("平台", "")),
            str(orig.get("買家", "")),
            str(orig.get("賣場售價", "")),
            str(orig.get("賣場手續費", "")),
            last_hint,
            str(raw_name or ""),
            str(order_created_at or ""),
            str(stock_tag or ""),
            str(expected_platform or ""),
        ],
        value_input_option="USER_ENTERED",
    )


def append_history_actions_batch(
    service_account_path: str,
    spreadsheet_id: str,
    actions: list[dict[str, Any]],
) -> int:
    """一次 append 多筆歷史動作，回傳實際寫入筆數。"""
    if not actions:
        return 0
    ws = ensure_history_worksheet(service_account_path, spreadsheet_id)
    rows: list[list[str]] = []
    for a in actions:
        orig = dict(a.get("original_data") or {})
        tr = a.get("target_row")
        rows.append(
            [
                str(a.get("batch_id", "") or ""),
                str(a.get("upload_time", "") or ""),
                str(a.get("filename", "") or ""),
                str(a.get("order_uid", "") or ""),
                str(a.get("action_type", "") or ""),
                "" if tr is None else str(int(tr)),
                str(orig.get("平台", "")),
                str(orig.get("買家", "")),
                str(orig.get("賣場售價", "")),
                str(orig.get("賣場手續費", "")),
                str(a.get("last_hint", "") or ""),
                str(a.get("raw_name", "") or ""),
                str(a.get("order_created_at", "") or ""),
                str(a.get("stock_tag", "") or ""),
                str(a.get("expected_platform", "") or ""),
            ]
        )
    ws.append_rows(rows, value_input_option="USER_ENTERED")
    return len(rows)


def _parse_order_created_at(text: str) -> datetime | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    s = raw.replace("T", " ").replace("/", "-")
    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y-%m-%d %p %I:%M:%S",
        "%Y-%m-%d %p %I:%M",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def latest_written_order_created_date(actions: list[dict[str, Any]]) -> str | None:
    """回傳 write 動作中最新的訂單成立日期（YYYY-MM-DD），無資料則 None。"""
    best: datetime | None = None
    for a in actions:
        if str(a.get("Action_Type", "")).strip().lower() != "write":
            continue
        dt = _parse_order_created_at(str(a.get("Order_Created_At", "") or ""))
        if dt is None:
            continue
        if best is None or dt > best:
            best = dt
    if best is None:
        return None
    return best.strftime("%Y-%m-%d")


def read_history_actions(service_account_path: str, spreadsheet_id: str) -> list[dict[str, Any]]:
    ws = ensure_history_worksheet(service_account_path, spreadsheet_id)
    values = ws.get_all_values()
    if len(values) <= 1:
        return []
    out: list[dict[str, Any]] = []
    for row_no, row in enumerate(values[1:], start=2):
        cells = list(row) + [""] * (len(HISTORY_HEADERS) - len(row))
        rec = dict(zip(HISTORY_HEADERS, cells))
        rec["_row_number"] = row_no
        out.append(rec)
    return out


def completed_uids_from_actions(actions: list[dict[str, Any]]) -> set[str]:
    return {
        str(a.get("Order_UID", "")).strip()
        for a in actions
        if str(a.get("Order_UID", "")).strip()
    }


def latest_uid_action_map(actions: list[dict[str, Any]]) -> dict[str, str]:
    out: dict[str, tuple[int, str]] = {}
    for a in actions:
        uid = str(a.get("Order_UID", "")).strip()
        if not uid:
            continue
        rn = int(a.get("_row_number", 0))
        t = str(a.get("Action_Type", "")).strip().lower()
        if uid not in out or rn > out[uid][0]:
            out[uid] = (rn, t)
    return {uid: t for uid, (_, t) in out.items()}


def processed_uids_from_actions(actions: list[dict[str, Any]]) -> set[str]:
    """僅回傳目前有效狀態為 write 的 UID（rollback 後會自動消失）。"""
    latest = latest_uid_action_map(actions)
    return {uid for uid, act in latest.items() if act == "write"}


def group_batches(actions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by: dict[str, dict[str, Any]] = {}
    for a in actions:
        bid = str(a.get("Batch_ID", "")).strip()
        if not bid:
            continue
        g = by.setdefault(
            bid,
            {
                "batch_id": bid,
                "upload_time": str(a.get("Upload_Time", "")).strip(),
                "filename": str(a.get("Filename", "")).strip(),
                "count": 0,
                "rows": [],
            },
        )
        g["count"] += 1
        g["rows"].append(int(a.get("_row_number", 0)))
    vals = list(by.values())
    vals.sort(key=lambda x: _parse_time(x.get("upload_time", "")), reverse=True)
    return vals


def _delete_rows(ws, row_numbers: list[int]) -> None:
    for rn in sorted(set(row_numbers), reverse=True):
        if rn >= 2:
            ws.delete_rows(rn)


def rollback_order_uid(
    service_account_path: str,
    spreadsheet_id: str,
    worksheet_name: str,
    order_uid: str,
) -> tuple[int, bool]:
    """
    回傳 (deleted_count, restored)
    - deleted_count: 刪除歷史列數（該 uid 所有紀錄）
    - restored: 是否有執行資料還原（write 類）
    """
    gc = open_gspread_client(service_account_path)
    sh = gc.open_by_key(spreadsheet_id)
    ws_target = sh.worksheet(worksheet_name)
    ws_hist = ensure_history_worksheet(service_account_path, spreadsheet_id)
    actions = read_history_actions(service_account_path, spreadsheet_id)
    hits = [a for a in actions if str(a.get("Order_UID", "")).strip() == order_uid]
    if not hits:
        return 0, False

    latest = max(hits, key=lambda a: int(a.get("_row_number", 0)))
    restored = False
    if str(latest.get("Action_Type", "")).strip().lower() == "write":
        row = int(str(latest.get("Target_Row", "0") or "0") or 0)
        if row > 0:
            hm = _target_header_map(ws_target)
            back = {
                "平台": str(latest.get("Orig_Platform", "")),
                "買家": str(latest.get("Orig_Buyer", "")),
                "賣場售價": str(latest.get("Orig_Price", "")),
                "賣場手續費": str(latest.get("Orig_Fee", "")),
            }
            cells = [
                Cell(row, hm["平台"], back["平台"]),
                Cell(row, hm["買家"], back["買家"]),
                Cell(row, hm["賣場售價"], back["賣場售價"]),
                Cell(row, hm["賣場手續費"], back["賣場手續費"]),
            ]
            ws_target.update_cells(cells, value_input_option="USER_ENTERED")
            restored = True

    del_rows = [int(a["_row_number"]) for a in hits]
    _delete_rows(ws_hist, del_rows)
    return len(del_rows), restored


def rollback_batch(
    service_account_path: str,
    spreadsheet_id: str,
    worksheet_name: str,
    batch_id: str,
) -> tuple[int, int, list[str]]:
    """回傳 (restored_count, deleted_history_rows, raw_names_for_dictionary_unlearn)。"""
    gc = open_gspread_client(service_account_path)
    sh = gc.open_by_key(spreadsheet_id)
    ws_target = sh.worksheet(worksheet_name)
    ws_hist = ensure_history_worksheet(service_account_path, spreadsheet_id)
    actions = read_history_actions(service_account_path, spreadsheet_id)
    hits = [a for a in actions if str(a.get("Batch_ID", "")).strip() == batch_id]
    if not hits:
        return 0, 0, []

    hm = _target_header_map(ws_target)
    restored = 0
    raw_for_unlearn: list[str] = []
    seen_raw: set[str] = set()
    for a in hits:
        if str(a.get("Action_Type", "")).strip().lower() != "write":
            continue
        row = int(str(a.get("Target_Row", "0") or "0") or 0)
        if row <= 0:
            continue
        rn = str(a.get("Raw_Name", "") or "").strip()
        if rn and rn not in seen_raw:
            seen_raw.add(rn)
            raw_for_unlearn.append(rn)
        back = {
            "平台": str(a.get("Orig_Platform", "")),
            "買家": str(a.get("Orig_Buyer", "")),
            "賣場售價": str(a.get("Orig_Price", "")),
            "賣場手續費": str(a.get("Orig_Fee", "")),
        }
        cells = [
            Cell(row, hm["平台"], back["平台"]),
            Cell(row, hm["買家"], back["買家"]),
            Cell(row, hm["賣場售價"], back["賣場售價"]),
            Cell(row, hm["賣場手續費"], back["賣場手續費"]),
        ]
        ws_target.update_cells(cells, value_input_option="USER_ENTERED")
        restored += 1

    del_rows = [int(a["_row_number"]) for a in hits]
    _delete_rows(ws_hist, del_rows)
    return restored, len(del_rows), raw_for_unlearn


def gc_keep_latest_batches(service_account_path: str, spreadsheet_id: str, keep: int = 5) -> int:
    ws_hist = ensure_history_worksheet(service_account_path, spreadsheet_id)
    actions = read_history_actions(service_account_path, spreadsheet_id)
    batches = group_batches(actions)
    if len(batches) <= keep:
        return 0
    old = batches[keep:]
    rows = [rn for b in old for rn in b["rows"]]
    _delete_rows(ws_hist, rows)
    return len(rows)

