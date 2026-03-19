# -*- coding: utf-8 -*-
"""從 appsetting.json 讀取應用程式設定（專案目錄下）。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from sheets_match import DEFAULT_SHEET_URL, DEFAULT_WORKSHEET_NAME

APPSETTING_FILENAME = "appsetting.json"
PROJECT_ROOT = Path(__file__).resolve().parent


def _resolve_path(path_str: str) -> str:
    """相對路徑相對於專案根目錄；絕對路徑則原樣回傳。"""
    p = Path(path_str.strip())
    if p.is_absolute():
        return str(p.resolve())
    return str((PROJECT_ROOT / p).resolve())


def _google_section(data: dict[str, Any]) -> dict[str, Any]:
    gs = data.get("googleSheets")
    if isinstance(gs, dict):
        return gs
    return data


def load_google_sheet_config() -> tuple[str, str, str, str | None]:
    """
    回傳：(spreadsheet_url, worksheet_name, service_account_json_abs_path, error_message)
    error_message 僅在找不到 appsetting.json 或 JSON 無效時非 None。
    """
    cfg_path = PROJECT_ROOT / APPSETTING_FILENAME
    if not cfg_path.is_file():
        return (
            DEFAULT_SHEET_URL,
            DEFAULT_WORKSHEET_NAME,
            _resolve_path("credentials.json"),
            f"找不到 {APPSETTING_FILENAME}，已使用內建預設。請複製 appsetting.example.json 為 {APPSETTING_FILENAME} 後再編輯。",
        )

    try:
        data = json.loads(cfg_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        return (
            DEFAULT_SHEET_URL,
            DEFAULT_WORKSHEET_NAME,
            _resolve_path("credentials.json"),
            f"{APPSETTING_FILENAME} JSON 格式錯誤：{e}",
        )

    sec = _google_section(data)
    url = (
        sec.get("spreadsheetUrl")
        or sec.get("spreadsheet_url")
        or DEFAULT_SHEET_URL
    )
    if not isinstance(url, str):
        url = DEFAULT_SHEET_URL
    url = url.strip()

    ws = (
        sec.get("worksheetName")
        or sec.get("worksheet_name")
        or DEFAULT_WORKSHEET_NAME
    )
    if not isinstance(ws, str):
        ws = DEFAULT_WORKSHEET_NAME
    ws = ws.strip()

    cred_key = sec.get("serviceAccountJsonPath") or sec.get(
        "service_account_json_path"
    )
    if not isinstance(cred_key, str) or not cred_key.strip():
        cred_key = "credentials.json"
    cred_abs = _resolve_path(cred_key)

    return url, ws, cred_abs, None
