# -*- coding: utf-8 -*-
"""從 appsetting.json 讀取應用程式設定（專案目錄下）。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from sheets_match import DEFAULT_SHEET_URL, DEFAULT_WORKSHEET_NAME

APPSETTING_FILENAME = "appsetting.json"
PROJECT_ROOT = Path(__file__).resolve().parent


def _read_appsetting_json() -> tuple[dict[str, Any], str | None]:
    cfg_path = PROJECT_ROOT / APPSETTING_FILENAME
    if not cfg_path.is_file():
        return {}, f"找不到 {APPSETTING_FILENAME}，已使用內建預設。請複製 appsetting.example.json 為 {APPSETTING_FILENAME} 後再編輯。"
    try:
        data = json.loads(cfg_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        return {}, f"{APPSETTING_FILENAME} JSON 格式錯誤：{e}"
    if not isinstance(data, dict):
        return {}, f"{APPSETTING_FILENAME} 內容格式錯誤：最外層必須是 JSON 物件。"
    return data, None


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


def _parse_single_profile(sec: dict[str, Any]) -> tuple[str, str, str]:
    url = sec.get("spreadsheetUrl") or sec.get("spreadsheet_url") or DEFAULT_SHEET_URL
    if not isinstance(url, str):
        url = DEFAULT_SHEET_URL
    url = url.strip()

    ws = sec.get("worksheetName") or sec.get("worksheet_name") or DEFAULT_WORKSHEET_NAME
    if not isinstance(ws, str):
        ws = DEFAULT_WORKSHEET_NAME
    ws = ws.strip()

    cred_key = sec.get("serviceAccountJsonPath") or sec.get("service_account_json_path")
    if not isinstance(cred_key, str) or not cred_key.strip():
        cred_key = "credentials.json"
    cred_abs = _resolve_path(cred_key)
    return url, ws, cred_abs


def load_google_sheet_profiles() -> tuple[dict[str, tuple[str, str, str]], str, str | None]:
    """
    回傳：(profiles, active_profile, error_message)
    - profiles: {profile_name: (spreadsheet_url, worksheet_name, service_account_abs_path)}
    - active_profile: 目前預設 profile 名稱
    """
    fallback = {
        "default": (
            DEFAULT_SHEET_URL,
            DEFAULT_WORKSHEET_NAME,
            _resolve_path("credentials.json"),
        )
    }
    data, err = _read_appsetting_json()
    if err is not None:
        return fallback, "default", err

    sec = _google_section(data)
    profiles_obj = sec.get("profiles")
    active_profile = str(sec.get("activeProfile") or sec.get("active_profile") or "default").strip() or "default"

    profiles: dict[str, tuple[str, str, str]] = {}
    if isinstance(profiles_obj, dict):
        for name, profile_sec in profiles_obj.items():
            if not isinstance(profile_sec, dict):
                continue
            profile_name = str(name or "").strip()
            if not profile_name:
                continue
            profiles[profile_name] = _parse_single_profile(profile_sec)
        if not profiles:
            profiles["default"] = _parse_single_profile(sec)
            active_profile = "default"
    else:
        profiles["default"] = _parse_single_profile(sec)
        active_profile = "default"

    if active_profile not in profiles:
        active_profile = next(iter(profiles.keys()))
    return profiles, active_profile, None


def load_report_password_default() -> str:
    """讀取報表解鎖密碼預設值（可在 UI 再手動覆蓋）。"""
    data, _ = _read_appsetting_json()
    sec = data.get("report")
    if not isinstance(sec, dict):
        sec = data.get("reportSettings")
    if not isinstance(sec, dict):
        return ""
    pwd = sec.get("defaultPassword") or sec.get("default_password")
    if not isinstance(pwd, str):
        return ""
    return pwd.strip()


def load_google_sheet_config(profile_name: str | None = None) -> tuple[str, str, str, str | None]:
    """
    回傳：(spreadsheet_url, worksheet_name, service_account_json_abs_path, error_message)
    相容舊版：若未提供 profile_name，使用設定檔 activeProfile 或 default。
    """
    profiles, active_profile, err = load_google_sheet_profiles()
    chosen = (profile_name or active_profile or "default").strip()
    if chosen not in profiles:
        chosen = active_profile
    url, ws, cred_abs = profiles[chosen]
    return url, ws, cred_abs, err


def save_active_google_sheet_profile(profile_name: str) -> str | None:
    """將目前選擇的 profile 回寫至 appsetting.json 的 googleSheets.activeProfile。"""
    chosen = str(profile_name or "").strip()
    if not chosen:
        return "無法儲存 activeProfile：profile 名稱不可為空。"
    cfg_path = PROJECT_ROOT / APPSETTING_FILENAME
    data, err = _read_appsetting_json()
    if err is not None:
        return err
    sec = data.get("googleSheets")
    if not isinstance(sec, dict):
        sec = {}
        data["googleSheets"] = sec
    sec["activeProfile"] = chosen
    try:
        cfg_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    except Exception as e:
        return f"寫入 {APPSETTING_FILENAME} 失敗：{e}"
    return None
