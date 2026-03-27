# -*- coding: utf-8 -*-
"""
蝦皮訂單 CSV 預處理與清洗（Streamlit）
階段一：預處理、整數手續費分攤、驗證
階段二：Google Sheets 讀取與模糊比對（thefuzz）
"""

from __future__ import annotations

import codecs
import hashlib
import io
import math
import os
from datetime import datetime

import pandas as pd
import streamlit as st
import zhconv

from app_settings import load_google_sheet_config
from cloud_history import (
    append_history_action,
    completed_uids_from_actions,
    gc_keep_latest_batches,
    group_batches,
    latest_uid_action_map,
    read_history_actions,
    rollback_batch,
    rollback_order_uid,
)
from sheets_match import (
    SHEET_FIRST_DATA_ROW_1BASED,
    candidate_pool_for_stock_tag,
    extract_spreadsheet_id,
    fetch_worksheet_catalog,
    fuzzy_top3_matches,
    get_catalog_row_by_sheet_row,
    row_has_order_like_data,
    get_row_values_by_columns,
    write_order_values_to_sheet_row,
)

# ---------------------------------------------------------------------------
# 欄位與常數
# ---------------------------------------------------------------------------
REQUIRED_COLUMNS = [
    "訂單成立日期",
    "訂單編號",
    "買家帳號",
    "商品原價",
    "成交手續費",
    "其他服務費",
    "金流與系統處理費",
    "商品名稱",
    "商品選項名稱",
    "數量",
]

# 自動偵測時嘗試順序（strict）：勿在 Big5 之前用 UTF-8 replace，
# 否則 Big5 檔會被「錯誤的 UTF-8」解出而不拋錯，欄位變亂碼／。
_CSV_ENCODING_TRY_ORDER: list[str] = [
    "utf-8-sig",
    "utf-8",
    "big5",
    "cp950",
    "gb18030",
    "gbk",
    "big5hkscs",
    "utf-16",
    "utf-16-le",
    "utf-16-be",
]

# 側邊欄手動選項（label, encoding 或 None=自動）
MANUAL_ENCODING_OPTIONS: list[tuple[str, str | None]] = [
    ("自動（比對必要欄位選最佳）", None),
    ("UTF-8（含 BOM）", "utf-8-sig"),
    ("UTF-8", "utf-8"),
    ("Big5（台灣 Excel／蝦皮常見）", "big5"),
    ("CP950", "cp950"),
    ("GB18030", "gb18030"),
    ("GBK", "gbk"),
]


def _csv_encoding_candidates() -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for name in _CSV_ENCODING_TRY_ORDER:
        if name in seen:
            continue
        try:
            codecs.lookup(name)
        except LookupError:
            continue
        seen.add(name)
        out.append(name)
    return out


def _read_csv_try(raw_bytes: bytes, encoding: str, errors: str) -> pd.DataFrame | None:
    try:
        return pd.read_csv(
            io.BytesIO(raw_bytes),
            encoding=encoding,
            encoding_errors=errors,
        )
    except (
        UnicodeError,
        LookupError,
        pd.errors.EmptyDataError,
        pd.errors.ParserError,
    ):
        return None


def _required_columns_match_score(df: pd.DataFrame) -> int:
    return sum(1 for c in REQUIRED_COLUMNS if c in df.columns)


def read_csv_bytes(
    raw_bytes: bytes,
    encoding_override: str | None = None,
) -> pd.DataFrame:
    """
    讀取 CSV 位元組。

    - **自動**：對多種編碼只用 `strict` 嘗試，並以「命中幾個必要欄位名」選最佳結果；
      若仍無法找齊欄位，第二輪才對各編碼試 `replace`（給少數壞位元組的 UTF-8 檔用）。
    - **手動**：依使用者指定編碼，先 strict 再 replace。

    先前錯誤：在 UTF-8 strict 失敗後立刻用 UTF-8 replace，會讓 Big5 檔「成功」讀入但表頭全毀。
    """
    if encoding_override:
        df = _read_csv_try(raw_bytes, encoding_override, "strict")
        if df is None:
            df = _read_csv_try(raw_bytes, encoding_override, "replace")
        if df is None:
            raise UnicodeDecodeError(
                encoding_override, b"", 0, 1, "無法以此編碼解析 CSV"
            )
        return df

    candidates = _csv_encoding_candidates()
    best_df: pd.DataFrame | None = None
    best_score = -1
    best_pri = 10**9

    def consider(df: pd.DataFrame | None, pri: int) -> None:
        nonlocal best_df, best_score, best_pri
        if df is None:
            return
        sc = _required_columns_match_score(df)
        if sc > best_score or (sc == best_score and pri < best_pri):
            best_score = sc
            best_pri = pri
            best_df = df

    for pri, enc in enumerate(candidates):
        consider(_read_csv_try(raw_bytes, enc, "strict"), pri)

    if best_score == len(REQUIRED_COLUMNS) and best_df is not None:
        return best_df

    for pri, enc in enumerate(candidates):
        consider(_read_csv_try(raw_bytes, enc, "replace"), pri)

    if best_df is not None:
        return best_df

    raise UnicodeDecodeError("utf-8", b"", 0, 1, "無法以常見編碼解析 CSV")

from text_normalize import normalize_for_match


def clean_name_for_simplified(text: str) -> str:
    """與 `text_normalize.normalize_for_match` 相同，供階段一產出 `清洗後簡體名稱`。"""
    return normalize_for_match(text)


def classify_stock_type(product_name: str, option_name: str) -> str:
    """依「原始」商品名稱／選項判定：現貨 / 預定 / 未知。"""
    raw = ""
    for part in (option_name, product_name):
        if isinstance(part, str) and part.strip():
            raw += part
        elif part is not None and not (isinstance(part, float) and pd.isna(part)):
            raw += str(part)

    if "現貨" in raw:
        return "現貨"
    if "預定" in raw or "預購" in raw:
        return "預定"
    return "未知"


def row_fee_sum(row: pd.Series) -> float:
    a = row["成交手續費"] if pd.notna(row["成交手續費"]) else 0
    b = row["其他服務費"] if pd.notna(row["其他服務費"]) else 0
    c = row["金流與系統處理費"] if pd.notna(row["金流與系統處理費"]) else 0
    return float(a) + float(b) + float(c)


def normalize_qty(value: object) -> int:
    """將數量正規化為 >=1 的整數，與展開規則一致。"""
    try:
        q = int(float(value))
    except (TypeError, ValueError):
        q = 1
    return 1 if q < 1 else q


def compute_order_total_fees(df: pd.DataFrame) -> pd.Series:
    """
    蝦皮 CSV 常將「整筆訂單總手續費」重複寫在每一商品列上。
    因此以訂單編號分組後，只取該群組「第一列 (first row)」之三項手續費加總，
    不可對群組內所有列 sum，否則會重複加總（例如 110 變 220）。
    """
    first_per_order = df.drop_duplicates(subset=["訂單編號"], keep="first")
    idx = first_per_order["訂單編號"]
    totals = first_per_order.apply(row_fee_sum, axis=1).astype(float)
    return pd.Series(totals.values, index=idx.values, name="訂單總手續費")


def expand_quantity(df: pd.DataFrame) -> pd.DataFrame:
    """
    數量 > 1 時展開為多列（每列數量=1）。
    蝦皮 CSV 的「商品原價」為單價：展開時 **維持單價不變**，不可除以數量。
    """
    rows: list[dict] = []
    for _, row in df.iterrows():
        q = normalize_qty(row["數量"])

        base = row.to_dict()
        for _ in range(q):
            new_r = {**base, "數量": 1}
            rows.append(new_r)

    return pd.DataFrame(rows)


def allocate_fees(df_expanded: pd.DataFrame, order_total_fees: pd.Series) -> pd.DataFrame:
    """
    依訂單套用「最大餘數法 (Largest Remainder Method)」整數分攤手續費：
    1) exact_fee = 比例 * 訂單總手續費
    2) base_fee = floor(exact_fee)
    3) shortfall = 訂單總手續費 - sum(base_fee)
    4) 餘數由大到小，前 shortfall 列各 +1
    最終 `單件實扣手續費` 為整數，且每訂單加總恆等於訂單總手續費。
    """
    out = df_expanded.copy()
    out["單件實扣手續費"] = 0

    for order_id, idx in out.groupby("訂單編號", sort=False).groups.items():
        order_rows = out.loc[idx]
        prices = pd.to_numeric(order_rows["商品原價"], errors="coerce").fillna(0.0)
        total_price = float(prices.sum())
        total_fee = int(round(float(order_total_fees.get(order_id, 0.0))))

        if total_fee <= 0 or total_price <= 0:
            out.loc[idx, "單件實扣手續費"] = 0
            continue

        exact = (prices / total_price) * total_fee
        base = exact.apply(math.floor).astype(int)
        remainder = exact - base
        shortfall = total_fee - int(base.sum())

        alloc = base.copy()
        if shortfall > 0:
            # 同餘數時用原索引順序，確保結果穩定可重現
            top_idx = remainder.sort_values(ascending=False, kind="mergesort").index[:shortfall]
            alloc.loc[top_idx] = alloc.loc[top_idx] + 1

        out.loc[idx, "單件實扣手續費"] = alloc.astype(int).values

    out["單件實扣手續費"] = out["單件實扣手續費"].astype(int)
    return out


def validate_processed_data(
    raw_df: pd.DataFrame,
    processed_df: pd.DataFrame,
    order_total_fees: pd.Series,
    amount_tol: float = 0.01,
    fee_tol: float = 0.0,
) -> list[dict]:
    """
    以訂單編號驗證：
    1) 展開後商品原價加總 == 原始(商品原價*數量)加總
    2) 展開後單件實扣手續費加總 == 訂單總手續費
    """
    src = raw_df.copy()
    src["商品原價"] = pd.to_numeric(src["商品原價"], errors="coerce").fillna(0.0)
    src["數量"] = src["數量"].apply(normalize_qty)
    src["原始訂單總金額"] = src["商品原價"] * src["數量"]
    expected_amount = src.groupby("訂單編號", sort=False)["原始訂單總金額"].sum()

    actual_amount = (
        processed_df.groupby("訂單編號", sort=False)["商品原價"]
        .sum()
        .astype(float)
    )
    actual_fee = (
        processed_df.groupby("訂單編號", sort=False)["單件實扣手續費"]
        .sum()
        .astype(float)
    )
    expected_fee = order_total_fees.astype(float)

    issues: list[dict] = []
    all_order_ids = list(
        pd.Index(expected_amount.index)
        .union(actual_amount.index)
        .union(expected_fee.index)
        .union(actual_fee.index)
    )

    for oid in all_order_ids:
        exp_amt = float(expected_amount.get(oid, 0.0))
        act_amt = float(actual_amount.get(oid, 0.0))
        diff_amt = act_amt - exp_amt
        if abs(diff_amt) > amount_tol:
            issues.append(
                {
                    "訂單編號": str(oid),
                    "項目": "金額",
                    "預期值": exp_amt,
                    "實際值": act_amt,
                    "差額": diff_amt,
                }
            )

        exp_fee = float(int(round(float(expected_fee.get(oid, 0.0)))))
        act_fee = float(int(round(float(actual_fee.get(oid, 0.0)))))
        diff_fee = act_fee - exp_fee
        if abs(diff_fee) > fee_tol:
            issues.append(
                {
                    "訂單編號": str(oid),
                    "項目": "手續費",
                    "預期值": exp_fee,
                    "實際值": act_fee,
                    "差額": diff_fee,
                }
            )

    return issues


def build_accounting_reconciliation_df(
    raw_df: pd.DataFrame,
    processed_df: pd.DataFrame,
    order_total_fees: pd.Series,
) -> pd.DataFrame:
    """分攤前後逐訂單對帳明細（供會計核對）。"""
    src = raw_df.copy()
    src["商品原價"] = pd.to_numeric(src["商品原價"], errors="coerce").fillna(0.0)
    src["數量"] = src["數量"].apply(normalize_qty)
    src["_line_amt"] = src["商品原價"] * src["數量"]
    orig_amt = src.groupby("訂單編號", sort=False)["_line_amt"].sum()

    proc_amt = processed_df.groupby("訂單編號", sort=False)["商品原價"].sum()
    proc_fee = processed_df.groupby("訂單編號", sort=False)["單件實扣手續費"].sum()

    ids = (
        orig_amt.index.union(proc_amt.index)
        .union(order_total_fees.index)
        .union(proc_fee.index)
    )
    recs: list[dict] = []
    for oid in ids:
        oa = float(orig_amt.get(oid, 0.0))
        pa = float(proc_amt.get(oid, 0.0))
        ft = int(round(float(order_total_fees.get(oid, 0.0))))
        fa = int(proc_fee.get(oid, 0))
        da = round(pa - oa, 2)
        dfee = fa - ft
        amt_ok = abs(da) <= 0.01
        fee_ok = dfee == 0
        recs.append(
            {
                "訂單編號": oid,
                "原始訂單總金額(單價×數量)": oa,
                "展開後商品金額加總": pa,
                "金額差異": da,
                "金額驗證": "✓" if amt_ok else "✗",
                "訂單總手續費": ft,
                "分攤手續費加總(整數)": fa,
                "手續費差異": dfee,
                "手續費驗證": "✓" if fee_ok else "✗",
            }
        )
    return pd.DataFrame(recs)


# 雲端欄位或正規化邏輯變更時遞增，以刷新 `@st.cache_data` 快取
CLOUD_SHEET_CACHE_VERSION = 4


@st.cache_data(ttl=120, show_spinner="正在讀取 Google Sheet…")
def load_cloud_catalog_cached(
    cache_version: int,
    spreadsheet_id: str,
    worksheet_name: str,
    cred_path: str,
) -> pd.DataFrame:
    _ = cache_version  # 僅用於快取鍵
    return fetch_worksheet_catalog(cred_path, spreadsheet_id, worksheet_name)


def _invalidate_cloud_catalog_cache() -> None:
    """清除雲端商品目錄快取，確保下一輪 rerun 讀取最新狀態。"""
    try:
        load_cloud_catalog_cached.clear()
    except Exception:
        pass
    clear_fn = getattr(fetch_worksheet_catalog, "clear", None)
    if callable(clear_fn):
        try:
            clear_fn()
        except Exception:
            pass
    st.session_state.pop("cloud_catalog_df", None)


def _cloud_catalog_scope_key(spreadsheet_id: str, worksheet_name: str, cred_path: str) -> str:
    return f"{spreadsheet_id}|{worksheet_name.strip()}|{cred_path}"


def _load_cloud_catalog_local(
    spreadsheet_id: str,
    worksheet_name: str,
    cred_path: str,
    *,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """優先使用 session 內 catalog，必要時才從 API 全表重抓。"""
    scope = _cloud_catalog_scope_key(spreadsheet_id, worksheet_name, cred_path)
    same_scope = st.session_state.get("cloud_catalog_scope") == scope
    cached = st.session_state.get("cloud_catalog_df")
    can_use_cached = (
        (not force_refresh)
        and same_scope
        and isinstance(cached, pd.DataFrame)
    )
    if can_use_cached:
        return cached
    fresh = load_cloud_catalog_cached(
        CLOUD_SHEET_CACHE_VERSION,
        spreadsheet_id,
        worksheet_name.strip(),
        cred_path,
    )
    st.session_state["cloud_catalog_df"] = fresh
    st.session_state["cloud_catalog_scope"] = scope
    return fresh


def _mutate_local_catalog_row(sheet_row: int, values_by_col: dict[str, object]) -> None:
    """本地突變 catalog 快取，避免單筆操作後全表重抓。"""
    cached = st.session_state.get("cloud_catalog_df")
    if not isinstance(cached, pd.DataFrame) or cached.empty:
        return
    mask = cached["_sheet_row"] == int(sheet_row)
    if not bool(mask.any()):
        return
    for col, val in values_by_col.items():
        if col in cached.columns:
            cached.loc[mask, col] = "" if val is None else str(val)


def process_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict], pd.Series]:
    df = df.copy()
    # 僅保留需求欄位（若有多餘欄位）
    df = df[[c for c in REQUIRED_COLUMNS if c in df.columns]]

    for col in ["商品原價", "成交手續費", "其他服務費", "金流與系統處理費", "數量"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 手續費：在「展開前」依訂單編號只取第一列之三項手續費（避免同訂單多列重複加總）
    order_total_fees = compute_order_total_fees(df)

    df_exp = expand_quantity(df)

    df_exp = allocate_fees(df_exp, order_total_fees)

    # 移除原始手續費與訂單總額欄位，避免展開多列時重複顯示總額造成誤解
    _drop_fee_source_cols = [
        "成交手續費",
        "其他服務費",
        "金流與系統處理費",
    ]
    df_exp = df_exp.drop(
        columns=[c for c in _drop_fee_source_cols if c in df_exp.columns],
        errors="ignore",
    )

    # 合併原始名稱：抹除編碼殘留的問號（含全形），再收斂空白
    gn = df_exp["商品名稱"].fillna("").astype(str)
    go = df_exp["商品選項名稱"].fillna("").astype(str)
    merged = (gn + " " + go).str.replace(r"\s+", " ", regex=True).str.strip()
    df_exp["合併原始名稱"] = (
        merged.str.replace("?", "", regex=False)
        .str.replace("？", "", regex=False)
        .str.replace("\ufffd", "", regex=False)
    )

    # 現貨/預定/未知：使用「原始」商品名稱與選項（展開後仍與來源列相同）
    df_exp["現貨預定標記"] = [
        classify_stock_type(p, o)
        for p, o in zip(df_exp["商品名稱"], df_exp["商品選項名稱"])
    ]

    df_exp["清洗後簡體名稱"] = df_exp["合併原始名稱"].apply(clean_name_for_simplified)
    # 絕對唯一識別碼：所有狀態追蹤均以 uid 為準
    df_exp = df_exp.reset_index(drop=True)
    df_exp["uid"] = [
        f"{str(order_no)}_{i}"
        for i, order_no in enumerate(df_exp["訂單編號"].fillna("UNKNOWN"))
    ]

    validation_issues = validate_processed_data(
        raw_df=df,
        processed_df=df_exp,
        order_total_fees=order_total_fees,
        amount_tol=0.01,
        fee_tol=0.0,
    )
    return df_exp, validation_issues, order_total_fees


def _build_sheet_pick_options(
    query_simplified: str, filtered: pd.DataFrame
) -> list[tuple[str, int | None]]:
    """推薦列在前、略過在最後；預設 index=0 可直接選到最高分推薦。"""
    opts: list[tuple[str, int | None]] = []
    for sheet_row, score, merged in fuzzy_top3_matches(query_simplified, filtered):
        disp = zhconv.convert(merged.replace("\n", " "), "zh-tw")
        if len(disp) > 52:
            disp = disp[:52] + "…"
        opts.append(
            (
                f"第 {sheet_row} 列 · 相似度 {score} · {disp}",
                sheet_row,
            )
        )
    opts.append(("略過不寫入", None))
    return opts


def _build_candidate_pool_pick_options(
    candidate_df: pd.DataFrame,
    *,
    skip_first: bool,
) -> list[tuple[str, int | None]]:
    """將候選池資料列轉成選單（完整品名+款式）。"""
    opts: list[tuple[str, int | None]] = []
    if skip_first:
        opts.append(("略過不寫入", None))
    for _, row in candidate_df.iterrows():
        sheet_row = int(row.get("_sheet_row", 0) or 0)
        if sheet_row <= 0:
            continue
        pn = zhconv.convert(
            str(row.get("品名", "") or "").replace("\n", " ").strip(),
            "zh-tw",
        )
        xi = zhconv.convert(
            str(row.get("款式細項", "") or "").replace("\n", " ").strip(),
            "zh-tw",
        )
        opts.append((f"第 {sheet_row} 列 - {pn} {xi}".strip(), sheet_row))
    if not skip_first:
        opts.append(("略過不寫入", None))
    return opts


def _effective_sheet_row_from_state(
    uid: str,
    options: list[tuple[str, int | None]],
    *,
    default_index: int | None = 0,
) -> int | None:
    """依 session_state 取得本筆訂單目前選定的真實列號（略過 → None）。"""
    if st.session_state.get(f"manual_override_{uid}", False):
        v = st.session_state.get(f"manual_row_{uid}")
        if v is None:
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            return None
    sel_key = f"cloud_sel_{uid}"
    raw = st.session_state.get(sel_key, default_index)
    if raw is None:
        return None
    # 相容舊版 state：有機會殘留成「第 N 列 · 相似度...」字串
    if isinstance(raw, int):
        i = raw
    else:
        try:
            i = int(raw)
        except (TypeError, ValueError):
            labels = [o[0] for o in options]
            if isinstance(raw, str) and raw in labels:
                i = labels.index(raw)
            else:
                if default_index is None:
                    st.session_state[sel_key] = None
                    return None
                i = int(default_index)
            st.session_state[sel_key] = i
    if i < 0 or i >= len(options):
        return None
    return options[i][1]


def _fingerprint_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _new_batch_id(filename: str) -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S") + f"_{filename}"


def _latest_history_detail_by_uid(actions: list[dict]) -> dict[str, dict[str, object]]:
    """每個 Order_UID 只保留歷史表中最後一列（列號最大者）的 action 摘要。"""
    best: dict[str, tuple[int, dict[str, object]]] = {}
    for a in actions:
        uid = str(a.get("Order_UID", "")).strip()
        if not uid:
            continue
        rn = int(a.get("_row_number", 0))
        tr_raw = str(a.get("Target_Row", "")).strip()
        try:
            tr: int | None = int(tr_raw) if tr_raw else None
        except ValueError:
            tr = None
        detail: dict[str, object] = {
            "action_type": str(a.get("Action_Type", "")).strip().lower(),
            "target_row": tr,
            "orig_platform": str(a.get("Orig_Platform", "") or ""),
            "orig_buyer": str(a.get("Orig_Buyer", "") or ""),
            "orig_price": str(a.get("Orig_Price", "") or ""),
            "orig_fee": str(a.get("Orig_Fee", "") or ""),
        }
        if uid not in best or rn > best[uid][0]:
            best[uid] = (rn, detail)
    return {u: t[1] for u, t in best.items()}


def _latest_last_hint(actions: list[dict]) -> str:
    """取得歷史表最後一筆有效 Last_Hint（依列號最大）。"""
    best_row = -1
    best_hint = ""
    for a in actions:
        rn = int(a.get("_row_number", 0))
        hint = str(a.get("Last_Hint", "") or "").strip()
        if hint and rn > best_row:
            best_row = rn
            best_hint = hint
    return best_hint


def _next_unfinished_uid(result_df: pd.DataFrame) -> str | None:
    """回傳第一個未完成卡片 uid（依目前順序）。"""
    for pos, idx in enumerate(result_df.index):
        uid = f"r_{pos}_{idx}"
        if not st.session_state.get(f"done_{uid}", False):
            return uid
    return None


def main():
    st.set_page_config(page_title="蝦皮訂單預處理", layout="wide")
    st.title("蝦皮訂單 → 預處理、對帳與 Google Sheet 比對")
    st.caption(
        "階段一：數量展開、整數手續費分攤（**單件實扣手續費**）、名稱清洗與驗證。"
        "階段二：讀取 Google Sheet，以 **token_set_ratio** 模糊比對品名。"
    )

    with st.sidebar:
        st.subheader("CSV 編碼")
        enc_labels = [t[0] for t in MANUAL_ENCODING_OPTIONS]
        enc_values = [t[1] for t in MANUAL_ENCODING_OPTIONS]
        enc_idx = st.selectbox(
            "若表頭顯示亂碼或提示缺欄位，請改選編碼",
            range(len(enc_labels)),
            format_func=lambda i: enc_labels[i],
            index=0,
        )
        encoding_override = enc_values[enc_idx]

        st.divider()
        st.subheader("階段二 · Google Sheets")
        sheet_url, worksheet_name, cred_path, cfg_warn = load_google_sheet_config()
        st.caption("連線設定由專案目錄下的 **appsetting.json** 讀取。")
        st.caption(f"工作表：**{worksheet_name}**")
        if cfg_warn:
            st.warning(cfg_warn)

    uploaded = st.file_uploader("上傳 CSV", type=["csv"])

    if uploaded is None:
        st.info("請上傳包含指定欄位的蝦皮訂單 CSV。")
        st.markdown(
            "**必要欄位：** "
            + "、".join(f"`{c}`" for c in REQUIRED_COLUMNS)
        )
        st.markdown(
            "**編碼建議：** 蝦皮／Excel 在台灣匯出常為 **Big5**；"
            "若用記事本另存請選 **UTF-8**。"
        )
        return

    # getvalue() 可重複讀取；避免只用 read() 後指標在結尾
    raw_bytes = uploaded.getvalue() if hasattr(uploaded, "getvalue") else uploaded.read()
    csv_name = getattr(uploaded, "name", "unknown.csv")
    csv_fp = _fingerprint_bytes(raw_bytes)

    try:
        df = read_csv_bytes(raw_bytes, encoding_override=encoding_override)
    except UnicodeDecodeError:
        st.error(
            "無法解碼 CSV。請在左側將「CSV 編碼」改為 **Big5** 或 **UTF-8（含 BOM）** 後再試，"
            "或用 Excel「另存新檔 → CSV UTF-8」後重新上傳。"
        )
        return
    except Exception as e:
        st.error(f"讀取 CSV 失敗：{e}")
        return

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        st.error(
            "缺少以下欄位（常因 **編碼錯誤** 導致表頭變亂碼，程式才對不到欄位名）："
            + "、".join(f"`{m}`" for m in missing)
        )
        st.warning(
            "請到左側 **CSV 編碼** 改選「**Big5（台灣 Excel／蝦皮常見）**」或「**UTF-8（含 BOM）**」後，"
            "畫面會自動重新整理。"
        )
        st.dataframe(df.head(20), width="stretch")
        return

    try:
        result, validation_issues, order_total_fees = process_dataframe(df)
    except Exception as e:
        st.exception(e)
        return

    if not validation_issues:
        st.success("✅ 所有訂單的金額與手續費加總驗證通過！帳務吻合。")
    else:
        st.error("發現訂單驗證失敗，請先確認以下差異再進行後續帳務流程：")
        for issue in validation_issues:
            st.error(
                f"訂單 `{issue['訂單編號']}` 的「{issue['項目']}」對不上："
                f"預期={issue['預期值']:.2f}，實際={issue['實際值']:.2f}，"
                f"差額={issue['差額']:+.2f}"
            )

    st.success(f"處理完成，共 **{len(result)}** 列（展開後）。")
    st.info(
        "手續費已依單件金額比例分攤為 **單件實扣手續費**；"
        "已不顯示原始「成交／其他／金流」手續費欄位，避免與分攤結果混淆。"
    )

    # --- 階段二：載入雲端目錄（可快取；設定來自 appsetting.json）---
    spreadsheet_id = extract_spreadsheet_id(sheet_url)

    cloud_df: pd.DataFrame | None = None
    cloud_error: str | None = None
    if not spreadsheet_id:
        cloud_error = (
            "無法從 appsetting.json 的 spreadsheetUrl 解析 spreadsheetId，請確認為完整 Google Sheets 連結。"
        )
    elif not os.path.isfile(cred_path):
        cloud_error = (
            f"找不到服務帳戶 JSON：{cred_path}（請檢查 appsetting.json 的 serviceAccountJsonPath）"
        )
    else:
        try:
            force_catalog_sync = bool(st.session_state.get("force_catalog_sync", False))
            cloud_df = _load_cloud_catalog_local(
                spreadsheet_id=spreadsheet_id,
                worksheet_name=worksheet_name.strip(),
                cred_path=cred_path,
                force_refresh=force_catalog_sync,
            )
            st.session_state["force_catalog_sync"] = False
        except Exception as e:
            cloud_error = str(e)

    if cloud_error:
        st.warning(f"⚠️ Google Sheet：{cloud_error}")

    # 雲端歷史狀態（⚙️系統歷史紀錄）
    history_actions: list[dict] = []
    completed_uids: set[str] = set()
    uid_action_map: dict[str, str] = {}
    batch_groups: list[dict] = []

    can_history = bool(spreadsheet_id and not cloud_error)
    history_detail_map: dict[str, dict[str, object]] = {}
    history_scope_key = f"{spreadsheet_id}|{worksheet_name.strip()}|{cred_path}"
    force_history_sync = bool(st.session_state.get("force_history_sync", False))
    need_history_init = (
        ("initial_sync_done" not in st.session_state)
        or (st.session_state.get("initial_sync_scope") != history_scope_key)
        or force_history_sync
        or bool(st.session_state.get("history_cache_dirty", False))
    )
    if can_history:
        try:
            if need_history_init:
                history_actions = read_history_actions(cred_path, spreadsheet_id)
                st.session_state["history_actions_cache"] = history_actions
                st.session_state["initial_sync_done"] = True
                st.session_state["initial_sync_scope"] = history_scope_key
                st.session_state["force_history_sync"] = False
                st.session_state["history_cache_dirty"] = False
            else:
                history_actions = list(st.session_state.get("history_actions_cache", []))
            completed_uids = completed_uids_from_actions(history_actions)
            uid_action_map = latest_uid_action_map(history_actions)
            batch_groups = group_batches(history_actions)
            history_detail_map = _latest_history_detail_by_uid(history_actions)
            st.session_state["completed_uids"] = sorted(completed_uids)
            gc_deleted = gc_keep_latest_batches(cred_path, spreadsheet_id, keep=5)
            if gc_deleted > 0:
                st.info(f"歷史表已清理舊批次紀錄列 {gc_deleted} 筆（僅保留最近 5 批）。")
                st.session_state["history_cache_dirty"] = True
        except Exception as e:
            st.warning(f"⚠️ 讀取雲端歷史紀錄失敗：{e}")

    with st.sidebar:
        st.divider()
        st.subheader("系統控制台")
        if st.button("🔄 從雲端同步最新狀態", key="history_sync_button"):
            st.session_state["force_history_sync"] = True
            st.session_state["force_catalog_sync"] = True
            _invalidate_cloud_catalog_cache()
            st.rerun()
        latest_hint = _latest_last_hint(history_actions)
        if latest_hint:
            st.caption(f"上次做到：{latest_hint}")
        else:
            st.caption("上次做到：目前無可用紀錄")
        with st.expander("🕒 系統歷史與批次回溯", expanded=False):
            if not batch_groups:
                st.caption("目前沒有可回溯批次。")
            else:
                labels = [
                    f"{b.get('filename','?')}｜actions={b.get('count',0)}｜{b.get('upload_time','')}"
                    for b in batch_groups
                ]
                bi = st.selectbox(
                    "最近批次（最多 5 批）",
                    range(len(labels)),
                    format_func=lambda i: labels[i],
                    key="history_batch_pick",
                )
                selected_batch = batch_groups[bi]
                st.caption(f"Batch ID: `{selected_batch.get('batch_id','')}`")
                if st.button("還原此批次資料", key="rollback_selected_batch"):
                    try:
                        restored, deleted = rollback_batch(
                            cred_path,
                            spreadsheet_id,
                            worksheet_name.strip(),
                            selected_batch.get("batch_id", ""),
                        )
                        st.session_state["history_cache_dirty"] = True
                        st.session_state["force_catalog_sync"] = True
                        _invalidate_cloud_catalog_cache()
                        st.success(f"✅ 已回溯批次：還原 {restored} 筆、刪除歷史列 {deleted} 筆。")
                        st.rerun()
                    except Exception as e:
                        st.error(f"回溯失敗：{e}")

    st.subheader("逐筆審核")
    show_only_pending = st.checkbox("只顯示未完成", value=False, key="show_only_pending")
    next_uid = _next_unfinished_uid(result)
    if next_uid:
        st.caption(f"下一筆待處理：`{next_uid}`")
    else:
        st.caption("目前沒有待處理項目。")
    if cloud_df is None or cloud_df.empty:
        st.info("載入試算表後，此處將顯示每筆訂單與推薦雲端列、手動行號與寫入按鈕狀態。")

    # 預先建立每筆訂單的下拉選項
    review_meta: dict[str, dict] = {}
    active_batch_id = ""
    active_upload_time = ""
    if can_history and spreadsheet_id:
        batch_scope = f"{csv_name}|{csv_fp}|{spreadsheet_id}|{worksheet_name.strip()}"
        prev_scope = st.session_state.get("active_batch_scope")
        if prev_scope == batch_scope and st.session_state.get("active_batch_id"):
            active_batch_id = str(st.session_state.get("active_batch_id"))
            active_upload_time = str(st.session_state.get("active_upload_time") or _now_text())
        else:
            active_batch_id = _new_batch_id(csv_name)
            active_upload_time = _now_text()
            st.session_state["active_batch_scope"] = batch_scope
            st.session_state["active_batch_id"] = active_batch_id
            st.session_state["active_upload_time"] = active_upload_time

    if cloud_df is not None and not cloud_df.empty:
        for pos, (idx, row) in enumerate(result.iterrows()):
            uid = f"r_{pos}_{idx}"
            row_uid = str(row.get("uid", ""))
            st.session_state[f"done_{uid}"] = row_uid in completed_uids
            st.session_state[f"done_type_{uid}"] = uid_action_map.get(row_uid, "")
            tag = str(row.get("現貨預定標記", "") or "")
            candidate_df = candidate_pool_for_stock_tag(cloud_df, tag)
            q = str(row.get("清洗後簡體名稱", "") or "")
            review_meta[uid] = {
                "top_options": _build_sheet_pick_options(q, candidate_df),
                "all_options": _build_candidate_pool_pick_options(
                    candidate_df, skip_first=True
                ),
                "row": row,
                "pos": pos,
                "idx": idx,
                "row_uid": row_uid,
            }

    for pos, (idx, row) in enumerate(result.iterrows()):
        uid = f"r_{pos}_{idx}"
        done_key = f"done_{uid}"
        done = bool(st.session_state.get(done_key, False))
        done_type = str(st.session_state.get(f"done_type_{uid}", "") or "")
        if show_only_pending and done:
            continue
        buyer = row.get("買家帳號", "")
        tag = row.get("現貨預定標記", "")
        price = row.get("商品原價", "")
        if done_type == "skip":
            done_mark = " [⏭ 已略過]"
        else:
            done_mark = " [✅ 已完成]" if done else ""
        title = f"{buyer} ｜ {tag} ｜ 商品原價：{price}{done_mark}"
        expanded_by_default = (not done) and (uid == next_uid)
        with st.expander(title, expanded=expanded_by_default):
            merged_disp = str(row.get("合併原始名稱", "") or "")
            fee_int = int(row.get("單件實扣手續費", 0) or 0)
            row_uid = str(row.get("uid", ""))

            if done:
                if done_type == "skip":
                    st.success("⏭️ 已略過此筆。")
                else:
                    hd = history_detail_map.get(row_uid, {})
                    tr = hd.get("target_row")
                    if isinstance(tr, int) and tr > 0:
                        st.success(f"✅ 已寫入至第 {tr} 列。")
                    else:
                        st.success("✅ 此筆已完成。")
                clicked_rollback_one = st.button(
                    "🔙 撤銷此筆（單筆回溯）",
                    key=f"rollback_one_{uid}",
                    disabled=not can_history,
                    help="從雲端歷史表復原欄位或移除略過紀錄。",
                )
                if clicked_rollback_one:
                    try:
                        deleted, restored = rollback_order_uid(
                            cred_path,
                            spreadsheet_id,
                            worksheet_name.strip(),
                            row_uid,
                        )
                        if deleted <= 0:
                            st.warning("找不到此 UID 的歷史紀錄。")
                        else:
                            st.session_state[done_key] = False
                            st.session_state[f"done_type_{uid}"] = ""
                            st.session_state["history_cache_dirty"] = True
                            if restored:
                                hd = history_detail_map.get(row_uid, {})
                                tr = hd.get("target_row")
                                if isinstance(tr, int) and tr > 0:
                                    _mutate_local_catalog_row(
                                        tr,
                                        {
                                            "平台": hd.get("orig_platform", ""),
                                            "買家": hd.get("orig_buyer", ""),
                                            "賣場售價": hd.get("orig_price", ""),
                                            "賣場手續費": hd.get("orig_fee", ""),
                                        },
                                    )
                            if restored:
                                st.success("✅ 此筆已回溯並移除歷史紀錄。")
                            else:
                                st.success(
                                    "✅ 此筆歷史紀錄已移除（略過無需資料回寫）。"
                                )
                            st.rerun()
                    except Exception as e:
                        st.error(f"單筆回溯失敗：{e}")
                continue

            if cloud_df is None or cloud_df.empty or uid not in review_meta:
                st.info(merged_disp)
                st.metric("單件實扣手續費", f"{fee_int}")
                st.caption("（尚未載入雲端資料或工作表為空，無法比對）")
                continue

            top_options = review_meta[uid]["top_options"]
            all_options = review_meta[uid]["all_options"]

            left_c, right_c = st.columns([1, 1])
            with left_c:
                st.caption("商品與訂單")
                st.info(merged_disp)
                st.metric("單件實扣手續費", f"{fee_int}")

            with right_c:
                st.caption("比對與決策")
                opt_col1, opt_col2 = st.columns(2)
                with opt_col1:
                    show_all_available = st.toggle(
                        "🔍 找不到？展開所有可用空位",
                        value=False,
                        key=f"show_all_available_{uid}",
                    )
                with opt_col2:
                    manual = st.checkbox(
                        "⚙️ 進階：手動輸入列號",
                        key=f"manual_override_{uid}",
                    )
                mode_key = f"show_all_available_mode_{uid}"
                prev_mode = bool(st.session_state.get(mode_key, False))
                if prev_mode != bool(show_all_available):
                    st.session_state.pop(f"cloud_sel_{uid}", None)
                    st.session_state[mode_key] = bool(show_all_available)

                options = all_options if show_all_available else top_options
                labels = [o[0] for o in options]
                st.selectbox(
                    "可用空位清單（可鍵盤搜尋）"
                    if show_all_available
                    else "推薦雲端列（可選略過不寫入）",
                    range(len(labels)),
                    format_func=lambda i: labels[i],
                    index=None if show_all_available else 0,
                    placeholder=(
                        "請點擊此處並直接輸入鍵盤關鍵字搜尋 (例如: cd080)..."
                        if show_all_available
                        else None
                    ),
                    key=f"cloud_sel_{uid}",
                )
                if manual:
                    st.number_input(
                        "請輸入行號",
                        min_value=1,
                        step=1,
                        value=SHEET_FIRST_DATA_ROW_1BASED,
                        key=f"manual_row_{uid}",
                    )

                eff_preview = _effective_sheet_row_from_state(
                    uid,
                    options,
                    default_index=None if show_all_available else 0,
                )
                r_preview = (
                    get_catalog_row_by_sheet_row(cloud_df, eff_preview)
                    if eff_preview is not None
                    else None
                )
                if r_preview is not None:
                    plat = str(r_preview.get("平台", "") or "").strip() or "空白"
                    buyer_now = str(r_preview.get("買家", "") or "").strip() or "空白"
                    plat_raw = str(r_preview.get("平台", "") or "").strip()
                    buyer_raw = str(r_preview.get("買家", "") or "").strip()
                    risk_occupied = bool(buyer_raw) or (
                        bool(plat_raw) and plat_raw != "預現貨"
                    )
                    if risk_occupied:
                        st.warning(
                            f"⚠️ 覆蓋警告：此列已有資料！平台 **{plat}** ｜ 買家 **{buyer_now}**"
                        )
                    else:
                        st.success(
                            f"✅ 目標列狀態確認：平台 **{plat}** ｜ 買家 **{buyer_now}**（空位可安全寫入）"
                        )
                else:
                    st.caption("目前此列狀態：未選取目標列")

            eff_now = _effective_sheet_row_from_state(
                uid,
                options,
                default_index=None if show_all_available else 0,
            )

            r_target = (
                get_catalog_row_by_sheet_row(cloud_df, eff_now)
                if eff_now is not None
                else None
            )
            occupied = row_has_order_like_data(r_target)

            if occupied and eff_now is not None:
                st.checkbox(
                    "強制覆蓋已有資料（此行已有買家／售價／手續費）",
                    key=f"force_write_{uid}",
                )

            force_ok = (not occupied) or bool(
                st.session_state.get(f"force_write_{uid}", False)
            )
            valid_target = eff_now is not None and r_target is not None

            action_kind = "skip" if eff_now is None else "write"
            write_blocked = bool(
                eff_now is not None and occupied and (not force_ok)
            )
            clicked_action = False

            with right_c:
                if eff_now is not None and r_target is None:
                    st.error("無法寫入：找不到試算表中此列號（請確認為資料區且列號正確）。")
                elif occupied and eff_now is not None and not bool(
                    st.session_state.get(f"force_write_{uid}", False)
                ):
                    st.error("無法寫入：此列已有資料，請勾選「強制覆蓋」後再確認寫入。")

                if action_kind == "skip":
                    clicked_action = st.button(
                        "⏭️ 確認略過此筆",
                        key=f"action_btn_{uid}",
                        type="secondary",
                        disabled=False,
                        help="記錄為略過，不寫入試算表。",
                    )
                else:
                    clicked_action = st.button(
                        f"💾 確認寫入至第 {int(eff_now)} 行",
                        key=f"action_btn_{uid}",
                        type="primary",
                        disabled=write_blocked,
                        help=None
                        if not write_blocked
                        else "此筆目前不可寫入，請先排除上方警示。",
                    )

            if clicked_action and action_kind == "skip":
                if not can_history or not active_batch_id:
                    st.error("目前無法連線雲端歷史表，暫時不能標記略過。")
                    continue
                try:
                    hint = (
                        f"{str(row.get('訂單成立日期','') or '')} 的 "
                        f"{str(row.get('買家帳號','') or '')} "
                        f"(訂單: {str(row.get('訂單編號','') or '')})"
                    )
                    append_history_action(
                        cred_path,
                        spreadsheet_id,
                        batch_id=active_batch_id,
                        upload_time=active_upload_time,
                        filename=csv_name,
                        order_uid=row_uid,
                        action_type="skip",
                        target_row=None,
                        original_data=None,
                        last_hint=hint,
                    )
                    gc_keep_latest_batches(cred_path, spreadsheet_id, keep=5)
                    st.session_state[done_key] = True
                    st.session_state[f"done_type_{uid}"] = "skip"
                    st.session_state["history_cache_dirty"] = True
                    st.success("已標記為略過。")
                    st.rerun()
                except Exception as e:
                    st.error(f"標記略過失敗：{e}")

            if clicked_action and action_kind == "write":
                if not can_history or not active_batch_id:
                    st.error("目前無法連線雲端歷史表，暫時不能寫入。")
                    continue
                if not valid_target or r_target is None:
                    st.error("目前選取列無效，請重新確認行號。")
                    continue
                try:
                    header_map = dict(r_target.get("_header_index_1based", {}) or {})
                    cols4 = ["平台", "買家", "賣場售價", "賣場手續費"]
                    # 即時交易安全：點擊當下重新讀取雲端欄位，防止他人剛更新同列而被覆蓋。
                    preview_vals = {
                        "平台": str(r_target.get("平台", "") or ""),
                        "買家": str(r_target.get("買家", "") or ""),
                        "賣場售價": str(r_target.get("賣場售價", "") or ""),
                        "賣場手續費": str(r_target.get("賣場手續費", "") or ""),
                    }
                    before_vals = get_row_values_by_columns(
                        service_account_path=cred_path,
                        spreadsheet_id=spreadsheet_id,
                        worksheet_name=worksheet_name.strip(),
                        sheet_row=int(eff_now),
                        header_index_1based=header_map,
                        columns=cols4,
                    )
                    if any(
                        str(before_vals.get(k, "") or "").strip()
                        != str(preview_vals.get(k, "") or "").strip()
                        for k in cols4
                    ):
                        st.error(
                            "此列雲端資料已在你操作期間被更新，已中止本次寫入。請先重新確認該列狀態後再提交。"
                        )
                        st.session_state["force_history_sync"] = True
                        st.rerun()
                    write_order_values_to_sheet_row(
                        service_account_path=cred_path,
                        spreadsheet_id=spreadsheet_id,
                        worksheet_name=worksheet_name.strip(),
                        sheet_row=int(eff_now),
                        header_index_1based=header_map,
                        buyer_account=str(row.get("買家帳號", "") or ""),
                        sale_price=float(row.get("商品原價", 0) or 0),
                        fee_value=int(row.get("單件實扣手續費", 0) or 0),
                    )
                    hint = (
                        f"{str(row.get('訂單成立日期','') or '')} 的 "
                        f"{str(row.get('買家帳號','') or '')} "
                        f"(訂單: {str(row.get('訂單編號','') or '')})"
                    )
                    append_history_action(
                        cred_path,
                        spreadsheet_id,
                        batch_id=active_batch_id,
                        upload_time=active_upload_time,
                        filename=csv_name,
                        order_uid=row_uid,
                        action_type="write",
                        target_row=int(eff_now),
                        original_data=before_vals,
                        last_hint=hint,
                    )
                    gc_keep_latest_batches(cred_path, spreadsheet_id, keep=5)
                    st.session_state[done_key] = True
                    st.session_state[f"done_type_{uid}"] = "write"
                    st.session_state["history_cache_dirty"] = True
                    _mutate_local_catalog_row(
                        int(eff_now),
                        {
                            "平台": "蝦皮",
                            "買家": str(row.get("買家帳號", "") or ""),
                            "賣場售價": str(float(row.get("商品原價", 0) or 0)),
                            "賣場手續費": str(int(row.get("單件實扣手續費", 0) or 0)),
                        },
                    )
                    st.success("✅ 已成功寫入雲端表單！")
                    st.rerun()
                except Exception as e:
                    st.error(f"寫入失敗：{e}")

    # 本次完成進度（session 級）
    total_cards = len(result)
    done_count = sum(
        1
        for pos, idx in enumerate(result.index)
        if st.session_state.get(f"done_r_{pos}_{idx}", False)
    )
    st.progress(0 if total_cards == 0 else done_count / total_cards)
    st.caption(f"本次處理進度：{done_count}/{total_cards} 筆已完成")

    if st.button("重置本次完成標記（不影響雲端資料）", key="reset_done_flags"):
        for pos, idx in enumerate(result.index):
            st.session_state.pop(f"done_r_{pos}_{idx}", None)
        st.rerun()

    with st.expander("📋 檢視完整處理結果表格", expanded=False):
        st.dataframe(result, width="stretch", height=420)

    csv_out = result.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        label="下載處理結果 CSV（UTF-8 BOM）",
        data=csv_out,
        file_name="shopee_orders_processed.csv",
        mime="text/csv",
    )

    recon_df = build_accounting_reconciliation_df(df, result, order_total_fees)
    with st.expander("📊 查看會計對帳明細", expanded=False):
        st.caption("逐訂單：原始總金額 vs 展開後金額、訂單總手續費 vs 整數分攤加總。")
        st.dataframe(recon_df, width="stretch", height=360)


if __name__ == "__main__":
    main()
