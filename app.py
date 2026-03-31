# -*- coding: utf-8 -*-
"""
蝦皮訂單 CSV 預處理與清洗（Streamlit）
階段一：預處理、整數手續費分攤、驗證
階段二：Google Sheets 讀取與模糊比對（thefuzz）
"""

from __future__ import annotations

import hashlib
import io
import math
import os
from datetime import datetime

import pandas as pd
import streamlit as st
import zhconv

from app_settings import load_google_sheet_config
from sku_dictionary import (
    batch_learn_dictionary_entries,
    batch_forget_dictionary_entries,
    forget_dictionary_by_raw_name,
    merged_standard_keyword_from_catalog_row,
    read_dictionary_map,
)
from cloud_history import (
    append_history_actions_batch,
    completed_uids_from_actions,
    gc_keep_latest_batches,
    group_batches,
    latest_uid_action_map,
    processed_uids_from_actions,
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
    batch_write_order_values_to_sheet_rows,
)
from text_normalize import normalize_for_match

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
    # 確定性 UID：同訂單/買家/商品名稱在不同批次重上傳仍得到相同識別
    df_exp = df_exp.reset_index(drop=True)
    key_series = (
        df_exp["訂單編號"].fillna("").astype(str).str.strip()
        + "||"
        + df_exp["買家帳號"].fillna("").astype(str).str.strip()
        + "||"
        + df_exp["合併原始名稱"].fillna("").astype(str).str.strip()
    )
    part = key_series.groupby(key_series, sort=False).cumcount()
    df_exp["uid"] = [
        hashlib.sha256(f"{k}||{int(p)}".encode("utf-8")).hexdigest()[:20]
        for k, p in zip(key_series, part)
    ]

    validation_issues = validate_processed_data(
        raw_df=df,
        processed_df=df_exp,
        order_total_fees=order_total_fees,
        amount_tol=0.01,
        fee_tol=0.0,
    )
    return df_exp, validation_issues, order_total_fees


def _get_sku_dictionary_map(cred_path: str, spreadsheet_id: str) -> dict[str, str]:
    """雲端商品字典；session 快取，手動同步時標記 dirty 重讀。"""
    if not spreadsheet_id or not cred_path:
        return {}
    scope = spreadsheet_id
    if (
        st.session_state.get("sku_dictionary_scope") == scope
        and not st.session_state.get("sku_dictionary_dirty", False)
        and isinstance(st.session_state.get("sku_dictionary_cache"), dict)
    ):
        return dict(st.session_state["sku_dictionary_cache"])
    m = read_dictionary_map(cred_path, spreadsheet_id)
    st.session_state["sku_dictionary_cache"] = m
    st.session_state["sku_dictionary_scope"] = scope
    st.session_state["sku_dictionary_dirty"] = False
    return m


def _dict_hits_in_candidate_pool(
    candidate_df: pd.DataFrame, keyword: str
) -> list[tuple[int, pd.Series]]:
    """候選池內含 Standard_Keyword 的列（原文或正規化子字串）。"""
    kw = (keyword or "").strip()
    if not kw or candidate_df.empty:
        return []
    kn = normalize_for_match(kw)
    hits: list[tuple[int, pd.Series]] = []
    seen: set[int] = set()
    for _, row in candidate_df.iterrows():
        sr = int(row.get("_sheet_row", 0) or 0)
        if sr <= 0 or sr in seen:
            continue
        merged = str(row.get("_雲端合併比對字串", "") or "")
        mn = normalize_for_match(merged)
        if kw in merged or (kn and kn in mn):
            seen.add(sr)
            hits.append((sr, row))
    hits.sort(key=lambda x: x[0])
    return hits


def _format_row_label_dict_hit(row: pd.Series) -> str:
    pn = zhconv.convert(
        str(row.get("品名", "") or "").replace("\n", " ").strip(), "zh-tw"
    )
    xi = zhconv.convert(
        str(row.get("款式細項", "") or "").replace("\n", " ").strip(), "zh-tw"
    )
    sr = int(row.get("_sheet_row", 0) or 0)
    return f"[✨ 字典命中] 第 {sr} 列 - {pn} {xi}".strip()


def _build_top_pick_options(
    raw_name: str,
    query_simplified: str,
    candidate_df: pd.DataFrame,
    dict_map: dict[str, str],
) -> list[tuple[str, int | None]]:
    """字典命中置頂，其餘 fuzzy 推薦，略過最後。"""
    opts: list[tuple[str, int | None]] = []
    raw_key = (raw_name or "").strip()
    hit_rows: set[int] = set()
    if raw_key in dict_map:
        kw = dict_map[raw_key]
        for sr, row in _dict_hits_in_candidate_pool(candidate_df, kw):
            hit_rows.add(sr)
            opts.append((_format_row_label_dict_hit(row), sr))
    for sheet_row, score, merged in fuzzy_top3_matches(
        query_simplified, candidate_df
    ):
        if sheet_row in hit_rows:
            continue
        disp = zhconv.convert(merged.replace("\n", " "), "zh-tw")
        if len(disp) > 52:
            disp = disp[:52] + "…"
        opts.append(
            (f"第 {sheet_row} 列 · 相似度 {score} · {disp}", sheet_row)
        )
    opts.append(("略過不寫入", None))
    return opts


def _build_all_pick_options(
    candidate_df: pd.DataFrame,
    raw_name: str,
    dict_map: dict[str, str],
    *,
    skip_first: bool,
) -> list[tuple[str, int | None]]:
    """展開模式：略過後接字典命中，其餘候選池列依序。"""
    opts: list[tuple[str, int | None]] = []
    raw_key = (raw_name or "").strip()
    hit_rows: set[int] = set()
    dict_labels: list[tuple[str, int]] = []
    if raw_key in dict_map:
        kw = dict_map[raw_key]
        for sr, row in _dict_hits_in_candidate_pool(candidate_df, kw):
            hit_rows.add(sr)
            dict_labels.append((_format_row_label_dict_hit(row), sr))
    if skip_first:
        opts.append(("略過不寫入", None))
        opts.extend(dict_labels)
    else:
        opts.extend(dict_labels)
    for _, row in candidate_df.iterrows():
        sheet_row = int(row.get("_sheet_row", 0) or 0)
        if sheet_row <= 0 or sheet_row in hit_rows:
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


def _session_write_skip_counts(result_df: pd.DataFrame) -> tuple[int, int]:
    """依 session 完成的 done_type 統計寫入／略過筆數。"""
    n_write = n_skip = 0
    for pos, idx in enumerate(result_df.index):
        uid = f"r_{pos}_{idx}"
        dt = str(st.session_state.get(f"done_type_{uid}", "") or "").strip().lower()
        if dt == "write":
            n_write += 1
        elif dt == "skip":
            n_skip += 1
    return n_write, n_skip


def _staged_actions() -> list[dict]:
    cur = st.session_state.get("staged_actions")
    if isinstance(cur, list):
        return list(cur)
    return []


def _set_staged_actions(actions: list[dict]) -> None:
    st.session_state["staged_actions"] = list(actions)


def _staged_map_by_uid(actions: list[dict]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for a in actions:
        uid = str(a.get("order_uid", "") or "").strip()
        if uid:
            out[uid] = a
    return out


def _revert_optimistic_action(action: dict) -> None:
    if str(action.get("action_type", "")).lower() == "write":
        tr = int(action.get("target_row", 0) or 0)
        before_local = dict(action.get("before_local") or {})
        if tr > 0 and before_local:
            _mutate_local_catalog_row(tr, before_local)
    raw = str(action.get("raw_name", "") or "").strip()
    prev = action.get("prev_dict_keyword")
    if raw:
        msku = dict(st.session_state.get("sku_dictionary_cache") or {})
        if prev:
            msku[raw] = str(prev)
        else:
            msku.pop(raw, None)
        st.session_state["sku_dictionary_cache"] = msku


def _apply_optimistic_action(action: dict) -> None:
    if str(action.get("action_type", "")).lower() == "write":
        tr = int(action.get("target_row", 0) or 0)
        if tr > 0:
            _mutate_local_catalog_row(
                tr,
                {
                    "平台": "蝦皮",
                    "買家": str(action.get("buyer_account", "") or ""),
                    "賣場售價": str(action.get("sale_price", "") or ""),
                    "賣場手續費": str(action.get("fee_value", "") or ""),
                },
            )
    raw = str(action.get("raw_name", "") or "").strip()
    std_kw = str(action.get("std_keyword", "") or "").strip()
    if raw and std_kw:
        msku = dict(st.session_state.get("sku_dictionary_cache") or {})
        msku[raw] = std_kw
        st.session_state["sku_dictionary_cache"] = msku


def _build_batch_summary_df(
    result: pd.DataFrame,
    history_actions: list[dict],
    active_batch_id: str,
    history_detail_map: dict[str, dict[str, object]],
) -> pd.DataFrame:
    """當前批次結算明細（業務欄位）；優先使用雲端歷史該 Batch_ID 的紀錄。"""
    uid_to_row = {str(r.get("uid", "")): r for _, r in result.iterrows()}
    bid = (active_batch_id or "").strip()
    batch_actions = (
        [
            a
            for a in history_actions
            if str(a.get("Batch_ID", "")).strip() == bid
        ]
        if bid
        else []
    )
    rows: list[dict[str, str]] = []
    if batch_actions:
        for a in sorted(
            batch_actions, key=lambda x: int(x.get("_row_number", 0) or 0)
        ):
            uid = str(a.get("Order_UID", "") or "")
            r = uid_to_row.get(uid)
            action = str(a.get("Action_Type", "")).strip().lower()
            if action == "write":
                動作 = "寫入雲端"
                平台 = "蝦皮"
                tr = str(a.get("Target_Row", "") or "").strip() or "-"
            else:
                動作 = "略過"
                平台 = "-"
                tr = "-"
            buyer = (
                str(r.get("買家帳號", "") or "-") if r is not None else "-"
            )
            raw_nm = str(a.get("Raw_Name", "") or "").strip()
            if not raw_nm and r is not None:
                raw_nm = str(r.get("合併原始名稱", "") or "")
            rows.append(
                {
                    "處理動作": 動作,
                    "平台": 平台,
                    "買家": buyer,
                    "商品原始名稱": raw_nm or "-",
                    "寫入目標列": tr,
                }
            )
        return pd.DataFrame(rows)

    for pos, idx in enumerate(result.index):
        uid = f"r_{pos}_{idx}"
        row = result.loc[idx]
        row_uid = str(row.get("uid", ""))
        dt = str(st.session_state.get(f"done_type_{uid}", "") or "").strip().lower()
        if dt == "write":
            動作 = "寫入雲端"
            平台 = "蝦皮"
            det = history_detail_map.get(row_uid, {})
            trv = det.get("target_row")
            if isinstance(trv, int) and trv > 0:
                tr = str(trv)
            else:
                tr = "-"
        elif dt == "skip":
            動作 = "略過"
            平台 = "-"
            tr = "-"
        else:
            continue
        rows.append(
            {
                "處理動作": 動作,
                "平台": 平台,
                "買家": str(row.get("買家帳號", "") or "-"),
                "商品原始名稱": str(row.get("合併原始名稱", "") or "-"),
                "寫入目標列": tr,
            }
        )
    return pd.DataFrame(rows)


def main():
    st.set_page_config(page_title="蝦皮訂單預處理", layout="wide")
    st.title("📦 訂單入庫與雲端比對系統")

    with st.sidebar:
        st.subheader("Google Sheets")
        sheet_url, worksheet_name, cred_path, cfg_warn = load_google_sheet_config()
        st.caption("連線設定由專案目錄下的 **appsetting.json** 讀取。")
        st.caption(f"工作表：**{worksheet_name}**")
        if cfg_warn:
            st.warning(cfg_warn)

    st.info(
        "💡 **上傳前置作業**：請先將蝦皮後台下載的 Excel 報表打開，"
        "**另存新檔為 CSV 格式 (逗號分隔)**，再將 CSV 檔拖曳至下方上傳。"
    )
    uploaded = st.file_uploader("上傳 CSV", type=["csv"], key="uploaded_file")

    if uploaded is None:
        st.info("請上傳 CSV 檔案開始處理。")
        return

    # getvalue() 可重複讀取；避免只用 read() 後指標在結尾
    raw_bytes = uploaded.getvalue() if hasattr(uploaded, "getvalue") else uploaded.read()
    csv_name = getattr(uploaded, "name", "unknown.csv")
    csv_fp = _fingerprint_bytes(raw_bytes)

    df: pd.DataFrame | None = None
    for enc in ("utf-8-sig", "utf-8", "big5"):
        try:
            df = pd.read_csv(
                io.BytesIO(raw_bytes),
                encoding=enc,
                dtype=str,
                keep_default_na=False,
            )
            break
        except UnicodeDecodeError:
            continue
        except Exception as e:
            st.error(f"讀取 CSV 失敗：{e}")
            return
    if df is None:
        st.error("CSV 編碼無法辨識，請重新將 Excel 另存為 CSV（UTF-8）後再上傳。")
        return

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        st.error("CSV 欄位格式不符，缺少欄位：" + "、".join(f"`{m}`" for m in missing))
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
    processed_uids: set[str] = set()
    uid_action_map: dict[str, str] = {}
    batch_groups: list[dict] = []
    if not isinstance(st.session_state.get("staged_actions"), list):
        st.session_state["staged_actions"] = []

    can_history = bool(spreadsheet_id and not cloud_error)
    history_detail_map: dict[str, dict[str, object]] = {}
    history_scope_key = f"{spreadsheet_id}|{worksheet_name.strip()}|{cred_path}"
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
            processed_uids = processed_uids_from_actions(history_actions)
            uid_action_map = latest_uid_action_map(history_actions)
            batch_groups = group_batches(history_actions)
            history_detail_map = _latest_history_detail_by_uid(history_actions)
            st.session_state["completed_uids"] = sorted(completed_uids)
            st.session_state["processed_uids"] = sorted(processed_uids)
            gc_deleted = gc_keep_latest_batches(cred_path, spreadsheet_id, keep=5)
            if gc_deleted > 0:
                st.info(f"歷史表已清理舊批次紀錄列 {gc_deleted} 筆（僅保留最近 5 批）。")
                st.session_state["history_cache_dirty"] = True
        except Exception as e:
            st.warning(f"⚠️ 讀取雲端歷史紀錄失敗：{e}")

    with st.sidebar:
        st.divider()
        st.markdown("##### ☁️ 雲端同步")
        if st.button("🔄 從雲端同步最新狀態", key="history_sync_button"):
            st.session_state["force_history_sync"] = True
            st.session_state["force_catalog_sync"] = True
            st.session_state["sku_dictionary_dirty"] = True
            _invalidate_cloud_catalog_cache()
            st.rerun()
        st.divider()
        st.markdown("##### 🛒 批次提交與進度管理")
        staged_actions = _staged_actions()
        st.caption(f"🛒 待提交佇列：共有 {len(staged_actions)} 筆動作等待寫入")
        if st.button("🚀 一鍵同步至雲端", type="primary", key="staged_commit_button"):
            if not staged_actions:
                st.warning("目前沒有待提交動作。")
            elif not can_history or not active_batch_id:
                st.error("目前無法連線雲端歷史表，暫時不能提交。")
            else:
                try:
                    writes = [
                        a for a in staged_actions if str(a.get("action_type", "")).lower() == "write"
                    ]
                    write_ops = [
                        {
                            "sheet_row": int(a.get("target_row", 0) or 0),
                            "header_index_1based": dict(a.get("header_index_1based") or {}),
                            "buyer_account": str(a.get("buyer_account", "") or ""),
                            "sale_price": a.get("sale_price", ""),
                            "fee_value": a.get("fee_value", ""),
                        }
                        for a in writes
                    ]
                    batch_write_order_values_to_sheet_rows(
                        cred_path,
                        spreadsheet_id,
                        worksheet_name.strip(),
                        write_ops,
                    )
                    append_history_actions_batch(
                        cred_path,
                        spreadsheet_id,
                        staged_actions,
                    )
                    dict_entries = [
                        (str(a.get("raw_name", "") or ""), str(a.get("std_keyword", "") or ""))
                        for a in writes
                    ]
                    if dict_entries:
                        batch_learn_dictionary_entries(
                            cred_path,
                            spreadsheet_id,
                            dict_entries,
                        )
                    gc_keep_latest_batches(cred_path, spreadsheet_id, keep=5)
                    _set_staged_actions([])
                    st.session_state["history_cache_dirty"] = True
                    st.session_state["force_history_sync"] = True
                    st.session_state["sku_dictionary_dirty"] = True
                    st.balloons()
                    st.success(f"✅ 已完成批次同步：{len(staged_actions)} 筆。")
                    st.rerun()
                except Exception as e:
                    st.error(f"批次同步失敗：{e}")
        if st.button("🗑️ 捨棄當前進度", key="staged_discard_button"):
            for a in reversed(staged_actions):
                _revert_optimistic_action(a)
            _set_staged_actions([])
            st.session_state["force_catalog_sync"] = True
            st.session_state["force_history_sync"] = True
            st.session_state["sku_dictionary_dirty"] = True
            _invalidate_cloud_catalog_cache()
            st.rerun()

        st.divider()
        with st.expander("🕒 系統歷史與批次回溯", expanded=False):
            latest_hint = _latest_last_hint(history_actions)
            if latest_hint:
                st.caption(f"上次進度：{latest_hint}")
            else:
                st.caption("尚無歷程提示。")
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
                        restored, deleted, raw_batch = rollback_batch(
                            cred_path,
                            spreadsheet_id,
                            worksheet_name.strip(),
                            selected_batch.get("batch_id", ""),
                        )
                        if raw_batch:
                            try:
                                batch_forget_dictionary_entries(
                                    cred_path, spreadsheet_id, raw_batch
                                )
                            except Exception:
                                pass
                            msku = dict(
                                st.session_state.get("sku_dictionary_cache")
                                or {}
                            )
                            for r in raw_batch:
                                msku.pop(str(r).strip(), None)
                            st.session_state["sku_dictionary_cache"] = msku
                        st.session_state["history_cache_dirty"] = True
                        st.session_state["force_catalog_sync"] = True
                        st.session_state["sku_dictionary_dirty"] = True
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

    sku_dict_map: dict[str, str] = {}
    if spreadsheet_id and cred_path and (not cloud_error) and os.path.isfile(cred_path):
        try:
            sku_dict_map = _get_sku_dictionary_map(cred_path, spreadsheet_id)
        except Exception:
            sku_dict_map = {}

    staged_actions = _staged_actions()
    staged_by_uid = _staged_map_by_uid(staged_actions)

    if cloud_df is not None and not cloud_df.empty:
        for pos, (idx, row) in enumerate(result.iterrows()):
            uid = f"r_{pos}_{idx}"
            row_uid = str(row.get("uid", ""))
            staged = staged_by_uid.get(row_uid)
            done_by_history = row_uid in completed_uids
            if staged is not None:
                st.session_state[f"done_{uid}"] = True
                st.session_state[f"done_type_{uid}"] = str(staged.get("action_type", "")).lower()
            else:
                st.session_state[f"done_{uid}"] = done_by_history
                st.session_state[f"done_type_{uid}"] = uid_action_map.get(row_uid, "")
            tag = str(row.get("現貨預定標記", "") or "")
            candidate_df = candidate_pool_for_stock_tag(cloud_df, tag)
            q = str(row.get("清洗後簡體名稱", "") or "")
            raw_nm = str(row.get("合併原始名稱", "") or "").strip()
            review_meta[uid] = {
                "top_options": _build_top_pick_options(
                    raw_nm, q, candidate_df, sku_dict_map
                ),
                "all_options": _build_all_pick_options(
                    candidate_df, raw_nm, sku_dict_map, skip_first=True
                ),
                "row": row,
                "pos": pos,
                "idx": idx,
                "row_uid": row_uid,
            }

    for pos, (idx, row) in enumerate(result.iterrows()):
        uid = f"r_{pos}_{idx}"
        row_uid = str(row.get("uid", ""))
        staged_action = staged_by_uid.get(row_uid)
        is_processed_lock = row_uid in processed_uids and staged_action is None
        done_key = f"done_{uid}"
        done = bool(st.session_state.get(done_key, False))
        done_type = str(st.session_state.get(f"done_type_{uid}", "") or "")
        if show_only_pending and (done or is_processed_lock):
            continue
        buyer = row.get("買家帳號", "")
        tag = row.get("現貨預定標記", "")
        price = row.get("商品原價", "")
        if is_processed_lock:
            done_mark = " [🔒 已於先前批次完成]"
        elif done_type == "skip":
            done_mark = " [⏭ 已略過]"
        elif staged_action is not None:
            done_mark = " [🛒 已暫存]"
        else:
            done_mark = " [✅ 已完成]" if done else ""
        title = f"{buyer} ｜ {tag} ｜ 商品原價：{price}{done_mark}"
        expanded_by_default = (not done) and (not is_processed_lock) and (uid == next_uid)
        with st.expander(title, expanded=expanded_by_default):
            merged_disp = str(row.get("合併原始名稱", "") or "")
            fee_int = int(row.get("單件實扣手續費", 0) or 0)
            row_uid = str(row.get("uid", ""))

            if is_processed_lock:
                st.success("✅ 此訂單已於先前的批次同步完成，系統已自動鎖定保護。")
                st.info(merged_disp)
                st.metric("單件實扣手續費", f"{fee_int}")
                continue

            if staged_action is not None:
                st.info("🛒 這筆已加入待提交佇列，尚未寫入雲端。")
                if str(staged_action.get("action_type", "")).lower() == "skip":
                    st.success("⏭️ 已暫存為略過。")
                else:
                    st.success(f"💾 已暫存寫入：第 {int(staged_action.get('target_row', 0) or 0)} 列。")
                if st.button("↩️ 撤銷暫存", key=f"unstage_{uid}"):
                    acts = _staged_actions()
                    keep: list[dict] = []
                    removed: dict | None = None
                    for a in acts:
                        if str(a.get("order_uid", "") or "") == row_uid and removed is None:
                            removed = a
                            continue
                        keep.append(a)
                    if removed is not None:
                        _revert_optimistic_action(removed)
                    _set_staged_actions(keep)
                    st.session_state[done_key] = False
                    st.session_state[f"done_type_{uid}"] = ""
                    st.rerun()
                continue

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
                                raw_forget = str(
                                    row.get("合併原始名稱", "") or ""
                                ).strip()
                                if spreadsheet_id and raw_forget:
                                    try:
                                        forget_dictionary_by_raw_name(
                                            cred_path,
                                            spreadsheet_id,
                                            raw_forget,
                                        )
                                    except Exception:
                                        pass
                                    msku = dict(
                                        st.session_state.get(
                                            "sku_dictionary_cache"
                                        )
                                        or {}
                                    )
                                    msku.pop(raw_forget, None)
                                    st.session_state["sku_dictionary_cache"] = msku
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
                    acts = _staged_actions()
                    keep: list[dict] = []
                    old: dict | None = None
                    for a in acts:
                        if str(a.get("order_uid", "") or "") == row_uid and old is None:
                            old = a
                            continue
                        keep.append(a)
                    if old is not None:
                        _revert_optimistic_action(old)
                    keep.append(
                        {
                            "batch_id": active_batch_id,
                            "upload_time": active_upload_time,
                            "filename": csv_name,
                            "order_uid": row_uid,
                            "action_type": "skip",
                            "target_row": None,
                            "original_data": None,
                            "last_hint": hint,
                            "raw_name": str(row.get("合併原始名稱", "") or ""),
                        }
                    )
                    _set_staged_actions(keep)
                    st.session_state[done_key] = True
                    st.session_state[f"done_type_{uid}"] = "skip"
                    st.success("已加入待提交佇列（略過）。")
                    st.rerun()
                except Exception as e:
                    st.error(f"暫存略過失敗：{e}")

            if clicked_action and action_kind == "write":
                if not can_history or not active_batch_id:
                    st.error("目前無法連線雲端歷史表，暫時不能寫入。")
                    continue
                if not valid_target or r_target is None:
                    st.error("目前選取列無效，請重新確認行號。")
                    continue
                try:
                    header_map = dict(r_target.get("_header_index_1based", {}) or {})
                    before_vals = {
                        "平台": str(r_target.get("平台", "") or ""),
                        "買家": str(r_target.get("買家", "") or ""),
                        "賣場售價": str(r_target.get("賣場售價", "") or ""),
                        "賣場手續費": str(r_target.get("賣場手續費", "") or ""),
                    }
                    hint = (
                        f"{str(row.get('訂單成立日期','') or '')} 的 "
                        f"{str(row.get('買家帳號','') or '')} "
                        f"(訂單: {str(row.get('訂單編號','') or '')})"
                    )
                    raw_learn = str(row.get("合併原始名稱", "") or "").strip()
                    std_kw = merged_standard_keyword_from_catalog_row(r_target)
                    prev_dict_kw = str((st.session_state.get("sku_dictionary_cache") or {}).get(raw_learn, "") or "")
                    new_action = {
                        "batch_id": active_batch_id,
                        "upload_time": active_upload_time,
                        "filename": csv_name,
                        "order_uid": row_uid,
                        "action_type": "write",
                        "target_row": int(eff_now),
                        "original_data": before_vals,
                        "last_hint": hint,
                        "raw_name": raw_learn,
                        "header_index_1based": header_map,
                        "buyer_account": str(row.get("買家帳號", "") or ""),
                        "sale_price": float(row.get("商品原價", 0) or 0),
                        "fee_value": int(row.get("單件實扣手續費", 0) or 0),
                        "std_keyword": std_kw,
                        "prev_dict_keyword": prev_dict_kw,
                        "before_local": before_vals,
                    }
                    acts = _staged_actions()
                    keep: list[dict] = []
                    old: dict | None = None
                    for a in acts:
                        if str(a.get("order_uid", "") or "") == row_uid and old is None:
                            old = a
                            continue
                        keep.append(a)
                    if old is not None:
                        _revert_optimistic_action(old)
                    keep.append(new_action)
                    _apply_optimistic_action(new_action)
                    _set_staged_actions(keep)
                    st.session_state[done_key] = True
                    st.session_state[f"done_type_{uid}"] = "write"
                    st.success("✅ 已加入待提交佇列（寫入）。")
                    st.rerun()
                except Exception as e:
                    st.error(f"暫存寫入失敗：{e}")

    # 本次完成進度（session 級）
    total_cards = len(result)
    committed_count = sum(
        1
        for _, row in result.iterrows()
        if str(row.get("uid", "") or "") in completed_uids
    )
    staged_count = len(_staged_actions())
    st.progress(0 if total_cards == 0 else committed_count / total_cards)
    st.caption(
        f"本次處理進度：已提交 {committed_count}/{total_cards} 筆"
        + (f"（另有 {staged_count} 筆待提交）" if staged_count else "")
    )

    all_done = total_cards > 0 and committed_count >= total_cards and staged_count == 0
    if all_done:
        celeb_key = f"batch_done_balloons_{csv_fp}_{active_batch_id or 'na'}"
        if not st.session_state.get(celeb_key, False):
            st.balloons()
            st.session_state[celeb_key] = True
        st.success("🎉 本批次所有訂單皆已處理完畢！")
        n_write, n_skip = _session_write_skip_counts(result)
        bid = (active_batch_id or "").strip()
        if bid and history_actions:
            batch_rows = [
                a
                for a in history_actions
                if str(a.get("Batch_ID", "")).strip() == bid
            ]
            if batch_rows:
                n_write = sum(
                    1
                    for a in batch_rows
                    if str(a.get("Action_Type", "")).strip().lower() == "write"
                )
                n_skip = sum(
                    1
                    for a in batch_rows
                    if str(a.get("Action_Type", "")).strip().lower() == "skip"
                )
        mc1, mc2, mc3 = st.columns(3)
        mc1.metric("總訂單數", total_cards)
        mc2.metric("成功寫入", n_write)
        mc3.metric("略過", n_skip)
        summary_df = _build_batch_summary_df(
            result, history_actions, active_batch_id, history_detail_map
        )
        if not summary_df.empty:
            st.markdown("##### 📋 本批次成效明細")
            st.dataframe(
                summary_df,
                hide_index=True,
                width="stretch",
            )
            report_csv = summary_df.to_csv(index=False).encode("utf-8-sig")
            safe_fn = "".join(
                c if c.isalnum() or c in "._-" else "_" for c in (active_batch_id or csv_name)
            )[:72]
            st.download_button(
                label="下載批次結算報告 (CSV)",
                data=report_csv,
                file_name=f"batch_summary_{safe_fn}.csv",
                mime="text/csv",
                key="download_batch_summary_csv",
            )

        if st.button("🔄 處理下一份報表 (清除目前畫面)", type="primary"):
            # Graceful Reset：只清「本批次」相關的 UI/進度狀態，保留雲端目錄與商品字典快取
            keep_keys = {
                "cloud_catalog_df",
                "cloud_catalog_scope",
                "sku_dictionary_cache",
                "sku_dictionary_scope",
                "sku_dictionary_dirty",
                # history cache keys（不是硬性禁止，但通常有助於速度）
                "history_actions_cache",
                "initial_sync_done",
                "initial_sync_scope",
                "completed_uids",
            }

            clear_prefixes = (
                "done_r_",
                "done_type_r_",
                "manual_override_r_",
                "manual_row_r_",
                "show_all_available_r_",
                "show_all_available_mode_r_",
                "cloud_sel_r_",
                "force_write_r_",
                "action_btn_r_",
                "rollback_one_r_",
                "batch_done_balloons_",
                "unstage_r_",
            )
            clear_exact = {
                # 當前批次識別與進度（本次流程結束後要回到乾淨畫面）
                "active_batch_scope",
                "active_batch_id",
                "active_upload_time",
                # 目前上傳檔
                "uploaded_file",
                # 逐筆審核/彙總 UI 的 widget 狀態
                "show_only_pending",
                "reset_done_flags",
                "history_batch_pick",
                "download_batch_summary_csv",
                "staged_actions",
                "staged_commit_button",
                "staged_discard_button",
            }

            for k in list(st.session_state.keys()):
                if k in keep_keys:
                    continue
                if k in clear_exact:
                    st.session_state.pop(k, None)
                    continue
                if k.startswith(clear_prefixes):
                    st.session_state.pop(k, None)

            st.rerun()

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
