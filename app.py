# -*- coding: utf-8 -*-
"""
蝦皮訂單 CSV 預處理與清洗（Streamlit）
階段一：預處理、整數手續費分攤、驗證
階段二：Google Sheets 讀取與模糊比對（thefuzz）
"""

from __future__ import annotations

import codecs
import io
import math
import os

import pandas as pd
import streamlit as st

from app_settings import load_google_sheet_config
from sheets_match import (
    SHEET_FIRST_DATA_ROW_1BASED,
    extract_spreadsheet_id,
    fetch_worksheet_catalog,
    filter_catalog_for_stock_tag,
    format_platform_buyer_status,
    fuzzy_top3_matches,
    get_catalog_row_by_sheet_row,
    row_has_order_like_data,
    sheet_rows_with_duplicate_selection,
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
    """第一項固定為略過；其餘為展開後的模糊推薦列。"""
    opts: list[tuple[str, int | None]] = [("略過不寫入", None)]
    for sheet_row, score, merged in fuzzy_top3_matches(query_simplified, filtered):
        disp = merged.replace("\n", " ")
        if len(disp) > 52:
            disp = disp[:52] + "…"
        opts.append(
            (
                f"第 {sheet_row} 列 · 相似度 {score} · {disp}",
                sheet_row,
            )
        )
    return opts


def _effective_sheet_row_from_state(
    uid: str, options: list[tuple[str, int | None]]
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
    i = int(st.session_state.get(sel_key, 0))
    if i < 0 or i >= len(options):
        return None
    return options[i][1]


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
            cloud_df = load_cloud_catalog_cached(
                CLOUD_SHEET_CACHE_VERSION,
                spreadsheet_id,
                worksheet_name.strip(),
                cred_path,
            )
        except Exception as e:
            cloud_error = str(e)

    if cloud_error:
        st.warning(f"⚠️ Google Sheet：{cloud_error}")

    st.subheader("逐筆審核（雲端模糊比對 · 階段三雛形）")
    if cloud_df is None or cloud_df.empty:
        st.info("載入試算表後，此處將顯示每筆訂單與推薦雲端列、手動行號與寫入按鈕狀態。")

    # 預先建立每筆訂單的下拉選項，並以上一輪互動後的 session_state 偵測「列號衝突」
    review_meta: dict[str, dict] = {}
    uid_order: list[str] = []
    if cloud_df is not None and not cloud_df.empty:
        for pos, (idx, row) in enumerate(result.iterrows()):
            uid = f"r_{pos}_{idx}"
            uid_order.append(uid)
            tag = str(row.get("現貨預定標記", "") or "")
            filtered = filter_catalog_for_stock_tag(cloud_df, tag)
            q = str(row.get("清洗後簡體名稱", "") or "")
            review_meta[uid] = {
                "options": _build_sheet_pick_options(q, filtered),
                "row": row,
                "pos": pos,
                "idx": idx,
            }

    effective_prev = {
        uid: _effective_sheet_row_from_state(uid, review_meta[uid]["options"])
        for uid in uid_order
    }

    for pos, (idx, row) in enumerate(result.iterrows()):
        uid = f"r_{pos}_{idx}"
        buyer = row.get("買家帳號", "")
        tag = row.get("現貨預定標記", "")
        price = row.get("商品原價", "")
        title = f"{buyer} ｜ {tag} ｜ 商品原價：{price}"
        with st.expander(title, expanded=False):
            st.write("**合併原始名稱：**", row.get("合併原始名稱", ""))
            st.write("**單件實扣手續費：**", int(row.get("單件實扣手續費", 0)))

            if cloud_df is None or cloud_df.empty or uid not in review_meta:
                st.caption("（尚未載入雲端資料或工作表為空）")
                continue

            options = review_meta[uid]["options"]

            manual = st.checkbox(
                "手動指定 Google Sheet 行號",
                key=f"manual_override_{uid}",
            )

            if manual:
                st.number_input(
                    "請輸入行號",
                    min_value=1,
                    step=1,
                    value=SHEET_FIRST_DATA_ROW_1BASED,
                    key=f"manual_row_{uid}",
                )
                mr = int(st.session_state.get(f"manual_row_{uid}", SHEET_FIRST_DATA_ROW_1BASED))
                r_preview = get_catalog_row_by_sheet_row(cloud_df, mr)
                st.caption(
                    "目前此列狀態："
                    + format_platform_buyer_status(r_preview)
                )
            else:
                labels = [o[0] for o in options]
                st.selectbox(
                    "選擇雲端對應列（預設略過不寫入）",
                    range(len(labels)),
                    format_func=lambda i: labels[i],
                    key=f"cloud_sel_{uid}",
                )

            eff_now = _effective_sheet_row_from_state(uid, options)
            eff_merged = dict(effective_prev)
            eff_merged[uid] = eff_now
            bad_for_btn = sheet_rows_with_duplicate_selection(eff_merged)
            conflict_now = eff_now is not None and eff_now in bad_for_btn

            if conflict_now:
                st.error(
                    f"🚨 嚴重警告：本次作業中有其他訂單也選擇了此【第 {eff_now} 行】，"
                    "將導致重複寫入！請更改行號。"
                )

            r_target = (
                get_catalog_row_by_sheet_row(cloud_df, eff_now)
                if eff_now is not None
                else None
            )
            if eff_now is not None and r_target is None:
                st.warning(
                    "找不到試算表中此列號（請確認為資料區，且列號存在）。"
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
            can_write = bool(valid_target and not conflict_now and force_ok)

            st.button(
                "確認寫入",
                key=f"confirm_write_{uid}",
                disabled=not can_write,
                help=None
                if can_write
                else (
                    "略過、列號衝突、找不到該列、或需勾選強制覆蓋時無法寫入"
                ),
            )

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
