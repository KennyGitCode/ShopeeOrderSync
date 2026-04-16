# -*- coding: utf-8 -*-
"""
蝦皮訂單 CSV 預處理與清洗（Streamlit）
階段一：預處理、整數手續費分攤、驗證
階段二：Google Sheets 讀取與模糊比對（thefuzz）
"""

from __future__ import annotations

import hashlib
import html
import io
import json
import math
import os
import re
import time
from datetime import date
from datetime import datetime
from datetime import timedelta

import msoffcrypto
import pandas as pd
import streamlit as st
import zhconv
try:
    import plotly.graph_objects as go
except Exception:
    go = None

from app_settings import (
    load_history_keep_batches,
    load_google_sheet_config,
    load_google_sheet_profiles,
    load_profit_compare_settings,
    load_report_password_default,
    save_active_google_sheet_profile,
    save_profit_compare_settings,
)
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
    locate_header_column_index,
    read_max_date_by_column_index,
    row_has_order_like_data,
    batch_write_order_values_to_sheet_rows,
    open_gspread_client,
)
from text_normalize import normalize_for_match
from battle_report import (
    list_report_archive_json_paths,
    render_battle_report,
    save_to_archive,
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

LOCAL_DRAFT_DIRNAME = ".cache"
LOCAL_STAGED_DRAFT_FILENAME = "staged_actions_draft.json"
LOCAL_REVIEW_RESUME_FILENAME = "review_resume_cache.json"
DEFAULT_REPORT_SNAPSHOT_WORKSHEET = "Batch_Sync_Logs"

REPORT_SNAPSHOT_HEADERS = [
    "同步時間",
    "同步年月",
    "本次寫入筆數",
    "總實收",
    "總平台手續費",
    "總成本",
    "總利潤",
    "平均手續費率",
]
COST_COLUMN_WHITELIST = ["成本", "單價成本", "unit_cost", "cost", "original_cost"]

def clean_name_for_simplified(text: str) -> str:
    """與 `text_normalize.normalize_for_match` 相同，供階段一產出 `清洗後簡體名稱`。"""
    return normalize_for_match(text)


def normalize_item_name(raw_name: str) -> str:
    """顯示/寫入用：僅收斂空白，盡量保留原始文字。"""
    s = str(raw_name or "")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_item_name_key(raw_name: str) -> str:
    """UID/比對用：去 emoji + 小寫，確保鍵值穩定。"""
    s = normalize_item_name(raw_name)
    s = re.sub(
        r"[\U0001F000-\U0001FAFF\U00002700-\U000027BF\U000024C2-\U0001F251]",
        "",
        s,
    )
    return s.lower()


def generate_order_uid(order_no: str, buyer: str, normalized_name: str, part: int) -> str:
    key = (
        str(order_no or "").strip()
        + "||"
        + str(buyer or "").strip()
        + "||"
        + str(normalized_name or "").strip()
        + "||"
        + str(int(part))
    )
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:20]


def classify_stock_type(product_name: str, option_name: str) -> str:
    """優先依「商品選項名稱逗號後片段」判定；避免被固定文案誤導。"""
    def _judge_text(text: str) -> str | None:
        s = str(text or "").strip()
        if not s:
            return None
        if ("預定" in s) or ("預購" in s):
            return "預定"
        if "現貨" in s:
            return "現貨"
        return None

    opt = "" if option_name is None else str(option_name).strip()
    if opt:
        # 先看逗號（半形/全形）後的買家實際選項片段
        opt_norm = opt.replace("，", ",")
        tail = opt_norm.split(",")[-1].strip() if "," in opt_norm else ""
        picked = _judge_text(tail)
        if picked:
            return picked
        # 其次才看整段選項
        picked = _judge_text(opt)
        if picked:
            return picked

    # 若選項無法判定，才退回商品名稱；先移除常見固定文案，避免誤判。
    name = "" if product_name is None else str(product_name).strip()
    fixed_noise = [
        "台灣現貨＋預定",
        "台灣現貨+預定",
        "現貨＋預定",
        "現貨+預定",
    ]
    for t in fixed_noise:
        name = name.replace(t, "")
    picked = _judge_text(name)
    if picked:
        return picked
    return "未知"


def expected_platform_for_stock_tag(stock_tag: str) -> str:
    """
    由左欄「來源／庫存標籤」推導右欄「候選平台」顯示字串。
    徽章配色嚴格使用固定映射（來源：預定/現貨；候選：空白/預現貨）。
    """
    t = str(stock_tag or "").strip()
    if t == "現貨":
        return "預現貨"
    if t == "預定":
        return "空白"
    return "不限"


def _paired_source_platform_badge_colors(
    stock_tag: str,
) -> tuple[tuple[str, str], tuple[str, str]]:
    """左右列徽章固定成對配色 (bg, fg)。"""
    return _left_tag_badge_colors(stock_tag), _right_platform_badge_colors(
        expected_platform_for_stock_tag(stock_tag)
    )


def _tag_badge_html(text: str, bg: str, fg: str = "#111827") -> str:
    t = str(text or "").strip()
    return (
        f"<span style='display:inline-block;padding:2px 10px;border-radius:999px;"
        f"background:{bg};color:{fg};font-size:12px;font-weight:700;'>{t}</span>"
    )


def _name_panel_html(title: str, content: str, *, min_height_px: int = 88) -> str:
    c = html.escape(str(content or "").replace("\n", " ").strip() or "（無資料）")
    t = html.escape(str(title or "").strip())
    return (
        "<div style='border:none;border-radius:12px;padding:11px 13px;"
        f"min-height:{int(min_height_px)}px;background:#f8fafc;"
        "box-shadow:0 1px 2px rgba(15,23,42,0.06);'>"
        f"<div style='font-size:12px;color:#6b7280;margin-bottom:6px;'>{t}</div>"
        f"<div style='font-size:17px;line-height:1.45;color:#0f172a;font-weight:600;'>{c}</div>"
        "</div>"
    )


def _label_with_hint_html(text: str) -> str:
    s = str(text or "").strip()
    if "（" in s and s.endswith("）"):
        main, rest = s.split("（", 1)
        hint = rest[:-1]
        return (
            f"<span style='font-weight:700;color:#344054;'>{html.escape(main.strip())}</span>"
            f"<span style='color:#98A2B3;'>（{html.escape(hint.strip())}）</span>"
        )
    return f"<span style='font-weight:700;color:#344054;'>{html.escape(s)}</span>"


# 財務欄位統一底：減少大面積色塊，正負面改以字色與字重表現
_METRIC_SURFACE = "#ffffff"
_METRIC_BORDER = "rgba(15, 23, 42, 0.07)"


def _metric_block_html(
    title: str,
    value: str,
    *,
    tone: str = "#101828",
    bg: str | None = None,
    border: str | None = None,
    value_size: str = "md",
    soft: bool = True,
    html_class: str | None = None,
) -> str:
    """
    財務數字區塊：預設白底 + 淡邊線；強調依賴字色（tone），避免整塊填色。
    value_size: md / lg / hero（利潤用 hero 強調）
    """
    sizes = {"md": "2.15rem", "lg": "2.38rem", "hero": "2.72rem"}
    fs = sizes.get(value_size, sizes["md"])
    surf = _METRIC_SURFACE if bg is None else bg
    border_style = (
        f"border:1px solid {_METRIC_BORDER};"
        if border is None
        else f"border:1px solid {border};"
    )
    shadow = (
        "box-shadow:0 1px 2px rgba(15, 23, 42, 0.04);"
        if soft
        else ""
    )
    cls = f' class="{html.escape(html_class)}"' if html_class else ""
    return (
        f"<div{cls} style='{border_style}border-radius:12px;padding:10px 12px;background:{surf};{shadow}'>"
        f"<div style='font-size:0.86rem;line-height:1.3;'>{_label_with_hint_html(title)}</div>"
        f"<div style='font-size:{fs};font-weight:800;line-height:1.08;color:{tone};margin-top:6px;letter-spacing:-0.02em;'>"
        f"{html.escape(str(value or '—'))}</div>"
        "</div>"
    )


def _min_selling_price_for_target_profit_pct(
    cost: float,
    fee_rate: float,
    target_profit_pct: float,
) -> float | None:
    """
    假設手續費與售價成固定比例 fee_rate（本筆實扣/實收），
    目標利潤率 target_profit_pct 定義為「利潤 / 實售收入（實收-手續費）」。
    設 P=實收、k=fee_rate、t=target，
    利潤率 = (P-P*k-cost)/(P-P*k) = 1 - cost/(P*(1-k)) >= t
    得 P >= cost / ((1-k)*(1-t))。
    若分母 <= 0 則無解。
    """
    t = float(target_profit_pct) / 100.0
    k = float(fee_rate)
    den = (1.0 - k) * (1.0 - t)
    if den <= 1e-9 or cost <= 0:
        return None
    return float(cost) / den


def _profit_alert_card(
    kind: str,
    title_line: str,
    subtitle: str,
) -> str:
    """精簡 Alert：左側色條 + 白底（紅色僅作警示條與標題字色，不大面積鋪色）。"""
    styles = {
        "danger": ("#fecaca", "#b91c1c", "#ffffff", "#991b1b"),
        "warning": ("#fcd34d", "#d97706", "#ffffff", "#92400e"),
        "success": ("#6ee7b7", "#047857", "#ffffff", "#065f46"),
        "neutral": ("#e5e7eb", "#64748b", "#ffffff", "#475569"),
    }
    _, bar, bg, title_c = styles.get(kind, styles["neutral"])
    sub_esc = html.escape(subtitle)
    title_esc = html.escape(title_line)
    return (
        "<div class=\"shopee-fin-compare-panel\" style='margin-top:0;padding:0;border-radius:12px;overflow:hidden;"
        f"background:{bg};border:1px solid #e5e7eb;box-shadow:0 1px 2px rgba(15,23,42,0.04);'>"
        "<div style='display:flex;min-height:100%;'>"
        f"<div style='width:4px;flex-shrink:0;background:{bar};'></div>"
        "<div style='padding:10px 12px 10px 12px;flex:1;'>"
        f"<div style='font-size:1.02rem;font-weight:900;color:{title_c};line-height:1.35;'>{title_esc}</div>"
        f"<div style='font-size:0.76rem;color:#98A2B3;margin-top:6px;line-height:1.45;'>{sub_esc}</div>"
        "</div></div></div>"
    )


def _profit_analysis_panel(
    kind: str,
    title_line: str,
    subtitle: str,
    footer_html: str = "",
) -> str:
    """
    負利潤時：警示 + 建議售價整合為單一面板，較連貫且易與左欄實售收入對齊。
    """
    styles = {
        "danger": ("#b91c1c", "#991b1b"),
        "warning": ("#d97706", "#92400e"),
        "success": ("#047857", "#065f46"),
        "neutral": ("#64748b", "#475569"),
    }
    bar, title_c = styles.get(kind, styles["neutral"])
    sub_esc = html.escape(subtitle)
    title_esc = html.escape(title_line)
    top = (
        "<div style='display:flex;min-height:100%;'>"
        f"<div style='width:4px;flex-shrink:0;background:{bar};'></div>"
        "<div style='padding:12px 14px 12px 12px;flex:1;'>"
        f"<div style='font-size:0.88rem;font-weight:800;color:{title_c};line-height:1.4;'>{title_esc}</div>"
        f"<div style='font-size:0.76rem;color:#98A2B3;margin-top:6px;line-height:1.45;'>{sub_esc}</div>"
        "</div></div>"
    )
    foot = (
        f"<div style='border-top:1px solid #f1f5f9;padding:11px 14px 12px 16px;background:#fafafa;'>{footer_html}</div>"
        if footer_html
        else ""
    )
    return (
        "<div class=\"shopee-fin-compare-panel\" style='margin-top:0;border-radius:12px;border:1px solid #e5e7eb;background:#ffffff;"
        "box-shadow:0 1px 3px rgba(15,23,42,0.06);overflow:hidden;'>"
        f"{top}{foot}</div>"
    )


def _suggested_price_footer_html(p_sug: int, hint_title: str) -> str:
    """負利潤時建議售價：標題與金額分行，金額加大且支援長數字換行。"""
    amt = f"{int(p_sug):,}"
    label = "💡 " + "建議實收售價（台幣）"
    line2 = f"至少 {amt} 元"
    return (
        "<div style='display:flex;flex-direction:column;gap:10px;align-items:stretch;"
        "width:100%;max-width:100%;box-sizing:border-box;'>"
        "<div style='display:flex;align-items:center;justify-content:flex-start;"
        "gap:6px;flex-wrap:nowrap;'>"
        "<span style='font-size:0.82rem;font-weight:800;color:#0f766e;line-height:1.35;'>"
        f"{html.escape(label)}</span>"
        "<span style='display:inline-flex;align-items:center;justify-content:center;"
        "flex-shrink:0;min-width:1.2rem;height:1.2rem;border-radius:999px;"
        "border:1px solid #d1d5db;background:#ffffff;color:#64748b;"
        "font-size:0.72rem;font-weight:900;cursor:help;line-height:1;' "
        f'title="{hint_title}">?</span>'
        "</div>"
        "<div style='font-size:clamp(1.35rem, 4vw, 1.85rem);font-weight:900;"
        "line-height:1.25;color:#047857;letter-spacing:-0.03em;"
        "font-variant-numeric: tabular-nums;"
        "word-break:break-word;overflow-wrap:anywhere;padding:6px 8px 6px 0;"
        "border-top:1px dashed #d1fae5;margin-top:2px;'>"
        f"{html.escape(line2)}"
        "</div>"
        "</div>"
    )


def _amount_gap_hint_html(shopee_twd: int, est_twd: int, amount_gap_pct: float) -> str:
    """金額差距提示：沿用系統卡片語彙（白底+左色條），減少突兀感。"""
    gap_abs = abs(int(shopee_twd) - int(est_twd))
    title = "⚠️ 金額差距偏大"
    detail = (
        f"實收 {int(shopee_twd):,} / 成本 {int(est_twd):,} / "
        f"差距 {int(gap_abs):,}（{float(amount_gap_pct):.1f}%）"
    )
    return (
        "<div style='margin-top:2px;border:1px solid #e5e7eb;border-radius:10px;"
        "background:#ffffff;box-shadow:0 1px 2px rgba(15,23,42,0.04);overflow:hidden;'>"
        "<div style='display:flex;'>"
        "<div style='width:4px;flex-shrink:0;background:#f59e0b;'></div>"
        "<div style='padding:8px 10px;flex:1;'>"
        f"<div style='font-size:0.80rem;font-weight:800;color:#92400e;line-height:1.3;'>{html.escape(title)}</div>"
        f"<div style='font-size:0.75rem;color:#6b7280;line-height:1.4;margin-top:4px;'>{html.escape(detail)}</div>"
        "</div></div></div>"
    )


def _extract_cost_from_cloud_row(row: pd.Series | None) -> float | None:
    """從雲端列以白名單+模糊匹配抓取成本值。"""
    if row is None:
        return None
    keys = [str(k or "") for k in row.index.tolist()]
    key_norm = {k: k.lower().replace(" ", "").replace("_", "") for k in keys}
    wl_norm = [w.lower().replace(" ", "").replace("_", "") for w in COST_COLUMN_WHITELIST]
    for k in keys:
        kn = key_norm.get(k, "")
        if any((w in kn) or (kn in w) for w in wl_norm):
            v = _money_to_float(row.get(k, ""))
            if v is not None and v > 0:
                return float(v)
    return None


def _left_tag_badge_colors(stock_tag: str) -> tuple[str, str]:
    """來源判定固定映射：預定→Color A，現貨→Color B，其餘→Error/Default。"""
    color_map = {
        "預定": ("#1e3a8a", "#ffffff"),  # Color A
        "現貨": ("#047857", "#ffffff"),  # Color B
    }
    return color_map.get(str(stock_tag or "").strip(), ("#ef4444", "#ffffff"))


def _right_platform_badge_colors(expected_platform: str) -> tuple[str, str]:
    """候選平台固定映射：空白→Color A，預現貨→Color B，其餘→Error/Default。"""
    color_map = {
        "空白": ("#1e3a8a", "#ffffff"),   # Color A
        "預現貨": ("#047857", "#ffffff"),  # Color B
    }
    return color_map.get(str(expected_platform or "").strip(), ("#ef4444", "#ffffff"))


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


def _assign_deterministic_uids(df_exp: pd.DataFrame) -> pd.DataFrame:
    out = df_exp.reset_index(drop=True).copy()
    name_series = out["合併原始名稱"].fillna("").astype(str).map(normalize_item_name_key)
    key_series = (
        out["訂單編號"].fillna("").astype(str).str.strip()
        + "||"
        + out["買家帳號"].fillna("").astype(str).str.strip()
        + "||"
        + name_series
    )
    part = key_series.groupby(key_series, sort=False).cumcount()
    out["uid"] = [
        generate_order_uid(order_no, buyer, nm, int(p))
        for order_no, buyer, nm, p in zip(
            out["訂單編號"].fillna("").astype(str),
            out["買家帳號"].fillna("").astype(str),
            name_series,
            part,
        )
    ]
    return out


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

    # 合併原始名稱：保留可讀原文（僅收斂空白），避免過度簡化
    gn = df_exp["商品名稱"].fillna("").astype(str)
    go = df_exp["商品選項名稱"].fillna("").astype(str)
    merged = (gn + " " + go).str.replace(r"\s+", " ", regex=True).str.strip()
    df_exp["合併原始名稱"] = merged.map(normalize_item_name)

    # 現貨/預定/未知：使用「原始」商品名稱與選項（展開後仍與來源列相同）
    df_exp["現貨預定標記"] = [
        classify_stock_type(p, o)
        for p, o in zip(df_exp["商品名稱"], df_exp["商品選項名稱"])
    ]

    df_exp["清洗後簡體名稱"] = df_exp["合併原始名稱"].apply(clean_name_for_simplified)
    # 確定性 UID：同訂單/買家/商品名稱在不同批次重上傳仍得到相同識別
    df_exp = _assign_deterministic_uids(df_exp)

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


def _with_occupied_prefix(label: str, row: pd.Series) -> str:
    """若該列已有資料，於標籤前加上佔用提示。"""
    if _row_is_occupied_for_ui(row):
        return f"[⚠️ 已佔用] {label}"
    return label


def _row_is_occupied_for_ui(row: pd.Series | None) -> bool:
    """
    UI 端「已佔用」判斷：與覆蓋警告一致。
    - 買家非空 -> 佔用
    - 平台非空且不是「預現貨」-> 佔用
    - 或既有的訂單欄位（買家/售價/手續費）有值 -> 佔用
    """
    if row is None:
        return False
    buyer_raw = str(row.get("買家", "") or "").strip()
    plat_raw = str(row.get("平台", "") or "").strip()
    return bool(buyer_raw) or (bool(plat_raw) and plat_raw != "預現貨") or row_has_order_like_data(row)


def _catalog_full_name(row: pd.Series | None) -> str:
    if row is None:
        return ""
    pn = zhconv.convert(
        str(row.get("品名", "") or "").replace("\n", " ").strip(),
        "zh-tw",
    )
    xi = zhconv.convert(
        str(row.get("款式細項", "") or "").replace("\n", " ").strip(),
        "zh-tw",
    )
    return f"{pn} {xi}".strip()


def _has_useful_candidate_data(row: pd.Series | None) -> bool:
    """
    展開所有候選列時的最小資料門檻：
    - 具備可辨識名稱（檔名/品名/款式/商品名稱）之一，或
    - 具備有效售價（> 0）
    以避免顯示「第 N 列 -」這類幾乎全空白列。
    """
    if row is None:
        return False
    name_cols = ("檔名", "品名", "款式細項", "商品名稱")
    for c in name_cols:
        if str(row.get(c, "") or "").strip():
            return True
    price_cols = ("賣場售價", "售價", "價格")
    for c in price_cols:
        v = _money_to_int(row.get(c, ""))
        if isinstance(v, int) and v > 0:
            return True
    return False


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
    """展開模式：略過後先顯示已佔用列，再顯示其餘列（字典命中仍保留）。"""
    opts: list[tuple[str, int | None]] = []
    raw_key = (raw_name or "").strip()
    hit_rows: set[int] = set()
    occupied_labels: list[tuple[str, int]] = []
    normal_labels: list[tuple[str, int]] = []
    if raw_key in dict_map:
        kw = dict_map[raw_key]
        for sr, row in _dict_hits_in_candidate_pool(candidate_df, kw):
            if not _has_useful_candidate_data(row):
                continue
            hit_rows.add(sr)
            lb = _with_occupied_prefix(_format_row_label_dict_hit(row), row)
            if _row_is_occupied_for_ui(row):
                occupied_labels.append((lb, sr))
            else:
                normal_labels.append((lb, sr))
    other_occupied: list[tuple[str, int]] = []
    other_normal: list[tuple[str, int]] = []
    if skip_first:
        opts.append(("略過不寫入", None))
    for _, row in candidate_df.iterrows():
        if not _has_useful_candidate_data(row):
            continue
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
        base_label = f"第 {sheet_row} 列 - {pn} {xi}".strip()
        label = _with_occupied_prefix(base_label, row)
        if _row_is_occupied_for_ui(row):
            other_occupied.append((label, sheet_row))
        else:
            other_normal.append((label, sheet_row))
    # 佔用列永遠靠前，便於人工辨識風險；其次才是可安全寫入列。
    opts.extend(occupied_labels)
    opts.extend(other_occupied)
    opts.extend(normal_labels)
    opts.extend(other_normal)
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


def _smart_default_pick_index(
    options: list[tuple[str, int | None]],
    blocked_rows: set[int],
    occupied_rows: set[int],
) -> int | None:
    """
    預設先選未重複列；options 既有順序已把字典命中放前面。
    """
    if not options:
        return None
    for i, (_, sr) in enumerate(options):
        if (
            isinstance(sr, int)
            and sr > 0
            and sr not in blocked_rows
            and sr not in occupied_rows
        ):
            return i
    for i, (_, sr) in enumerate(options):
        if isinstance(sr, int) and sr > 0 and sr not in blocked_rows:
            return i
    return 0


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


def _latest_write_upload_time_by_row(actions: list[dict]) -> dict[int, str]:
    """每個 Target_Row 對應最新 write 的 Upload_Time。"""
    out: dict[int, tuple[int, str]] = {}
    for a in actions:
        if str(a.get("Action_Type", "")).strip().lower() != "write":
            continue
        rn = int(a.get("_row_number", 0) or 0)
        tr_raw = str(a.get("Target_Row", "") or "").strip()
        if not tr_raw:
            continue
        try:
            tr = int(tr_raw)
        except ValueError:
            continue
        up = str(a.get("Upload_Time", "") or "").strip()
        if tr not in out or rn > out[tr][0]:
            out[tr] = (rn, up)
    return {k: v[1] for k, v in out.items()}


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


def _build_report_snapshot_row_from_actions(
    write_actions: list[dict],
    executed_at: datetime | None = None,
    *,
    cloud_df: pd.DataFrame | None = None,
) -> list[object]:
    now = executed_at or datetime.now()
    ts = now.strftime("%Y-%m-%d %H:%M:%S")
    ym = now.strftime("%Y-%m")

    dfw = pd.DataFrame(write_actions or [])
    print(f"[BatchSyncLogs] write_actions columns: {list(dfw.columns)}")
    total_count = int(len(dfw))

    def _pick_col(candidates: list[str]) -> str | None:
        for c in candidates:
            if c in dfw.columns:
                return c
        return None

    col_rev = _pick_col(["sale_price", "實售收入", "revenue", "net_sales", "商品原價"])
    col_fee = _pick_col(["fee_value", "est_fee", "platform_fee", "單件實扣手續費", "賣場手續費"])
    col_profit = _pick_col(["est_profit_twd", "profit", "最終利潤", "淨利潤"])
    col_cost = _pick_col(["unit_cost", "original_cost", "單件成本", "原始成本", "cost"])

    rev_s = pd.to_numeric(dfw[col_rev], errors="coerce").fillna(0.0) if col_rev else pd.Series([0.0] * total_count)
    fee_s = pd.to_numeric(dfw[col_fee], errors="coerce").fillna(0.0) if col_fee else pd.Series([0.0] * total_count)
    profit_s = pd.to_numeric(dfw[col_profit], errors="coerce").fillna(0.0) if col_profit else pd.Series([0.0] * total_count)
    if col_cost:
        cost_s = pd.to_numeric(dfw[col_cost], errors="coerce")
    else:
        cost_s = pd.Series([float("nan")] * total_count)

    # 若本地 action 沒成本，依 target_row 直接抓雲端該列單件成本（與填寫流程一致）。
    if cloud_df is not None and not cloud_df.empty and "target_row" in dfw.columns:
        for i, tr0 in enumerate(dfw["target_row"].tolist()):
            if i >= len(cost_s):
                break
            if pd.notna(cost_s.iloc[i]) and float(cost_s.iloc[i]) > 0:
                continue
            try:
                tr = int(tr0 or 0)
            except Exception:
                tr = 0
            if tr <= 0:
                continue
            r_cloud = get_catalog_row_by_sheet_row(cloud_df, tr)
            if r_cloud is None:
                continue
            c0 = _extract_cost_from_cloud_row(r_cloud)
            if c0 is not None and c0 > 0:
                cost_s.iloc[i] = float(c0)

    fallback_s = rev_s - fee_s - profit_s
    cost_s = cost_s.where(cost_s.notna(), fallback_s).fillna(0.0)

    total_revenue = float(rev_s.sum())
    total_fee = float(fee_s.sum())
    total_profit = float(profit_s.sum())
    total_cost = float(cost_s.sum())

    avg_fee_rate = (total_fee / total_revenue) if total_revenue > 0 else 0.0

    return [
        ts,
        ym,
        total_count,
        int(round(total_revenue)),
        int(round(total_fee)),
        int(round(total_cost)),
        int(round(total_profit)),
        round(avg_fee_rate, 6),
    ]


def _append_report_snapshot_row(
    cred_path: str,
    spreadsheet_id: str,
    worksheet_name: str,
    row_values: list[object],
) -> None:
    gc = open_gspread_client(cred_path)
    sh = gc.open_by_key(spreadsheet_id)
    ws = None
    try:
        ws = sh.worksheet(worksheet_name)
    except Exception:
        ws = sh.add_worksheet(
            title=worksheet_name,
            rows=2000,
            cols=max(20, len(REPORT_SNAPSHOT_HEADERS) + 2),
        )
    hdr = ws.row_values(1)
    need_header = (not hdr) or [str(x).strip() for x in hdr[: len(REPORT_SNAPSHOT_HEADERS)]] != REPORT_SNAPSHOT_HEADERS
    if need_header:
        ws.update(
            "A1",
            [REPORT_SNAPSHOT_HEADERS],
            value_input_option="USER_ENTERED",
        )
    ws.append_row([str(v) if v is not None else "" for v in row_values], value_input_option="USER_ENTERED")


def _build_final_report_snapshot(
    write_actions: list[dict],
    *,
    cloud_df: pd.DataFrame,
    batch_id: str,
    csv_fp: str,
    csv_name: str,
) -> dict[str, object]:
    """
    Golden Record：同步當下即固定本批次完整對帳資料。
    規則：成本優先取單筆已確認快照，其次回讀雲端，最後才用預估利潤率估算。
    """
    if cloud_df is None or cloud_df.empty:
        raise ValueError("無法建立戰報快照：雲端目錄未載入。")
    try:
        target_profit_pct = float(st.session_state.get("amount_tolerance_pct", 25.0) or 25.0) / 100.0
    except Exception:
        target_profit_pct = 0.25
    target_profit_pct = max(0.0, min(1.0, target_profit_pct))
    rows: list[dict[str, object]] = []
    errs: list[str] = []
    for a in write_actions:
        try:
            tr = int(a.get("target_row", 0) or 0)
        except Exception:
            tr = 0
        if tr <= 0:
            errs.append(f"UID={str(a.get('order_uid', '') or '').strip() or 'unknown'}：target_row 無效")
            continue
        r_cloud = get_catalog_row_by_sheet_row(cloud_df, tr)
        sale = float(a.get("sale_price", 0) or 0)
        fee = float(a.get("fee_value", 0) or 0)
        local_cost = float(a.get("selected_cost_snapshot", 0) or 0)
        if local_cost <= 0:
            local_cost = float(a.get("unit_cost", 0) or 0)
        cloud_cost = _extract_cost_from_cloud_row(r_cloud)
        cloud_cost_num = float(cloud_cost or 0)
        if local_cost > 0:
            cost = local_cost
            cost_src = "✅ 本地快照"
        elif cloud_cost_num > 0:
            cost = cloud_cost_num
            cost_src = "✅ 雲端回讀"
        else:
            est_cost = float(sale - fee - (sale * target_profit_pct))
            if est_cost > 0:
                cost = est_cost
                cost_src = "⚠️ 系統估算"
            else:
                errs.append(f"ROW {tr}：成本缺失且估算失敗")
                continue
        profit = sale - fee - float(cost)
        fee_rate = (fee / sale) if sale > 0 else 0.0
        rows.append(
            {
                "商品原始名稱": str(a.get("raw_name", "") or "").strip() or f"ROW:{tr}",
                "實售收入": sale,
                "成本": float(cost),
                "成本來源": cost_src,
                "手續費": fee,
                "淨利潤": profit,
                "手續費率": fee_rate,
                "目標列": tr,
            }
        )
    if errs:
        raise ValueError("同步前預檢未通過：\n- " + "\n- ".join(errs[:8]))
    if not rows and write_actions:
        raise ValueError("同步前預檢未通過：沒有可用的寫入明細。")
    df = pd.DataFrame(rows)
    total_rev = int(round(float(df["實售收入"].sum())))
    total_fee = int(round(float(df["手續費"].sum())))
    total_cost = int(round(float(df["成本"].sum())))
    total_profit = int(round(float(df["淨利潤"].sum())))
    avg_fee_rate = (float(total_fee) / float(total_rev)) if total_rev > 0 else 0.0
    return {
        "batch_id": str(batch_id or ""),
        "csv_fp": str(csv_fp or ""),
        "csv_name": str(csv_name or ""),
        "writes_count": int(len(write_actions)),
        "total_revenue": total_rev,
        "total_fee": total_fee,
        "total_cost": total_cost,
        "total_profit": total_profit,
        "avg_fee_rate": avg_fee_rate,
        "audit_rows": df.to_dict(orient="records"),
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def _build_batch_battle_report(
    write_actions: list[dict],
    *,
    cloud_df: pd.DataFrame | None = None,
) -> dict[str, object]:
    """由本次 write actions 產生同步成功頁的戰報資料。"""
    dfw = pd.DataFrame(write_actions or [])
    print(f"[BattleReport] write_actions columns: {list(dfw.columns)}")

    def _pick_col(candidates: list[str]) -> str | None:
        for c in candidates:
            if c in dfw.columns:
                return c
        return None

    col_name = _pick_col(["raw_name", "商品原始名稱", "商品名稱", "合併原始名稱", "name"])
    col_rev = _pick_col(["sale_price", "實售收入", "revenue", "net_sales", "商品原價"])
    col_fee = _pick_col(["fee_value", "est_fee", "platform_fee", "單件實扣手續費", "賣場手續費"])
    col_profit = _pick_col(["est_profit_twd", "profit", "最終利潤", "淨利潤"])
    col_cost = _pick_col(["unit_cost", "original_cost", "單件成本", "原始成本", "cost"])

    def _clean_num_series(src: pd.Series | object, size: int) -> pd.Series:
        if isinstance(src, pd.Series):
            s = src.copy()
        else:
            s = pd.Series([src] * size)
        # 強制清洗：去逗號 + 空值補零 + 非數字字元移除
        s = (
            s.astype(str)
            .str.replace(",", "", regex=False)
            .str.replace(r"[^\d\.\-]", "", regex=True)
            .replace("", "0")
            .fillna("0")
        )
        return pd.to_numeric(s, errors="coerce").fillna(0.0)

    if dfw.empty:
        rows_df = pd.DataFrame(columns=["商品原始名稱", "實售收入", "成本", "手續費", "淨利潤", "手續費率"])
    else:
        names = (
            dfw[col_name].fillna("").astype(str).str.strip()
            if col_name
            else pd.Series([""] * len(dfw))
        )
        names = names.where(names != "", dfw.get("order_uid", "").astype(str).str.slice(0, 8).radd("UID:"))
        rev_s = _clean_num_series(dfw[col_rev], len(dfw)) if col_rev else pd.Series([0.0] * len(dfw))
        fee_s = _clean_num_series(dfw[col_fee], len(dfw)) if col_fee else pd.Series([0.0] * len(dfw))
        local_cost_s = _clean_num_series(dfw[col_cost], len(dfw)) if col_cost else pd.Series([0.0] * len(dfw))
        cost_s = pd.Series([float("nan")] * len(dfw))
        cost_source_s = pd.Series(["⚠️ 系統估算"] * len(dfw))

        # 核心修正：優先用 target_row 回抓雲端該列成本（與填寫流程一致）。
        if cloud_df is not None and not cloud_df.empty and "target_row" in dfw.columns:
            for i, tr0 in enumerate(dfw["target_row"].tolist()):
                if i >= len(cost_s):
                    break
                if pd.notna(cost_s.iloc[i]) and float(cost_s.iloc[i] or 0) > 0:
                    continue
                try:
                    tr = int(tr0 or 0)
                except Exception:
                    tr = 0
                if tr <= 0:
                    continue
                r_cloud = get_catalog_row_by_sheet_row(cloud_df, tr)
                if r_cloud is None:
                    continue
                c0 = _extract_cost_from_cloud_row(r_cloud)
                if c0 is not None and c0 > 0:
                    cost_s.iloc[i] = float(c0)
                    cost_source_s.iloc[i] = "✅ 雲端回讀"

        # 次選：action 本身已帶成本（仍歸類為系統估算，避免誤標雲端回讀）。
        cost_s = cost_s.where(cost_s.notna(), local_cost_s.where(local_cost_s > 0, pd.NA))

        # 最後：雲端仍取不到才估算。
        try:
            target_profit_pct = float(st.session_state.get("amount_tolerance_pct", 25.0) or 25.0) / 100.0
        except Exception:
            target_profit_pct = 0.25
        target_profit_pct = max(0.0, min(1.0, target_profit_pct))
        fallback_cost_s = (rev_s - fee_s - (rev_s * target_profit_pct)).fillna(0.0)
        cost_s = cost_s.where(cost_s > 0, fallback_cost_s).fillna(0.0)
        fee_rate_s = (fee_s / rev_s.where(rev_s != 0, pd.NA)).fillna(0.0)
        profit_s = (rev_s - fee_s - cost_s).fillna(0.0)
        rows_df = pd.DataFrame(
            {
                "商品原始名稱": names,
                "實售收入": rev_s,
                "成本": cost_s,
                "成本來源": cost_source_s,
                "手續費": fee_s,
                "淨利潤": profit_s,
                "手續費率": fee_rate_s,
            }
        )

    total_revenue = float(rows_df["實售收入"].sum()) if not rows_df.empty else 0.0
    total_fee = float(rows_df["手續費"].sum()) if not rows_df.empty else 0.0
    total_profit = float(rows_df["淨利潤"].sum()) if not rows_df.empty else 0.0
    total_cost = float(rows_df["成本"].sum()) if not rows_df.empty else 0.0
    avg_fee_rate = (total_fee / total_revenue) if total_revenue > 0 else 0.0
    top_mvp_src = rows_df.sort_values(by="淨利潤", ascending=False).head(3) if not rows_df.empty else rows_df
    top_fee_src = rows_df.sort_values(by="手續費率", ascending=False).head(3) if not rows_df.empty else rows_df
    top_mvp_rows_out = [
        {"商品原始名稱": str(r.get("商品原始名稱", "") or ""), "利潤": int(round(float(r.get("淨利潤", 0) or 0)))}
        for _, r in top_mvp_src.iterrows()
    ]
    top_fee_rows_out = [
        {"商品原始名稱": str(r.get("商品原始名稱", "") or ""), "手續費率": f"{round(float(r.get('手續費率', 0) or 0) * 100.0, 2):.2f}%"}
        for _, r in top_fee_src.iterrows()
    ]
    return {
        "writes_count": int(len(write_actions)),
        "total_revenue": int(round(total_revenue)),
        "total_fee": int(round(total_fee)),
        "total_cost": int(round(total_cost)),
        "avg_fee_rate": float(avg_fee_rate),
        "total_profit": int(round(total_profit)),
        "top_mvp_rows": top_mvp_rows_out,
        "top_fee_rows": top_fee_rows_out,
        "detail_rows": rows_df.to_dict(orient="records"),
    }


def _staged_actions() -> list[dict]:
    cur = st.session_state.get("staged_actions")
    if isinstance(cur, list):
        return list(cur)
    return []


def _set_staged_actions(actions: list[dict]) -> None:
    st.session_state["staged_actions"] = list(actions)
    scope = str(st.session_state.get("staged_draft_scope", "") or "").strip()
    if scope:
        _save_local_staged_draft(scope, list(actions))


def _merge_staged_dictionary_entries(
    base_map: dict[str, str], staged_actions: list[dict]
) -> dict[str, str]:
    """
    將「未同步但已暫存」的字典學習併入當前字典映射。
    避免重整頁面後，暫存中的字典命中提示消失。
    """
    out = dict(base_map or {})
    for a in staged_actions:
        if str(a.get("action_type", "") or "").strip().lower() != "write":
            continue
        raw = str(a.get("raw_name", "") or "").strip()
        kw = str(a.get("std_keyword", "") or "").strip()
        if raw and kw:
            out[raw] = kw
    return out


def _dedupe_staged_actions_by_uid(actions: list[dict]) -> list[dict]:
    """同一 Order_UID 只保留最後一筆動作，避免暫存計數膨脹。"""
    out_rev: list[dict] = []
    seen: set[str] = set()
    for a in reversed(list(actions or [])):
        uid = str(a.get("order_uid", "") or "").strip()
        if not uid:
            continue
        if uid in seen:
            continue
        seen.add(uid)
        out_rev.append(a)
    out_rev.reverse()
    return out_rev


def _local_staged_draft_path() -> str:
    draft_dir = os.path.join(os.path.dirname(__file__), LOCAL_DRAFT_DIRNAME)
    os.makedirs(draft_dir, exist_ok=True)
    return os.path.join(draft_dir, LOCAL_STAGED_DRAFT_FILENAME)


def _read_all_local_staged_drafts() -> dict[str, list[dict]]:
    p = _local_staged_draft_path()
    if not os.path.isfile(p):
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            obj = json.load(f)
    except Exception:
        return {}
    if not isinstance(obj, dict):
        return {}
    out: dict[str, list[dict]] = {}
    for k, v in obj.items():
        key = str(k or "").strip()
        if not key:
            continue
        if isinstance(v, list):
            out[key] = [x for x in v if isinstance(x, dict)]
    return out


def _write_all_local_staged_drafts(drafts: dict[str, list[dict]]) -> None:
    p = _local_staged_draft_path()
    with open(p, "w", encoding="utf-8") as f:
        json.dump(drafts, f, ensure_ascii=False, indent=2)


def _load_local_staged_draft(scope: str) -> list[dict]:
    key = str(scope or "").strip()
    if not key:
        return []
    return list(_read_all_local_staged_drafts().get(key, []))


def _save_local_staged_draft(scope: str, actions: list[dict]) -> None:
    key = str(scope or "").strip()
    if not key:
        return
    drafts = _read_all_local_staged_drafts()
    if actions:
        drafts[key] = [x for x in actions if isinstance(x, dict)]
    else:
        drafts.pop(key, None)
    _write_all_local_staged_drafts(drafts)


def _clear_local_staged_draft(scope: str) -> None:
    _save_local_staged_draft(scope, [])


def _clear_all_local_staged_drafts() -> None:
    """清空 .cache/staged_actions_draft.json 的所有 scope 暫存。"""
    _write_all_local_staged_drafts({})


def _local_review_resume_path() -> str:
    draft_dir = os.path.join(os.path.dirname(__file__), LOCAL_DRAFT_DIRNAME)
    os.makedirs(draft_dir, exist_ok=True)
    return os.path.join(draft_dir, LOCAL_REVIEW_RESUME_FILENAME)


def _read_all_local_review_resumes() -> dict[str, dict]:
    p = _local_review_resume_path()
    if not os.path.isfile(p):
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            obj = json.load(f)
    except Exception:
        return {}
    if not isinstance(obj, dict):
        return {}
    out: dict[str, dict] = {}
    for k, v in obj.items():
        key = str(k or "").strip()
        if (not key) or (not isinstance(v, dict)):
            continue
        out[key] = dict(v)
    return out


def _write_all_local_review_resumes(data: dict[str, dict]) -> None:
    p = _local_review_resume_path()
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _load_local_review_resume(scope: str) -> dict:
    key = str(scope or "").strip()
    if not key:
        return {}
    return dict(_read_all_local_review_resumes().get(key, {}))


def _save_local_review_resume(scope: str, payload: dict) -> None:
    key = str(scope or "").strip()
    if not key:
        return
    rows = payload.get("result_rows", [])
    if not isinstance(rows, list):
        rows = []
    data = _read_all_local_review_resumes()
    data[key] = {
        "updated_at": _now_text(),
        "csv_name": str(payload.get("csv_name", "") or ""),
        "csv_fp": str(payload.get("csv_fp", "") or ""),
        "raw_file_total_count": int(payload.get("raw_file_total_count", 0) or 0),
        "watermark_filtered_count": int(payload.get("watermark_filtered_count", 0) or 0),
        "effective_new_order_count": int(payload.get("effective_new_order_count", 0) or 0),
        "duplicate_filter_notice": str(payload.get("duplicate_filter_notice", "") or ""),
        "date_gap_notice": str(payload.get("date_gap_notice", "") or ""),
        "validation_issues": list(payload.get("validation_issues", []) or []),
        "result_rows": [r for r in rows if isinstance(r, dict)],
    }
    _write_all_local_review_resumes(data)


def _clear_local_review_resume(scope: str) -> None:
    key = str(scope or "").strip()
    if not key:
        return
    data = _read_all_local_review_resumes()
    data.pop(key, None)
    _write_all_local_review_resumes(data)


def _clear_all_local_review_resumes() -> None:
    _write_all_local_review_resumes({})


def _parse_draft_scope(scope: str) -> tuple[str, str, str, str, str] | None:
    txt = str(scope or "").strip()
    parts = txt.rsplit("|", 4)
    if len(parts) != 5:
        return None
    return parts[0], parts[1], parts[2], parts[3], parts[4]


def _pick_latest_resume_scope(
    staged_drafts: dict[str, list[dict]],
    resume_cache: dict[str, dict],
    spreadsheet_id: str,
    worksheet_name: str,
    cred_path: str,
) -> str:
    target_sheet = str(spreadsheet_id or "").strip()
    target_ws = str(worksheet_name or "").strip()
    target_cred = str(cred_path or "").strip()
    best_scope = ""
    best_ts = ""
    for scope, actions in staged_drafts.items():
        if not isinstance(actions, list) or len(actions) <= 0:
            continue
        parsed = _parse_draft_scope(scope)
        if parsed is None:
            continue
        _, _, scope_sheet, scope_ws, scope_cred = parsed
        if (
            str(scope_sheet).strip() != target_sheet
            or str(scope_ws).strip() != target_ws
            or str(scope_cred).strip() != target_cred
        ):
            continue
        payload = resume_cache.get(scope, {})
        ts = str(payload.get("updated_at", "") or "")
        if (not best_scope) or (ts >= best_ts):
            best_scope = scope
            best_ts = ts
    return best_scope


def _retry_call(fn, *args, retries: int = 3, delay_sec: float = 0.7, **kwargs):
    last_err: Exception | None = None
    attempts = max(1, int(retries))
    for i in range(attempts):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_err = e
            if i >= attempts - 1:
                break
            time.sleep(delay_sec)
    if last_err is not None:
        raise last_err
    raise RuntimeError("retry_call failed without exception")


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
            before_local = dict(action.get("before_local") or {})
            _mutate_local_catalog_row(
                tr,
                {
                    # 未同步前保留原平台值，避免候選池被提前縮減。
                    # 重複防呆仍由買家/價格/手續費與 blocked_rows 處理。
                    "平台": str(before_local.get("平台", "") or ""),
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


def _money_to_int(value: object) -> int | None:
    try:
        if value is None:
            return None
        s = str(value).strip()
        if not s:
            return None
        return int(round(float(s.replace(",", ""))))
    except Exception:
        return None


def _money_to_float(value: object) -> float | None:
    try:
        if value is None:
            return None
        s = str(value).strip()
        if not s:
            return None
        s = s.replace(",", "")
        m = re.search(r"-?\d+(?:\.\d+)?", s)
        if not m:
            return None
        return float(m.group(0))
    except Exception:
        return None


def _read_uploaded_report_dataframe(
    raw_bytes: bytes,
    filename: str,
    *,
    password: str,
) -> pd.DataFrame:
    ext = os.path.splitext(str(filename or ""))[1].lower()
    if ext == ".xlsx":
        office = msoffcrypto.OfficeFile(io.BytesIO(raw_bytes))
        office.load_key(password=password or "")
        decrypted_file = io.BytesIO()
        office.decrypt(decrypted_file)
        decrypted_file.seek(0)
        return pd.read_excel(
            decrypted_file,
            engine="openpyxl",
            dtype=str,
        ).fillna("")
    for enc in ("utf-8-sig", "utf-8", "big5"):
        try:
            return pd.read_csv(
                io.BytesIO(raw_bytes),
                encoding=enc,
                dtype=str,
                keep_default_na=False,
            )
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("utf-8", b"", 0, 1, "檔案編碼無法辨識")


def _to_date_text(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    s = (
        raw.replace("／", "/")
        .replace("-", "/")
        .replace(".", "/")
        .replace("年", "/")
        .replace("月", "/")
        .replace("日", "")
        .strip()
    )
    # 常見雲端格式：只填月/日（例如 8/3）→ 以今年補齊
    md = re.fullmatch(r"(\d{1,2})/(\d{1,2})", s)
    if md:
        try:
            y = datetime.now().year
            m = int(md.group(1))
            d = int(md.group(2))
            return datetime(y, m, d).strftime("%Y-%m-%d")
        except Exception:
            pass
    # Excel serial day（例如 45231 / 45231.0）
    if re.fullmatch(r"\d+(?:\.\d+)?", s):
        try:
            n = int(float(s))
            if 20000 <= n <= 80000:
                dt0 = pd.to_datetime(n, unit="D", origin="1899-12-30", errors="coerce")
                if pd.notna(dt0):
                    return dt0.strftime("%Y-%m-%d")
        except Exception:
            pass
    try:
        return pd.to_datetime(raw, errors="coerce").strftime("%Y-%m-%d")
    except Exception:
        return ""


def _sheet_open_date_text(row: pd.Series | None) -> str:
    """優先回傳雲端列上的開單日期（yyyy-mm-dd）；無可用值則回傳（未知）。"""
    if row is None:
        return "（未知）"
    candidates = [
        "開單日期",
        "訂單成立日期",
        "填單日期",
        "填單時間",
        "建立日期",
        "建立時間",
        "日期",
    ]
    for key in candidates:
        raw = str(row.get(key, "") or "").strip()
        dt = _to_date_text(raw)
        if dt:
            return dt
    # 有些工作表把日期放在無標題欄（例如 A 欄 header 為空）
    for key in ("", " ", "Unnamed: 0"):
        raw = str(row.get(key, "") or "").strip()
        dt = _to_date_text(raw)
        if dt:
            return dt
    for key in row.index:
        k = str(key or "").strip()
        if ("日期" not in k) and ("時間" not in k):
            continue
        raw = str(row.get(key, "") or "").strip()
        dt = _to_date_text(raw)
        if dt:
            return dt
    return "（未知）"


def _earliest_order_date_in_report(df: pd.DataFrame) -> date | None:
    """從本次上傳報表取最早訂單成立日。"""
    if df is None or df.empty:
        return None
    best: date | None = None
    for _, row in df.iterrows():
        dt_txt = _to_date_text(row.get("訂單成立日期"))
        if not dt_txt:
            continue
        try:
            d = datetime.strptime(dt_txt, "%Y-%m-%d").date()
        except Exception:
            continue
        if best is None or d < best:
            best = d
    return best


def _dict_fingerprint(m: dict[str, str]) -> str:
    if not m:
        return ""
    pairs = [f"{k}=>{v}" for k, v in sorted((str(k), str(v)) for k, v in m.items())]
    src = "\n".join(pairs)
    return hashlib.sha256(src.encode("utf-8")).hexdigest()[:16]


def _clear_review_runtime_state() -> None:
    """切換 profile 時，清除本次審核/上傳狀態，避免不同表單交叉污染。"""
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
        "confirm_rollback_one_r_",
        "batch_done_balloons_",
        "unstage_r_",
    )
    clear_exact = {
        "active_batch_scope",
        "active_batch_id",
        "active_upload_time",
        "uploaded_file",
        "show_all_rows",
        "history_batch_pick",
        "download_batch_summary_csv",
        "staged_actions",
        "baseline_completed_scope",
        "baseline_completed_uids",
        "local_synced_scope",
        "local_synced_action_map",
        "staged_draft_scope",
        "staged_draft_loaded_scope",
        "preferred_resume_scope",
        "review_meta_scope",
        "review_meta_cache",
    }
    for k in list(st.session_state.keys()):
        if k in clear_exact or k.startswith(clear_prefixes):
            st.session_state.pop(k, None)


def _profile_display_name(profile_name: str) -> str:
    p = str(profile_name or "").strip().lower()
    if p in {"prod", "production"}:
        return "正式"
    if p in {"test", "testing"}:
        return "測試"
    return str(profile_name or "")


def _get_spreadsheet_title(service_account_path: str, spreadsheet_id: str) -> str:
    """讀取試算表文件名稱；失敗時回空字串。"""
    if not service_account_path or not spreadsheet_id or (not os.path.isfile(service_account_path)):
        return ""
    cache_key = f"{service_account_path}|{spreadsheet_id}"
    if st.session_state.get("spreadsheet_title_scope") == cache_key:
        return str(st.session_state.get("spreadsheet_title_cache", "") or "")
    try:
        gc = open_gspread_client(service_account_path)
        title = str(gc.open_by_key(spreadsheet_id).title or "").strip()
    except Exception:
        title = ""
    st.session_state["spreadsheet_title_scope"] = cache_key
    st.session_state["spreadsheet_title_cache"] = title
    return title


def _show_user_error(message: str, err: Exception | None = None) -> None:
    """顯示給一般使用者的錯誤訊息，技術細節收在可展開區塊。"""
    st.error(message)
    if err is not None:
        with st.expander("查看技術細節（提供工程師排查）", expanded=False):
            st.code(str(err))


def main():
    st.set_page_config(page_title="蝦皮訂單預處理", layout="wide")
    st.markdown(
        """
<style>
/* 主要 CTA：翠綠漸層，傳達「安全、建議執行」，與警示紅區隔 */
div[data-testid="stButton"] button[kind="primary"] {
  background: linear-gradient(180deg, #059669 0%, #047857 100%) !important;
  color: #ffffff !important;
  border: 1px solid #065f46 !important;
  font-weight: 600 !important;
  box-shadow: 0 1px 2px rgba(4, 120, 87, 0.22);
  transition: transform 0.08s ease, box-shadow 0.12s ease, filter 0.12s ease, border-color 0.12s ease !important;
}
div[data-testid="stButton"] button[kind="primary"]:hover:not(:disabled) {
  background: linear-gradient(180deg, #047857 0%, #065f46 100%) !important;
  border-color: #064e3b !important;
  filter: brightness(0.97);
  box-shadow: 0 2px 6px rgba(4, 120, 87, 0.28);
}
div[data-testid="stButton"] button[kind="primary"]:active:not(:disabled) {
  transform: scale(0.97);
  box-shadow: 0 1px 2px rgba(4, 120, 87, 0.18);
}
div[data-testid="stButton"] button[kind="primary"]:disabled {
  background: #e5e7eb !important;
  color: #9ca3af !important;
  border-color: #d1d5db !important;
  box-shadow: none !important;
}

/* Row: net-income metric card + profit panel equal height (both use marker classes) */
div[data-testid="stHorizontalBlock"]:has(.shopee-fin-metric-card):has(.shopee-fin-compare-panel) {
  align-items: stretch !important;
  gap: 0.65rem;
}
div[data-testid="stHorizontalBlock"]:has(.shopee-fin-metric-card):has(.shopee-fin-compare-panel) > div[data-testid="column"] {
  display: flex !important;
  flex-direction: column !important;
}
div[data-testid="stHorizontalBlock"]:has(.shopee-fin-metric-card):has(.shopee-fin-compare-panel) > div[data-testid="column"] > div {
  flex: 1 1 auto !important;
  min-height: 0 !important;
}
div[data-testid="stHorizontalBlock"]:has(.shopee-fin-metric-card):has(.shopee-fin-compare-panel) [data-testid="stMarkdownContainer"] {
  height: 100%;
}
div[data-testid="stHorizontalBlock"]:has(.shopee-fin-metric-card):has(.shopee-fin-compare-panel) [data-testid="stMarkdownContainer"] > p {
  height: 100%;
  margin: 0;
}
div[data-testid="stHorizontalBlock"]:has(.shopee-fin-metric-card):has(.shopee-fin-compare-panel) .shopee-fin-metric-card,
div[data-testid="stHorizontalBlock"]:has(.shopee-fin-metric-card):has(.shopee-fin-compare-panel) .shopee-fin-compare-panel {
  height: 100%;
  min-height: 6.85rem;
  box-sizing: border-box;
}
div[data-testid="stHorizontalBlock"]:has(.shopee-fin-metric-card):has(.shopee-fin-compare-panel) .shopee-fin-metric-card {
  display: flex;
  flex-direction: column;
  justify-content: center;
}
div[data-testid="stHorizontalBlock"]:has(.shopee-fin-metric-card):has(.shopee-fin-compare-panel) .shopee-fin-compare-panel {
  display: flex;
  flex-direction: column;
}
div[data-testid="stHorizontalBlock"]:has(.shopee-fin-metric-card):has(.shopee-fin-compare-panel) .shopee-fin-compare-panel > div:first-child {
  flex: 1 1 auto;
}

/* Selectbox hover border */
div[data-testid="stSelectbox"] [data-baseweb="select"] > div {
  transition: border-color 0.15s ease, box-shadow 0.15s ease !important;
}
div[data-testid="stSelectbox"]:hover [data-baseweb="select"] > div {
  border-color: #059669 !important;
  box-shadow: 0 0 0 1px rgba(5, 150, 105, 0.22) !important;
}

/* Checkbox hover */
div[data-testid="stCheckbox"] label {
  transition: color 0.12s ease, background-color 0.12s ease, border-color 0.12s ease !important;
  border-radius: 6px;
}
div[data-testid="stCheckbox"]:hover label {
  color: #065f46 !important;
}
div[data-testid="stCheckbox"]:hover label span[style] {
  border-color: #059669 !important;
}
</style>
""",
        unsafe_allow_html=True,
    )
    st.title("📦 訂單入庫與雲端比對系統")

    with st.sidebar:
        app_main_menu = st.radio(
            "主功能",
            options=["order_flow", "report_history"],
            format_func=lambda v: ("📦 訂單同步" if v == "order_flow" else "📜 歷史紀錄"),
            key="app_main_menu",
        )
        st.divider()
        profile_cfg, active_profile, cfg_warn = load_google_sheet_profiles()
        profile_names = list(profile_cfg.keys())
        default_idx = profile_names.index(active_profile) if active_profile in profile_names else 0
        selected_profile = st.selectbox(
            "環境",
            options=profile_names,
            index=default_idx,
            key="sheet_profile",
            format_func=_profile_display_name,
        )
        last_profile = str(st.session_state.get("sheet_profile_runtime", selected_profile))
        if last_profile != selected_profile:
            _clear_review_runtime_state()
            save_err = save_active_google_sheet_profile(selected_profile)
            if save_err:
                st.warning(f"⚠️ 無法儲存預設環境：{save_err}")
        st.session_state["sheet_profile_runtime"] = selected_profile
        sheet_url, worksheet_name, cred_path, cfg_warn2 = load_google_sheet_config(selected_profile)
        if cfg_warn2 and not cfg_warn:
            cfg_warn = cfg_warn2
        sheet_id_for_caption = extract_spreadsheet_id(sheet_url) or ""
        sheet_title_for_caption = _get_spreadsheet_title(cred_path, sheet_id_for_caption)
        with st.expander("🔗 雲端資料庫狀態", expanded=False):
            st.caption(f"目前環境：**{_profile_display_name(selected_profile)}**")
            if sheet_title_for_caption:
                st.caption(f"試算表：**{sheet_title_for_caption}**")
            elif sheet_id_for_caption:
                st.caption(f"試算表代碼：`{sheet_id_for_caption}`")
            st.caption(f"工作表：**{worksheet_name}**")
        if cfg_warn:
            st.warning(cfg_warn)

    if app_main_menu == "report_history":
        st.subheader("📜 歷史戰報")
        paths = list_report_archive_json_paths()
        if not paths:
            st.info(
                "目前 `reports_archive/` 內尚無任何已存檔的戰報 JSON。"
                "完成一次含「寫入」的同步後，會自動儲存至該資料夾。"
            )
            return
        labels = [os.path.basename(p) for p in paths]
        picked = st.multiselect(
            "選擇歷史報表（可多選）",
            options=list(range(len(paths))),
            format_func=lambda i: labels[i],
            key="history_archive_report_pick",
            default=[0] if labels else [],
        )
        if not picked:
            st.info("請至少選擇一份歷史報表。")
            return
        try:
            loaded_reports: list[dict[str, object]] = []
            merged_rows: list[dict[str, object]] = []
            for idx in picked:
                path = paths[int(idx)]
                with open(path, "r", encoding="utf-8") as f:
                    historic = json.load(f)
                if not isinstance(historic, dict):
                    st.warning(f"略過格式異常檔案：`{os.path.basename(path)}`")
                    continue
                historic = dict(historic)
                if int(historic.get("writes_count", 0) or 0) <= 0:
                    st.warning(f"略過無寫入筆數檔案：`{os.path.basename(path)}`")
                    continue
                historic["_archive_basename"] = os.path.splitext(os.path.basename(path))[0]
                loaded_reports.append(historic)
                rows = historic.get("audit_rows") or []
                if isinstance(rows, list):
                    merged_rows.extend([r for r in rows if isinstance(r, dict)])

            if not loaded_reports:
                st.error("選取的歷史檔案皆無法顯示戰報。")
                return

            # 單檔：維持原本呈現；多檔：合併明細後重算總額，交給同一 renderer。
            if len(loaded_reports) == 1:
                single = loaded_reports[0]
                single["_widget_key_scope"] = str(single.get("_archive_basename", "history_single"))
                render_battle_report(single)
            else:
                combined_df = pd.DataFrame(merged_rows)
                if not combined_df.empty:
                    for c in ["實售收入", "手續費", "成本", "淨利潤"]:
                        if c in combined_df.columns:
                            combined_df[c] = pd.to_numeric(combined_df[c], errors="coerce").fillna(0.0)
                    total_revenue = int(round(float(combined_df["實售收入"].sum()))) if "實售收入" in combined_df.columns else 0
                    total_fee = int(round(float(combined_df["手續費"].sum()))) if "手續費" in combined_df.columns else 0
                    total_cost = int(round(float(combined_df["成本"].sum()))) if "成本" in combined_df.columns else 0
                    total_profit = int(round(float(combined_df["淨利潤"].sum()))) if "淨利潤" in combined_df.columns else 0
                else:
                    total_revenue = sum(int(r.get("total_revenue", 0) or 0) for r in loaded_reports)
                    total_fee = sum(int(r.get("total_fee", 0) or 0) for r in loaded_reports)
                    total_cost = sum(int(r.get("total_cost", 0) or 0) for r in loaded_reports)
                    total_profit = sum(int(r.get("total_profit", 0) or 0) for r in loaded_reports)
                avg_fee_rate = (float(total_fee) / float(total_revenue)) if total_revenue > 0 else 0.0
                combined_report_data = {
                    "batch_id": f"combined_{len(loaded_reports)}",
                    "csv_fp": "history_combined",
                    "csv_name": f"合併報表_{len(loaded_reports)}批次",
                    "writes_count": int(sum(int(r.get("writes_count", 0) or 0) for r in loaded_reports)),
                    "total_revenue": int(total_revenue),
                    "total_fee": int(total_fee),
                    "total_cost": int(total_cost),
                    "total_profit": int(total_profit),
                    "avg_fee_rate": float(avg_fee_rate),
                    "audit_rows": combined_df.to_dict(orient="records") if not combined_df.empty else [],
                    "_widget_key_scope": f"history_combined_{len(loaded_reports)}_{len(merged_rows)}",
                }
                st.markdown(f"### 📊 合併報表 (共 {len(loaded_reports)} 批次)")
                render_battle_report(combined_report_data)
        except Exception as e:
            _show_user_error("無法讀取歷史戰報。", e)
        return

    spreadsheet_id = extract_spreadsheet_id(sheet_url)
    hwm_col_idx: int | None = None
    hwm_error: str | None = None
    if spreadsheet_id and os.path.isfile(cred_path):
        try:
            hwm_col_idx = locate_header_column_index(
                cred_path,
                spreadsheet_id,
                worksheet_name.strip(),
                "訂單完成日期",
            )
            if hwm_col_idx is None:
                hwm_error = "❌ 找不到雲端欄位「訂單完成日期」（掃描 Rows 2:3 失敗），請檢查工作表標題。"
        except Exception as e:
            hwm_error = f"❌ 無法掃描雲端標題欄位：{e}"
    elif spreadsheet_id:
        hwm_error = "❌ 找不到服務帳戶憑證，無法讀取雲端標題水位線。"

    hwm_default_date = date(2026, 1, 1)
    hwm_date = hwm_default_date
    if hwm_col_idx is not None:
        try:
            d0 = read_max_date_by_column_index(
                cred_path,
                spreadsheet_id,
                worksheet_name.strip(),
                hwm_col_idx,
            )
            if d0 is not None:
                hwm_date = d0
        except Exception as e:
            hwm_error = f"❌ 無法讀取雲端最新進度日期：{e}"
    suggest_start_dt = hwm_date + timedelta(days=1)
    watermark_line1 = f"📊 雲端最新進度：**{hwm_date.strftime('%Y-%m-%d')}**"
    suggest_line = f"💡 下次建議下載區間：自 **{suggest_start_dt.strftime('%Y-%m-%d')}** 起"
    local_drafts_all = _read_all_local_staged_drafts()
    local_resume_cache = _read_all_local_review_resumes()
    local_staging_count = sum(len(v or []) for v in local_drafts_all.values())
    preferred_resume_scope = str(st.session_state.get("preferred_resume_scope", "") or "").strip()
    if preferred_resume_scope:
        parsed_pref = _parse_draft_scope(preferred_resume_scope)
        if parsed_pref is None:
            preferred_resume_scope = ""
        else:
            _, _, pref_sheet, pref_ws, pref_cred = parsed_pref
            if (
                str(pref_sheet).strip() != str(spreadsheet_id or "").strip()
                or str(pref_ws).strip() != str(worksheet_name.strip())
                or str(pref_cred).strip() != str(cred_path or "").strip()
            ):
                preferred_resume_scope = ""
    if not preferred_resume_scope:
        preferred_resume_scope = _pick_latest_resume_scope(
            local_drafts_all,
            local_resume_cache,
            spreadsheet_id,
            worksheet_name.strip(),
            cred_path,
        )
        if preferred_resume_scope:
            st.session_state["preferred_resume_scope"] = preferred_resume_scope
    resume_payload = (
        _load_local_review_resume(preferred_resume_scope) if preferred_resume_scope else {}
    )
    has_resume_rows = bool(isinstance(resume_payload.get("result_rows"), list) and resume_payload.get("result_rows"))
    if (not has_resume_rows) and preferred_resume_scope:
        staged_only = list(local_drafts_all.get(preferred_resume_scope, []) or [])
        fallback_rows: list[dict[str, object]] = []
        for i, a in enumerate(staged_only):
            if not isinstance(a, dict):
                continue
            fallback_rows.append(
                {
                    "uid": str(a.get("order_uid", "") or f"resume_{i}"),
                    "訂單成立日期": str(a.get("order_created_at", "") or ""),
                    "買家帳號": str(a.get("buyer_account", "") or ""),
                    "商品原價": float(a.get("sale_price", 0) or 0),
                    "單件實扣手續費": int(a.get("fee_value", 0) or 0),
                    "合併原始名稱": str(a.get("raw_name", "") or ""),
                    "清洗後簡體名稱": clean_name_for_simplified(str(a.get("raw_name", "") or "")),
                    "現貨預定標記": str(a.get("stock_tag", "") or ""),
                    "單件成本": float(a.get("selected_cost_snapshot", a.get("unit_cost", 0)) or 0),
                    "原始成本": float(a.get("original_cost", 0) or 0),
                }
            )
        if fallback_rows:
            resume_payload = {
                "csv_name": "resume_from_staged_actions",
                "csv_fp": "resume_from_staged_actions",
                "raw_file_total_count": len(fallback_rows),
                "watermark_filtered_count": 0,
                "effective_new_order_count": len(fallback_rows),
                "duplicate_filter_notice": "",
                "date_gap_notice": "",
                "validation_issues": [],
                "result_rows": fallback_rows,
            }
            has_resume_rows = True
    resume_ready = bool(preferred_resume_scope and has_resume_rows)
    uploaded_exists = st.session_state.get("uploaded_file") is not None
    collapsed_in_review = uploaded_exists or resume_ready
    csv_name = ""
    csv_fp = ""
    result = pd.DataFrame()
    raw_file_total_count = 0
    watermark_filtered_count = 0
    duplicate_filter_notice = ""
    date_gap_notice = ""
    effective_new_order_count = 0
    validation_issues: list[dict] = []
    with st.expander("對帳三步曲", expanded=not collapsed_in_review):
        if hwm_error:
            st.error(hwm_error)
        if local_staging_count > 0:
            st.warning(
                f"⚠️ 偵測到本地尚有 {local_staging_count} 筆訂單正在核對中，"
                "請優先完成同步或清除暫存。"
            )
        info_lines = [
            "**📍 第一步：前往蝦皮匯出報表**",
            f"* {watermark_line1}",
        ]
        if local_staging_count <= 0:
            info_lines.append(f"* {suggest_line}")
        info_lines.append("* 操作提示：請在蝦皮後台將訂單狀態選為「已完成」，匯出建議起點之後的報表。")
        st.info(
            "\n".join(info_lines)
        )
        st.markdown("**🔐 第二步：輸入報表解鎖密碼**")
        pw_default = load_report_password_default() or "請在此處填寫使用者的預設密碼"
        shopee_password = st.text_input(
            "蝦皮報表解鎖密碼 (預設手機末六碼)",
            type="password",
            value=pw_default,
            key="shopee_password",
        )
        if resume_ready:
            resumed_csv_name, resumed_csv_fp, _, _, _ = _parse_draft_scope(preferred_resume_scope) or ("", "", "", "", "")
            csv_name = str(resume_payload.get("csv_name", resumed_csv_name) or resumed_csv_name or "resume_restored")
            csv_fp = str(resume_payload.get("csv_fp", resumed_csv_fp) or resumed_csv_fp or "resume_restored")
            result_rows = resume_payload.get("result_rows", [])
            result = pd.DataFrame(result_rows if isinstance(result_rows, list) else [])
            raw_file_total_count = int(resume_payload.get("raw_file_total_count", len(result)) or len(result))
            watermark_filtered_count = int(resume_payload.get("watermark_filtered_count", 0) or 0)
            effective_new_order_count = int(resume_payload.get("effective_new_order_count", len(result)) or len(result))
            duplicate_filter_notice = str(resume_payload.get("duplicate_filter_notice", "") or "")
            date_gap_notice = str(resume_payload.get("date_gap_notice", "") or "")
            raw_issues = resume_payload.get("validation_issues", [])
            validation_issues = [x for x in raw_issues if isinstance(x, dict)]
            st.info("📦 偵測到未完成的任務，正在從本地暫存載入中...")
            st.caption(
                f"已恢復：`{os.path.basename(csv_name)}`｜"
                f"待審核資料 {len(result)} 筆。"
            )
        else:
            st.markdown("**📤 第三步：上傳報表檔案 (CSV / 加密 XLSX)**")
            uploaded = st.file_uploader("上傳 CSV / XLSX", type=["csv", "xlsx"], key="uploaded_file")
            if uploaded is None:
                cached_bytes = st.session_state.get("uploaded_file_cached_bytes")
                cached_name = str(st.session_state.get("uploaded_file_cached_name", "") or "")
                if isinstance(cached_bytes, (bytes, bytearray)) and cached_name:
                    raw_bytes = bytes(cached_bytes)
                    csv_name = cached_name
                else:
                    st.info("請完成第二步密碼確認後，於第三步上傳報表檔案。")
            else:
                # getvalue() 可重複讀取；避免只用 read() 後指標在結尾
                raw_bytes = uploaded.getvalue() if hasattr(uploaded, "getvalue") else uploaded.read()
                csv_name = getattr(uploaded, "name", "unknown.csv")
                st.session_state["uploaded_file_cached_bytes"] = bytes(raw_bytes)
                st.session_state["uploaded_file_cached_name"] = str(csv_name)
            if csv_name:
                csv_fp = _fingerprint_bytes(raw_bytes)
                try:
                    df = _read_uploaded_report_dataframe(
                        raw_bytes,
                        csv_name,
                        password=shopee_password,
                    )
                except Exception:
                    st.error("密碼錯誤或檔案無法解密，請確認第二步的密碼設定")
                    df = None

                if isinstance(df, pd.DataFrame):
                    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
                    if missing:
                        st.error("CSV 欄位格式不符，缺少欄位：" + "、".join(f"`{m}`" for m in missing))
                        st.dataframe(df.head(20), width="stretch")
                    else:
                        try:
                            result, validation_issues, _ = process_dataframe(df)
                        except Exception as e:
                            _show_user_error("檔案讀取完成，但資料整理失敗。請先確認報表格式是否正確。", e)
                            result = pd.DataFrame()
                            validation_issues = []

                        # Auto-Slice Loader：僅保留日期 > 水位線的訂單進入後續流程。
                        raw_file_total_count = int(len(result))
                        if isinstance(result, pd.DataFrame) and (not result.empty):
                            _date_txt = result["訂單成立日期"].map(_to_date_text)
                            _dt_series = pd.to_datetime(_date_txt, errors="coerce")
                            _hwm_ts = pd.Timestamp(hwm_date)
                            old_mask = _dt_series.notna() & (_dt_series <= _hwm_ts)
                            old_cnt = int(old_mask.sum())
                            if old_cnt > 0:
                                watermark_filtered_count = old_cnt
                                result = result.loc[~old_mask].copy().reset_index(drop=True)
                                duplicate_filter_notice = f"🛡️ 已過濾 {old_cnt} 筆與雲端重複之訂單。"
                            # 斷層警示：最小日期 > (MaxDate + 1天)
                            valid_new_dates = _dt_series.loc[~old_mask & _dt_series.notna()]
                            if not valid_new_dates.empty:
                                min_new_dt = valid_new_dates.min().date()
                                expected_next = hwm_date + timedelta(days=1)
                                if min_new_dt > expected_next:
                                    date_gap_notice = "⚠️ 偵測到日期不連續（可能漏抓報表）。"
                        effective_new_order_count = int(len(result))
                        current_display_scope = f"{csv_name}|{csv_fp}|{spreadsheet_id}|{worksheet_name.strip()}"
                        current_draft_scope = f"{current_display_scope}|{cred_path}"
                        _save_local_review_resume(
                            current_draft_scope,
                            {
                                "csv_name": csv_name,
                                "csv_fp": csv_fp,
                                "raw_file_total_count": raw_file_total_count,
                                "watermark_filtered_count": watermark_filtered_count,
                                "effective_new_order_count": effective_new_order_count,
                                "duplicate_filter_notice": duplicate_filter_notice,
                                "date_gap_notice": date_gap_notice,
                                "validation_issues": validation_issues,
                                "result_rows": result.to_dict(orient="records"),
                            },
                        )
                        st.session_state["preferred_resume_scope"] = current_draft_scope

        if result.empty:
            return

        reminder_items = [m for m in [duplicate_filter_notice, date_gap_notice] if m]
        if reminder_items:
            remind_body = "🔍 數據勾稽提醒：\n\n" + "\n".join([f"- {m}" for m in reminder_items])
            if date_gap_notice:
                st.warning(remind_body)
            else:
                st.info(remind_body)

        if not validation_issues:
            validation_status_line = "✅ 所有訂單的金額與手續費加總驗證通過！帳務吻合。"
        else:
            st.error("發現訂單驗證失敗，請先確認以下差異再進行後續帳務流程：")
            for issue in validation_issues:
                st.error(
                    f"訂單 `{issue['訂單編號']}` 的「{issue['項目']}」對不上："
                    f"預期={issue['預期值']:.2f}，實際={issue['實際值']:.2f}，"
                    f"差額={issue['差額']:+.2f}"
                )
            validation_status_line = "❌ 金額與手續費驗證未通過，請先修正後再進行後續流程。"

        with st.expander("📊 帳務校驗細節", expanded=False):
            st.caption(validation_status_line)
            st.caption(f"處理完成，共 **{len(result)}** 列（展開後）。")
            st.caption(
                "手續費已依單件金額比例分攤為 **單件實扣手續費**；"
                "已不顯示原始「成交／其他／金流」手續費欄位，避免與分攤結果混淆。"
            )

    # --- 階段二：載入雲端目錄（可快取；設定來自 appsetting.json）---

    cloud_df: pd.DataFrame | None = None
    cloud_error: str | None = None
    if not spreadsheet_id:
        cloud_error = (
            "試算表連結設定不完整，請檢查系統設定檔中的試算表網址。"
        )
    elif not os.path.isfile(cred_path):
        cloud_error = (
            "找不到連線憑證檔，請確認系統設定檔中的憑證路徑。"
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
        st.warning(f"⚠️ 無法讀取試算表資料：{cloud_error}")

    # 雲端歷史狀態（⚙️系統歷史紀錄）
    history_actions: list[dict] = []
    history_keep_batches = load_history_keep_batches()
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
    display_scope = f"{csv_name}|{csv_fp}|{spreadsheet_id}|{worksheet_name.strip()}"
    draft_scope = f"{display_scope}|{cred_path}"
    st.session_state["staged_draft_scope"] = draft_scope
    if st.session_state.get("staged_draft_loaded_scope") != draft_scope:
        restored_actions = _load_local_staged_draft(draft_scope)
        restored_actions = _dedupe_staged_actions_by_uid(restored_actions)
        st.session_state["staged_actions"] = list(restored_actions)
        st.session_state["staged_draft_loaded_scope"] = draft_scope
        _save_local_staged_draft(draft_scope, list(restored_actions))
        if restored_actions:
            for a in restored_actions:
                try:
                    _apply_optimistic_action(a)
                except Exception:
                    pass
    if st.session_state.get("local_synced_scope") != display_scope:
        st.session_state["local_synced_scope"] = display_scope
        st.session_state["local_synced_action_map"] = {}
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
            gc_deleted = gc_keep_latest_batches(
                cred_path, spreadsheet_id, keep=history_keep_batches
            )
            if gc_deleted > 0:
                st.info(
                    f"歷史表已清理舊批次紀錄列 {gc_deleted} 筆"
                    f"（僅保留最近 {history_keep_batches} 批）。"
                )
                st.session_state["history_cache_dirty"] = True
        except Exception as e:
            _show_user_error("⚠️ 讀取歷史紀錄失敗，先確認網路與試算表權限後再試一次。", e)
    # 記錄「本次上傳檔」在初始讀取時已存在的歷史 UID（write + skip）。
    # 之後同一份檔案再次 rerun，就不會把本次剛同步的訂單誤判成舊歷史而隱藏。
    if can_history and spreadsheet_id:
        baseline_scope = f"{csv_name}|{csv_fp}|{spreadsheet_id}|{worksheet_name.strip()}"
        if st.session_state.get("baseline_completed_scope") != baseline_scope:
            st.session_state["baseline_completed_scope"] = baseline_scope
            st.session_state["baseline_completed_uids"] = sorted(completed_uids)
        baseline_completed_uids = set(st.session_state.get("baseline_completed_uids") or [])
    else:
        baseline_completed_uids = set(completed_uids)
    # 同一批次內剛同步的訂單不應被「舊歷史過濾」吃掉，需留在清單顯示為已完成。
    if active_batch_id and history_actions:
        current_batch_uids = {
            str(a.get("Order_UID", "")).strip()
            for a in history_actions
            if str(a.get("Batch_ID", "")).strip() == str(active_batch_id).strip()
            and str(a.get("Action_Type", "")).strip().lower() == "write"
            and str(a.get("Order_UID", "")).strip()
        }
        baseline_completed_uids = baseline_completed_uids - current_batch_uids
    local_synced_action_map = dict(st.session_state.get("local_synced_action_map") or {})
    effective_uid_action_map = dict(uid_action_map)
    effective_uid_action_map.update(local_synced_action_map)
    effective_completed_uids = set(completed_uids) | set(local_synced_action_map.keys())

    all_parsed_orders = result.copy()
    st.session_state["legacy_archive_actions"] = []
    duplicate_idx: list[int] = []
    legacy_archive_idx: list[int] = []
    pending_idx: list[int] = []
    legacy_archive_actions: list[dict] = []

    for i, row in all_parsed_orders.iterrows():
        row_uid = str(row.get("uid", "") or "")
        if row_uid in baseline_completed_uids:
            duplicate_idx.append(i)
            continue

        dt_txt = _to_date_text(row.get("訂單成立日期"))
        is_old_or_duplicate = False
        if dt_txt:
            try:
                row_dt = datetime.strptime(dt_txt, "%Y-%m-%d").date()
                is_old_or_duplicate = row_dt <= hwm_date
            except Exception:
                is_old_or_duplicate = False
        if is_old_or_duplicate:
            legacy_archive_idx.append(i)
            legacy_archive_actions.append(
                {
                    "batch_id": active_batch_id,
                    "upload_time": active_upload_time,
                    "filename": csv_name,
                    "order_uid": row_uid,
                    "action_type": "skip",
                    "target_row": None,
                    "original_data": None,
                    "last_hint": (
                        f"{str(row.get('訂單成立日期', '') or '')} 的 "
                        f"{str(row.get('買家帳號', '') or '')} "
                        f"(訂單: {str(row.get('訂單編號', '') or '')})"
                    ),
                    "raw_name": normalize_item_name(str(row.get("合併原始名稱", "") or "")),
                    "order_created_at": str(row.get("訂單成立日期", "") or ""),
                    "stock_tag": str(row.get("現貨預定標記", "") or ""),
                    "expected_platform": expected_platform_for_stock_tag(
                        str(row.get("現貨預定標記", "") or "")
                    ),
                }
            )
            continue

        pending_idx.append(i)

    st.session_state["legacy_archive_actions"] = legacy_archive_actions
    duplicate_orders = all_parsed_orders.loc[duplicate_idx].copy()
    legacy_archive_orders = all_parsed_orders.loc[legacy_archive_idx].copy()
    pending_orders = all_parsed_orders.loc[pending_idx].copy().reset_index(drop=True)

    with st.expander("📥 本次分流摘要（審核前總覽）", expanded=not collapsed_in_review):
        st.success("📥 報表讀取與智慧分流成功！")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("📄 檔案原始總數", int(raw_file_total_count))
        water_label = "🟧 水位線已過濾" if int(watermark_filtered_count) > 0 else "🛡️ 水位線已過濾"
        m2.metric(water_label, int(watermark_filtered_count))
        m3.metric("🚀 本次待處理", len(pending_orders))
        m4.metric("📄 有效新單", int(effective_new_order_count))
        if int(watermark_filtered_count) > 0:
            st.caption(
                f"💡 註：有 {int(watermark_filtered_count)} 筆 "
                f"{hwm_date.strftime('%m/%d')}（含）以前的訂單已存在於雲端紀錄，"
                "系統已自動排除以防止重複。"
            )
        if len(duplicate_orders) > 0:
            st.caption(f"另有 {len(duplicate_orders)} 筆已在歷史紀錄，已從待處理清單移除。")
        if st.checkbox("顯示歷史已處理（唯讀）", key="show_filtered_history", value=False):
            filtered_rows: list[dict[str, str]] = []
            for _, r in duplicate_orders.iterrows():
                uid0 = str(r.get("uid", "") or "")
                act0 = str(effective_uid_action_map.get(uid0, "") or "").strip().lower()
                if act0 == "write":
                    status = "已同步寫入"
                elif act0 == "skip":
                    status = "已同步略過"
                else:
                    status = "已在歷史紀錄"
                filtered_rows.append(
                    {
                        "狀態": status,
                        "訂單成立日期": str(r.get("訂單成立日期", "") or ""),
                        "買家": str(r.get("買家帳號", "") or ""),
                        "商品原價": str(r.get("商品原價", "") or ""),
                        "商品名稱": str(r.get("合併原始名稱", "") or ""),
                    }
                )
            for _, r in legacy_archive_orders.iterrows():
                filtered_rows.append(
                    {
                        "狀態": "水位線重複",
                        "訂單成立日期": str(r.get("訂單成立日期", "") or ""),
                        "買家": str(r.get("買家帳號", "") or ""),
                        "商品原價": str(r.get("商品原價", "") or ""),
                        "商品名稱": str(r.get("合併原始名稱", "") or ""),
                    }
                )
            if filtered_rows:
                filtered_df = pd.DataFrame(filtered_rows)
                status_order = ["已同步寫入", "已同步略過", "水位線重複", "已在歷史紀錄"]
                status_options = [s for s in status_order if s in set(filtered_df["狀態"].tolist())]
                picked_statuses = st.multiselect(
                    "篩選狀態",
                    options=status_options,
                    default=status_options,
                    key="filtered_history_statuses",
                )
                if picked_statuses:
                    filtered_df = filtered_df[filtered_df["狀態"].isin(picked_statuses)].copy()
                else:
                    filtered_df = filtered_df.iloc[0:0].copy()
                st.dataframe(filtered_df, hide_index=True, width="stretch", height=280)
            else:
                st.caption("本次沒有被篩出的歷史資料。")
    result = pending_orders

    with st.sidebar:
        if not st.session_state.get("profit_compare_loaded", False):
            profit_cfg = load_profit_compare_settings()
            st.session_state["fx_cny_to_twd"] = float(profit_cfg.get("fx_cny_to_twd", 5.0))
            st.session_state["card_extra_pct"] = float(profit_cfg.get("card_extra_pct", 25.0))
            st.session_state["amount_tolerance_pct"] = float(profit_cfg.get("amount_tolerance_pct", 25.0))
            st.session_state["profit_compare_loaded"] = True
            st.session_state["profit_compare_persisted"] = (
                float(st.session_state["fx_cny_to_twd"]),
                float(st.session_state["card_extra_pct"]),
                float(st.session_state["amount_tolerance_pct"]),
            )
        with st.expander("💱 利潤比對設定", expanded=False):
            st.number_input(
                "自訂匯率（1 CNY = ? TWD）",
                min_value=0.1,
                max_value=20.0,
                step=0.01,
                format="%.2f",
                key="fx_cny_to_twd",
            )
            st.number_input(
                "估計調整率（運費%）",
                min_value=0.0,
                max_value=100.0,
                step=0.5,
                format="%.1f",
                key="card_extra_pct",
            )
            st.number_input(
                "合理利潤率（%）",
                min_value=1.0,
                max_value=100.0,
                step=1.0,
                format="%.1f",
                key="amount_tolerance_pct",
            )
        current_profit_cfg = (
            float(st.session_state.get("fx_cny_to_twd", 5.0) or 5.0),
            float(st.session_state.get("card_extra_pct", 25.0) or 25.0),
            float(st.session_state.get("amount_tolerance_pct", 25.0) or 25.0),
        )
        if st.session_state.get("profit_compare_persisted") != current_profit_cfg:
            save_err = save_profit_compare_settings(
                fx_cny_to_twd=current_profit_cfg[0],
                card_extra_pct=current_profit_cfg[1],
                amount_tolerance_pct=current_profit_cfg[2],
            )
            if save_err:
                st.warning(f"⚠️ 利潤比對設定儲存失敗：{save_err}")
            else:
                st.session_state["profit_compare_persisted"] = current_profit_cfg
        st.markdown("##### 🛒 同步與進度")
        if resume_ready:
            reset_resume_clicked = st.button(
                "🗑️ 捨棄目前暫存，重新上傳新報表",
                key="reset_resume_and_reupload",
                use_container_width=True,
            )
            if reset_resume_clicked:
                _clear_local_staged_draft(preferred_resume_scope)
                _clear_local_review_resume(preferred_resume_scope)
                st.session_state["preferred_resume_scope"] = ""
                st.session_state["staged_actions"] = []
                st.session_state["staged_draft_scope"] = ""
                st.session_state["staged_draft_loaded_scope"] = ""
                st.session_state["uploaded_file_cached_bytes"] = None
                st.session_state["uploaded_file_cached_name"] = ""
                st.session_state["uploaded_file"] = None
                st.rerun()
        staged_actions = _dedupe_staged_actions_by_uid(_staged_actions())
        if len(staged_actions) != len(_staged_actions()):
            _set_staged_actions(staged_actions)
        legacy_archive_actions = list(st.session_state.get("legacy_archive_actions") or [])
        manual_count = len(staged_actions)
        staged_write_count = sum(
            1 for a in staged_actions
            if str(a.get("action_type", "")).strip().lower() == "write"
        )
        auto_archive_count = len(legacy_archive_actions)
        kpi_pending, kpi_filtered = st.columns(2)
        kpi_pending.metric("⏳ 編輯中（待入庫）", f"{manual_count} 筆")
        kpi_filtered.metric("✅ 已過濾（不處理）", f"{auto_archive_count} 筆")
        st.caption("🏠 換電腦接關提醒：記得 Push 到 Git 讓進度同步！")
        if auto_archive_count > 0:
            st.warning(
                f"⚠️ 偵測到 {auto_archive_count} 筆資料的完成日期 <= 雲端水位線 "
                f"({hwm_date.strftime('%Y-%m-%d')})，已自動排除以防止重複入帳。"
            )
            with st.expander("查看自動排除明細", expanded=False):
                filtered_view = legacy_archive_orders.copy()
                if not filtered_view.empty:
                    show_cols = [c for c in ["合併原始名稱", "訂單成立日期", "買家帳號", "商品原價"] if c in filtered_view.columns]
                    st.dataframe(
                        filtered_view[show_cols],
                        hide_index=True,
                        width="stretch",
                        height=220,
                    )
                    filtered_csv = filtered_view.to_csv(index=False).encode("utf-8-sig")
                    filtered_fn = f"filtered_orders_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
                    st.download_button(
                        label="下載自動排除明細 (CSV)",
                        data=filtered_csv,
                        file_name=filtered_fn,
                        mime="text/csv",
                        key=f"dl_filtered_orders_{active_batch_id or 'na'}",
                    )
                else:
                    st.caption("目前無可顯示的自動排除明細。")
        sync_btn_text = f"🚀 確認寫入雲端正式帳本 ({staged_write_count} 筆)"
        invalid_cost_actions: list[dict] = []
        for a in staged_actions:
            if str(a.get("action_type", "")).strip().lower() != "write":
                continue
            c0 = _money_to_float(a.get("selected_cost_snapshot", ""))
            if c0 is None or float(c0) <= 0:
                c0 = _money_to_float(a.get("unit_cost", ""))
            if c0 is None or float(c0) <= 0:
                invalid_cost_actions.append(a)
        atomic_gate_ok = (staged_write_count > 0) and (len(invalid_cost_actions) == 0)
        if staged_write_count <= 0:
            st.warning("⚠️ 目前沒有可同步寫入的有效暫存資料。")
        elif invalid_cost_actions:
            st.error(
                f"⛔ 原子化同步閘門已啟動：尚有 {len(invalid_cost_actions)} 筆暫存成本未填寫或 <= 0，"
                "暫時禁止同步雲端。"
            )
        btn_preview_col, btn_sync_col = st.columns([1, 1])
        preview_clicked = btn_preview_col.button(
            "🔍 預覽本次利潤戰報",
            key="preview_battle_report_button",
            use_container_width=True,
        )
        sync_clicked = btn_sync_col.button(
            sync_btn_text,
            type="primary",
            key="staged_commit_button",
            use_container_width=True,
            disabled=not atomic_gate_ok,
        )
        st.divider()
        with st.expander("⚙️ 暫存管理 / 進階維護", expanded=False):
            confirm_discard = st.checkbox(
                "我確認要清空本次暫存",
                key="confirm_discard_staged",
                value=False,
            )
            discard_clicked = st.button(
                "🗑️ 清空暫存並重來",
                key="staged_discard_button",
                help="💡 僅清空本次網頁上點選的進度與水位線重複過濾項目，讓您重新操作。絕對不會刪除雲端 Google Sheet 上的既有資料。",
                disabled=not confirm_discard,
            )
        if preview_clicked:
            if not staged_actions:
                st.warning("目前沒有可預覽的寫入項目。")
            elif cloud_df is None or cloud_df.empty:
                st.error("目前無法讀取雲端目錄，暫時不能預覽戰報。")
            elif not active_batch_id:
                st.error("目前批次資訊不完整，暫時不能預覽戰報。")
            else:
                try:
                    preview_writes = [
                        a for a in staged_actions
                        if str(a.get("action_type", "")).lower() == "write"
                    ]
                    if not preview_writes:
                        st.warning("目前暫存皆為略過項目，沒有可預覽的戰報。")
                    else:
                        preview_snapshot = _build_final_report_snapshot(
                            preview_writes,
                            cloud_df=cloud_df,
                            batch_id=str(active_batch_id or ""),
                            csv_fp=str(csv_fp or ""),
                            csv_name=str(csv_name or ""),
                        )
                        st.session_state["PREVIEW_REPORT_SNAPSHOT"] = preview_snapshot
                        st.session_state["preview_report_expanded"] = True
                        st.success("已產生同步前預覽戰報（未寫入雲端、未存檔）。")
                except Exception as e:
                    _show_user_error("戰報預覽建立失敗，請先修正資料後再試。", e)
        if sync_clicked:
            if not staged_actions:
                st.warning("目前沒有可同步的項目。")
            elif (not can_history) or (not active_batch_id) or (hwm_col_idx is None):
                st.error("目前無法連線歷史紀錄，暫時不能同步。")
            else:
                final_snapshot: dict[str, object] | None = None
                try:
                    writes = [
                        a for a in staged_actions if str(a.get("action_type", "")).lower() == "write"
                    ]
                    # 同步前預檢：先驗證 target_row / 成本來源，未通過不得進入 API 呼叫。
                    if writes:
                        final_snapshot = _build_final_report_snapshot(
                            writes,
                            cloud_df=cloud_df,
                            batch_id=str(active_batch_id or ""),
                            csv_fp=str(csv_fp or ""),
                            csv_name=str(csv_name or ""),
                        )
                        st.session_state["FINAL_REPORT_SNAPSHOT"] = final_snapshot
                    with st.spinner("同步中，請稍候..."):
                        write_ops = [
                            {
                                "sheet_row": int(a.get("target_row", 0) or 0),
                                "header_index_1based": dict(a.get("header_index_1based") or {}),
                                "buyer_account": str(a.get("buyer_account", "") or ""),
                                "sale_price": a.get("sale_price", ""),
                                "fee_value": a.get("fee_value", ""),
                                "order_completed_date": str(a.get("order_created_at", "") or ""),
                                "completed_date_col_index": int(hwm_col_idx or 0),
                            }
                            for a in writes
                        ]
                        _retry_call(
                            batch_write_order_values_to_sheet_rows,
                            cred_path,
                            spreadsheet_id,
                            worksheet_name.strip(),
                            write_ops,
                            retries=3,
                        )
                        _retry_call(
                            append_history_actions_batch,
                            cred_path,
                            spreadsheet_id,
                            staged_actions,
                            retries=3,
                        )
                        if writes:
                            if final_snapshot is None:
                                final_snapshot = _build_final_report_snapshot(
                                    writes,
                                    cloud_df=cloud_df,
                                    batch_id=str(active_batch_id or ""),
                                    csv_fp=str(csv_fp or ""),
                                    csv_name=str(csv_name or ""),
                                )
                            st.session_state["FINAL_REPORT_SNAPSHOT"] = final_snapshot
                            snapshot_row = [
                                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                datetime.now().strftime("%Y-%m"),
                                int(final_snapshot["writes_count"]),
                                int(final_snapshot["total_revenue"]),
                                int(final_snapshot["total_fee"]),
                                int(final_snapshot["total_cost"]),
                                int(final_snapshot["total_profit"]),
                                round(float(final_snapshot["avg_fee_rate"]), 6),
                            ]
                            _retry_call(
                                _append_report_snapshot_row,
                                cred_path,
                                spreadsheet_id,
                                DEFAULT_REPORT_SNAPSHOT_WORKSHEET,
                                snapshot_row,
                                retries=3,
                            )
                            save_to_archive(final_snapshot)
                        else:
                            st.session_state["FINAL_REPORT_SNAPSHOT"] = None
                    local_map_now = dict(st.session_state.get("local_synced_action_map") or {})
                    for a in staged_actions:
                        uid_now = str(a.get("order_uid", "") or "").strip()
                        act_now = str(a.get("action_type", "") or "").strip().lower()
                        if uid_now and act_now in {"write", "skip"}:
                            local_map_now[uid_now] = act_now
                    st.session_state["local_synced_action_map"] = local_map_now
                    dict_entries = [
                        (str(a.get("raw_name", "") or ""), str(a.get("std_keyword", "") or ""))
                        for a in writes
                    ]
                    if dict_entries:
                        _retry_call(
                            batch_learn_dictionary_entries,
                            cred_path,
                            spreadsheet_id,
                            dict_entries,
                            retries=3,
                        )
                    _retry_call(
                        gc_keep_latest_batches,
                        cred_path, spreadsheet_id, keep=history_keep_batches
                        ,retries=3
                    )
                    _set_staged_actions([])
                    _clear_all_local_staged_drafts()
                    _clear_all_local_review_resumes()
                    # 成功後 rerun，會重新從雲端計算最新水位線並更新第一步建議。
                    st.session_state["legacy_archive_actions"] = []
                    st.session_state["history_cache_dirty"] = True
                    st.session_state["force_history_sync"] = True
                    st.session_state["sku_dictionary_dirty"] = True
                    st.session_state["sync_success_payload"] = {
                        "csv_fp": str(csv_fp or ""),
                        "batch_id": str(active_batch_id or ""),
                        "manual_count": int(len(staged_actions)),
                        "archive_count": 0,
                        "writes_count": int(len(writes)),
                    }
                    st.balloons()
                    st.success(
                        "✅ 本次同步完成："
                        f"一般 {len(staged_actions)} 筆，"
                        "自動排除資料未進入同步 payload。"
                    )
                    if writes:
                        st.caption(
                            f"📊 已自動寫入批次結算摘要：{DEFAULT_REPORT_SNAPSHOT_WORKSHEET}"
                        )
                    else:
                        st.caption("ℹ️ 本次同步皆為略過，未產生新的批次摘要。")
                    st.session_state["PREVIEW_REPORT_SNAPSHOT"] = None
                    st.session_state["preview_report_expanded"] = False
                    st.rerun()
                except Exception as e:
                    # 若同步末段失敗，保留快照供人工補跑/對帳。
                    if isinstance(final_snapshot, dict):
                        st.session_state["FINAL_REPORT_SNAPSHOT"] = final_snapshot
                    _show_user_error("同步失敗，請稍後再試。", e)
        if discard_clicked:
            for a in reversed(staged_actions):
                _revert_optimistic_action(a)
            _set_staged_actions([])
            scope_now = str(st.session_state.get("staged_draft_scope", "") or "")
            _clear_local_staged_draft(scope_now)
            _clear_local_review_resume(scope_now)
            st.session_state["PREVIEW_REPORT_SNAPSHOT"] = None
            st.session_state["preview_report_expanded"] = False
            st.session_state["legacy_archive_actions"] = []
            st.session_state["force_catalog_sync"] = True
            st.session_state["force_history_sync"] = True
            st.session_state["sku_dictionary_dirty"] = True
            _invalidate_cloud_catalog_cache()
            st.rerun()

        st.divider()
        with st.expander("🕒 系統歷史與批次回溯", expanded=False):
            st.caption(f"歷史批次保留上限：最近 {history_keep_batches} 批")
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
                st.caption(f"批次代碼：`{selected_batch.get('batch_id','')}`")
                confirm_batch_rollback = st.checkbox(
                    "我確認要還原這個批次",
                    key="confirm_batch_rollback",
                    value=False,
                )
                if st.button(
                    "還原此批次資料",
                    key="rollback_selected_batch",
                    disabled=not confirm_batch_rollback,
                ):
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
                        st.session_state["local_synced_action_map"] = {}
                        _invalidate_cloud_catalog_cache()
                        st.success(f"✅ 已回溯批次：還原 {restored} 筆、刪除歷史列 {deleted} 筆。")
                        st.rerun()
                    except Exception as e:
                        _show_user_error("回溯失敗，請稍後再試。", e)

    st.subheader("逐筆審核")
    row_uids_in_view = [str(r.get("uid", "") or "") for _, r in result.iterrows()]
    done_write_count = sum(1 for u in row_uids_in_view if effective_uid_action_map.get(u, "") == "write")
    done_skip_count = sum(1 for u in row_uids_in_view if effective_uid_action_map.get(u, "") == "skip")
    done_total_count = done_write_count + done_skip_count
    pending_review_count = max(0, len(row_uids_in_view) - done_total_count)
    p1, p2, p3, p4 = st.columns(4)
    p1.metric("本次新單總數", len(row_uids_in_view))
    p2.metric("已同步寫入", done_write_count)
    p3.metric("已同步略過", done_skip_count)
    p4.metric("尚待處理", pending_review_count)
    if "show_all_rows" not in st.session_state:
        st.session_state["show_all_rows"] = False
    show_all_rows = st.checkbox("顯示所有（含已完成）", key="show_all_rows")
    if "enable_low_conf_hint" not in st.session_state:
        st.session_state["enable_low_conf_hint"] = True
    if "same_buyer_high_threshold" not in st.session_state:
        st.session_state["same_buyer_high_threshold"] = 70
    if "same_buyer_hint_max_hits" not in st.session_state:
        st.session_state["same_buyer_hint_max_hits"] = 5
    with st.expander("⚙️ 提醒設定", expanded=False):
        st.checkbox(
            "啟用同買家提醒",
            key="enable_low_conf_hint",
            help="關閉後將不再顯示同買家高分命中提醒。",
        )
        c1, c2 = st.columns(2)
        with c1:
            st.number_input(
                "高分命中",
                min_value=0,
                max_value=100,
                step=1,
                key="same_buyer_high_threshold",
            )
        with c2:
            st.number_input(
                "顯示筆數",
                min_value=1,
                max_value=20,
                step=1,
                key="same_buyer_hint_max_hits",
            )
    next_uid = _next_unfinished_uid(result)
    if next_uid:
        st.caption("已自動展開下一筆待處理訂單。")
    else:
        st.caption("目前沒有待處理新單。")
    done_log_rows: list[dict[str, str]] = []
    for _, r in result.iterrows():
        uid0 = str(r.get("uid", "") or "")
        act0 = str(effective_uid_action_map.get(uid0, "") or "").strip().lower()
        if act0 not in {"write", "skip"}:
            continue
        done_log_rows.append(
            {
                "狀態": "已同步寫入" if act0 == "write" else "已同步略過",
                "買家": str(r.get("買家帳號", "") or ""),
                "商品原價": str(r.get("商品原價", "") or ""),
                "商品名稱": str(r.get("合併原始名稱", "") or ""),
            }
        )
    if done_log_rows:
        with st.expander(f"查看本次已同步紀錄（{len(done_log_rows)} 筆）", expanded=False):
            st.dataframe(pd.DataFrame(done_log_rows), hide_index=True, width="stretch")
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

    staged_actions = _dedupe_staged_actions_by_uid(_staged_actions())
    if len(staged_actions) != len(_staged_actions()):
        _set_staged_actions(staged_actions)
    sku_dict_map = _merge_staged_dictionary_entries(sku_dict_map, staged_actions)
    staged_by_uid = _staged_map_by_uid(staged_actions)
    same_buyer_high_threshold = int(
        st.session_state.get("same_buyer_high_threshold", 70) or 70
    )
    same_buyer_hint_max_hits = int(
        st.session_state.get("same_buyer_hint_max_hits", 5) or 5
    )
    enable_low_conf_hint = bool(st.session_state.get("enable_low_conf_hint", True))
    if cloud_df is not None and not cloud_df.empty:
        # done 標記需每次依目前 session/staged 重新計算（不可快取）
        for pos, (idx, row) in enumerate(result.iterrows()):
            uid = f"r_{pos}_{idx}"
            row_uid = str(row.get("uid", ""))
            staged = staged_by_uid.get(row_uid)
            done_by_history = row_uid in effective_completed_uids
            if staged is not None:
                st.session_state[f"done_{uid}"] = True
                st.session_state[f"done_type_{uid}"] = str(staged.get("action_type", "")).lower()
            else:
                st.session_state[f"done_{uid}"] = done_by_history
                st.session_state[f"done_type_{uid}"] = effective_uid_action_map.get(row_uid, "")

        result_uid_fp = hashlib.sha256(
            "|".join(str(r.get("uid", "") or "") for _, r in result.iterrows()).encode("utf-8")
        ).hexdigest()[:16]
        review_scope = "|".join(
            [
                str(st.session_state.get("cloud_catalog_scope", "") or ""),
                str(display_scope),
                str(enable_low_conf_hint),
                str(same_buyer_high_threshold),
                str(same_buyer_hint_max_hits),
                result_uid_fp,
                _dict_fingerprint(sku_dict_map),
            ]
        )
        cached_scope = str(st.session_state.get("review_meta_scope", "") or "")
        cached_meta = st.session_state.get("review_meta_cache")
        if cached_scope == review_scope and isinstance(cached_meta, dict):
            review_meta = dict(cached_meta)
        else:
            buyer_hits_cache: dict[tuple[str, str], list[dict[str, str]]] = {}
            for pos, (idx, row) in enumerate(result.iterrows()):
                uid = f"r_{pos}_{idx}"
                row_uid = str(row.get("uid", ""))
                tag = str(row.get("現貨預定標記", "") or "")
                candidate_df = candidate_pool_for_stock_tag(cloud_df, tag)
                # 「展開所有」不受預定/現貨平台池限制，提供全表候選供人工挑選。
                candidate_df_all = cloud_df.copy()
                q = str(row.get("清洗後簡體名稱", "") or "")
                raw_nm = str(row.get("合併原始名稱", "") or "").strip()
                top_matches = fuzzy_top3_matches(q, candidate_df)
                top_score = int(top_matches[0][1]) if top_matches else 0
                buyer_high_hits: list[dict[str, str]] = []
                if enable_low_conf_hint:
                    buyer = str(row.get("買家帳號", "") or "").strip()
                    if buyer:
                        k = (buyer, q)
                        if k in buyer_hits_cache:
                            buyer_high_hits = list(buyer_hits_cache[k])
                        else:
                            buyer_pool = cloud_df[
                                cloud_df["買家"].fillna("").astype(str).str.strip() == buyer
                            ].copy()
                            if not buyer_pool.empty:
                                for sr, score, merged in fuzzy_top3_matches(q, buyer_pool):
                                    if int(score) < same_buyer_high_threshold:
                                        continue
                                    row_hit = get_catalog_row_by_sheet_row(cloud_df, int(sr))
                                    buyer_high_hits.append(
                                        {
                                            "sheet_row": str(int(sr)),
                                            "score": str(int(score)),
                                            "name": zhconv.convert(
                                                str(merged or "").replace("\n", " ").strip(),
                                                "zh-tw",
                                            ),
                                            "buyer": str(
                                                (row_hit.get("買家", "") if row_hit is not None else "")
                                                or ""
                                            ).strip()
                                            or "（空白）",
                                            "upload_time": _sheet_open_date_text(
                                                row_hit
                                            ),
                                        }
                                    )
                            buyer_hits_cache[k] = list(buyer_high_hits)
                review_meta[uid] = {
                    "top_options": _build_top_pick_options(
                        raw_nm, q, candidate_df, sku_dict_map
                    ),
                    "all_options": _build_all_pick_options(
                        candidate_df_all, raw_nm, sku_dict_map, skip_first=True
                    ),
                    "top_score": top_score,
                    "buyer_high_hits": buyer_high_hits[:same_buyer_hint_max_hits],
                    "row": row,
                    "pos": pos,
                    "idx": idx,
                    "row_uid": row_uid,
                }
            st.session_state["review_meta_scope"] = review_scope
            st.session_state["review_meta_cache"] = review_meta

    for pos, (idx, row) in enumerate(result.iterrows()):
        uid = f"r_{pos}_{idx}"
        row_uid = str(row.get("uid", ""))
        staged_action = staged_by_uid.get(row_uid)
        done_key = f"done_{uid}"
        done = bool(st.session_state.get(done_key, False))
        done_type = str(st.session_state.get(f"done_type_{uid}", "") or "")
        if (not show_all_rows) and done:
            continue
        buyer = row.get("買家帳號", "")
        tag = row.get("現貨預定標記", "")
        price = row.get("商品原價", "")
        if staged_action is not None:
            staged_type = str(staged_action.get("action_type", "") or "").strip().lower()
            if staged_type == "skip":
                done_mark = " [🛒 已暫存略過｜尚未同步雲端]"
            else:
                done_mark = " [🛒 已暫存寫入｜尚未同步雲端]"
        elif done_type == "skip":
            done_mark = " [⏭ 已略過｜☁️ 已同步雲端]"
        else:
            done_mark = " [✅ 已完成｜☁️ 已同步雲端]" if done else ""
        title = f"{buyer} ｜ {tag} ｜ 商品原價：{price}{done_mark}"
        expanded_by_default = (not done) and (uid == next_uid)
        with st.expander(title, expanded=expanded_by_default):
            merged_disp = str(row.get("合併原始名稱", "") or "")
            fee_int = int(row.get("單件實扣手續費", 0) or 0)
            row_uid = str(row.get("uid", ""))

            if staged_action is not None:
                st.info("🛒 這筆已加入同步佇列，尚未寫入雲端。")
                expected_platform = expected_platform_for_stock_tag(str(tag or ""))
                left_bg, left_fg = _left_tag_badge_colors(str(tag or ""))
                right_bg, right_fg = _right_platform_badge_colors(expected_platform)
                badge_l, badge_r = st.columns(2)
                with badge_l:
                    st.markdown(
                        _tag_badge_html(f"來源判定：{tag or '未知'}", left_bg, left_fg),
                        unsafe_allow_html=True,
                    )
                with badge_r:
                    st.markdown(
                        _tag_badge_html(f"候選平台：{expected_platform}", right_bg, right_fg),
                        unsafe_allow_html=True,
                    )
                left_p, right_p = st.columns([1, 1])
                with left_p:
                    st.markdown(
                        _name_panel_html("蝦皮商品名稱", merged_disp, min_height_px=92),
                        unsafe_allow_html=True,
                    )
                if str(staged_action.get("action_type", "")).lower() == "skip":
                    st.success("⏭️ 已暫存為略過。")
                    with right_p:
                        st.markdown(
                            _name_panel_html("雲端完整品項（快速對照）", "此筆為略過，未指定目標列。", min_height_px=92),
                            unsafe_allow_html=True,
                        )
                else:
                    target_row_now = int(staged_action.get("target_row", 0) or 0)
                    st.success(f"💾 已暫存寫入：第 {target_row_now} 列。")
                    row_now = (
                        get_catalog_row_by_sheet_row(cloud_df, target_row_now)
                        if (cloud_df is not None and not cloud_df.empty and target_row_now > 0)
                        else None
                    )
                    row_name = _catalog_full_name(row_now) if row_now is not None else ""
                    with right_p:
                        st.markdown(
                            _name_panel_html(
                                "雲端完整品項（快速對照）",
                                row_name or f"第 {target_row_now} 列（目前無法讀取名稱）",
                                min_height_px=92,
                            ),
                            unsafe_allow_html=True,
                        )
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
                confirm_one_rollback = st.checkbox(
                    "我確認要撤銷這筆",
                    key=f"confirm_rollback_one_{uid}",
                    value=False,
                )
                clicked_rollback_one = st.button(
                    "🔙 撤銷此筆",
                    key=f"rollback_one_{uid}",
                    disabled=(not can_history) or (not confirm_one_rollback),
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
                            st.warning("找不到這筆訂單的歷史紀錄。")
                        else:
                            local_map_now = dict(st.session_state.get("local_synced_action_map") or {})
                            local_map_now.pop(row_uid, None)
                            st.session_state["local_synced_action_map"] = local_map_now
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
                        _show_user_error("撤銷失敗，請稍後再試。", e)
                continue

            if cloud_df is None or cloud_df.empty or uid not in review_meta:
                st.info(merged_disp)
                st.metric("單件實扣手續費", f"{fee_int}")
                st.caption("（尚未載入雲端資料或工作表為空，無法比對）")
                continue

            top_options = review_meta[uid]["top_options"]
            all_options = review_meta[uid]["all_options"]
            top_score = int(review_meta[uid].get("top_score", 0) or 0)
            buyer_high_hits = list(review_meta[uid].get("buyer_high_hits", []) or [])
            fx_cny_to_twd = float(st.session_state.get("fx_cny_to_twd", 5.0) or 5.0)
            card_extra_pct = float(st.session_state.get("card_extra_pct", 25.0) or 0.0)
            reasonable_profit_pct = float(st.session_state.get("amount_tolerance_pct", 25.0) or 25.0)
            shopee_twd = _money_to_int(row.get("商品原價", ""))

            expected_platform = expected_platform_for_stock_tag(str(tag or ""))
            left_bg, left_fg = _left_tag_badge_colors(str(tag or ""))
            right_bg, right_fg = _right_platform_badge_colors(expected_platform)
            left_c, right_c = st.columns([1.03, 0.97])
            with left_c:
                st.caption("商品與訂單")
                st.markdown(
                    _tag_badge_html(
                        f"來源判定：{tag or '未知'}",
                        left_bg,
                        left_fg,
                    ),
                    unsafe_allow_html=True,
                )
                st.markdown(
                    _name_panel_html("蝦皮商品名稱", merged_disp, min_height_px=92),
                    unsafe_allow_html=True,
                )
                net_income = (
                    int(shopee_twd) - int(fee_int)
                    if shopee_twd is not None
                    else None
                )
                price_l, price_r = st.columns([1.05, 1.08])
                with price_l:
                    st.markdown(
                        _metric_block_html(
                            "實售收入（實售金額-手續費）",
                            f"{int(net_income):,}" if net_income is not None else "—",
                            tone="#0f766e",
                            value_size="lg",
                            html_class="shopee-fin-metric-card",
                        ),
                        unsafe_allow_html=True,
                    )
                with price_r:
                    amount_gap_hint_slot = st.empty()
                recv_l, recv_r = st.columns([1.0, 1.08])
                with recv_l:
                    st.markdown(
                        _metric_block_html(
                            "實收金額（台幣）",
                            f"{int(shopee_twd):,}" if shopee_twd is not None else "—",
                            tone="#111827",
                            value_size="md",
                        ),
                        unsafe_allow_html=True,
                    )
                with recv_r:
                    suggest_price_hint_slot = st.empty()
                fee_pct = (
                    (float(fee_int) / float(shopee_twd)) * 100.0
                    if (shopee_twd is not None and float(shopee_twd) > 0)
                    else None
                )
                fee_tone = "#c2410c"
                fee_l, fee_r = st.columns([1.0, 1.08])
                with fee_l:
                    st.markdown(
                        _metric_block_html(
                            "單件實扣手續費",
                            f"{int(fee_int):,}",
                            tone=fee_tone,
                            value_size="md",
                        ),
                        unsafe_allow_html=True,
                    )
                with fee_r:
                    if fee_pct is None:
                        st.markdown(
                            (
                                "<div style='margin-top:1.55rem;'>"
                                "<span style='display:inline-block;padding:0.26rem 0.62rem;border-radius:999px;"
                                "background:#f3f4f6;color:#6b7280;font-weight:700;'>"
                                "手續費率 —"
                                "</span></div>"
                            ),
                            unsafe_allow_html=True,
                        )
                    else:
                        st.markdown(
                            (
                                f"<div style='margin-top:1.55rem;'>"
                                f"<span style='display:inline-block;padding:0.26rem 0.62rem;border-radius:999px;"
                                f"background:#f9fafb;border:1px solid #e5e7eb;color:{fee_tone};font-weight:700;'>"
                                f"手續費率 {fee_pct:.1f}%"
                                f"</span></div>"
                            ),
                            unsafe_allow_html=True,
                        )
                profit_l, profit_r = st.columns([1.0, 1.08])
                with profit_l:
                    profit_metric_slot = st.empty()
                    profit_metric_slot.markdown(
                        _metric_block_html(
                            "利潤（台幣）",
                            "—",
                            tone="#047857",
                            value_size="hero",
                        ),
                        unsafe_allow_html=True,
                    )
                with profit_r:
                    compare_inline_slot = st.empty()

            with right_c:
                st.caption("比對與決策")
                st.markdown(
                    _tag_badge_html(
                        f"候選平台：{expected_platform}",
                        right_bg,
                        right_fg,
                    ),
                    unsafe_allow_html=True,
                )
                quick_compare_slot = st.container()
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

                base_options = all_options if show_all_available else top_options
                blocked_rows: set[int] = set()
                for a in staged_actions:
                    if str(a.get("action_type", "") or "").strip().lower() != "write":
                        continue
                    if str(a.get("order_uid", "") or "").strip() == row_uid:
                        continue
                    tr0 = a.get("target_row")
                    try:
                        tr_int = int(tr0 or 0)
                    except Exception:
                        tr_int = 0
                    if tr_int > 0:
                        blocked_rows.add(tr_int)
                occupied_rows: set[int] = set()
                for _, sr0 in base_options:
                    if not isinstance(sr0, int) or sr0 <= 0:
                        continue
                    r_cand = get_catalog_row_by_sheet_row(cloud_df, sr0)
                    if row_has_order_like_data(r_cand):
                        occupied_rows.add(sr0)
                if show_all_available:
                    options = list(base_options)
                else:
                    # 推薦模式：僅列出「可安全寫入」的空位，避免推薦到已有資料列。
                    options: list[tuple[str, int | None]] = []
                    for label0, sr0 in top_options:
                        if sr0 is None:
                            options.append((label0, sr0))
                            continue
                        if (
                            isinstance(sr0, int)
                            and sr0 > 0
                            and sr0 not in blocked_rows
                            and sr0 not in occupied_rows
                        ):
                            options.append((label0, sr0))
                    if not options:
                        options = [("略過不寫入", None)]
                # 將「雲端已佔用」或「本地暫存已占用(blocked)」列統一加前綴，
                # 避免使用者看不出 25/26 這類不可直接寫入的候選列。
                normalized_options: list[tuple[str, int | None]] = []
                for lb0, sr0 in options:
                    lb = str(lb0 or "")
                    if (
                        isinstance(sr0, int)
                        and sr0 > 0
                        and (sr0 in occupied_rows or sr0 in blocked_rows)
                        and not lb.startswith("[⚠️ 已佔用]")
                    ):
                        lb = f"[⚠️ 已佔用] {lb}"
                    normalized_options.append((lb, sr0))
                options = normalized_options
                default_pick_index = (
                    None
                    if show_all_available
                    else _smart_default_pick_index(
                        options, blocked_rows, occupied_rows
                    )
                )
                sel_key = f"cloud_sel_{uid}"
                if not manual:
                    cur_sel = st.session_state.get(sel_key)
                    cur_idx: int | None
                    try:
                        cur_idx = int(cur_sel) if cur_sel is not None else None
                    except Exception:
                        cur_idx = None
                    needs_reset = False
                    if cur_idx is None:
                        needs_reset = True
                    elif cur_idx < 0 or cur_idx >= len(options):
                        needs_reset = True
                    # 不要因為「已占用/被阻擋」而覆寫使用者手動選擇；
                    # 只在無值或索引失效時才回到預設推薦列。
                    if needs_reset and default_pick_index is not None:
                        st.session_state[sel_key] = int(default_pick_index)
                labels = [o[0] for o in options]
                select_kwargs: dict[str, object] = {
                    "format_func": (lambda i: labels[i]),
                    "key": sel_key,
                }
                if show_all_available:
                    select_kwargs["placeholder"] = "請點擊此處並直接輸入鍵盤關鍵字搜尋 (例如: cd080)..."
                if sel_key not in st.session_state:
                    select_kwargs["index"] = default_pick_index
                selected_option_index = st.selectbox(
                    "可用空位清單（可鍵盤搜尋）"
                    if show_all_available
                    else "推薦雲端列（可選略過不寫入）",
                    range(len(labels)),
                    **select_kwargs,
                )
                if manual:
                    st.number_input(
                        "請輸入行號",
                        min_value=1,
                        step=1,
                        value=SHEET_FIRST_DATA_ROW_1BASED,
                        key=f"manual_row_{uid}",
                    )

                if manual:
                    eff_preview = _effective_sheet_row_from_state(
                        uid,
                        options,
                        default_index=default_pick_index,
                    )
                else:
                    if selected_option_index is None:
                        eff_preview = None
                    elif 0 <= int(selected_option_index) < len(options):
                        eff_preview = options[int(selected_option_index)][1]
                    else:
                        eff_preview = None
                r_preview = (
                    get_catalog_row_by_sheet_row(cloud_df, eff_preview)
                    if eff_preview is not None
                    else None
                )
                if r_preview is not None:
                    plat = str(r_preview.get("平台", "") or "").strip() or "空白"
                    buyer_now = str(r_preview.get("買家", "") or "").strip() or "空白"
                    full_name = _catalog_full_name(r_preview)
                    card_cny = _money_to_float(r_preview.get("刷卡金額", ""))
                    unit_cost_twd = _money_to_float(r_preview.get("單件成本", ""))
                    has_unit_cost = unit_cost_twd is not None and float(unit_cost_twd) > 0
                    base_twd = (
                        float(card_cny) * fx_cny_to_twd
                        if card_cny is not None
                        else None
                    )
                    est_twd = (
                        int(round(float(unit_cost_twd)))
                        if has_unit_cost
                        else (
                            int(round(float(base_twd) * (1.0 + (card_extra_pct / 100.0))))
                            if base_twd is not None
                            else None
                        )
                    )
                    with quick_compare_slot:
                        if full_name:
                            st.markdown(
                                _name_panel_html("雲端完整品項（快速對照）", full_name, min_height_px=92),
                                unsafe_allow_html=True,
                            )
                        amt_l, amt_r = st.columns([1.05, 1.3])
                        with amt_l:
                            st.markdown(
                                _metric_block_html(
                                    "單件成本（台幣）" if has_unit_cost else "粗估計算成本（台幣）",
                                    f"{int(est_twd):,}" if est_twd is not None else "—",
                                    tone="#1e40af",
                                    value_size="lg",
                                ),
                                unsafe_allow_html=True,
                            )
                        with amt_r:
                            if has_unit_cost:
                                st.markdown(
                                    "<div style='margin-top:0.35rem;font-size:0.80rem;color:#98A2B3;'>"
                                    "已使用雲端單件成本（實際成本），不套用左側利潤比對設定。"
                                    "</div>",
                                    unsafe_allow_html=True,
                                )
                            elif card_cny is not None:
                                st.markdown(
                                    "<div style='margin-top:0.28rem;padding:8px 10px;border-radius:10px;"
                                    "border:1px solid #e5e7eb;background:#ffffff;'>"
                                    "<div style='font-size:0.74rem;color:#94a3b8;line-height:1.2;'>刷卡金額（人民幣）</div>"
                                    f"<div style='margin-top:2px;font-size:1.02rem;font-weight:900;color:#334155;line-height:1.25;'>¥{float(card_cny):,.2f}</div>"
                                    "<div style='font-size:0.74rem;color:#64748b;margin-top:4px;line-height:1.45;'>"
                                    "依左側"
                                    "<span style='display:inline-block;margin:0 0.24rem;padding:0.10rem 0.42rem;"
                                    "border:1px solid #D0D5DD;border-radius:6px;background:#F8FAFC;"
                                    "color:#344054;font-weight:800;letter-spacing:0.01em;'>"
                                    "【利潤比對設定】"
                                    "</span>"
                                    "估算成本"
                                    "</div>"
                                    "</div>",
                                    unsafe_allow_html=True,
                                )
                            else:
                                st.markdown(
                                    "<div style='margin-top:0.35rem;font-size:0.80rem;color:#98A2B3;'>"
                                    "刷卡金額（人民幣）：空白/無法解析"
                                    "</div>",
                                    unsafe_allow_html=True,
                                )
                    if shopee_twd is not None and est_twd is not None:
                        amount_gap_abs = abs(int(shopee_twd) - int(est_twd))
                        amount_gap_pct = (
                            float(amount_gap_abs) / max(1.0, float(shopee_twd))
                        ) * 100.0
                        # 額外防呆：金額差距過大通常代表配錯商品或成本資料異常
                        amount_gap_warn = amount_gap_pct >= 60.0
                        est_profit = int(shopee_twd) - int(fee_int) - int(est_twd)
                        net_income_den = max(
                            1.0, float(int(shopee_twd) - int(fee_int))
                        )
                        est_profit_pct = (float(est_profit) / net_income_den) * 100.0
                        if est_profit > 0:
                            p_tone = "#047857"
                        elif est_profit < 0:
                            p_tone = "#b91c1c"
                        else:
                            p_tone = "#b45309"
                        profit_metric_slot.markdown(
                            _metric_block_html(
                                "利潤（台幣）",
                                f"{est_profit:,}",
                                tone=p_tone,
                                value_size="hero",
                            ),
                            unsafe_allow_html=True,
                        )
                        if est_profit_pct < reasonable_profit_pct:
                            suggest_footer = ""
                            if float(shopee_twd or 0) > 0:
                                k_fee = float(fee_int) / float(shopee_twd)
                                p_need = _min_selling_price_for_target_profit_pct(
                                    float(est_twd),
                                    k_fee,
                                    reasonable_profit_pct,
                                )
                                if p_need is not None:
                                    p_sug = int(math.ceil(p_need - 1e-9))
                                    hint_body = (
                                        f"試算假設：手續費占實收比例維持本筆 {k_fee * 100:.1f}%"
                                        "（單件實扣÷實收），目標為左側「合理利潤率」"
                                        f"{reasonable_profit_pct:.1f}%（利潤÷實售收入）。"
                                        "實際蝦皮費率可能變動，僅供參考。"
                                    )
                                    hint_title = html.escape(hint_body, quote=True)
                                    suggest_footer = _suggested_price_footer_html(
                                        p_sug, hint_title
                                    )
                                else:
                                    suggest_footer = (
                                        "<div style='font-size:0.74rem;color:#64748b;line-height:1.45;'>"
                                        "在目前手續費率與目標利潤率組合下，無法僅靠調高售價達成目標（分母 ≤ 0）。"
                                        "請檢視成本、目標利潤率或費率假設。"
                                        "</div>"
                                    )
                            if est_profit < 0:
                                panel_kind = "danger"
                                panel_title = f"⛔ 估算利潤為負（{est_profit_pct:.1f}%）"
                                panel_sub = "依據利潤比對設定判斷（需為正利潤）"
                            elif est_profit == 0:
                                panel_kind = "warning"
                                panel_title = f"⚠️ 估算利潤為零（{est_profit_pct:.1f}%）"
                                panel_sub = "依據利潤比對設定判斷（需為正利潤）"
                            else:
                                panel_kind = "warning"
                                panel_title = f"⚠️ 估算利潤偏低（{est_profit_pct:.1f}%）"
                                panel_sub = (
                                    f"依據利潤比對設定判斷（建議 ≥ {reasonable_profit_pct:.1f}%）"
                                )
                            compare_inline_slot.markdown(
                                _profit_analysis_panel(
                                    panel_kind,
                                    panel_title,
                                    panel_sub,
                                    "",
                                ),
                                unsafe_allow_html=True,
                            )
                            if suggest_footer:
                                suggest_price_hint_slot.markdown(
                                    suggest_footer,
                                    unsafe_allow_html=True,
                                )
                            else:
                                suggest_price_hint_slot.empty()
                        else:
                            compare_inline_slot.markdown(
                                _profit_alert_card(
                                    "success",
                                    f"✅ 估算利潤健康（{est_profit_pct:.1f}%）",
                                    f"依據利潤比對設定判斷（已達 ≥ {reasonable_profit_pct:.1f}%）",
                                ),
                                unsafe_allow_html=True,
                            )
                            suggest_price_hint_slot.empty()
                        if amount_gap_warn:
                            amount_gap_hint_slot.markdown(
                                _amount_gap_hint_html(
                                    int(shopee_twd),
                                    int(est_twd),
                                    amount_gap_pct,
                                ),
                                unsafe_allow_html=True,
                            )
                        else:
                            amount_gap_hint_slot.empty()
                    else:
                        profit_metric_slot.markdown(
                            _metric_block_html(
                                "利潤（台幣）",
                                "—",
                                tone="#64748b",
                                value_size="hero",
                            ),
                            unsafe_allow_html=True,
                        )
                        compare_inline_slot.markdown(
                            _profit_alert_card(
                                "neutral",
                                "⏳ 尚無法判斷",
                                "依據利潤比對設定判斷（待估算利潤）",
                            ),
                            unsafe_allow_html=True,
                        )
                        amount_gap_hint_slot.empty()
                        suggest_price_hint_slot.empty()
                    if enable_low_conf_hint and buyer_high_hits:
                        with st.expander("更多參考資訊", expanded=True):
                            st.warning(
                                "⚠️ 偵測到同買家在整張表單有高分命中紀錄，"
                                "請先確認是否已填過："
                            )
                            for h in buyer_high_hits:
                                try:
                                    sr_i = int(str(h.get("sheet_row", "") or "0"))
                                except Exception:
                                    sr_i = 0
                                row_hit_now = (
                                    get_catalog_row_by_sheet_row(cloud_df, sr_i)
                                    if sr_i > 0
                                    else None
                                )
                                open_date_now = _sheet_open_date_text(row_hit_now)
                                st.caption(
                                    f"- 第 {h['sheet_row']} 列｜分數 {h['score']}｜"
                                    f"{h['name']}｜買家 {h.get('buyer', '（空白）')}｜開單日期 {open_date_now}"
                                )
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
                    with quick_compare_slot:
                        st.markdown(
                            _name_panel_html("雲端完整品項（快速對照）", "目前尚未選取目標列。", min_height_px=92),
                            unsafe_allow_html=True,
                        )
                        st.markdown(
                            _metric_block_html(
                                "單件成本（台幣）",
                                "—",
                                tone="#1e40af",
                                value_size="lg",
                            ),
                            unsafe_allow_html=True,
                        )
                        st.markdown(
                            "<div style='margin-top:0.35rem;font-size:0.80rem;color:#98A2B3;'>刷卡金額（人民幣）：尚未選取目標列</div>",
                            unsafe_allow_html=True,
                        )
                    profit_metric_slot.markdown(
                        _metric_block_html(
                            "利潤（台幣）",
                            "—",
                            tone="#64748b",
                            value_size="hero",
                        ),
                        unsafe_allow_html=True,
                    )
                    compare_inline_slot.markdown(
                        _profit_alert_card(
                            "neutral",
                            "⏳ 尚無法判斷",
                            "依據利潤比對設定判斷（待估算利潤）",
                        ),
                        unsafe_allow_html=True,
                    )
                    st.caption("目前此列狀態：未選取目標列")

            if manual:
                eff_now = _effective_sheet_row_from_state(
                    uid,
                    options,
                    default_index=default_pick_index,
                )
            else:
                if selected_option_index is None:
                    eff_now = None
                elif 0 <= int(selected_option_index) < len(options):
                    eff_now = options[int(selected_option_index)][1]
                else:
                    eff_now = None

            r_target = (
                get_catalog_row_by_sheet_row(cloud_df, eff_now)
                if eff_now is not None
                else None
            )
            occupied = row_has_order_like_data(r_target)

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
                    st.checkbox(
                        "強制覆蓋已有資料（此行已有買家／售價／手續費）",
                        key=f"force_write_{uid}",
                    )

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
                            "order_created_at": str(row.get("訂單成立日期", "") or ""),
                            "stock_tag": str(tag or ""),
                            "expected_platform": expected_platform_for_stock_tag(str(tag or "")),
                        }
                    )
                    _set_staged_actions(keep)
                    st.session_state[done_key] = True
                    st.session_state[f"done_type_{uid}"] = "skip"
                    st.success("已加入同步佇列（略過暫存）。")
                    st.rerun()
                except Exception as e:
                    _show_user_error("暫存略過失敗，請稍後再試。", e)

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
                    try:
                        est_profit_num = float(est_profit) if est_profit is not None else 0.0
                    except Exception:
                        est_profit_num = 0.0
                    unit_cost_num = _money_to_float(row.get("單件成本", ""))
                    if unit_cost_num is None:
                        unit_cost_num = _money_to_float(row.get("原始成本", ""))
                    # 單筆決策當下即固定成本快照：優先使用畫面原始列，若缺值則回讀目標雲端列。
                    selected_cost_source = "local_row"
                    if unit_cost_num is None or float(unit_cost_num or 0) <= 0:
                        cloud_cost_now = _extract_cost_from_cloud_row(r_target)
                        if cloud_cost_now is not None and float(cloud_cost_now) > 0:
                            unit_cost_num = float(cloud_cost_now)
                            selected_cost_source = "cloud_target_row"
                        else:
                            unit_cost_num = 0.0
                            selected_cost_source = "missing"
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
                        "est_profit_twd": est_profit_num,
                        "unit_cost": float(unit_cost_num or 0.0),
                        "original_cost": row.get("單件成本", row.get("原始成本", "")),
                        "selected_cost_source": selected_cost_source,
                        "selected_cost_snapshot": float(unit_cost_num or 0.0),
                        "std_keyword": std_kw,
                        "prev_dict_keyword": prev_dict_kw,
                        "before_local": before_vals,
                        "order_created_at": str(row.get("訂單成立日期", "") or ""),
                        "stock_tag": str(tag or ""),
                        "expected_platform": expected_platform_for_stock_tag(str(tag or "")),
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
                    st.success("✅ 已加入同步佇列（寫入暫存）。")
                    st.rerun()
                except Exception as e:
                    _show_user_error("暫存寫入失敗，請稍後再試。", e)

    # 本次完成進度（session 級）
    total_cards = len(result)
    committed_count = sum(
        1
        for _, row in result.iterrows()
        if str(row.get("uid", "") or "") in completed_uids
    )
    row_uid_set = {str(r.get("uid", "") or "") for _, r in result.iterrows()}
    staged_count = sum(
        1 for a in _dedupe_staged_actions_by_uid(_staged_actions())
        if str(a.get("order_uid", "") or "") in row_uid_set
    )
    st.progress(0 if total_cards == 0 else committed_count / total_cards)
    st.caption(
        f"狀態：已加入同步佇列 {committed_count}/{total_cards} 筆"
        + (f"（另有 {staged_count} 筆待確認）" if staged_count else "")
    )

    all_done = total_cards > 0 and committed_count >= total_cards and staged_count == 0
    success_payload = st.session_state.get("sync_success_payload")
    payload_match = (
        isinstance(success_payload, dict)
        and str(success_payload.get("csv_fp", "") or "") == str(csv_fp or "")
    )
    show_success_panel = bool(all_done or payload_match)
    preview_snapshot = st.session_state.get("PREVIEW_REPORT_SNAPSHOT")
    show_preview_panel = (
        isinstance(preview_snapshot, dict)
        and int(preview_snapshot.get("writes_count", 0) or 0) > 0
        and str(preview_snapshot.get("csv_fp", "") or "") == str(csv_fp or "")
        and not show_success_panel
    )
    if show_preview_panel:
        with st.expander(
            "👁️ 查看預覽戰報詳情",
            expanded=bool(st.session_state.get("preview_report_expanded", True)),
        ):
            preview_view = dict(preview_snapshot)
            preview_view["_widget_key_scope"] = "preview_" + str(active_batch_id or "na")
            preview_view["_preview_mode"] = True
            render_battle_report(preview_view)

    if show_success_panel:
        celeb_key = f"batch_done_balloons_{csv_fp}_{active_batch_id or 'na'}"
        if all_done and not st.session_state.get(celeb_key, False):
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
        final_snapshot = st.session_state.get("FINAL_REPORT_SNAPSHOT")
        if (
            isinstance(final_snapshot, dict)
            and int(final_snapshot.get("writes_count", 0) or 0) > 0
            and str(final_snapshot.get("csv_fp", "") or "") == str(csv_fp or "")
        ):
            with st.expander('🏆 檢視本批次結算戰報 (點擊展開)', expanded=False):
                snap = dict(final_snapshot)
                scope_live = str(bid or active_batch_id or csv_fp or "live")[:48]
                snap["_widget_key_scope"] = re.sub(r'[^\w\-]', '_', scope_live)
                render_battle_report(snap)

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
            scope_now = str(st.session_state.get("staged_draft_scope", "") or "")
            _clear_local_staged_draft(scope_now)
            _clear_local_review_resume(scope_now)
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
                "confirm_rollback_one_r_",
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
                "uploaded_file_cached_bytes",
                "uploaded_file_cached_name",
                # 逐筆審核/彙總 UI 的 widget 狀態
                "show_all_rows",
                "reset_done_flags",
                "history_batch_pick",
                "download_batch_summary_csv",
                "staged_actions",
                "staged_commit_button",
                "staged_discard_button",
                "confirm_discard_staged",
                "confirm_batch_rollback",
                "baseline_completed_scope",
                "baseline_completed_uids",
                "local_synced_scope",
                "local_synced_action_map",
                "staged_draft_scope",
                "staged_draft_loaded_scope",
                "preferred_resume_scope",
                "review_meta_scope",
                "review_meta_cache",
                "sync_success_payload",
                "FINAL_REPORT_SNAPSHOT",
                "PREVIEW_REPORT_SNAPSHOT",
                "preview_report_expanded",
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

    has_staged_pending = len(_dedupe_staged_actions_by_uid(_staged_actions())) > 0
    show_pre_sync_preview = has_staged_pending and (not all_done)
    if show_pre_sync_preview:
        st.caption("📝 以下資料為本地端即時解析與估算結果，尚未寫入雲端。")
        with st.expander("⚙️ 進階除錯（同步前）", expanded=False):
            if st.button("重新整理本頁完成狀態（不影響雲端資料）", key="reset_done_flags"):
                for pos, idx in enumerate(result.index):
                    st.session_state.pop(f"done_r_{pos}_{idx}", None)
                st.rerun()
            st.caption("📋 檢視完整處理結果表格")
            st.dataframe(result, width="stretch", height=420)

            st.caption("逐訂單：原始總金額 vs 展開後金額、訂單總手續費 vs 整數分攤加總。")
            recon_df = build_accounting_reconciliation_df(df, result, order_total_fees)
            st.dataframe(recon_df, width="stretch", height=360)
            csv_out = result.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                label="下載本地預覽結果 (CSV)",
                data=csv_out,
                file_name="shopee_orders_processed.csv",
                mime="text/csv",
            )


if __name__ == "__main__":
    main()
