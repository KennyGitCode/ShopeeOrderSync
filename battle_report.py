# -*- coding: utf-8 -*-
"""Batch battle report render + archive (no Streamlit session_state reads)."""

from __future__ import annotations

import hashlib
import html
import json
import os
import re
from datetime import datetime

import pandas as pd
import streamlit as st

try:
    import plotly.graph_objects as go
except Exception:
    go = None

REPORTS_ARCHIVE_DIRNAME = "reports_archive"


def _battle_metric_card_html(
    title: str,
    value_text: str,
    unit_text: str = "",
    *,
    accent: str = "#0f172a",
    badge_text: str = "",
    badge_bg: str = "#dcfce7",
    badge_fg: str = "#166534",
) -> str:
    unit_html = (
        f"<span style='font-size:0.78rem;color:#64748b;font-weight:700;margin-left:4px;'>{html.escape(unit_text)}</span>"
        if unit_text
        else ""
    )
    badge_html = (
        "<div style='margin-top:10px;'>"
        f"<span style='display:inline-flex;align-items:center;padding:2px 8px;border-radius:999px;"
        f"background:{badge_bg};color:{badge_fg};font-size:0.74rem;font-weight:800;'>"
        f"{html.escape(badge_text)}</span></div>"
        if badge_text
        else ""
    )
    return (
        "<div style='height:100%;min-height:104px;border:1px solid #eceff3;border-radius:8px;"
        "background:#fafafa;box-shadow:0 1px 3px rgba(15,23,42,0.06);padding:15px;"
        "display:flex;flex-direction:column;justify-content:flex-start;box-sizing:border-box;'>"
        f"<div style='font-size:14px;color:#666;font-weight:700;line-height:1.25;white-space:normal;'>{html.escape(title)}</div>"
        f"<div style='margin-top:8px;display:flex;align-items:baseline;gap:2px;flex-wrap:wrap;'>"
        f"<span style='font-size:2rem;line-height:1.1;font-weight:900;color:{accent};font-variant-numeric:tabular-nums;'>{html.escape(value_text)}</span>"
        f"{unit_html}</div>"
        f"{badge_html}</div>"
    )


def _reports_archive_dir() -> str:
    d = os.path.join(os.path.dirname(__file__), REPORTS_ARCHIVE_DIRNAME)
    os.makedirs(d, exist_ok=True)
    return d


def save_to_archive(report_data: dict[str, object]) -> str:
    if not isinstance(report_data, dict):
        return ""
    base_dir = _reports_archive_dir()
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    n = 0
    while True:
        suffix = f"_{n}" if n else ""
        name = f"report_{ts}{suffix}.json"
        path = os.path.join(base_dir, name)
        if not os.path.isfile(path):
            break
        n += 1
    payload = dict(report_data)
    payload["archived_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return name


def list_report_archive_json_paths() -> list[str]:
    d = _reports_archive_dir()
    if not os.path.isdir(d):
        return []
    out: list[str] = []
    for fn in os.listdir(d):
        if fn.startswith("report_") and fn.lower().endswith(".json"):
            out.append(os.path.join(d, fn))
    out.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return out


def _battle_report_widget_scope(report_data: dict[str, object]) -> str:
    for k in ("_widget_key_scope", "_archive_basename"):
        v = str(report_data.get(k) or "").strip()
        if v:
            return re.sub(r"[^\w\-]", "_", v)[:48]
    raw = "|".join(
        [
            str(report_data.get("batch_id") or ""),
            str(report_data.get("csv_fp") or ""),
            str(report_data.get("created_at") or ""),
            str(report_data.get("archived_at") or ""),
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def render_battle_report(report_data: dict[str, object]) -> None:
    scope = _battle_report_widget_scope(report_data)
    bid = str(report_data.get("batch_id", "") or "").strip() or "na"
    rows_df = pd.DataFrame(report_data.get("audit_rows") or [])

    col_rev = "\u5be6\u552e\u6536\u5165"
    col_fee = "\u624b\u7e8c\u8cbb"
    col_cost = "\u6210\u672c"
    col_profit = "\u6de8\u5229\u6f64"
    col_fee_rate = "\u624b\u7e8c\u8cbb\u7387"
    col_name = "\u5546\u54c1\u539f\u59cb\u540d\u7a31"

    if not rows_df.empty:
        total_rev = int(round(pd.to_numeric(rows_df[col_rev], errors="coerce").fillna(0.0).sum()))
        total_fee = int(round(pd.to_numeric(rows_df[col_fee], errors="coerce").fillna(0.0).sum()))
        total_cost = int(round(pd.to_numeric(rows_df[col_cost], errors="coerce").fillna(0.0).sum()))
        total_profit = int(round(pd.to_numeric(rows_df[col_profit], errors="coerce").fillna(0.0).sum()))
        avg_fee_rate = (float(total_fee) / float(total_rev)) if total_rev > 0 else 0.0
    else:
        total_rev = int(report_data.get("total_revenue", 0) or 0)
        total_fee = int(report_data.get("total_fee", 0) or 0)
        total_cost = int(report_data.get("total_cost", 0) or 0)
        avg_fee_rate = float(report_data.get("avg_fee_rate", 0.0) or 0.0)
        total_profit = int(report_data.get("total_profit", 0) or 0)
    margin_pct = (float(total_profit) / float(total_rev) * 100.0) if total_rev > 0 else 0.0
    rank_text = "\U0001f947 Rank A\uff1a\u7a69\u5b9a\u8f38\u51fa\uff01\u5065\u5eb7\u7684\u71df\u904b\u6279\u6b21\u3002"
    rank_color = "#2563eb"
    if margin_pct > 20:
        rank_text = "\U0001f3c6 Rank S\uff1a\u66b4\u5229\u6536\u5272\u6a5f\uff01\u672c\u6279\u6b21\u5229\u6f64\u6975\u4f73\uff01"
        rank_color = "#047857"
    elif 10 <= margin_pct <= 20:
        rank_text = "\U0001f947 Rank A\uff1a\u7a69\u5b9a\u8f38\u51fa\uff01\u5065\u5eb7\u7684\u71df\u904b\u6279\u6b21\u3002"
        rank_color = "#2563eb"
    elif 5 <= margin_pct < 10:
        rank_text = "\u26a0\ufe0f Rank B\uff1a\u8584\u5229\u591a\u92b7\u3002\u7559\u610f\u6f5b\u5728\u7684\u6210\u672c\u6ce2\u52d5\u3002"
        rank_color = "#b45309"
    elif margin_pct < 5:
        rank_text = (
            "\U0001f6a8 Rank C\uff1a\u8766\u76ae\u6253\u5de5\u4ed4\u8b66\u5831\uff01"
            "\u5229\u6f64\u904e\u4f4e\uff0c\u8acb\u7acb\u523b\u6aa2\u8996\u624b\u7e8c\u8cbb\u8207\u5b9a\u50f9\uff01"
        )
        rank_color = "#b91c1c"

    is_preview_mode = bool(report_data.get("_preview_mode", False))
    if is_preview_mode:
        st.warning("⚠️ 目前為預覽模式，資料尚未寫入雲端")

    tab_overview, tab_audit, tab_debug = st.tabs(
        ["📊 數據總覽", "🧾 查帳明細", "⚙️ 進階除錯"]
    )

    with tab_overview:
        st.markdown(
            (
                "<div style='border:1px solid #e5e7eb;border-radius:12px;padding:10px 12px;"
                "background:linear-gradient(135deg,#f8fafc 0%,#ffffff 100%);margin-bottom:10px;'>"
                f"<div style='font-size:1.05rem;font-weight:900;color:{rank_color};'>{html.escape(rank_text)}</div>"
                f"<div style='font-size:0.82rem;color:#64748b;margin-top:3px;'>\u6bdb\u5229\u7387\uff1a{margin_pct:.2f}%</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )

        left_metrics, right_chart = st.columns([1.35, 1.0])
        with left_metrics:
            h1, h2, h3, h4 = st.columns(4)
            with h1:
                st.markdown(
                    _battle_metric_card_html(
                        "\U0001f4b0 \u7e3d\u5be6\u6536",
                        f"{total_rev:,}",
                        "TWD",
                        accent="#0f172a",
                    ),
                    unsafe_allow_html=True,
                )
            with h2:
                st.markdown(
                    _battle_metric_card_html(
                        "\U0001f4b8 \u7e3d\u624b\u7e8c\u8cbb",
                        f"{total_fee:,}",
                        "TWD",
                        accent="#7c2d12",
                    ),
                    unsafe_allow_html=True,
                )
            with h3:
                fee_title = (
                    "\U0001f4ca \u5e73\u5747\u8cbb\u7387 \u26a0\ufe0f"
                    if (avg_fee_rate * 100.0) > 11.0
                    else "\U0001f4ca \u5e73\u5747\u8cbb\u7387"
                )
                st.markdown(
                    _battle_metric_card_html(
                        fee_title,
                        f"{avg_fee_rate * 100.0:.2f}",
                        "%",
                        accent="#1d4ed8",
                    ),
                    unsafe_allow_html=True,
                )
            with h4:
                badge_text = f"\u6bdb\u5229\u7387 {margin_pct:.1f}%"
                badge_bg = "#dcfce7" if total_profit >= 0 else "#fee2e2"
                badge_fg = "#166534" if total_profit >= 0 else "#b91c1c"
                st.markdown(
                    _battle_metric_card_html(
                        "\U0001f3c6 \u6de8\u5229\u6f64",
                        f"{total_profit:,}",
                        "TWD",
                        accent="#047857" if total_profit >= 0 else "#b91c1c",
                        badge_text=badge_text,
                        badge_bg=badge_bg,
                        badge_fg=badge_fg,
                    ),
                    unsafe_allow_html=True,
                )
        with right_chart:
            donut_profit = max(0, int(total_profit))
            if go is not None:
                fig = go.Figure(
                    data=[
                        go.Pie(
                            labels=[
                                "\u7e3d\u6210\u672c",
                                "\u7e3d\u624b\u7e8c\u8cbb",
                                "\u7e3d\u6de8\u5229\u6f64",
                            ],
                            values=[max(0, int(total_cost)), max(0, int(total_fee)), donut_profit],
                            hole=0.62,
                            marker=dict(colors=["#64748b", "#f59e0b", "#10b981"]),
                            textinfo="label+percent",
                        )
                    ]
                )
                fig.update_layout(
                    margin=dict(l=0, r=0, t=6, b=0),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    showlegend=False,
                )
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            else:
                st.caption(
                    "\u2139\ufe0f \u672a\u5b89\u88dd plotly\uff0c\u66ab\u4ee5\u6587\u5b57\u7248\u71df\u6536\u7d50\u69cb\u986f\u793a\u3002"
                )
                st.dataframe(
                    pd.DataFrame(
                        [
                            {"\u9805\u76ee": "\u7e3d\u6210\u672c", "\u91d1\u984d (TWD)": f"{max(0, int(total_cost)):,}"},
                            {"\u9805\u76ee": "\u7e3d\u624b\u7e8c\u8cbb", "\u91d1\u984d (TWD)": f"{max(0, int(total_fee)):,}"},
                            {"\u9805\u76ee": "\u7e3d\u6de8\u5229\u6f64", "\u91d1\u984d (TWD)": f"{donut_profit:,}"},
                        ]
                    ),
                    hide_index=True,
                    width="stretch",
                    height=145,
                )
            if total_profit < 0:
                st.caption(
                    "\u26a0\ufe0f \u672c\u6279\u6b21\u6de8\u5229\u70ba\u8ca0\uff0c"
                    "\u5713\u9905\u5716\u4e4b\u6de8\u5229\u6247\u5340\u4ee5 0 \u5448\u73fe\u3002"
                )

        if rows_df.empty:
            st.info(
                "\u76ee\u524d\u50c5\u6709\u6458\u8981\u8cc7\u6599\uff0c\u672a\u80fd\u53d6\u5f97\u672c\u6279\u6b21\u660e\u7d30\u9805\u76ee\uff0c"
                "\u884c\u52d5\u6d1e\u5bdf\u66ab\u4e0d\u53ef\u7528\u3002"
            )
        else:
            low_profit_df = rows_df[rows_df[col_profit] < 0].copy()
            high_fee_df = rows_df[rows_df[col_fee_rate] > 0.13].copy()
            guardians_df = rows_df.reindex(
                rows_df[col_profit].abs().sort_values(ascending=False).index
            ).head(2).copy()

            st.markdown("#### \U0001f9ed \u71df\u904b\u884c\u52d5\u6e05\u55ae")
            if not low_profit_df.empty:
                st.error(
                    "\U0001fa78 \u505a\u767d\u5de5/\u865b\u640d\u6e05\u55ae\uff1a\u5f37\u70c8\u5efa\u8b70\u8abf\u6f32\u552e\u50f9\u3002"
                )
                low_profit_show = (
                    low_profit_df[[col_name, col_profit]]
                    .rename(columns={col_profit: "\u5229\u6f64 (TWD)"})
                    .assign(
                        **{
                            "\u5229\u6f64 (TWD)": lambda d: d["\u5229\u6f64 (TWD)"].round(0).astype(int)
                        }
                    )
                )
                with st.expander(
                    f"\U0001fa78 \u67e5\u770b\u505a\u767d\u5de5/\u865b\u640d\u6e05\u55ae\uff08{len(low_profit_show)} \u7b46\uff09",
                    expanded=False,
                ):
                    st.dataframe(
                        low_profit_show,
                        hide_index=True,
                        width="stretch",
                        height=170,
                    )
                    st.download_button(
                        label="\u4e0b\u8f09\u505a\u767d\u5de5/\u865b\u640d\u6e05\u55ae (CSV)",
                        data=low_profit_show.to_csv(index=False).encode("utf-8-sig"),
                        file_name="audit_low_profit_list.csv",
                        mime="text/csv",
                        key=f"dl_low_profit_{scope}_{bid}",
                    )
            else:
                st.success(
                    "\U0001fa78 \u505a\u767d\u5de5/\u865b\u640d\u6e05\u55ae\uff1a"
                    "\u672c\u6279\u6b21\u672a\u767c\u73fe\u5229\u6f64 < 0 \u7684\u5546\u54c1\u3002"
                )

            if not high_fee_df.empty:
                st.error(
                    "\U0001f9db \u9ad8\u62bd\u6210\u523a\u5ba2\u6e05\u55ae\uff1a"
                    "\u8acb\u6aa2\u67e5\u662f\u5426\u8aa4\u958b\u514d\u904b\u6216\u4fc3\u92b7\u6d3b\u52d5\u3002"
                )
                high_fee_show = (
                    high_fee_df[[col_name, col_fee_rate]]
                    .assign(
                        **{
                            col_fee_rate: lambda d: (d[col_fee_rate] * 100.0)
                            .round(2)
                            .map(lambda v: f"{v:.2f}%")
                        }
                    )
                )
                with st.expander(
                    f"\U0001f9db \u67e5\u770b\u9ad8\u62bd\u6210\u523a\u5ba2\u6e05\u55ae\uff08{len(high_fee_show)} \u7b46\uff09",
                    expanded=False,
                ):
                    st.dataframe(
                        high_fee_show,
                        hide_index=True,
                        width="stretch",
                        height=170,
                    )
                    st.download_button(
                        label="\u4e0b\u8f09\u9ad8\u62bd\u6210\u523a\u5ba2\u6e05\u55ae (CSV)",
                        data=high_fee_show.to_csv(index=False).encode("utf-8-sig"),
                        file_name="audit_high_fee_list.csv",
                        mime="text/csv",
                        key=f"dl_high_fee_{scope}_{bid}",
                    )
            else:
                st.success(
                    "\U0001f9db \u9ad8\u62bd\u6210\u523a\u5ba2\u6e05\u55ae\uff1a"
                    "\u672c\u6279\u6b21\u672a\u767c\u73fe\u624b\u7e8c\u8cbb\u7387 > 13% \u7684\u5546\u54c1\u3002"
                )

            st.info(
                "\U0001f451 \u5229\u6f64\u5b88\u8b77\u8005\uff1a\u4e3b\u529b\u63a8\u5ee3\u5546\u54c1"
                "\uff08\u55ae\u7b46\u5229\u6f64\u7d55\u5c0d\u503c\u6700\u9ad8\u524d 2 \u540d\uff09\u3002"
            )
            guardians_show = (
                guardians_df[[col_name, col_profit]]
                .rename(columns={col_profit: "\u5229\u6f64 (TWD)"})
                .assign(
                    **{
                        "\u5229\u6f64 (TWD)": lambda d: d["\u5229\u6f64 (TWD)"].round(0).astype(int)
                    }
                )
            )
            with st.expander(
                f"\U0001f451 \u67e5\u770b\u5229\u6f64\u5b88\u8b77\u8005\uff08{len(guardians_show)} \u7b46\uff09",
                expanded=False,
            ):
                st.dataframe(
                    guardians_show,
                    hide_index=True,
                    width="stretch",
                    height=150,
                )
                st.download_button(
                    label="\u4e0b\u8f09\u5229\u6f64\u5b88\u8b77\u8005\u6e05\u55ae (CSV)",
                    data=guardians_show.to_csv(index=False).encode("utf-8-sig"),
                    file_name="audit_profit_guardians.csv",
                    mime="text/csv",
                    key=f"dl_guardians_{scope}_{bid}",
                )

    with tab_audit:
        audit_df = rows_df.copy() if isinstance(rows_df, pd.DataFrame) else pd.DataFrame()
        if audit_df.empty:
            st.info("\u76ee\u524d\u7121\u53ef\u986f\u793a\u7684\u67e5\u5e33\u660e\u7d30\u3002")
        else:
            audit_cols = [col_name, col_rev, col_cost, "\u6210\u672c\u4f86\u6e90", col_fee, col_profit]
            audit_view = audit_df[[c for c in audit_cols if c in audit_df.columns]].copy()
            for c in [col_rev, col_cost, col_fee, col_profit]:
                if c in audit_view.columns:
                    audit_view[c] = pd.to_numeric(audit_view[c], errors="coerce").fillna(0.0)
            totals = {
                col_rev: int(round(float(audit_view[col_rev].sum()))),
                col_cost: int(round(float(audit_view[col_cost].sum()))),
                col_fee: int(round(float(audit_view[col_fee].sum()))),
                col_profit: int(round(float(audit_view[col_profit].sum()))),
            }
            for c in [col_rev, col_cost, col_fee, col_profit]:
                audit_view[c] = audit_view[c].round(0).astype(int)
            st.dataframe(audit_view, hide_index=True, width="stretch", height=320)
            st.markdown(
                f"**\u7e3d\u8a08\u56de\u994b\uff5c{col_rev}\uff1a{totals[col_rev]:,}\uff5c"
                f"{col_cost}\uff1a{totals[col_cost]:,}\uff5c{col_fee}\uff1a{totals[col_fee]:,}\uff5c"
                f"{col_profit}\uff1a{totals[col_profit]:,}**"
            )
            st.download_button(
                label="\u4e0b\u8f09\u67e5\u5e33\u660e\u7d30\u8868 (CSV)",
                data=audit_view.to_csv(index=False).encode("utf-8-sig"),
                file_name="audit_detail_with_total.csv",
                mime="text/csv",
                key=f"dl_audit_detail_{scope}_{bid}",
            )

    with tab_debug:
        st.caption("\u9019\u88e1\u986f\u793a\u6e32\u67d3\u7528\u7684\u539f\u59cb\u6230\u5831\u8cc7\u6599\uff0c\u4fbf\u65bc\u5feb\u901f\u5c0d\u5e33\u8207\u9664\u932f\u3002")
        st.dataframe(rows_df if not rows_df.empty else pd.DataFrame(), width="stretch", height=320)
        debug_cols = [
            c for c in [col_name, col_rev, col_fee, col_cost, col_profit, col_fee_rate]
            if c in rows_df.columns
        ]
        if debug_cols:
            st.dataframe(rows_df[debug_cols], width="stretch", height=260)
