from pathlib import Path

p = Path(__file__).resolve().parent / "app.py"
text = p.read_text(encoding="utf-8")

new = '''def _paired_source_platform_badge_colors(
    stock_tag: str,
) -> tuple[tuple[str, str], tuple[str, str]]:
    """\u5de6\u53f3\u5217\u5fbd\u7ae0\u6210\u5c0d\u914d\u8272 (bg, fg)\u3002\u53f3\u6b04\u6587\u5b57\u8acb\u4ecd\u7528 expected_platform_for_stock_tag\u3002"""
    t = str(stock_tag or "").strip()
    if t == "\u9810\u5b9a":
        return ("#1e293b", "#f1f5f9"), ("#64748b", "#ffffff")
    if t == "\u73fe\u8ca8":
        return ("#0f766e", "#ecfdf5"), ("#2563eb", "#eff6ff")
    return ("#475569", "#f8fafc"), ("#94a3b8", "#ffffff")


'''

start = text.index("def _paired_source_platform_badge_colors(")
end = text.index("\ndef _tag_badge_html", start)
text = text[:start] + new + text[end + 1 :]

p.write_text(text, encoding="utf-8")
print("ok")
