# -*- coding: utf-8 -*-
"""
雙邊共用的「比對用」字串正規化：與階段一 `clean_name_for_simplified` 邏輯一致。
用於蝦皮 `清洗後簡體名稱` 與 Google Sheet 雲端列之 `_雲端正規化簡體比對用`。
"""

from __future__ import annotations

import re

import pandas as pd
from opencc import OpenCC

# 與階段一相同：標點／括號等 → 半形空白
SPECIAL_CHARS_PATTERN = re.compile(
    r"[\[\]【】()（）｜_×,，、?？]+"
)

EMOJI_PATTERN = re.compile(
    "["
    "\U0001F300-\U0001F9FF"
    "\U00002600-\U000027BF"
    "\U0001F600-\U0001F64F"
    "\U0001F680-\U0001F6FF"
    "\U00002700-\U000027BF"
    "\U0000FE00-\U0000FE0F"
    "\U0001F900-\U0001F9FF"
    "]+",
    flags=re.UNICODE,
)

NOISE_PHRASES = [
    "🏠",
    "💥",
    "宅玩瘋",
    "現貨：24 小時內出貨",
    "台灣現貨＋預定",
    "台灣現貨+預定",
    "現貨/預定",
]

_cc_t2s: OpenCC | None = None


def get_opencc_t2s() -> OpenCC:
    global _cc_t2s
    if _cc_t2s is None:
        _cc_t2s = OpenCC("t2s")
    return _cc_t2s


def normalize_for_match(text: object) -> str:
    """
    Regex 清洗（標點、括號、Emoji、雜訊片語等）→ 繁轉簡。
    與階段一 `clean_name_for_simplified` 完全相同。
    """
    if not isinstance(text, str):
        text = "" if pd.isna(text) else str(text)

    s = re.sub(r"[?？]+", " ", text)
    s = EMOJI_PATTERN.sub(" ", s)
    s = SPECIAL_CHARS_PATTERN.sub(" ", s)
    for phrase in NOISE_PHRASES:
        s = s.replace(phrase, " ")
    s = re.sub(r"\s+", " ", s).strip()
    return get_opencc_t2s().convert(s)
