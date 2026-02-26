"""Encoding detection for raw file bytes.

Strategy (in order):
1. BOM sniffing  — instant, zero-cost, reliable for UTF-8/16/32 files.
2. chardet       — statistical analysis; used when BOM is absent.
3. Hardcoded fallbacks — latin-1 never fails (every byte is valid).
"""
from __future__ import annotations

import codecs
import logging
from typing import NamedTuple

logger = logging.getLogger(__name__)

# Minimum confidence from chardet before we trust the result.
_MIN_CONFIDENCE = 0.70

# Ordered list of encodings to try if detection confidence is too low.
_FALLBACK_CHAIN = ["utf-8", "latin-1", "cp1252", "utf-16"]

# BOM → encoding mapping (checked before chardet).
_BOM_MAP: list[tuple[bytes, str]] = [
    (codecs.BOM_UTF32_LE, "utf-32-le"),
    (codecs.BOM_UTF32_BE, "utf-32-be"),
    (codecs.BOM_UTF16_LE, "utf-16-le"),
    (codecs.BOM_UTF16_BE, "utf-16-be"),
    (codecs.BOM_UTF8,     "utf-8-sig"),
]


class EncodingResult(NamedTuple):
    encoding: str
    confidence: float
    method: str   # "bom" | "chardet" | "fallback"


def detect(raw: bytes, sample_size: int = 65_536) -> EncodingResult:
    """Return the best encoding guess for *raw* bytes.

    Args:
        raw:         The complete file content (or at least a leading sample).
        sample_size: How many bytes to pass to chardet (default 64 KB).

    Returns:
        :class:`EncodingResult` with encoding name, confidence, and method.
    """
    # ── 1. BOM sniffing ───────────────────────────────────────────────────
    for bom, enc in _BOM_MAP:
        if raw.startswith(bom):
            logger.debug("encoding_bom_detected", encoding=enc)
            return EncodingResult(encoding=enc, confidence=1.0, method="bom")

    # ── 2. chardet ────────────────────────────────────────────────────────
    try:
        import chardet  # optional dependency — graceful fallback if absent
        result = chardet.detect(raw[:sample_size])
        enc = result.get("encoding") or ""
        conf = float(result.get("confidence") or 0.0)

        if enc and conf >= _MIN_CONFIDENCE:
            # Normalise aliases (e.g. "ascii" → "utf-8" is always safe)
            if enc.lower() == "ascii":
                enc = "utf-8"
            logger.debug("encoding_chardet", encoding=enc, confidence=conf)
            return EncodingResult(encoding=enc, confidence=conf, method="chardet")

        logger.warning(
            "encoding_chardet_low_confidence",
            encoding=enc,
            confidence=conf,
        )
    except ImportError:
        logger.warning("chardet_not_installed_falling_back")

    # ── 3. Fallback chain ─────────────────────────────────────────────────
    for enc in _FALLBACK_CHAIN:
        try:
            raw[:sample_size].decode(enc)
            logger.warning("encoding_fallback_used", encoding=enc)
            return EncodingResult(encoding=enc, confidence=0.0, method="fallback")
        except (UnicodeDecodeError, LookupError):
            continue

    # latin-1 is a guaranteed last resort (every byte 0x00-0xFF is defined).
    return EncodingResult(encoding="latin-1", confidence=0.0, method="fallback")
