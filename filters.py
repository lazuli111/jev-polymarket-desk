"""三道关：硬过滤（纯代码）、盘口过滤（CLOB）、软过滤（Jev 的回答）。

每个函数返回拒绝原因，通过时返回 None。
"""

from __future__ import annotations

import config


def hard_reject(m, now: float) -> str | None:
    if not m.tradable:
        return "not_tradable"
    if m.category.startswith(config.EXCLUDE_CATEGORIES):
        return "excluded_category"
    hours = (m.end_ts - now) / 3600
    if hours < config.MIN_HOURS_TO_END:
        return "ends_too_soon"
    if hours > config.MAX_DAYS_TO_END * 24:
        return "ends_too_late"
    if m.liquidity < config.MIN_LIQUIDITY:
        return "low_liquidity"
    if m.volume_24h < config.MIN_VOLUME_24H:
        return "low_volume"
    if m.mid is None:
        return "no_quote"
    low, high = config.PRICE_BAND
    if not low <= m.mid <= high:
        return "price_extreme"
    if m.best_ask - m.best_bid > config.MAX_SPREAD:
        return "wide_spread"
    if m.move_1h > config.MAX_MOVE_1H:
        return "moving_fast"
    return None


def book_reject(d) -> str | None:
    for book in d.books:
        if book.best_bid is None or book.best_ask is None:
            return "empty_book"
        if (book.depth_usd("ask", config.DEPTH_BAND) < config.MIN_DEPTH_USD
                or book.depth_usd("bid", config.DEPTH_BAND) < config.MIN_DEPTH_USD):
            return "thin_book"
    if d.trades_24h < config.MIN_TRADES_24H:
        return "few_trades"
    return None


def soft_reject(j) -> str | None:
    if j.rules_clear < config.MIN_RULES_CLEAR:
        return "rules_unclear"
    if j.dispute_risk > config.MAX_DISPUTE_RISK:
        return "dispute_risk"
    if j.info < config.MIN_INFO_SCORE:
        return "too_little_info"
    return None
