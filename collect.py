"""宇宙 → 硬过滤 → 盘口过滤 → 档案（给 Jev 看的 state）。"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone

import config
from filters import book_reject, hard_reject
from news import NewsFeed
from polymarket import Book, Market, Polymarket

log = logging.getLogger("collect")


@dataclass
class Dossier:
    market: Market
    books: list[Book]  # 与 market.outcomes 一一对应
    trades_24h: int
    state: dict


def _utc(ts: float, fmt: str) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime(fmt)


def time_left(seconds: float) -> str:
    # Jev 不会做日期运算，剩余时间由代码算好再给它
    hours = seconds / 3600
    if hours < 48:
        return f"about {round(hours)} hours"
    return f"about {round(hours / 24)} days"


_MONTH = r"(?:january|february|march|april|may|june|july|august|september|october|november|december)"
_DATE = re.compile(
    rf"\b(?:(?:the\s+)?end\s+of\s+)?{_MONTH}(?:\s+\d{{1,2}}(?:st|nd|rd|th)?)?(?:\s*[-–]\s*\d{{1,2}})?(?:,?\s+\d{{4}})?\b"
    r"|\b\d{4}-\d{2}-\d{2}\b",
    re.I,
)
# 年份、价格、比分线、区间：文章里几乎不会原样出现，留着只会让搜索变窄
_NUMBER = re.compile(r"(?<!\w)[$€£]?\d[\d,.]*(?:\s*(?:%|bps\b|bn\b|[kmb]\b))?(?:\s*[-–]\s*\d[\d,.]*%?)?", re.I)
_STOPWORDS = frozenset("""
    a an the and or of to in on at by for with from as into than
    will would be is are was there any no not have has do does
    who which what when how next this that its it win wins won
    after before through until during between out vs v x fc
    above below less more over under spread o/u am pm et
""".split())


def _keywords(text: str) -> list[str]:
    text = re.sub(r"\([^)]*\)", " ", text)  # (BO3)、(-2.5)、(WTI)
    text = _NUMBER.sub(" ", _DATE.sub(" ", text))
    words = [w.strip(".'-") for w in re.findall(r"[\w'.&/-]+", text)]
    return [w for w in words if len(w) > 1 and w.lower() not in _STOPWORDS]


def news_query(m: Market) -> tuple[str, str]:
    """题目 → (搜索词, 相关性检查用的词)。纯代码完成，不让 Jev 参与。"""
    vs = re.compile(r"\svs?\.?\s", re.I)
    if not vs.search(m.event_title):
        query = " ".join(_keywords(m.question)[:8])  # 词太多搜索引擎会要求全部命中，结果反而变少
        return query, query
    # 比赛（让分、大小分、谁赢）：事件名里两边都有，比 "Spread: Seahawks (-2.5)" 好搜。
    # 去掉 " - 赛事阶段" 后缀；"赛事:" 前缀留在搜索词里帮搜索引擎定位，
    # 但相关性只看两边的名字，否则同一赛事的别的场次也会混进来
    event = re.sub(r"\s[-–—]\s.*", " ", m.event_title)
    sides = next(part for part in event.split(":") if vs.search(part))
    return " ".join(_keywords(event)[:8]), " ".join(_keywords(sides))


async def fetch_news(feed: NewsFeed, m: Market) -> list[str]:
    # 档案里最值钱的一块：Jev 只能看到你喂给它的东西，没有新闻它基本只能靠题目判断。
    # 返回 "日期 - 标题 - 摘要"，按时间从新到旧；搜不到或出错都是 []（news.py 里已软失败）
    query, focus = news_query(m)
    if not query:
        return []
    return [" - ".join(filter(None, (_utc(n.ts, "%Y-%m-%d"), n.title, n.summary)))
            for n in await feed.search(query, focus)]


def build_state(m: Market, now: float, news: list[str]) -> dict:
    # 故意不放价格：要的是 Jev 独立的判断，放了价格它会锚定市场价
    state = {
        "today": _utc(now, "%Y-%m-%d"),
        "question": m.question,
        "outcomes": m.outcomes,
        "event": m.event_title,
        "resolution_rules": m.rules,
        "resolution_source": m.resolution_source or "not stated",
        "trading_closes": _utc(m.end_ts, "%Y-%m-%d %H:%M UTC"),
        "time_left": time_left(m.end_ts - now),
    }
    if news:
        state["news"] = news
    return state


def rules_state(m: Market) -> dict:
    # 单独问规则质量时用：只有题目和规则，没有新闻和时间
    return {
        "question": m.question,
        "outcomes": m.outcomes,
        "resolution_rules": m.rules,
        "resolution_source": m.resolution_source or "not stated",
    }


async def _dossier(pm: Polymarket, feed: NewsFeed, m: Market, now: float) -> Dossier:
    books, trades, news = await asyncio.gather(
        pm.books(m.token_ids), pm.trades_24h(m.condition_id), fetch_news(feed, m)
    )
    empty = Book([], [])
    return Dossier(m, [books.get(t, empty) for t in m.token_ids], trades, build_state(m, now, news))


async def collect(pm: Polymarket, ledger) -> list[Dossier]:
    now = time.time()
    universe = await pm.universe(config.UNIVERSE_PAGES, config.MAX_DAYS_TO_END)
    skip = ledger.benched(now) | ledger.held_condition_ids()
    held_events = ledger.held_event_ids()

    rejects: Counter[str] = Counter()
    shortlist = []
    for m in universe:
        reason = hard_reject(m, now)
        if reason is None and m.condition_id in skip:
            reason = "benched_or_held"
        if reason is None and m.event_id in held_events:
            reason = "event_held"  # 同一事件下的互斥结果高度相关，只拿一个
        if reason:
            rejects[reason] += 1
            continue
        shortlist.append(m)
        if len(shortlist) >= config.MAX_DOSSIERS:
            break

    async with NewsFeed(now) as feed:  # 新闻缓存只在这一轮有效
        results = await asyncio.gather(*(_dossier(pm, feed, m, now) for m in shortlist), return_exceptions=True)
    dossiers = []
    for m, d in zip(shortlist, results):
        if isinstance(d, Exception):
            log.warning("dossier failed %s: %s", m.condition_id[:10], d)
            continue
        reason = book_reject(d)
        if reason:
            rejects[reason] += 1
            ledger.bench(m.condition_id, config.BENCH_BOOK_HOURS, reason, now)
            continue
        dossiers.append(d)

    log.info("universe %d → dossiers %d (with news %d) | rejects %s",
             len(universe), len(dossiers), sum("news" in d.state for d in dossiers), dict(rejects))
    return dossiers
