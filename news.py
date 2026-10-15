"""新闻源：Bing News 和 Google News 的 RSS 搜索。都免费、不要 key，也不抓任何网页正文。

两边同时搜、合并去重：Bing 带一句摘要，Google 更新更快、结果更多。
标题和摘要是第三方写的不可信文本，会进 Jev 的 state：只保留纯文本、截短，链接一律不打开。
任何错误都只记一条 warning 然后返回 []，不影响这一轮。
"""

from __future__ import annotations

import asyncio
import html
import logging
import re
import time
import unicodedata
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from itertools import zip_longest

import httpx

import config

log = logging.getLogger("news")

BING = "https://www.bing.com/news/search"
GOOGLE = "https://news.google.com/rss/search"
RETRY_STATUS = {429, 500, 502, 503, 504}
MIN_INTERVAL = 0.5        # 同一个源两次请求之间至少隔这么多秒（collect 会并发建十几份档案）
MAX_FEED_BYTES = 2_000_000
# 预测市场自己的盘口报道（含 "62.5% YES" 这种）等于把价格塞回 state，违背“state 里不放价格”的设计；
# 再加上直播盗链这类垃圾标题
SKIP = re.compile(r"polymarket|kalshi|predictit|prediction market|\d%\s*(?:yes|no)\b|live\s*stream", re.I)


@dataclass
class NewsItem:
    ts: float
    title: str
    summary: str  # Bing 的摘要；Google 没有摘要，用来源名代替


def clean(text: str | None, limit: int) -> str:
    """不可信文本 → 一行纯文本：去 HTML、实体、链接、控制字符和零宽/方向字符，再截短。"""
    text = html.unescape(re.sub(r"<[^>]*>", " ", text or ""))
    text = re.sub(r"https?://\S+", " ", text)
    text = "".join(" " if unicodedata.category(c) in ("Cc", "Cf") else c for c in text)
    text = re.sub(r"\s+", " ", text).strip()
    return text if len(text) <= limit else text[:limit - 1].rstrip() + "…"


def _key(title: str) -> str:
    return re.sub(r"\W+", "", title.lower())[:50]


def parse_rss(raw: bytes) -> list[NewsItem]:
    items = []
    for it in ET.fromstring(raw).iter("item"):
        try:
            ts = parsedate_to_datetime(it.findtext("pubDate") or "").timestamp()
        except (TypeError, ValueError):
            continue
        title = clean(it.findtext("title"), 300)
        # Google 的 <source>、Bing 的 <News:Source>（带命名空间）
        source = clean(it.findtext("source") or it.findtext("{*}Source"), 300)
        if source and title.endswith(f" - {source}"):
            title = title[:-len(source) - 3]  # Google 会把来源拼在标题后面
        summary = clean(it.findtext("description"), 400)
        if _key(summary).startswith(_key(title)):
            summary = ""  # Google 的 description 只是标题加来源的链接
        title = clean(title, 140)
        if title:
            items.append(NewsItem(ts, title, clean(summary, 160) or clean(source, 40)))
    return items


class NewsFeed:
    """一轮一个实例：缓存只在本轮有效，下一轮重新搜。"""

    def __init__(self, now: float):
        self.since = now - config.NEWS_MAX_AGE_DAYS * 86_400
        self._http = httpx.AsyncClient(
            timeout=15, follow_redirects=True,
            headers={"user-agent": "Mozilla/5.0 (X11; Linux x86_64) jev-desk/0.1"},
        )
        self._locks = {BING: asyncio.Lock(), GOOGLE: asyncio.Lock()}
        self._last = {BING: 0.0, GOOGLE: 0.0}
        self._cache: dict[tuple[str, str], asyncio.Task] = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self._http.aclose()

    async def _get(self, url: str, params: dict) -> list[NewsItem]:
        for attempt in range(2):
            async with self._locks[url]:  # 按源排队，保证请求间隔
                await asyncio.sleep(max(0.0, self._last[url] + MIN_INTERVAL - time.monotonic()))
                self._last[url] = time.monotonic()
                r = await self._http.get(url, params=params)
            if r.status_code == 200:
                if len(r.content) > MAX_FEED_BYTES:
                    raise ValueError(f"feed too large ({len(r.content)} bytes)")
                return parse_rss(r.content)
            if r.status_code not in RETRY_STATUS or attempt:
                r.raise_for_status()
            await asyncio.sleep(2)
        return []

    async def _source(self, url: str, params: dict) -> list[NewsItem]:
        try:
            return await self._get(url, params)
        except Exception as e:  # 网络、HTTP 状态、XML 解析，统统软失败
            log.warning("news %s failed for %r: %s %s", url.split("/")[2], params["q"], type(e).__name__, e)
            return []

    async def _search(self, query: str, focus: str) -> list[NewsItem]:
        bing, google = await asyncio.gather(
            self._source(BING, {"q": query, "format": "rss", "mkt": "en-US"}),
            self._source(GOOGLE, {"q": f"{query} when:{config.NEWS_MAX_AGE_DAYS}d",
                                  "hl": "en-US", "gl": "US", "ceid": "US:en"}),
        )
        # 粗略的相关性门槛：focus 里至少命中三分之一的词（按词首匹配）。搜索引擎搜不全时会拿
        # 部分命中的结果凑数，比如同一赛事的别的场次
        stems = [t[:-1] if len(t) > 4 and t.endswith("s") else t for t in focus.split() if len(t) >= 3]
        terms = [re.compile(rf"\b{re.escape(t)}", re.I) for t in stems]  # "rates" 也能命中 "rate"
        need = -(-len(terms) // 3)  # 向上取整；没有可用的词时为 0，不做检查

        def keep(n: NewsItem) -> bool:
            text = f"{n.title} {n.summary}"
            return (n.ts >= self.since and not SKIP.search(text)
                    and sum(bool(t.search(text)) for t in terms) >= need)

        # 两边各自已按相关性排好，交替取，Bing 优先（有摘要），标题去重；最后按时间从新到旧
        picked, seen = [], set()
        for pair in zip_longest(filter(keep, bing), filter(keep, google)):
            for n in pair:
                if n and _key(n.title) not in seen and len(picked) < config.NEWS_MAX_ITEMS:
                    seen.add(_key(n.title))
                    picked.append(n)
        return sorted(picked, key=lambda n: n.ts, reverse=True)

    async def search(self, query: str, focus: str) -> list[NewsItem]:
        """focus 是相关性检查用的词。同一轮里相同的查询只发一次请求，并发的相同查询共享同一个任务。"""
        key = (query, focus)
        if key not in self._cache:
            self._cache[key] = asyncio.ensure_future(self._search(query, focus))
        return await self._cache[key]
