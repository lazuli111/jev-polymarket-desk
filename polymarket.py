"""Polymarket 公开只读接口：Gamma（市场）、CLOB（盘口）、Data API（成交）。都不需要 key。"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import httpx

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
DATA = "https://data-api.polymarket.com"
RETRY_STATUS = {429, 500, 502, 503, 504}


def _jsonish(value, default):
    # Gamma 把 outcomes / outcomePrices / clobTokenIds 存成 JSON 字符串
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return default
    return default if value is None else value


def _float(value, default=None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


@dataclass
class Market:
    condition_id: str
    question: str
    rules: str
    resolution_source: str
    event_id: str
    event_title: str
    outcomes: list[str]      # 两个结果，通常是 ["Yes", "No"]，体育盘可能是两支队名
    token_ids: list[str]     # 与 outcomes 一一对应
    end_ts: float
    best_bid: float | None   # 第一个结果的最优买价
    best_ask: float | None
    liquidity: float
    volume_24h: float
    volume_1wk: float
    move_1h: float
    fee_rate: float          # taker 费率，不收费的市场为 0
    min_order_size: float
    tradable: bool
    resolved_a: float | None  # 已结算时第一个结果的结算价（1 / 0 / 0.5），否则 None

    @property
    def mid(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2

    @classmethod
    def from_gamma(cls, raw: dict) -> Market | None:
        outcomes = _jsonish(raw.get("outcomes"), [])
        tokens = _jsonish(raw.get("clobTokenIds"), [])
        if len(outcomes) != 2 or len(tokens) != 2 or not raw.get("endDate"):
            return None
        event = (raw.get("events") or [{}])[0]
        fee = raw.get("feeSchedule") or {}
        resolved_a = None
        if raw.get("closed"):
            prices = _jsonish(raw.get("outcomePrices"), [])
            price = _float(prices[0]) if prices else None
            if price in (0.0, 0.5, 1.0):
                resolved_a = price
        return cls(
            condition_id=raw["conditionId"],
            question=raw.get("question") or "",
            rules=raw.get("description") or "",
            resolution_source=raw.get("resolutionSource") or "",
            event_id=str(event.get("id") or raw["conditionId"]),
            event_title=event.get("title") or "",
            outcomes=[str(o) for o in outcomes],
            token_ids=[str(t) for t in tokens],
            end_ts=datetime.fromisoformat(raw["endDate"].replace("Z", "+00:00")).timestamp(),
            best_bid=_float(raw.get("bestBid")),
            best_ask=_float(raw.get("bestAsk")),
            liquidity=_float(raw.get("liquidityNum"), 0.0),
            volume_24h=_float(raw.get("volume24hr"), 0.0),
            volume_1wk=_float(raw.get("volume1wk"), 0.0),
            move_1h=abs(_float(raw.get("oneHourPriceChange"), 0.0)),
            fee_rate=_float(fee.get("rate"), 0.0) if raw.get("feesEnabled") else 0.0,
            min_order_size=_float(raw.get("orderMinSize"), 5.0),
            tradable=bool(
                raw.get("active") and not raw.get("closed")
                and raw.get("acceptingOrders") and raw.get("enableOrderBook")
            ),
            resolved_a=resolved_a,
        )


@dataclass
class Book:
    bids: list[tuple[float, float]]  # (价格, 股数)，价格从高到低
    asks: list[tuple[float, float]]  # 价格从低到高

    @classmethod
    def from_clob(cls, raw: dict) -> Book:
        def levels(side):
            return [(float(lv["price"]), float(lv["size"])) for lv in raw.get(side) or []]
        return cls(sorted(levels("bids"), reverse=True), sorted(levels("asks")))

    @property
    def best_bid(self) -> float | None:
        return self.bids[0][0] if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return self.asks[0][0] if self.asks else None

    def depth_usd(self, side: str, band: float) -> float:
        levels = self.asks if side == "ask" else self.bids
        if not levels:
            return 0.0
        best = levels[0][0]
        return sum(p * s for p, s in levels if abs(p - best) <= band + 1e-9)

    def buy(self, usd: float) -> list[tuple[float, float]]:
        """花 usd 往上吃卖单，返回逐档成交的 (价格, 股数)。"""
        fills, left = [], usd
        for price, size in self.asks:
            if left <= 1e-9:
                break
            take = min(size, left / price)
            fills.append((price, take))
            left -= take * price
        return fills

    def sell(self, shares: float) -> list[tuple[float, float]]:
        """卖出 shares 股，往下吃买单。"""
        fills, left = [], shares
        for price, size in self.bids:
            if left <= 1e-9:
                break
            take = min(size, left)
            fills.append((price, take))
            left -= take
        return fills


class Polymarket:
    def __init__(self, concurrency: int = 4):
        self._http = httpx.AsyncClient(timeout=20, headers={"user-agent": "jev-desk/0.1"})
        self._sem = asyncio.Semaphore(concurrency)

    async def close(self):
        await self._http.aclose()

    async def _call(self, method: str, url: str, **kw):
        error = None
        for attempt in range(4):
            async with self._sem:
                try:
                    r = await self._http.request(method, url, **kw)
                except httpx.TransportError as e:
                    error = e
                else:
                    if r.status_code == 200:
                        return r.json()
                    if r.status_code not in RETRY_STATUS:
                        r.raise_for_status()
                    error = f"HTTP {r.status_code}"
            await asyncio.sleep(2 ** attempt)
        raise RuntimeError(f"{method} {url}: {error}")

    async def universe(self, pages: int, max_days: float) -> list[Market]:
        now = datetime.now(timezone.utc)
        fmt = "%Y-%m-%dT%H:%M:%SZ"
        params = {
            "active": "true", "closed": "false", "limit": 100,
            "order": "volume24hr", "ascending": "false",
            "end_date_min": now.strftime(fmt),
            "end_date_max": (now + timedelta(days=max_days)).strftime(fmt),
        }
        out = []
        for page in range(pages):
            rows = await self._call("GET", f"{GAMMA}/markets", params={**params, "offset": page * 100})
            out += [m for m in map(Market.from_gamma, rows) if m]
            if len(rows) < 100:
                break
        return out

    async def by_condition(self, condition_ids: list[str]) -> list[Market]:
        """按 conditionId 查市场，已结算的也能查到（用来结算持仓和校准记录）。"""
        # Gamma 的坑：每页默认只回 20 条；不写 closed=true 查不到已结算的；带 limit 时已结算的也会丢
        found: dict[str, Market] = {}
        ids = list(dict.fromkeys(condition_ids))
        for i in range(0, len(ids), 20):
            for closed in ("false", "true"):
                params = [("condition_ids", c) for c in ids[i:i + 20]] + [("closed", closed)]
                rows = await self._call("GET", f"{GAMMA}/markets", params=params)
                for m in filter(None, map(Market.from_gamma, rows)):
                    found[m.condition_id] = m
        return list(found.values())

    async def raw_markets(self, params: dict, pages: int) -> list[dict]:
        """Gamma 原始市场列表（分页）。短周期分析要用 gameStartTime、seriesSlug 这些原始字段。"""
        rows: list[dict] = []
        for page in range(min(pages, 21)):  # Gamma 的 offset 超过 2000 会返回 422
            batch = await self._call("GET", f"{GAMMA}/markets", params={**params, "limit": 100, "offset": page * 100})
            rows += batch
            if len(batch) < 100:
                break
        return rows

    async def price_history(self, token_id: str, start_ts: int, end_ts: int, fidelity: int = 1) -> list[tuple[int, float]]:
        """CLOB 价格历史，fidelity 是分钟粒度。返回按时间排好的 (时间戳, 价格)。"""
        raw = await self._call("GET", f"{CLOB}/prices-history",
                               params={"market": token_id, "startTs": start_ts, "endTs": end_ts, "fidelity": fidelity})
        return sorted((int(p["t"]), float(p["p"])) for p in raw.get("history", []))

    async def books(self, token_ids: list[str]) -> dict[str, Book]:
        rows = await self._call("POST", f"{CLOB}/books", json=[{"token_id": t} for t in token_ids])
        return {row["asset_id"]: Book.from_clob(row) for row in rows}

    async def trades_24h(self, condition_id: str) -> int:
        rows = await self._call("GET", f"{DATA}/trades", params={"market": condition_id, "limit": 1000})
        cutoff = time.time() - 86_400
        return sum(1 for t in rows if t.get("timestamp", 0) >= cutoff)
