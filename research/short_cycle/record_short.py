"""5 分钟 / 15 分钟加密货币涨跌盘的前向记录器：秒级的盘口和现货价，用来检验现货是否领先市场。

short_cycle.py 的回测用的是 CLOB prices-history（大约一分钟一个点，中位过时 37 秒）和 Coinbase 分钟 K 线。
模型只比“t 之前最近的市场价”准，比“t 之后第一个市场价”差，分不清是现货真的领先，还是市场价本身过时。
这里改成前向记录：
- 每 ~2 秒一轮，拉 BTC / ETH / SOL / XRP 当前 5 分钟和 15 分钟窗口 Up 代币的盘口（CLOB POST /books，
  一次请求全部代币），同时记下那一刻的 Coinbase 现货价。同一轮的盘口和现货共用一个 poll_ts，回放时按它对齐到同一秒。
  Down 的盘口就是 Up 的镜像（Down 卖价 = 1 − Up 买价，数量相同），不用单独拉。
- 现货有三路，都记：Coinbase websocket 成交推送（source = 'ws'，延迟几毫秒，回放用这一路）、Coinbase REST ticker
  （source = 'rest'，Coinbase 那边就比成交晚 0.6–0.9 秒，再加 1 秒 CDN 缓存），和 Binance websocket
  （source = 'binance'，USDT 计价，做市商更可能参照的那家）。REST 那一路留着做对照：拿它算领先滞后，
  会看到“盘口比现货先动”的假象。没装 websockets 时只记 REST。
- 每分钟补一次 Coinbase 分钟 K 线：模型的 σ 要用前 60 分钟的最高 / 最低价。
- 窗口结束后去 Gamma 查结算结果。
- 全部追加写进 data/short_ticks.db。只用公开只读接口，不下单。

回放（--report）：在每个窗口的检查时点，用 short_cycle 的 model_z / model_p 算模型 P(Up)
（k 用回测拟合的值，对这批数据是样本外），先和同一轮的盘口比，再和几秒之后的盘口比：
Brier、按真实卖价买入并扣手续费的模型打法 ROI，以及现货变动和盘口变动的领先滞后相关。

用法：
  .venv/bin/python research/short_cycle/record_short.py               # 一直记录，Ctrl+C 停
  .venv/bin/python research/short_cycle/record_short.py --minutes 60  # 记录 60 分钟后退出
  .venv/bin/python research/short_cycle/record_short.py --report      # 回放已记录的数据，打印报告
  .venv/bin/python research/short_cycle/record_short.py --db data/test.db --minutes 30  # 写到另一个库（试跑用，不和正在跑的记录器抢同一个库）
"""

from __future__ import annotations

import argparse
import asyncio
import bisect
import json
import logging
import math
import os
import random
import signal
import sqlite3
import statistics
import time
from collections import defaultdict
from datetime import datetime

import httpx

try:
    import websockets
except ImportError:  # 没装就只记 REST ticker
    websockets = None

import sys

# 归档在 research/short_cycle/ 下，polymarket.py 在项目根目录
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from polymarket import CLOB, GAMMA, Book
from short_cycle import (CHECKPOINTS, COINBASE, CYCLES, DATA_DIR, MIN_MODEL_EDGE, SIGMA_LOOKBACK_MIN, brier, fee,
                         fetch_spot, model_p, model_z, strategy)

log = logging.getLogger("ticks")

DB_PATH = os.path.join(DATA_DIR, "short_ticks.db")
ASSETS = ["btc", "eth", "sol", "xrp"]
INTERVAL = 2.0          # 每轮间隔秒数
TIMEOUT = 5.0           # 单个请求的超时；超时就当这一轮没拿到
DEPTH_BAND = 0.02       # 另记最优价 2¢ 以内的挂单股数
CRYPTO_FEE_RATE = 0.07  # Gamma 没给费率时的默认值
RESOLVE_GIVE_UP = 6 * 3600  # 结束 6 小时还查不到结果就不再查
COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"
# Binance 只读行情镜像。api.binance.com 在这台机器上返回 451（地区限制），stream.binance.com:9443 握手超时
BINANCE_WS = "wss://data-stream.binance.vision/stream"
BINANCE = {"btc": "BTCUSDT", "eth": "ETHUSDT", "sol": "SOLUSDT", "xrp": "XRPUSDT", "doge": "DOGEUSDT"}
WARN_EVERY = 30.0       # 同一个数据源的警告最多 30 秒一条

SCHEMA = """
CREATE TABLE IF NOT EXISTS windows (
    slug TEXT PRIMARY KEY, asset TEXT, cycle TEXT, t0 INTEGER, t_end INTEGER,
    condition_id TEXT, up_token TEXT, fee_rate REAL,
    outcome INTEGER, volume REAL, resolved_ts REAL       -- outcome: 1 = Up，0 = Down，NULL = 还没结算
);
-- 每轮每个市场一行。poll_ts = 这一轮开始的本地时间，recv_ts = 收到响应的本地时间，server_ts = CLOB 返回的时间戳
CREATE TABLE IF NOT EXISTS books (
    poll_ts REAL, recv_ts REAL, server_ts REAL, slug TEXT,
    bid REAL, bid_size REAL, ask REAL, ask_size REAL,    -- Up 代币的最优买 / 卖价和那一档的股数
    bid_depth REAL, ask_depth REAL, last_trade REAL      -- 最优价 2¢ 以内的股数；最近成交价
);
CREATE INDEX IF NOT EXISTS books_slug ON books (slug, poll_ts);
-- 每轮每个币种每路一行。price = (bid + ask) / 2；trade_ts = 交易所最近一笔成交的时间；
-- source = 'ws'（Coinbase websocket，recv_ts = 收到那条推送的时间）、'rest'（Coinbase ticker，recv_ts = 收到响应的时间）
-- 或 'binance'（Binance websocket，bid / ask 来自 bookTicker，last / trade_ts 来自 aggTrade）
CREATE TABLE IF NOT EXISTS spot (
    poll_ts REAL, recv_ts REAL, asset TEXT, price REAL, bid REAL, ask REAL, last REAL, trade_ts REAL, source TEXT
);
CREATE INDEX IF NOT EXISTS spot_asset ON spot (asset, poll_ts);
-- websocket 推来的每一笔成交（Coinbase 约每秒 10 条，Binance 约 25 条）。用来在交易所时间上做亚秒级的领先滞后，
-- 不受 2 秒一轮的限制。price = 成交时的中间价；source = 'coinbase' 或 'binance'
CREATE TABLE IF NOT EXISTS ws_ticks (recv_ts REAL, trade_ts REAL, asset TEXT, price REAL, source TEXT);
-- Coinbase 分钟 K 线，ts = 这根 K 线的结束时刻
CREATE TABLE IF NOT EXISTS candles (
    asset TEXT, ts INTEGER, close REAL, high REAL, low REAL, PRIMARY KEY (asset, ts)
);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
"""


def connect(path: str | None = None) -> sqlite3.Connection:
    path = path or DB_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)
    db = sqlite3.connect(path, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")  # 记录器在写的时候，看板和回放照样能读
    db.execute("PRAGMA synchronous=NORMAL")
    db.executescript(SCHEMA)
    if "source" not in [r[1] for r in db.execute("PRAGMA table_info(spot)")]:  # 早期的库只有 REST 一路
        db.execute("ALTER TABLE spot ADD COLUMN source TEXT DEFAULT 'rest'")
    if "source" not in [r[1] for r in db.execute("PRAGMA table_info(ws_ticks)")]:  # 加 Binance 之前只有 Coinbase
        db.execute("ALTER TABLE ws_ticks ADD COLUMN source TEXT DEFAULT 'coinbase'")
        db.execute("UPDATE ws_ticks SET source = 'coinbase' WHERE source IS NULL")
    db.execute("DROP INDEX IF EXISTS ws_ticks_asset")
    db.execute("CREATE INDEX IF NOT EXISTS ws_ticks_source ON ws_ticks (source, asset, trade_ts)")
    db.commit()
    return db


def parse_ts(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _float(value) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ---------- 记录 ----------

class SpotFeed:
    """Coinbase websocket 的 ticker 频道：每笔成交推一条，带当时的最优买卖价。断线自动重连。"""
    name, spot_source, tick_source = "Coinbase", "ws", "coinbase"  # spot 表 / ws_ticks 表里的 source

    def __init__(self, assets: list[str]):
        self.products = {COINBASE[a]: a for a in assets}
        self.latest: dict[str, tuple] = {}  # 币种 → (recv_ts, price, bid, ask, last, trade_ts)
        self.pending: list[tuple] = []      # 还没写进 ws_ticks 的推送，每轮写一次
        self.errors = 0
        self.last_warn, self.unreported, self.announce = 0.0, 0, True

    def url(self) -> str:
        return COINBASE_WS

    async def subscribe(self, ws):
        await ws.send(json.dumps({"type": "subscribe", "product_ids": list(self.products), "channels": ["ticker"]}))

    def warn(self, problem: str, delay: float):
        """断线警告最多 30 秒一条，中间没报的次数并进下一条。"""
        self.unreported += 1
        if time.time() - self.last_warn > WARN_EVERY:
            log.warning("%s websocket 断开：%s，%.0f 秒后重连（距上一条警告断开 %d 次）",
                        self.name, problem, delay, self.unreported)
            self.last_warn, self.unreported, self.announce = time.time(), 0, True

    async def run(self):
        delay = 1.0
        while True:
            try:
                async with websockets.connect(self.url(), open_timeout=10, ping_interval=20, ping_timeout=20,
                                              max_size=2 ** 20) as ws:
                    await self.subscribe(ws)
                    if self.announce:  # 只在第一次和报过断线之后说一声，免得反复断连时刷屏
                        log.info("%s websocket 已连接", self.name)
                        self.announce = False
                    async for raw in ws:
                        self.on_message(raw)
                        delay = 1.0  # 收到过数据才算连上；一连上就断的话，退避照样加长
                problem = "服务器关闭连接"
            except asyncio.CancelledError:
                raise
            except Exception as e:
                problem = str(e) or type(e).__name__
            self.errors += 1
            self.warn(problem, delay)
            self.latest.clear()  # 断线期间的旧价格不能当成“现在”
            await asyncio.sleep(delay)
            delay = min(60.0, delay * 2)

    def take(self) -> list[tuple]:
        ticks, self.pending = self.pending, []
        return ticks

    def on_message(self, raw):
        try:
            m = json.loads(raw)
        except ValueError:
            return
        asset = self.products.get(m.get("product_id"))
        if m.get("type") != "ticker" or asset is None:
            return
        bid, ask, last = _float(m.get("best_bid")), _float(m.get("best_ask")), _float(m.get("price"))
        price = (bid + ask) / 2 if bid and ask else last
        if price:
            recv, trade_ts = time.time(), parse_ts(m.get("time"))
            self.latest[asset] = (recv, price, bid, ask, last, trade_ts)
            self.pending.append((recv, trade_ts, asset, price, self.tick_source))


class BinanceFeed(SpotFeed):
    """Binance 的 bookTicker（最优买卖价，每秒几十条，只留在内存里）+ aggTrade（逐笔成交，带成交时间，写进 ws_ticks）。
    订阅写在 URL 里（组合流），连上就推。服务器 24 小时断一次，和其他断线一样重连。"""
    name, spot_source, tick_source = "Binance", "binance", "binance"

    def __init__(self, assets: list[str]):
        super().__init__([])
        self.symbols = {BINANCE[a]: a for a in assets if a in BINANCE}

    def url(self) -> str:
        streams = "/".join(f"{s.lower()}@{kind}" for s in self.symbols for kind in ("bookTicker", "aggTrade"))
        return f"{BINANCE_WS}?streams={streams}"

    async def subscribe(self, ws):
        pass

    def on_message(self, raw):
        try:
            m = json.loads(raw)
            stream, d = m["stream"], m["data"]
            asset = self.symbols.get(d.get("s"))
        except (ValueError, KeyError, TypeError, AttributeError):
            return
        if asset is None:
            return
        recv = time.time()
        if stream.endswith("@bookTicker"):
            bid, ask = _float(d.get("b")), _float(d.get("a"))
            if not (bid and ask):
                return
            _, _, _, _, last, trade_ts = self.latest.get(asset) or (None,) * 6
            self.latest[asset] = (recv, (bid + ask) / 2, bid, ask, last, trade_ts)
        elif stream.endswith("@aggTrade"):
            last, trade_ms = _float(d.get("p")), _float(d.get("T"))
            if not (last and trade_ms):
                return
            # 和 Coinbase 一样存成交那一刻的中间价（bookTicker 和成交谁先到不确定，差几十毫秒）；还没有盘口时用成交价
            _, _, bid, ask, _, _ = self.latest.get(asset) or (None,) * 6  # 断线时 latest 清空，不会用到旧盘口
            price = (bid + ask) / 2 if bid and ask else last
            self.latest[asset] = (recv, price, bid, ask, last, trade_ms / 1000)
            self.pending.append((recv, trade_ms / 1000, asset, price, self.tick_source))


class Recorder:
    def __init__(self, db: sqlite3.Connection, client: httpx.AsyncClient, assets: list[str], interval: float,
                 feeds: list[SpotFeed]):
        self.db, self.client, self.assets, self.interval, self.feeds = db, client, assets, interval, feeds
        self.windows: dict[str, dict] = {}       # slug → 窗口信息（当前和下一个窗口）
        self.paused: dict[str, float] = {}       # 数据源 → 暂停到什么时候（被限速或连续出错）
        self.streak: dict[str, int] = defaultdict(int)
        self.errors: dict[str, int] = defaultdict(int)
        self.last_warn: dict[str, float] = {}
        self.polls = self.book_rows = self.spot_rows = 0
        self.started = time.time()

    async def request(self, source: str, method: str, url: str, **kw):
        """一次请求，出任何错都只记账、返回 None，不抛异常。连续出错或被限速时，这个数据源暂停一会儿。"""
        if time.time() < self.paused.get(source, 0):
            return None
        problem, pause = None, 0.0
        try:
            r = await self.client.request(method, url, **kw)
        except httpx.HTTPError as e:
            problem = type(e).__name__
        else:
            if r.status_code == 200:
                try:
                    data = r.json()
                except ValueError:
                    problem = "bad json"
                else:
                    self.streak[source] = 0
                    return data
            else:
                problem = f"HTTP {r.status_code}"
                if r.status_code == 429:
                    pause = min(120.0, 10.0 * 2 ** self.streak[source])
        self.streak[source] += 1
        self.errors[source] += 1
        if not pause and self.streak[source] >= 3:
            pause = min(60.0, 2.0 ** self.streak[source])
        if pause:
            self.paused[source] = time.time() + pause
        if time.time() - self.last_warn.get(source, 0) > WARN_EVERY:  # 同一个数据源 30 秒内只报一次
            self.last_warn[source] = time.time()
            log.warning("%s: %s（连续 %d 次%s）", source, problem, self.streak[source],
                        f"，暂停 {pause:.0f} 秒" if pause else "")
        return None

    # --- 窗口：当前和下一个，提前拿到代币，换窗口时不断档 ---

    def wanted(self, now: int) -> dict[str, tuple[str, str, int]]:
        out = {}
        for asset in self.assets:
            for cycle, width in CYCLES.items():
                t0 = now - now % width
                for start in (t0, t0 + width):
                    out[f"{asset}-updown-{cycle}-{start}"] = (asset, cycle, start)
        return out

    async def refresh_windows(self):
        now = int(time.time())
        want = self.wanted(now)
        for slug in [s for s, w in self.windows.items() if w["t_end"] < now]:
            del self.windows[slug]
        missing = [s for s in want if s not in self.windows]
        if not missing:
            return
        rows = await self.request("gamma", "GET", f"{GAMMA}/markets", params=[("slug", s) for s in missing])
        for raw in rows or []:
            slug = raw.get("slug")
            if slug not in want:
                continue
            try:
                tokens = json.loads(raw.get("clobTokenIds") or "[]")
                outcomes = json.loads(raw.get("outcomes") or "[]")
            except ValueError:
                continue
            if outcomes != ["Up", "Down"] or len(tokens) != 2:
                continue
            asset, cycle, t0 = want[slug]
            rate = _float((raw.get("feeSchedule") or {}).get("rate")) if raw.get("feesEnabled") else 0.0
            w = {"slug": slug, "asset": asset, "cycle": cycle, "t0": t0, "t_end": t0 + CYCLES[cycle],
                 "condition_id": raw.get("conditionId"), "up_token": str(tokens[0]),
                 "fee_rate": CRYPTO_FEE_RATE if rate is None else rate}
            self.windows[slug] = w
            self.db.execute("INSERT OR IGNORE INTO windows (slug, asset, cycle, t0, t_end, condition_id, up_token, fee_rate)"
                            " VALUES (:slug, :asset, :cycle, :t0, :t_end, :condition_id, :up_token, :fee_rate)", w)
        self.db.commit()

    # --- 每一轮：盘口 + 现货 ---

    async def fetch_books(self, live: dict[str, dict]):
        if not live:
            return None
        by_token = {w["up_token"]: slug for slug, w in live.items()}
        rows = await self.request("clob", "POST", f"{CLOB}/books", json=[{"token_id": t} for t in by_token])
        if rows is None:
            return None
        recv = time.time()
        out = []
        for raw in rows:
            slug = by_token.get(raw.get("asset_id"))
            if slug is None:
                continue
            try:
                book = Book.from_clob(raw)
            except (TypeError, ValueError, KeyError):
                continue
            server = _float(raw.get("timestamp"))
            out.append((recv, server / 1000 if server else None, slug,
                        book.best_bid, book.bids[0][1] if book.bids else None,
                        book.best_ask, book.asks[0][1] if book.asks else None,
                        sum(s for p, s in book.bids if book.best_bid - p <= DEPTH_BAND + 1e-9) if book.bids else 0.0,
                        sum(s for p, s in book.asks if p - book.best_ask <= DEPTH_BAND + 1e-9) if book.asks else 0.0,
                        _float(raw.get("last_trade_price"))))
        return out

    async def fetch_ticker(self, asset: str):
        raw = await self.request("coinbase", "GET", f"https://api.exchange.coinbase.com/products/{COINBASE[asset]}/ticker")
        if not isinstance(raw, dict):
            return None
        bid, ask, last = _float(raw.get("bid")), _float(raw.get("ask")), _float(raw.get("price"))
        price = (bid + ask) / 2 if bid and ask else last
        if not price:
            return None
        return (time.time(), asset, price, bid, ask, last, parse_ts(raw.get("time")), "rest")

    async def poll(self):
        poll_ts = time.time()
        # websocket 那几路：发出盘口请求这一刻已知的最新价格
        pushed = [(recv, asset, *rest, feed.spot_source) for feed in self.feeds for asset, (recv, *rest) in feed.latest.items()]
        live = {s: w for s, w in self.windows.items() if w["t0"] <= poll_ts < w["t_end"]}
        books, *spots = await asyncio.gather(self.fetch_books(live), *(self.fetch_ticker(a) for a in self.assets))
        if books:
            self.db.executemany("INSERT INTO books VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                [(poll_ts, *row) for row in books])
            self.book_rows += len(books)
        ticks = [tick for feed in self.feeds for tick in feed.take()]
        if ticks:
            self.db.executemany("INSERT INTO ws_ticks (recv_ts, trade_ts, asset, price, source) VALUES (?, ?, ?, ?, ?)", ticks)
        spots = pushed + [row for row in spots if row]
        if spots:
            self.db.executemany("INSERT INTO spot (poll_ts, recv_ts, asset, price, bid, ask, last, trade_ts, source)"
                                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", [(poll_ts, *row) for row in spots])
            self.spot_rows += len(spots)
        self.polls += 1
        self.db.execute("INSERT OR REPLACE INTO meta VALUES ('last_poll', ?)", (str(poll_ts),))
        self.db.commit()

    # --- 后台：K 线、结算结果、状态日志 ---

    async def refresh_candles(self, minutes: int = 10):
        now = int(time.time())
        for asset in self.assets:
            # 从库里最后一根往后补：VM 暂停、断网之后，模型算 σ 要的前 60 分钟 K 线才不会缺一段
            last = self.db.execute("SELECT MAX(ts) FROM candles WHERE asset = ?", (asset,)).fetchone()[0] or 0
            span = min(max(minutes * 60, now - last + 120), (SIGMA_LOOKBACK_MIN + 20) * 60)
            try:
                candles = await fetch_spot(self.client, COINBASE[asset], now - span, now)
            except Exception as e:  # fetch_spot 自己会重试 3 次，还失败就等下一分钟
                self.errors["candles"] += 1
                log.warning("candles %s: %s", asset, e)
                continue
            # 还没收完的那根（结束时刻在未来）不存；刚收完的那根 Coinbase 可能还会微调，下一分钟会覆盖
            self.db.executemany("INSERT OR REPLACE INTO candles VALUES (?, ?, ?, ?, ?)",
                                [(asset, ts, c, hi, lo) for ts, (c, hi, lo) in candles.items() if ts <= now])
        self.db.commit()

    async def resolve(self):
        now = time.time()
        slugs = [r[0] for r in self.db.execute(
            "SELECT slug FROM windows WHERE outcome IS NULL AND t_end < ? AND t_end > ? ORDER BY t0 LIMIT 200",
            (now - 30, now - RESOLVE_GIVE_UP))]
        done = 0
        for i in range(0, len(slugs), 20):
            # Gamma 的坑：不写 closed=true 查不到已结算的市场
            rows = await self.request("gamma", "GET", f"{GAMMA}/markets",
                                      params=[("slug", s) for s in slugs[i:i + 20]] + [("closed", "true")])
            for raw in rows or []:
                try:
                    up = float(json.loads(raw.get("outcomePrices") or "[]")[0])
                except (ValueError, IndexError, TypeError):
                    continue
                if up in (0.0, 1.0):
                    self.db.execute("UPDATE windows SET outcome = ?, volume = ?, resolved_ts = ? WHERE slug = ?",
                                    (int(up), _float(raw.get("volumeNum")), now, raw.get("slug")))
                    done += 1
            await asyncio.sleep(0.5)
        self.db.commit()
        if done:
            log.info("新结算 %d 个窗口", done)

    def status(self):
        resolved, total = self.db.execute("SELECT COUNT(outcome), COUNT(*) FROM windows").fetchone()
        live = sum(1 for w in self.windows.values() if w["t0"] <= time.time() < w["t_end"])
        for feed in self.feeds:
            self.errors[f"{feed.tick_source}_ws"] = feed.errors
        errors = "，".join(f"{k} {v}" for k, v in sorted(self.errors.items()) if v) or "无"
        log.info("已运行 %.0f 分钟：%d 轮，盘口 %d 行，现货 %d 行；正在记录 %d 个窗口；已结算 %d / %d 个窗口；出错：%s",
                 (time.time() - self.started) / 60, self.polls, self.book_rows, self.spot_rows, live,
                 resolved, total, errors)
        self.db.execute("INSERT OR REPLACE INTO meta VALUES ('errors', ?)", (json.dumps(self.errors),))
        self.db.commit()

    async def heartbeat(self):
        self.status()


async def every(period: float, fn, first_delay: float = 0.0):
    await asyncio.sleep(first_delay)
    while True:
        try:
            await fn()
        except Exception:  # 后台任务出错只记日志，下一轮再试
            log.exception("%s failed", getattr(fn, "__name__", fn))
        await asyncio.sleep(period)


async def record(interval: float, assets: list[str], minutes: float | None):
    db = connect()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    deadline = time.time() + minutes * 60 if minutes else None
    db.executemany("INSERT OR REPLACE INTO meta VALUES (?, ?)",
                   [("started", str(time.time())), ("interval", str(interval)), ("assets", ",".join(assets)),
                    ("pid", str(os.getpid()))])
    db.commit()
    feeds = [SpotFeed(assets), BinanceFeed(assets)] if websockets else []
    if not feeds:
        log.warning("没装 websockets，现货只记 Coinbase REST ticker（比成交晚约 1 秒）。安装：.venv/bin/pip install websockets")
    async with httpx.AsyncClient(timeout=TIMEOUT, headers={"user-agent": "jev-desk/0.1"}) as client:
        rec = Recorder(db, client, assets, interval, feeds)
        await rec.refresh_windows()
        await rec.refresh_candles(SIGMA_LOOKBACK_MIN + 20)  # 开头先补够模型算 σ 要用的 K 线
        # 下一个窗口提前 5 分钟以上就拿到了代币，15 秒查一次足够
        tasks = [asyncio.create_task(every(15, rec.refresh_windows, 15)),
                 asyncio.create_task(every(60, rec.refresh_candles, 65 - time.time() % 60)),  # 每分钟过 5 秒
                 asyncio.create_task(every(60, rec.resolve, 20)),
                 asyncio.create_task(every(60, rec.heartbeat, 60))]
        tasks += [asyncio.create_task(feed.run()) for feed in feeds]
        log.info("开始记录：%s，每 %.1f 秒一轮，写到 %s", ",".join(assets), interval, DB_PATH)
        next_at = math.ceil(time.time() / interval) * interval
        try:
            while not stop.is_set() and (deadline is None or time.time() < deadline):
                try:
                    await asyncio.wait_for(stop.wait(), timeout=max(0.0, next_at - time.time()))
                    break
                except asyncio.TimeoutError:
                    pass
                try:
                    await rec.poll()
                except Exception:  # 单轮出错（比如数据库被锁）跳过这一轮
                    log.exception("poll failed")
                next_at += interval
                if next_at < time.time():  # 落后了就直接对齐到下一个整点，不补拍
                    next_at = math.ceil(time.time() / interval) * interval
        finally:
            for t in tasks:
                t.cancel()
            rec.status()
            db.close()
            log.info("记录器已停止")


# ---------- 回放 ----------

LAGS = [0, 2, 4, 10, 30]  # 盘口比模型晚几秒（0 = 同一轮：现货和盘口是同时拉的）
ALIGN = 3.0               # 检查时点 t 之后这么多秒内要有一轮记录，否则跳过这个时点
LEADLAG_STEPS = range(-5, 11)  # 领先滞后：盘口变动比现货变动晚几轮（负数 = 盘口先动）
MID_RANGE = (0.05, 0.95)  # 领先滞后只看中间价在这个区间的时候；太靠边的盘几乎不动
MIN_SHARES = 5            # 卖一至少有这么多股才算能成交（CLOB 最小下单量）
SPOT_STALE = 30.0         # 最近一笔成交比这一轮早这么多秒以上就不用（断线、VM 暂停之后积压的旧推送）
# 亚秒级领先滞后：盘口变动（两次快照之间，CLOB 时间）vs 同样长、但提前 δ 秒的现货涨跌（交易所成交时间）。
# δ > 0 = 现货在前；相关最高的 δ ≈ 盘口比现货晚多少秒。0 附近加密一些，好分辨 Coinbase 和 Binance 差的那零点几秒
FINE_SHIFTS = [-3, -2, -1.5, -1, -0.75, -0.5, -0.25, -0.1, 0, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 4, 6]
TICK_SOURCE = {"ws": "coinbase", "binance": "binance"}  # spot 表的 source → ws_ticks 表的 source
BOOT_ROUNDS = 200         # 比较两路现货的峰值：按窗口重抽样的次数
BOOT_CLUSTERS = 200       # 窗口比这多时，按时间顺序把相邻窗口并成这么多块再抽（控制回放耗时）


def load_k() -> tuple[dict[str, float], str]:
    """模型的波动率放大系数 k 用 short_cycle.py 回测拟合的值：对前向数据是样本外，不会拿这批数据调参。"""
    try:
        with open(os.path.join(DATA_DIR, "short_cycle.json")) as f:
            return {fit["cycle"]: fit["k"] for fit in json.load(f)["backtest"]["fits"]}, "回测拟合，样本外"
    except (OSError, ValueError, KeyError):
        return {}, "没有 data/short_cycle.json，用 k = 1"


def _corr(acc: list[float]) -> float | None:
    n, sa, sb, saa, sbb, sab = acc
    if n < 3:
        return None
    va, vb = saa - sa * sa / n, sbb - sb * sb / n
    return (sab - sa * sb / n) / math.sqrt(va * vb) if va > 0 and vb > 0 else None


def _add(a: list[float], v: float, u: float):
    a[0] += 1; a[1] += v; a[2] += u; a[3] += v * v; a[4] += u * u; a[5] += v * u


def _shifts() -> dict[float, list[float]]:
    return {s: [0.0] * 6 for s in FINE_SHIFTS}


def _curve(shifts: dict[float, list[float]]) -> list[dict]:
    return [{"lag": s, "corr": _corr(a), "n": int(a[0])} for s, a in shifts.items()]


def _peak(rows: list[dict]) -> dict | None:
    ok = [r for r in rows if r["corr"] is not None]
    return max(ok, key=lambda r: r["corr"]) if ok else None


def _ticks_have_source(db: sqlite3.Connection) -> bool:
    return "source" in [r[1] for r in db.execute("PRAGMA table_info(ws_ticks)")]


class Replay:
    """把记录下来的数据读进内存，按窗口逐个回放。source = 用哪一路现货（'ws'、'rest' 或 'binance'）。"""

    def __init__(self, db: sqlite3.Connection, source: str):
        self.db, self.source = db, source
        self.meta = dict(db.execute("SELECT key, value FROM meta"))
        self.interval = float(self.meta.get("interval") or INTERVAL)
        self.k, self.k_source = load_k()
        self.spot_ts: dict[str, list[float]] = defaultdict(list)
        self.spot_px: dict[str, list[float]] = defaultdict(list)
        self.spot_at: dict[str, dict[float, float]] = defaultdict(dict)  # 同一轮的现货：poll_ts → 价格
        for asset, poll_ts, price in db.execute(
                "SELECT asset, poll_ts, price FROM spot WHERE COALESCE(source, 'rest') = ?"
                " AND (trade_ts IS NULL OR poll_ts - trade_ts < ?) ORDER BY asset, poll_ts", (source, SPOT_STALE)):
            self.spot_ts[asset].append(poll_ts)
            self.spot_px[asset].append(price)
            self.spot_at[asset][poll_ts] = price
        self.tick_ts: dict[str, list[float]] = defaultdict(list)  # websocket 逐笔成交，按交易所成交时间排序
        self.tick_px: dict[str, list[float]] = defaultdict(list)
        if source in TICK_SOURCE and db.execute("SELECT 1 FROM sqlite_master WHERE name = 'ws_ticks'").fetchone():
            if _ticks_have_source(db):
                rows = db.execute("SELECT asset, trade_ts, price FROM ws_ticks WHERE source = ? AND trade_ts IS NOT NULL"
                                  " ORDER BY asset, trade_ts", (TICK_SOURCE[source],))
            else:  # 还没迁移的旧库：全是 Coinbase
                rows = db.execute("SELECT asset, trade_ts, price FROM ws_ticks WHERE trade_ts IS NOT NULL"
                                  " ORDER BY asset, trade_ts") if source == "ws" else []
            for asset, ts, price in rows:
                self.tick_ts[asset].append(ts)
                self.tick_px[asset].append(price)
        self.candles: dict[str, dict[int, tuple[float, float, float]]] = defaultdict(dict)
        for asset, ts, close, high, low in db.execute("SELECT asset, ts, close, high, low FROM candles"):
            self.candles[asset][ts] = (close, high, low)

    def books(self, slug: str) -> tuple[list[float], list[tuple]]:
        rows = self.db.execute("SELECT poll_ts, bid, bid_size, ask, ask_size FROM books WHERE slug = ? ORDER BY poll_ts",
                               (slug,)).fetchall()
        return [r[0] for r in rows], [r[1:] for r in rows]

    def tick_price(self, asset: str, m: int) -> float | None:
        """m 时刻的现货：m 之后第一轮记录的价格（ALIGN 秒以内）。"""
        ts = self.spot_ts[asset]
        i = bisect.bisect_left(ts, m)
        return self.spot_px[asset][i] if i < len(ts) and ts[i] <= m + ALIGN else None

    def spot_dict(self, asset: str, t0: int, t: int, now_price: float) -> tuple[dict, int] | None:
        """拼出 model_z 要的 {分钟结束时刻: (收盘, 最高, 最低)}：
        σ 用前 60 分钟 K 线的最高 / 最低价；窗口内的开盘价和各分钟收盘价换成逐轮记录的现货价（缺了才用 K 线收盘），
        t 时刻的现价就是和盘口同一轮拉到的那个价格。返回值第二项 = 窗口内用到逐轮现货的分钟数。"""
        cs = self.candles[asset]
        spot = {m: cs[m] for m in range(t - 60 * (SIGMA_LOOKBACK_MIN - 1), t + 1, 60) if m in cs}
        from_ticks = 0
        for m in range(t0, t + 1, 60):
            if m not in cs:
                return None
            price = now_price if m == t else self.tick_price(asset, m)
            from_ticks += price is not None
            spot[m] = (price if price is not None else cs[m][0], cs[m][1], cs[m][2])
        return spot, from_ticks

    def checkpoints(self, w: dict, books_ts: list[float], books: list[tuple]) -> tuple[list[dict], dict]:
        out, skipped = [], defaultdict(int)
        width = CYCLES[w["cycle"]]
        for offset in CHECKPOINTS[w["cycle"]]:
            if offset == 0:
                continue  # 刚开盘时模型就是 0.5，没有信息
            t = w["t0"] + offset
            i = bisect.bisect_left(books_ts, t)
            if i == len(books_ts) or books_ts[i] > t + ALIGN:
                skipped["没有盘口"] += 1
                continue
            poll = books_ts[i]
            now_price = self.spot_at[w["asset"]].get(poll)
            if now_price is None:
                skipped["同一轮没有现货"] += 1
                continue
            built = self.spot_dict(w["asset"], w["t0"], t, now_price)
            z = model_z(built[0], w["t0"], width, t) if built else None
            if z is None:
                skipped["缺 K 线"] += 1
                continue
            later = []
            for lag in LAGS:
                j = i if lag == 0 else bisect.bisect_left(books_ts, poll + lag - 0.5, i)
                if j == len(books_ts) or books_ts[j] > poll + lag + ALIGN or books_ts[j] >= w["t_end"]:
                    break
                later.append((books_ts[j] - poll, *books[j]))
            if len(later) < len(LAGS) or any(b[1] is None or b[3] is None for b in later):
                skipped["之后的盘口不全"] += 1
                continue
            out.append({"slug": w["slug"], "asset": w["asset"], "cycle": w["cycle"], "t0": w["t0"], "offset": offset,
                        "up": w["outcome"], "rate": w["fee_rate"] if w["fee_rate"] is not None else CRYPTO_FEE_RATE,
                        "model": model_p(z, self.k.get(w["cycle"], 1.0)), "from_ticks": built[1],
                        "minutes": offset // 60 + 1, "book": later})
        return out, skipped

    def leadlag(self, w: dict, books_ts: list[float], books: list[tuple], acc: dict[int, list[float]],
                only: dict[float, float] | None = None):
        """现货每轮的对数涨跌 vs 盘口中间价每轮的变动。都按轮次编号对齐（round(poll_ts / interval)）。
        only：只用这些轮次（两路现货对比时，用同一批轮次）。"""
        x, m = {}, {}
        for ts, (bid, _bs, ask, _as) in zip(books_ts, books):
            if only is not None and ts not in only:
                continue
            price = self.spot_at[w["asset"]].get(ts)
            g = round(ts / self.interval)
            if price:
                x[g] = math.log(price)
            if bid is not None and ask is not None and MID_RANGE[0] <= (bid + ask) / 2 <= MID_RANGE[1]:
                m[g] = (bid + ask) / 2
        dx = {g: x[g] - x[g - 1] for g in x if g - 1 in x}
        dm = {g: m[g] - m[g - 1] for g in m if g - 1 in m}
        for step in LEADLAG_STEPS:
            a = acc[step]
            for g, v in dx.items():
                u = dm.get(g + step)
                if u is not None:
                    a[0] += 1; a[1] += v; a[2] += u; a[3] += v * v; a[4] += u * u; a[5] += v * u


    def price_at(self, asset: str, t: float) -> float | None:
        """t（交易所时间）之前最后一笔成交的价格；没有逐笔数据或早于第一笔就返回 None。"""
        ts = self.tick_ts[asset]
        i = bisect.bisect_right(ts, t)
        return self.tick_px[asset][i - 1] if i and t - ts[i - 1] < SPOT_STALE else None


def leadlag_fine(db: sqlite3.Connection, w: dict, ticks: dict[str, Replay], interval: float
                 ) -> tuple[dict[str, dict], dict[str, dict]]:
    """一个窗口的亚秒级领先滞后，每一路有逐笔成交的现货（ticks：source → Replay）各算一份累加量。
    返回 (own, same)：own[src] 用这一路自己有价格的全部快照对；same[src] 只用所有路都有价格的快照对，
    几路的样本完全相同，并排比较看这个（Binance 是后来才加的，own 里 Coinbase 多出来的那几个小时不算）。"""
    own, same = {src: _shifts() for src in ticks}, {src: _shifts() for src in ticks}
    srcs = [src for src, rp in ticks.items() if rp.tick_ts[w["asset"]]]
    if not srcs:
        return own, same
    rows = db.execute("SELECT poll_ts, server_ts, bid, ask FROM books WHERE slug = ? AND server_ts IS NOT NULL"
                      " ORDER BY poll_ts", (w["slug"],)).fetchall()
    for (p1, t1, b1, a1), (p2, t2, b2, a2) in zip(rows, rows[1:]):
        if None in (b1, a1, b2, a2) or p2 - p1 > 1.5 * interval or t2 <= t1:
            continue
        m1, m2 = (b1 + a1) / 2, (b2 + a2) / 2
        if not (MID_RANGE[0] <= m1 <= MID_RANGE[1] and MID_RANGE[0] <= m2 <= MID_RANGE[1]):
            continue
        for shift in FINE_SHIFTS:
            got = {}
            for src in srcs:
                s1, s2 = ticks[src].price_at(w["asset"], t1 - shift), ticks[src].price_at(w["asset"], t2 - shift)
                if s1 and s2:
                    got[src] = math.log(s2 / s1)
                    _add(own[src][shift], got[src], m2 - m1)
            if len(ticks) > 1 and len(got) == len(ticks):
                for src, v in got.items():
                    _add(same[src][shift], v, m2 - m1)
    return own, same


def leadlag_exchanges(a: Replay, b: Replay, interval: float) -> list[dict]:
    """两家交易所之间：a 每轮的涨跌（两轮的 poll_ts 之间，a 的成交时间）vs 同样长、提前 δ 秒的 b 的涨跌。
    δ > 0 = b 先动。只用两路都记到的相邻轮次，VM 暂停的空档不算。"""
    acc = _shifts()
    for asset in a.tick_ts:
        polls = sorted(set(a.spot_at[asset]) & set(b.spot_at[asset]))
        for t1, t2 in zip(polls, polls[1:]):
            if t2 - t1 > 1.5 * interval:
                continue
            x1, x2 = a.price_at(asset, t1), a.price_at(asset, t2)
            if not (x1 and x2):
                continue
            for shift in FINE_SHIFTS:
                y1, y2 = b.price_at(asset, t1 - shift), b.price_at(asset, t2 - shift)
                if y1 and y2:
                    _add(acc[shift], math.log(y2 / y1), math.log(x2 / x1))
    return _curve(acc)


def compare_peaks(per_window: list[dict[str, dict]], a: str, b: str) -> dict | None:
    """同一批快照对上，b 的峰值相关比 a 高多少：按窗口重抽样（窗口多时相邻窗口并成块），给 95% 区间，
    以及两路峰值所在 δ 的 95% 范围。两路现货高度相关，差值的不确定性比单条曲线的噪声带小得多。"""
    if len(per_window) < 5:
        return None
    size = math.ceil(len(per_window) / BOOT_CLUSTERS)
    width = len(FINE_SHIFTS) * 6
    blocks = []  # 每块一条扁平的累加量：a 的各 δ，然后 b 的各 δ
    for i in range(0, len(per_window), size):
        vec = [0.0] * (2 * width)
        for win in per_window[i:i + size]:
            flat = [x for src in (a, b) for shift in FINE_SHIFTS for x in win[src][shift]]
            vec = [s + x for s, x in zip(vec, flat)]
        blocks.append(vec)

    def peak(vec: list[float], half: int) -> dict | None:
        part = vec[half * width:(half + 1) * width]
        return _peak(_curve({s: part[j * 6:(j + 1) * 6] for j, s in enumerate(FINE_SHIFTS)}))

    rng = random.Random(0)
    diffs, lags = [], {a: [], b: []}
    for _ in range(BOOT_ROUNDS):
        counts = defaultdict(int)
        for _ in blocks:
            counts[rng.randrange(len(blocks))] += 1
        tot = [0.0] * (2 * width)
        for i, c in counts.items():
            tot = [s + c * x for s, x in zip(tot, blocks[i])]
        pa, pb = peak(tot, 0), peak(tot, 1)
        if pa and pb:
            diffs.append(pb["corr"] - pa["corr"])
            lags[a].append(pa["lag"])
            lags[b].append(pb["lag"])
    if len(diffs) < BOOT_ROUNDS // 2:
        return None
    diffs.sort()
    q = lambda xs, p: sorted(xs)[min(len(xs) - 1, int(p * len(xs)))]
    return {"a": a, "b": b, "blocks": len(blocks), "diff_lo": q(diffs, 0.025), "diff_hi": q(diffs, 0.975),
            "lag_range": {src: (q(v, 0.025), q(v, 0.975)) for src, v in lags.items()}}


def edge_trades(recs: list[dict], lag_idx: int, limit: bool = False) -> list[tuple[float, int]]:
    """模型打法：模型概率 − (卖价 + taker 费) ≥ 2¢ 就按卖一买入。返回 (每股成本, 是否赢)。
    默认口径：用 lag 秒之后的盘口判断并成交（市场多看了 lag 秒）。
    limit=True：用同一轮的盘口判断，在那一刻的卖价挂限价单，lag 秒后卖价没涨上去才成交、按那时的卖价成交。"""
    trades = []
    for r in recs:
        p, rate, up = r["model"], r["rate"], r["up"]
        _, bid, bid_size, ask, ask_size = r["book"][lag_idx]
        _, bid0, _, ask0, _ = r["book"][0]
        # 买 Down 就是吃 Up 的买单：Down 卖价 = 1 − Up 买价，股数相同
        sides = ((p, ask, ask_size, ask0, up), (1 - p, 1 - bid, bid_size, 1 - bid0, 1 - up))
        for p_side, price, size, price0, won in sides:
            seen = price0 if limit else price
            if not 0.02 <= seen <= 0.98 or p_side - (seen + fee(rate, seen)) < MIN_MODEL_EDGE:
                continue
            if size is None or size < MIN_SHARES or (limit and price > price0 + 1e-9):
                continue
            trades.append((price + fee(rate, price), won))
    return trades


def summarize_group(recs: list[dict]) -> dict:
    windows = defaultdict(list)
    for r in recs:
        mid0 = (r["book"][0][1] + r["book"][0][3]) / 2
        windows[r["slug"]].append((r["model"] - r["up"]) ** 2 - (mid0 - r["up"]) ** 2)
    # 同一个窗口的几个时点高度相关，标准误按窗口聚类：先在窗口内平均，再看窗口之间的离散
    per_window = [statistics.fmean(v) for v in windows.values()]
    diff_se = statistics.stdev(per_window) / math.sqrt(len(per_window)) if len(per_window) >= 2 else None
    return {
        "n": len(recs), "windows": len(windows), "up_rate": statistics.fmean(r["up"] for r in recs),
        "brier_model": brier([(r["model"], r["up"]) for r in recs]),
        "brier_mid": [brier([((r["book"][i][1] + r["book"][i][3]) / 2, r["up"]) for r in recs]) for i in range(len(LAGS))],
        "actual_lag": [statistics.median(r["book"][i][0] for r in recs) for i in range(len(LAGS))],
        "diff": statistics.fmean(per_window), "diff_se": diff_se,
        "spread": statistics.median(r["book"][0][3] - r["book"][0][1] for r in recs),
        "edge": [strategy(edge_trades(recs, i)) for i in range(len(LAGS))],
        "edge_limit": [strategy(edge_trades(recs, i, limit=True)) for i in range(len(LAGS))],
    }


def summarize_ticks(path: str | None = None) -> dict | None:
    path = path or DB_PATH
    if not os.path.exists(path):
        return None
    db = sqlite3.connect(path, timeout=30)
    try:
        return _summarize(db)
    except sqlite3.OperationalError as e:  # 表还没建好之类
        log.warning("summarize_ticks: %s", e)
        return None
    finally:
        db.close()


def _summarize(db: sqlite3.Connection) -> dict:
    sources = {src for (src,) in db.execute("SELECT DISTINCT COALESCE(source, 'rest') FROM spot")}
    primary = "ws" if "ws" in sources else "rest"
    replays = {src: Replay(db, src) for src in sorted(sources)}
    rp = replays[primary]
    cols = ["slug", "asset", "cycle", "t0", "t_end", "fee_rate", "outcome"]
    windows = [dict(zip(cols, r)) for r in db.execute(f"SELECT {', '.join(cols)} FROM windows ORDER BY t0")]
    recorded = {s for (s,) in db.execute("SELECT DISTINCT slug FROM books")}
    recs, skipped = [], defaultdict(int)
    acc = {src: {c: {s: [0.0] * 6 for s in LEADLAG_STEPS} for c in CYCLES} for src in replays}
    # 亚秒级：有逐笔成交的几路现货（Coinbase 排第一）
    ticks = {src: replays[src] for src in sorted(replays, key=lambda x: x != primary)
             if src in TICK_SOURCE and any(replays[src].tick_ts.values())}
    fine = {src: {c: _shifts() for c in CYCLES} for src in ticks}       # 各自的全部样本
    fine_same = {src: {c: _shifts() for c in CYCLES} for src in ticks}  # 几路都有价格的同一批快照对
    fine_windows = defaultdict(list)  # 周期 → 每个窗口的 same 累加量（比较两路峰值时重抽样用）
    for w in windows:
        if w["slug"] not in recorded:
            continue
        books_ts, books = rp.books(w["slug"])
        for src, other in replays.items():  # 领先滞后不需要结算结果
            other.leadlag(w, books_ts, books, acc[src][w["cycle"]],
                          None if src == primary else rp.spot_at[w["asset"]])
        if ticks:
            own, same = leadlag_fine(db, w, ticks, rp.interval)
            for src in ticks:
                for total, part in ((fine[src][w["cycle"]], own[src]), (fine_same[src][w["cycle"]], same[src])):
                    for shift, a in part.items():
                        total[shift] = [x + y for x, y in zip(total[shift], a)]
            if len(ticks) > 1 and all(same[src][0][0] for src in ticks):
                fine_windows[w["cycle"]].append(same)
        if w["outcome"] is not None:
            got, why = rp.checkpoints(w, books_ts, books)
            recs += got
            for k, v in why.items():
                skipped[k] += v

    groups = []
    for cycle in CYCLES:
        rs = [r for r in recs if r["cycle"] == cycle]
        for offset in CHECKPOINTS[cycle]:
            sub = [r for r in rs if r["offset"] == offset]
            if sub:
                groups.append({"cycle": cycle, "offset": offset, **summarize_group(sub)})
        if rs:
            groups.append({"cycle": cycle, "offset": None, **summarize_group(rs)})
    if recs:
        groups.append({"cycle": None, "offset": None, **summarize_group(recs)})

    leadlag = defaultdict(dict)
    for src, by_cycle in acc.items():
        for cycle, steps in by_cycle.items():
            rows = [{"lag": step * rp.interval, "corr": _corr(a), "n": int(a[0])} for step, a in steps.items()]
            if any(r["corr"] is not None for r in rows):
                leadlag[src][cycle] = rows
    def curves(by_cycle: dict[str, dict]) -> dict[str, list[dict]]:
        return {cycle: rows for cycle, shifts in by_cycle.items() if _peak(rows := _curve(shifts))}
    # leadlag_fine：主现货（Coinbase websocket）自己的全部样本，看板用这一份；
    # leadlag_fine_by_source：几路现货在同一批快照对上并排比较
    fine_primary = curves(fine[primary]) if primary in fine else {}
    by_source = {src: c for src in ticks if (c := curves(fine_same[src]))} if len(ticks) > 1 else {}
    names = list(ticks)
    compare = {cycle: cmp for cycle, wins in fine_windows.items()
               if (cmp := compare_peaks(wins, names[0], names[1]))} if len(names) > 1 else {}
    exchanges = leadlag_exchanges(ticks[names[0]], ticks[names[1]], rp.interval) if len(names) > 1 else []
    if not _peak(exchanges):
        exchanges = []

    polls = [ts for (ts,) in db.execute("SELECT DISTINCT poll_ts FROM spot ORDER BY poll_ts")]
    gaps = [b - a for a, b in zip(polls, polls[1:])]
    last_poll = float(rp.meta.get("last_poll") or (polls[-1] if polls else 0))
    by_cycle = {c: {"recorded": sum(1 for w in windows if w["cycle"] == c and w["slug"] in recorded),
                    "resolved": sum(1 for w in windows if w["cycle"] == c and w["slug"] in recorded and w["outcome"] is not None),
                    "used": len({r["slug"] for r in recs if r["cycle"] == c})} for c in CYCLES}
    latency = [r[0] for r in db.execute("SELECT recv_ts - poll_ts FROM books ORDER BY rowid DESC LIMIT 4000")]
    book_age = sorted(r[0] for r in db.execute(
        "SELECT recv_ts - server_ts FROM books WHERE server_ts IS NOT NULL ORDER BY rowid DESC LIMIT 4000"))
    # BTC 成交最密，最近一笔成交距这一轮多久 ≈ 这一路现货本身有多旧
    spot_age = {src: statistics.median(r[0] for r in rows) for src in sources if (rows := db.execute(
        "SELECT poll_ts - trade_ts FROM spot WHERE asset = 'btc' AND COALESCE(source, 'rest') = ?"
        " AND trade_ts IS NOT NULL ORDER BY rowid DESC LIMIT 2000", (src,)).fetchall())}
    # 逐笔推送晚到多少：收到时间 − 交易所成交时间（含两边的时钟差，本机和 Binance 差约 3 毫秒）
    tick_delay = {src: statistics.median(r[0] for r in rows) for src in ticks if _ticks_have_source(db) and (rows := db.execute(
        "SELECT recv_ts - trade_ts FROM ws_ticks WHERE source = ? AND asset = 'btc' AND trade_ts IS NOT NULL"
        " ORDER BY rowid DESC LIMIT 2000", (TICK_SOURCE[src],)).fetchall())}
    tick_hours = {src: len({ts for by_poll in replays[src].spot_at.values() for ts in by_poll}) * rp.interval / 3600
                  for src in ticks}  # 有这一路现货的轮次折成小时
    return {
        "generated_at": time.time(),
        "recorder": {
            "started": polls[0] if polls else None, "last_poll": last_poll or None,
            "running": bool(last_poll) and time.time() - last_poll < 3 * rp.interval + 10,
            "interval": rp.interval, "polls": len(polls),
            "source_polls": len({ts for by_poll in rp.spot_at.values() for ts in by_poll}),
            "hours": (polls[-1] - polls[0]) / 3600 if len(polls) > 1 else 0.0,
            "gaps_over_10s": sum(g > 10 for g in gaps), "max_gap": max(gaps) if gaps else None,
            "book_rows": db.execute("SELECT COUNT(*) FROM books").fetchone()[0],
            "spot_rows": db.execute("SELECT COUNT(*) FROM spot").fetchone()[0],
            "book_latency": statistics.median(latency) if latency else None,
            "book_age": statistics.median(book_age) if book_age else None,
            "book_age_p95": book_age[int(len(book_age) * 0.95)] if book_age else None,
            "spot_age": spot_age, "tick_delay": tick_delay, "tick_hours": tick_hours,
            "errors": json.loads(rp.meta.get("errors") or "{}"),
        },
        "spot_source": primary, "k": rp.k, "k_source": rp.k_source, "lags": LAGS, "min_edge": MIN_MODEL_EDGE, "min_shares": MIN_SHARES,
        "windows": by_cycle, "checkpoints": len(recs),
        "skipped": dict(skipped), "from_ticks": sum(r["from_ticks"] for r in recs) / max(1, sum(r["minutes"] for r in recs)),
        "groups": groups, "leadlag": leadlag, "leadlag_fine": fine_primary,
        "leadlag_fine_by_source": by_source, "leadlag_compare": compare, "leadlag_exchanges": exchanges,
    }


SOURCE_NAME = {"ws": "Coinbase websocket", "rest": "Coinbase REST", "binance": "Binance websocket"}
SOURCE_SHORT = {"ws": "CB", "rest": "CB-REST", "binance": "BN"}


def _sec(v: float) -> str:
    return "0" if v == 0 else f"{v:+g}"


def print_leadlag(s: dict):
    print("\n== 领先滞后：现货每轮涨跌 vs Up 中间价每轮变动的相关（正的滞后 = 盘口在现货之后动）==")
    for src in sorted(s["leadlag"], key=lambda x: x != s["spot_source"]):
        for cycle, rows in s["leadlag"][src].items():
            n = max(r["n"] for r in rows)
            print(f"  {cycle} · {SOURCE_NAME[src]}（每个滞后约 {n} 对，噪声带 ±{2 / math.sqrt(max(n, 1)):.3f}）")
            print("    " + " ".join(f"{r['lag']:+5.0f}s" for r in rows))
            print("    " + " ".join(f"{r['corr']:6.3f}" if r["corr"] is not None else "     -" for r in rows))
    if len(s["leadlag"]) > 1:
        print(f"  （其他几路只用 {SOURCE_NAME[s['spot_source']]} 也有的轮次；各路开始记录的时间不同，对数见每行。）")
    print_leadlag_fine(s)


def print_leadlag_fine(s: dict):
    by_src, compare = s.get("leadlag_fine_by_source") or {}, s.get("leadlag_compare") or {}
    cross = s.get("leadlag_exchanges") or []
    if not (s["leadlag_fine"] or by_src):
        return
    cols = []  # (表头, 按 FINE_SHIFTS 顺序的结果)
    for cycle in CYCLES:
        cols += [(f"{cycle} {SOURCE_SHORT[src]}", by_cycle[cycle]) for src, by_cycle in by_src.items() if cycle in by_cycle]
    cols += [(f"{cycle} {SOURCE_SHORT[s['spot_source']]}{' all' if by_src else ''}", rows)
             for cycle, rows in s["leadlag_fine"].items()]
    if cross:
        cols.append(("BN>CB", cross))
    print("\n== 亚秒级领先滞后：两次盘口快照之间的中间价变动（CLOB 时间）vs 同样长、提前 δ 秒的现货涨跌（交易所成交时间）==")
    print("  δ > 0 = 现货在前；相关最高的 δ ≈ 盘口比现货晚多少秒。CB = Coinbase，BN = Binance（USDT 计价）")
    if by_src:
        print("  “5m CB / 5m BN”这样成对的列：两路现货用同一批快照对（两路都有价格的），可以直接比；"
              "“CB all”= Coinbase 自己的全部样本（包括加 Binance 之前的）")
    if cross:
        print("  BN>CB：交易所之间，Coinbase 每轮涨跌 vs 提前 δ 秒的 Binance 涨跌；δ > 0 = Binance 先动")
    width = 11
    print(f"  {'delta':>6}" + "".join(f"{h:>{width}}" for h, _ in cols))
    for i, shift in enumerate(FINE_SHIFTS):
        print(f"  {_sec(shift):>6}" + "".join(f"{rows[i]['corr']:>{width}.3f}" if rows[i]["corr"] is not None
                                             else f"{'-':>{width}}" for _, rows in cols))
    ns = [max(r["n"] for r in rows) for _, rows in cols]
    print(f"  {'n':>6}" + "".join(f"{n:>{width}}" for n in ns))
    print(f"  {'band':>6}" + "".join(f"{2 / math.sqrt(max(n, 1)):>{width}.3f}" for n in ns))
    print(f"  {'peak':>6}" + "".join(f"{_sec(_peak(rows)['lag']):>{width}}" for _, rows in cols))
    print("  （n = 快照对数；band = 单条曲线的噪声带 ±2/√n；peak = 相关最高的 δ）")
    for cycle in CYCLES:
        peaks = {src: _peak(by_cycle[cycle]) for src, by_cycle in by_src.items() if cycle in by_cycle}
        if len(peaks) < 2:
            continue
        line = "，".join(f"{SOURCE_SHORT[src]} 最高在 δ = {_sec(p['lag'])} 秒（{p['corr']:.3f}）" for src, p in peaks.items())
        cmp = compare.get(cycle)
        if cmp:
            a, b, lo, hi = cmp["a"], cmp["b"], cmp["diff_lo"], cmp["diff_hi"]
            verdict = (f"{SOURCE_NAME[b]} 明显更贴盘口" if lo > 0 else f"{SOURCE_NAME[a]} 明显更贴盘口" if hi < 0
                       else "两路分不出高下")
            ranges = "，".join(f"{SOURCE_SHORT[src]} {_sec(r[0])} ~ {_sec(r[1])}" for src, r in cmp["lag_range"].items())
            line += (f"；峰值相关 {SOURCE_SHORT[b]} − {SOURCE_SHORT[a]} 的 95% 区间 [{lo:+.3f}, {hi:+.3f}]"
                     f"（按窗口重抽样，{cmp['blocks']} 块），{verdict}；峰值所在 δ 的 95% 范围：{ranges} 秒")
        else:
            line += "；窗口太少，不做重抽样比较"
        print(f"  {cycle}：{line}")
    if cross:
        p, n = _peak(cross), max(r["n"] for r in cross)
        who = (f"Binance 大约早 {p['lag']:g} 秒" if p["lag"] > 0 else f"Coinbase 大约早 {-p['lag']:g} 秒" if p["lag"] < 0
               else "两家几乎同时")
        print(f"  交易所之间：相关在 δ = {_sec(p['lag'])} 秒最高（{p['corr']:.3f}，{n} 对），{who}")


def print_report(s: dict | None):
    if not s:
        print(f"还没有记录数据（{DB_PATH}）。先运行 .venv/bin/python research/short_cycle/record_short.py")
        return
    rec, fmt_t = s["recorder"], (lambda ts: time.strftime("%m-%d %H:%M", time.localtime(ts)) if ts else "-")
    print(f"\n== 前向记录：{DB_PATH} ==")
    print(f"  {fmt_t(rec['started'])} → {fmt_t(rec['last_poll'])}，共 {rec['hours']:.2f} 小时，每 {rec['interval']:.0f} 秒一轮，"
          f"{rec['polls']} 轮；超过 10 秒的断档 {rec['gaps_over_10s']} 次，最长 {rec['max_gap'] or 0:.0f} 秒；"
          f"记录器{'运行中' if rec['running'] else '已停止'}")
    ages = "，".join(f"{SOURCE_NAME[k]} {v:.2f} 秒" for k, v in sorted(rec["spot_age"].items(), key=lambda kv: kv[1]))
    print(f"  盘口请求往返中位 {rec['book_latency'] or 0:.2f} 秒；CLOB 盘口时间戳距收到中位 {rec['book_age'] or 0:.2f} 秒、"
          f"95% 分位 {rec['book_age_p95'] or 0:.2f} 秒；BTC 最近一笔成交距这一轮中位：{ages or '-'}")
    if rec.get("tick_delay"):
        print("  逐笔推送晚到（收到 − 成交时间，BTC 中位）：" + "，".join(
            f"{SOURCE_NAME[k]} {v * 1000:.0f} 毫秒（有这一路的轮次约 {rec['tick_hours'].get(k, 0):.2f} 小时）"
            for k, v in rec["tick_delay"].items()))
    errors = {k: v for k, v in rec["errors"].items() if v}
    print(f"  回放用的现货：{SOURCE_NAME[s['spot_source']]}（有这一路现货的轮次 {rec['source_polls']}，"
          f"约 {rec['source_polls'] * rec['interval'] / 3600:.2f} 小时）；出错次数 {errors or '无'}")
    for c, w in s["windows"].items():
        print(f"  {c:4} 窗口：记录到 {w['recorded']}，已结算 {w['resolved']}，进入回放 {w['used']}")
    print(f"  模型 k：{', '.join(f'{c}={k}' for c, k in s['k'].items()) or '1'}（{s['k_source']}）；"
          f"窗口内收盘价 {s['from_ticks']:.0%} 来自逐轮现货；跳过的时点 {s['skipped'] or '无'}")
    print_leadlag(s)
    if not s["groups"]:
        print("\n  还没有已结算、能回放的窗口。窗口结束后几分钟 Gamma 才给结果，多记录一会儿再看。")
        return

    def label(g):
        return f"{g['cycle'] or '合计':4} {('+' + str(g['offset']) + 's') if g['offset'] else '全部':>6}"

    lag_head = " ".join(f"{'+' + str(lag) + 's':>7}" for lag in s["lags"])
    print("\n== 同一秒的比较：Brier（越低越准）。市场 = Up 中间价，+Ns = 盘口比模型晚 N 秒 ==")
    print(f"  {'周期':4} {'时点':>6} {'n':>4} {'窗口':>4} {'模型':>7} {lag_head}   模型−市场(+0s) ± 标准误")
    for g in s["groups"]:
        mids = " ".join(f"{b:7.4f}" for b in g["brier_mid"])
        se = f"± {g['diff_se']:.4f}" if g["diff_se"] is not None else ""
        print(f"  {label(g)} {g['n']:>4} {g['windows']:>4} {g['brier_model']:7.4f} {mids}   {g['diff']:+.4f} {se}")
    lags = s["groups"][-1]["actual_lag"]
    print(f"  （实际的盘口滞后中位：{', '.join(f'{x:.1f}s' for x in lags)}；+Ns 的市场多看了 N 秒的价格走势，"
          f"本来就该更准，公平比较看 +0s 那一列）")

    def roi(st):
        return f"{st['roi']:+.1%}({st['n']})" if st["n"] else "-"
    print(f"\n== 模型打法：模型 − (卖价 + taker 费) ≥ {s['min_edge'] * 100:.0f}¢ 就按卖一买入，"
          f"卖一 ≥ {s['min_shares']} 股；ROI(笔数)，+Ns = 用 N 秒后的盘口判断和成交 ==")
    print(f"  {'周期':4} {'时点':>6} " + " ".join(f"{'+' + str(lag) + 's':>13}" for lag in s["lags"]))
    for g in s["groups"]:
        print(f"  {label(g)} " + " ".join(f"{roi(st):>13}" for st in g["edge"]))
    print("  限价单口径：同一轮判断，在当时的卖价挂单，N 秒后卖价没涨上去才成交")
    for g in s["groups"]:
        if g["offset"] is None:
            print(f"  {label(g)} " + " ".join(f"{roi(st):>13}" for st in g["edge_limit"]))

    total = s["groups"][-1]
    used = sum(w["used"] for w in s["windows"].values())
    print(f"\n  注意：只覆盖 {used} 个已结算窗口（{', '.join(f"{c} {w['used']} 个" for c, w in s['windows'].items())}），"
          f"有效记录约 {rec['source_polls'] * rec['interval'] / 3600:.1f} 小时。同一窗口的几个时点高度相关，有效样本更接近窗口数；"
          f"模型−市场的 Brier 差 {total['diff']:+.4f} ± {total['diff_se'] or 0:.4f}（按窗口聚类的标准误），"
          f"{'在噪声范围内' if total['diff_se'] is None or abs(total['diff']) < 2 * total['diff_se'] else '超过两倍标准误'}。")


def main():
    global DB_PATH
    parser = argparse.ArgumentParser(description="5 / 15 分钟涨跌盘的秒级前向记录和回放")
    parser.add_argument("--report", action="store_true", help="不记录，回放已记录的数据并打印报告")
    parser.add_argument("--interval", type=float, default=INTERVAL, help="每轮间隔秒数（建议 2–5）")
    parser.add_argument("--minutes", type=float, default=None, help="记录多少分钟后退出；不写就一直记录")
    parser.add_argument("--assets", default=",".join(ASSETS))
    parser.add_argument("--db", default=None, help=f"数据库文件，默认 {os.path.relpath(DB_PATH)}。"
                        "同一个库只能有一个记录器在写，否则会重复记录")
    args = parser.parse_args()
    if args.db:
        DB_PATH = os.path.abspath(args.db)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)-5s %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    if args.report:
        print_report(summarize_ticks())
        return
    assets = [a.strip().lower() for a in args.assets.split(",") if a.strip().lower() in COINBASE]
    asyncio.run(record(max(1.0, args.interval), assets, args.minutes))


if __name__ == "__main__":
    main()
