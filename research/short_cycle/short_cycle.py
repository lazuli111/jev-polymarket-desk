"""半小时内结算的短周期市场：现状快照 + 5 分钟 / 15 分钟加密货币涨跌盘的回测。

结算规则（市场描述原文的意思）：窗口内 Chainlink 价格的 TWAP ≥ 窗口开始时的价格 → Up，否则 Down。

回测回答三个问题：
1. 窗口内各个时点，市场价准不准（Brier、按热门方价格分桶的校准）？
2. 用现货价（Coinbase 分钟 K 线）按 TWAP 规则算出的“模型概率”，能不能比市场价更准？
3. 扣掉 taker 手续费（rate × p × (1−p)）和半个价差之后，几种简单打法还剩多少？

用法：.venv/bin/python research/short_cycle/short_cycle.py [--hours 24] [--assets btc,eth,sol,xrp]
结果写到 data/short_cycle.json（ui.py 读取）和 data/short_cycle_records.csv。
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import math
import os
import statistics
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import httpx

import sys

# 归档在 research/short_cycle/ 下，polymarket.py 在项目根目录
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from polymarket import GAMMA, Polymarket

log = logging.getLogger("short")

CYCLES = {"5m": 300, "15m": 900}
# 窗口开始后多少秒取一次价格（0 = 刚开盘）
CHECKPOINTS = {"5m": [0, 60, 120, 180, 240], "15m": [0, 300, 600, 840]}
COINBASE = {"btc": "BTC-USD", "eth": "ETH-USD", "sol": "SOL-USD", "xrp": "XRP-USD", "doge": "DOGE-USD"}
HALF_SPREAD = 0.005     # 历史价格按中间价算，买入时再加半个价差（现在的盘口价差多在 1¢）
MIN_MODEL_EDGE = 0.02   # 模型打法：扣完成本后至少 2¢ 才进场
SIGMA_LOOKBACK_MIN = 60
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
FMT = "%Y-%m-%dT%H:%M:%SZ"


def fee(rate: float, p: float) -> float:
    return rate * p * (1 - p)


def norm_cdf(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


# ---------- 现货价：Coinbase 分钟 K 线 ----------

async def fetch_spot(client: httpx.AsyncClient, product: str, start: int, end: int) -> dict[int, tuple[float, float, float]]:
    """返回 {时刻: (收盘, 最高, 最低)}，时刻是这根分钟 K 线的结束时间，也就是这些价格“已知”的时刻。"""
    prices: dict[int, tuple[float, float, float]] = {}
    step = 300 * 60  # 每次最多 300 根
    for chunk in range(start, end, step):
        params = {"granularity": 60, "start": time.strftime(FMT, time.gmtime(chunk)),
                  "end": time.strftime(FMT, time.gmtime(min(chunk + step, end)))}
        for attempt in range(3):
            r = await client.get(f"https://api.exchange.coinbase.com/products/{product}/candles", params=params)
            if r.status_code == 200:
                break
            await asyncio.sleep(1 + attempt)
        else:
            r.raise_for_status()
        for bucket, low, high, _open, close, _vol in r.json():
            prices[int(bucket) + 60] = (float(close), float(high), float(low))
        await asyncio.sleep(0.2)  # Coinbase 公共接口限速
    return prices


def model_z(spot: dict[int, tuple[float, float, float]], t0: int, width: int, t: int) -> float | None:
    """按 TWAP 规则算 Up 的 z 值，P(Up) = Φ(z / k)，k 是从数据里拟合的波动率放大系数。

    对数价格当布朗运动：已走完的部分按分钟收盘价近似，剩下 τ 分钟的平均值 ~ N(当前价, σ²τ/3)。
    Up ⇔ Σ已实现 + τ·x_t ≥ 0（都是相对开盘价的对数偏离），所以 z = (Σ已实现 + τ·x_t) / (τ·σ·√(τ/3))。
    σ 用 Parkinson 估计（分钟最高/最低价），能看到分钟内的波动；只用收盘价会严重低估，模型会过度自信。
    窗口刚开时 z = 0。
    """
    start, now = spot.get(t0), spot.get(t)
    if not start or not now:
        return None
    s0 = start[0]
    elapsed = [spot.get(t0 + 60 * k) for k in range(1, (t - t0) // 60 + 1)]
    if any(c is None for c in elapsed):
        return None
    realized = sum(math.log(c[0] / s0) for c in elapsed)
    tau = (t0 + width - t) / 60
    if tau <= 0:
        return math.inf if realized >= 0 else -math.inf
    past = [spot.get(t - 60 * k) for k in range(SIGMA_LOOKBACK_MIN)]
    if any(c is None for c in past):
        return None
    sigma = math.sqrt(statistics.fmean(math.log(h / lo) ** 2 for _, h, lo in past) / (4 * math.log(2))) or 1e-6
    return (realized + tau * math.log(now[0] / s0)) / (tau * sigma * math.sqrt(tau / 3))


def model_p(z: float | None, k: float) -> float | None:
    return None if z is None else norm_cdf(z / k)


# ---------- Polymarket ----------

def price_at(points: list[tuple[int, float]], t: int, max_age: int = 90) -> tuple[float, int] | None:
    """t 时刻已知的最新市场价和它的“年龄”（秒）；超过 max_age 秒没有更新就当作没有数据。"""
    best = None
    for ts, p in points:
        if ts > t:
            break
        best = (ts, p)
    if best is None or t - best[0] > max_age:
        return None
    return best[1], t - best[0]


def price_after(points: list[tuple[int, float]], t: int, max_wait: int = 75) -> float | None:
    """t 之后第一个市场价。价格历史大约一分钟一个点，t 之前那个点可能已经过时，
    用它当成交价会让“现货领先市场”的优势虚高；保守口径用 t 之后的第一个点成交。"""
    for ts, p in points:
        if t <= ts <= t + max_wait:
            return p
        if ts > t + max_wait:
            break
    return None


async def fetch_windows(pm: Polymarket, asset: str, cycle: str, starts: list[int]) -> list[dict]:
    slugs = [f"{asset}-updown-{cycle}-{t0}" for t0 in starts]
    rows: list[dict] = []
    for i in range(0, len(slugs), 20):
        params = [("slug", s) for s in slugs[i:i + 20]] + [("closed", "true")]
        rows += await pm._call("GET", f"{GAMMA}/markets", params=params)
    return rows


async def window_records(pm: Polymarket, raw: dict, asset: str, cycle: str, spot: dict[int, float]) -> list[dict]:
    prices = json.loads(raw.get("outcomePrices") or "[]")
    outcomes = json.loads(raw.get("outcomes") or "[]")
    if outcomes != ["Up", "Down"] or not prices or float(prices[0]) not in (0.0, 1.0):
        return []
    t0, width = int(raw["slug"].rsplit("-", 1)[1]), CYCLES[cycle]
    up_token = json.loads(raw["clobTokenIds"])[0]
    rate = float((raw.get("feeSchedule") or {}).get("rate") or 0) if raw.get("feesEnabled") else 0.0
    history = await pm.price_history(up_token, t0 - 180, t0 + width + 30)
    out = []
    for offset in CHECKPOINTS[cycle]:
        t = t0 + offset
        known = price_at(history, t)
        if known is None:
            continue
        out.append({
            "asset": asset, "cycle": cycle, "t0": t0, "offset": offset,
            "market": known[0], "market_age": known[1], "market_next": price_after(history, t),
            "z": model_z(spot, t0, width, t),
            "up": int(float(prices[0]) == 1.0), "fee_rate": rate,
            "volume": float(raw.get("volumeNum") or 0),
        })
    return out


# ---------- 汇总 ----------

def brier(pairs):
    return sum((p - y) ** 2 for p, y in pairs) / len(pairs) if pairs else None


def strategy(trades: list[tuple[float, int]]) -> dict:
    """trades: (每股成本, 是否赢)。返回笔数、命中率、每股平均盈亏、ROI。"""
    if not trades:
        return {"n": 0}
    cost = sum(c for c, _ in trades)
    pnl = sum(w - c for c, w in trades)
    return {"n": len(trades), "win_rate": sum(w for _, w in trades) / len(trades),
            "avg_pnl": pnl / len(trades), "roi": pnl / cost}


FAV_BINS = [(0.5, 0.6), (0.6, 0.7), (0.7, 0.8), (0.8, 0.9), (0.9, 0.95), (0.95, 1.0001)]


def summarize_group(rs: list[dict]) -> dict:
    pairs = [(r["market"], r["up"]) for r in rs]
    # 模型相关的指标只在测试集上算：k 是用训练集拟合的
    with_model = [r for r in rs if r["model"] is not None and r["split"] == "test"]
    # 热门方 = 价格 ≥ 0.5 的那一边；看热门方实际赢的比例是否等于它的价格
    fav = [(max(r["market"], 1 - r["market"]), int((r["market"] >= 0.5) == bool(r["up"]))) for r in rs]
    buckets = []
    for lo, hi in FAV_BINS:
        b = [(f, w) for f, w in fav if lo <= f < hi]
        if b:
            buckets.append({"lo": lo, "hi": min(hi, 1.0), "n": len(b),
                            "price": sum(f for f, _ in b) / len(b), "win_rate": sum(w for _, w in b) / len(b)})

    follow, fade, edge, edge_next = [], [], [], []
    for r in rs:
        p_up, rate = r["market"], r["fee_rate"]
        fav_up = p_up >= 0.5
        f = max(p_up, 1 - p_up)
        fav_won = int(fav_up == bool(r["up"]))
        if 0.55 <= f <= 0.95:
            follow.append((f + HALF_SPREAD + fee(rate, f), fav_won))
            fade.append((1 - f + HALF_SPREAD + fee(rate, 1 - f), 1 - fav_won))
        if r["model"] is not None and r["split"] == "test":
            for side_p_model, side_p_mkt, won in ((r["model"], p_up, r["up"]), (1 - r["model"], 1 - p_up, 1 - r["up"])):
                cost = side_p_mkt + HALF_SPREAD + fee(rate, side_p_mkt)
                if 0.02 <= side_p_mkt <= 0.98 and side_p_model - cost >= MIN_MODEL_EDGE:
                    edge.append((cost, won))
            # 保守口径：用 t 之后第一个市场价判断和成交
            if r["market_next"] is not None:
                nxt = r["market_next"]
                for side_p_model, side_p_mkt, won in ((r["model"], nxt, r["up"]), (1 - r["model"], 1 - nxt, 1 - r["up"])):
                    cost = side_p_mkt + HALF_SPREAD + fee(rate, side_p_mkt)
                    if 0.02 <= side_p_mkt <= 0.98 and side_p_model - cost >= MIN_MODEL_EDGE:
                        edge_next.append((cost, won))

    return {
        "n": len(rs),
        "up_rate": sum(r["up"] for r in rs) / len(rs),
        "brier_market": brier(pairs),
        "n_model": len(with_model),
        "brier_model": brier([(r["model"], r["up"]) for r in with_model]),
        "brier_market_same": brier([(r["market"], r["up"]) for r in with_model]),
        "buckets": buckets,
        "market_age_median": statistics.median(r["market_age"] for r in rs),
        "brier_market_next": brier([(r["market_next"], r["up"]) for r in with_model if r["market_next"] is not None]),
        "strategies": {"follow_favorite": strategy(follow), "fade_favorite": strategy(fade),
                       "model_edge": strategy(edge), "model_edge_next": strategy(edge_next)},
    }


K_GRID = [0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0, 6.0]


def fit_k(records: list[dict]) -> list[dict]:
    """每个周期：较早的一半窗口拟合 k（让模型 Brier 最小），较晚的一半留作测试。结果直接写回 records。"""
    fits = []
    for cycle in CYCLES:
        rs = [r for r in records if r["cycle"] == cycle]
        starts = sorted({r["t0"] for r in rs})
        if len(starts) < 4:
            continue
        split = starts[len(starts) // 2]
        train = [r for r in rs if r["t0"] < split and r["z"] is not None]
        scores = {k: brier([(model_p(r["z"], k), r["up"]) for r in train]) for k in K_GRID}
        k = min(scores, key=scores.get)
        for r in rs:
            r["split"] = "train" if r["t0"] < split else "test"
            r["model"] = model_p(r["z"], k)
        fits.append({"cycle": cycle, "k": k, "train_windows": sum(t < split for t in starts),
                     "test_windows": sum(t >= split for t in starts), "train_brier": scores[k],
                     "train_brier_k1": scores[1.0]})
    for r in records:
        r.setdefault("split", "train")
        r.setdefault("model", None)
    return fits


def summarize(records: list[dict]) -> dict:
    fits = fit_k(records)
    by_cp, windows = defaultdict(list), defaultdict(set)
    for r in records:
        by_cp[(r["cycle"], r["offset"])].append(r)
        windows[(r["asset"], r["cycle"])].add((r["t0"], r["up"], r["volume"]))
    checkpoints = [{"cycle": c, "offset": o, **summarize_group(rs)}
                   for (c, o), rs in sorted(by_cp.items(), key=lambda kv: (CYCLES[kv[0][0]], kv[0][1]))]
    assets = []
    for (a, c), ws in sorted(windows.items()):
        vols = sorted(v for _, _, v in ws)
        assets.append({"asset": a, "cycle": c, "windows": len(ws),
                       "up_rate": sum(u for _, u, _ in ws) / len(ws),
                       "median_volume": vols[len(vols) // 2], "total_volume": sum(vols)})
    return {"fits": fits, "checkpoints": checkpoints, "assets": assets}


# ---------- 现状快照 ----------

def snapshot_row(raw: dict, now: float) -> dict:
    ev = (raw.get("events") or [{}])[0]
    bid, ask = raw.get("bestBid"), raw.get("bestAsk")
    end = datetime.fromisoformat(raw["endDate"].replace("Z", "+00:00")).timestamp()
    return {
        "question": raw.get("question"), "slug": raw.get("slug"), "series": ev.get("seriesSlug") or "",
        "end_ts": end, "minutes_left": round((end - now) / 60, 1),
        "bid": bid, "ask": ask, "spread": (ask - bid) if bid is not None and ask is not None else None,
        "liquidity": float(raw.get("liquidityNum") or 0), "volume_24h": float(raw.get("volume24hr") or 0),
        "fee_rate": float((raw.get("feeSchedule") or {}).get("rate") or 0) if raw.get("feesEnabled") else 0.0,
    }


async def snapshot(pm: Polymarket, minutes: int = 30) -> dict:
    now = datetime.now(timezone.utc)
    rows = await pm.raw_markets({
        "active": "true", "closed": "false", "order": "volume24hr", "ascending": "false",
        "end_date_min": now.strftime(FMT), "end_date_max": (now + timedelta(minutes=minutes)).strftime(FMT),
    }, pages=10)
    markets = [snapshot_row(r, now.timestamp()) for r in rows if r.get("acceptingOrders")]
    groups = defaultdict(list)
    for m in markets:
        groups[m["series"] or "other"].append(m)
    summary = []
    for series, ms in groups.items():
        spreads = sorted(m["spread"] for m in ms if m["spread"] is not None)
        summary.append({"series": series, "n": len(ms), "volume_24h": sum(m["volume_24h"] for m in ms),
                        "median_spread": spreads[len(spreads) // 2] if spreads else None,
                        "liquidity": sum(m["liquidity"] for m in ms)})
    summary.sort(key=lambda g: g["volume_24h"], reverse=True)
    return {"taken_at": now.timestamp(), "minutes": minutes, "count": len(markets),
            "groups": summary, "markets": sorted(markets, key=lambda m: m["end_ts"])}


async def live_windows(pm: Polymarket, client: httpx.AsyncClient, assets: list[str],
                       k_by_cycle: dict[str, float]) -> list[dict]:
    """当前正在进行的 5 分钟 / 15 分钟窗口：开盘价、现价、市场价、模型概率。给 ui.py 用。"""
    now = int(time.time())
    minute = now - now % 60  # 最近一根已收完的分钟 K 线的结束时刻
    spots = dict(zip(assets, await asyncio.gather(*(
        fetch_spot(client, COINBASE[a], now - (SIGMA_LOOKBACK_MIN + 15) * 60, now + 60) for a in assets))))
    slugs = {f"{a}-updown-{c}-{now - now % w}": (a, c) for a in assets for c, w in CYCLES.items()}
    raws = await pm._call("GET", f"{GAMMA}/markets", params=[("slug", s) for s in slugs] + [("closed", "false")])
    raws = [r for r in raws if r.get("slug") in slugs]
    books = await pm.books([json.loads(r["clobTokenIds"])[0] for r in raws]) if raws else {}
    out = []
    for raw in raws:
        asset, cycle = slugs[raw["slug"]]
        width, spot = CYCLES[cycle], spots[asset]
        t0 = int(raw["slug"].rsplit("-", 1)[1])
        book = books.get(json.loads(raw["clobTokenIds"])[0])
        bid = book.best_bid if book else None
        ask = book.best_ask if book else None
        latest = spot[max(spot)][0] if spot else None
        s0 = spot.get(t0, (None,))[0]
        z = model_z(spot, t0, width, minute) if minute >= t0 else 0.0
        out.append({
            "asset": asset, "cycle": cycle, "t0": t0, "end": t0 + width, "seconds_left": t0 + width - now,
            "question": raw.get("question"), "slug": raw["slug"],
            "open_price": s0, "spot": latest,
            "dev_bp": math.log(latest / s0) * 1e4 if latest and s0 else None,
            "bid": bid, "ask": ask, "mid": (bid + ask) / 2 if bid is not None and ask is not None else None,
            "model": model_p(z, k_by_cycle.get(cycle, 1.0)), "k": k_by_cycle.get(cycle, 1.0),
            "fee_rate": float((raw.get("feeSchedule") or {}).get("rate") or 0) if raw.get("feesEnabled") else 0.0,
            "liquidity": float(raw.get("liquidityNum") or 0), "volume": float(raw.get("volumeNum") or 0),
        })
    out.sort(key=lambda w: (CYCLES[w["cycle"]], assets.index(w["asset"])))
    return out


# ---------- 主流程 ----------

async def run(hours: float, assets: list[str]) -> dict:
    pm = Polymarket(concurrency=8)
    now = int(time.time())
    try:
        snap = await snapshot(pm)
        records: list[dict] = []
        async with httpx.AsyncClient(timeout=20, headers={"user-agent": "jev-desk/0.1"}) as client:
            for asset in assets:
                start = now - int(hours * 3600)
                spot = await fetch_spot(client, COINBASE[asset], start - (SIGMA_LOOKBACK_MIN + 10) * 60, now) if asset in COINBASE else {}
                for cycle, width in CYCLES.items():
                    first = start - start % width
                    starts = [t0 for t0 in range(first, now, width) if t0 + width <= now - 120]  # 结束 2 分钟以上，基本已结算
                    raws = await fetch_windows(pm, asset, cycle, starts)
                    results = await asyncio.gather(*(window_records(pm, r, asset, cycle, spot) for r in raws),
                                                   return_exceptions=True)
                    errors = sum(isinstance(x, Exception) for x in results)
                    got = [rec for x in results if not isinstance(x, Exception) for rec in x]
                    records += got
                    log.info("%s %s: %d windows → %d resolved, %d records, %d errors",
                             asset, cycle, len(starts), len(raws), len({r['t0'] for r in got}), errors)
    finally:
        await pm.close()

    result = {"generated_at": time.time(), "params": {"hours": hours, "assets": assets, "half_spread": HALF_SPREAD,
                                                       "min_model_edge": MIN_MODEL_EDGE},
              "snapshot": snap, "backtest": summarize(records)}
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(os.path.join(DATA_DIR, "short_cycle.json"), "w") as f:
        json.dump(result, f, ensure_ascii=False, indent=1)
    with open(os.path.join(DATA_DIR, "short_cycle_records.csv"), "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["asset", "cycle", "t0", "offset", "market", "market_age", "market_next",
                                               "z", "model", "split", "up", "fee_rate", "volume"])
        writer.writeheader()
        writer.writerows(records)
    return result


def print_report(result: dict):
    snap, bt = result["snapshot"], result["backtest"]
    print(f"\n== 现在：未来 {snap['minutes']} 分钟内结算的市场 {snap['count']} 个 ==")
    for g in snap["groups"][:12]:
        spread = f"{g['median_spread']:.3f}" if g["median_spread"] is not None else "-"
        print(f"  {g['series'][:32]:32} n={g['n']:<3} 24h 成交 ${g['volume_24h']:>9,.0f}  价差中位 {spread}")
    print("\n== 回测：各币种 ==")
    for a in bt["assets"]:
        print(f"  {a['asset']:4} {a['cycle']:4} 窗口 {a['windows']:<4} Up 比例 {a['up_rate']:.2f}  单盘成交中位 ${a['median_volume']:,.0f}")
    for fit in bt["fits"]:
        print(f"  {fit['cycle']:4} 波动率放大系数 k={fit['k']}（训练 {fit['train_windows']} 个窗口，"
              f"Brier {fit['train_brier_k1']:.4f} → {fit['train_brier']:.4f}；测试 {fit['test_windows']} 个窗口）")
    print("\n== 回测：各时点（全部币种合并；模型列只算测试集，括号里是同一批样本的市场 Brier）==")
    print(f"  {'周期':4} {'时点':>6} {'n':>5} {'价格年龄':>6} {'Brier市场':>9} {'Brier模型(市场/之后)':>22} "
          f"{'跟热门ROI':>9} {'押冷门ROI':>9} {'模型打法':>16} {'模型打法(保守)':>16}")
    for c in bt["checkpoints"]:
        s = c["strategies"]
        def roi(k):
            return f"{s[k]['roi']:+.1%} (n={s[k]['n']})" if s[k]["n"] else "-"
        nxt = f"{c['brier_market_next']:.4f}" if c["brier_market_next"] is not None else "-"
        bm = f"{c['brier_model']:.4f}({c['brier_market_same']:.4f}/{nxt})" if c["brier_model"] is not None else "-"
        print(f"  {c['cycle']:4} {'+' + str(c['offset']) + 's':>6} {c['n']:>5} {c['market_age_median']:>5.0f}s {c['brier_market']:>9.4f} "
              f"{bm:>22} {s['follow_favorite'].get('roi', 0):>+9.1%} {s['fade_favorite'].get('roi', 0):>+9.1%} "
              f"{roi('model_edge'):>16} {roi('model_edge_next'):>16}")


def main():
    parser = argparse.ArgumentParser(description="半小时内结算的短周期市场分析")
    parser.add_argument("--hours", type=float, default=24, help="回测最近多少小时")
    parser.add_argument("--assets", default="btc,eth,sol,xrp")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)-5s %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    result = asyncio.run(run(args.hours, [a.strip().lower() for a in args.assets.split(",") if a.strip()]))
    print_report(result)


if __name__ == "__main__":
    main()
