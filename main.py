"""每 15 分钟一轮：管理持仓 → 补结算结果 → 收集 → Jev 判断 → 定价 → Jev 挑一个 → 影子开仓。

目前只有影子模式：所有成交都按当时真实盘口模拟，不会下单。
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import time

import config
from book import Ledger
from collect import collect
from filters import soft_reject
from judge import JevJudge, MockJudge
from pick import pick
from polymarket import Polymarket
from sizing import evaluate, fee_per_share, fill_summary

log = logging.getLogger("desk")


async def manage_positions(pm: Polymarket, ledger: Ledger):
    positions = ledger.open_positions()
    if not positions:
        return
    markets = {m.condition_id: m for m in await pm.by_condition([p["condition_id"] for p in positions])}
    books = await pm.books([p["token_id"] for p in positions])
    now = time.time()
    for p in positions:
        m = markets.get(p["condition_id"])
        if m is None:
            continue
        if m.resolved_a is not None:
            payout = m.resolved_a if p["side"] == 0 else 1 - m.resolved_a
            pnl = ledger.close_position(p, payout, 0.0, "resolved", now)
            log.info("CLOSE resolved  %+.2f  %s", pnl, p["question"][:70])
            continue

        book = books.get(p["token_id"])
        if book is None or book.best_bid is None:
            continue  # 已过截止时间、等待结算时盘口可能是空的
        bid = book.best_bid
        reason = None
        if bid - fee_per_share(m.fee_rate, bid) >= p["fair"]:
            reason = "edge_captured"
        elif m.volume_1wk > 0 and m.volume_24h < config.EXIT_VOLUME_DECAY * m.volume_1wk / 7:
            reason = "volume_decay"
        if reason is None:
            unrealized = p["shares"] * (bid - p["entry_price"]) - p["entry_fee"]
            log.info("HOLD  bid %.3f  entry %.3f  fair %.3f  uPnL %+.2f  %s",
                     bid, p["entry_price"], p["fair"], unrealized, p["question"][:60])
            continue

        shares, avg, fee = fill_summary(book.sell(p["shares"]), m.fee_rate)
        if shares < p["shares"] - 1e-6:
            log.warning("want to exit (%s) but book too thin: %s", reason, p["question"][:60])
            continue
        pnl = ledger.close_position(p, avg, fee, reason, now)
        log.info("CLOSE %-13s %+.2f  %s", reason, pnl, p["question"][:70])


async def settle_judgments(pm: Polymarket, ledger: Ledger):
    """给已到期的历史判断补上真实结果，report.py 用它算校准。"""
    cids = ledger.unsettled_condition_ids(time.time())
    if not cids:
        return
    settled = 0
    for m in await pm.by_condition(cids):
        if m.resolved_a is not None:
            ledger.settle(m.condition_id, m.resolved_a)
            settled += 1
    log.info("settled %d / %d past-due judgments", settled, len(cids))


async def cycle(pm: Polymarket, judge, ledger: Ledger):
    started = time.time()
    await manage_positions(pm, ledger)
    await settle_judgments(pm, ledger)

    dossiers = await collect(pm, ledger)
    results = await asyncio.gather(*(judge.judge(d) for d in dossiers), return_exceptions=True)
    now, tokens, opps = time.time(), 0, []
    for d, j in zip(dossiers, results):
        cid = d.market.condition_id
        if isinstance(j, Exception):
            log.warning("jev failed on %s: %s", cid[:10], j)
            continue
        tokens += j.input_tokens
        reason = soft_reject(j)
        opp = None if reason else evaluate(d, j, ledger.cash())
        verdict = reason or ("candidate" if opp else "no_edge")
        ledger.log_judgment(d, j, verdict, now)
        log.info("%-15s p=%.2f mid=%.3f clear=%.2f dispute=%.2f info=%.1f  %s",
                 verdict, j.p_a, d.market.mid, j.rules_clear, j.dispute_risk, j.info, d.market.question[:60])
        if opp:
            opps.append(opp)
        else:
            hours = config.BENCH_REJECT_HOURS if reason else config.BENCH_PASS_HOURS
            ledger.bench(cid, hours, verdict, now)

    chosen = None
    slots = config.MAX_POSITIONS - len(ledger.open_positions())
    if opps and slots > 0:
        opps.sort(key=lambda o: o.edge, reverse=True)
        shortlist, events = [], set()
        for o in opps:
            if o.dossier.market.event_id not in events:
                events.add(o.dossier.market.event_id)
                shortlist.append(o)
        shortlist = shortlist[:config.SHORTLIST]
        chosen, label, confidence = await pick(judge, shortlist)
        if chosen:
            ledger.open_position(chosen, time.time())
            log.info("OPEN  buy %s × %.1f @ %.3f (fee %.2f)  fair %.3f  edge %.3f  pick %.2f  %s",
                     chosen.outcome, chosen.shares, chosen.avg_price, chosen.fee, chosen.fair,
                     chosen.edge, confidence, chosen.dossier.market.question[:60])
        else:
            log.info("no pick (jev said %s @ %.2f)", label, confidence)
    for o in opps:
        if o is not chosen:
            ledger.bench(o.dossier.market.condition_id, config.BENCH_PASS_HOURS, "not_picked", now)

    log.info("cycle done in %.0fs | cash %.2f | open %d | jev tokens %d (~$%.5f)",
             time.time() - started, ledger.cash(), len(ledger.open_positions()),
             tokens, tokens * config.JEV_PRICE_PER_MTOK / 1e6)


async def run(args):
    ledger = Ledger(config.MOCK_DB_PATH if args.mock else config.DB_PATH)
    pm = Polymarket()
    judge = MockJudge() if args.mock else JevJudge()
    try:
        while True:
            try:
                await cycle(pm, judge, ledger)
            except Exception:
                log.exception("cycle failed")
            if args.once:
                break
            await asyncio.sleep(config.CYCLE_SECONDS)
    finally:
        await pm.close()
        await judge.close()


def load_dotenv(path: str = ".env"):
    """读取 .env 里的 KEY=VALUE（不覆盖已有环境变量），省得每次手动 export。"""
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Jev × Polymarket 影子交易台")
    parser.add_argument("--once", action="store_true", help="只跑一轮")
    parser.add_argument("--mock", action="store_true", help="不调用 Jev，用伪随机数跑通流程（数据存到单独的库）")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)-7s %(message)s", datefmt="%H:%M:%S")
    for noisy in ("httpx", "httpx2", "typesafe_sdk"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    if not args.mock and not os.environ.get("TYPESAFE_API_KEY"):
        raise SystemExit("缺少 TYPESAFE_API_KEY。没有 key 可以先加 --mock 跑通流程。")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
