"""Jev 历史回测：拿最近已结算的市场，把时间倒回结算前 24 小时（T），看 Jev 比市场价准不准。

每个市场：
- 市场价 = T 时刻 CLOB 价格历史里第一个结果的价格
- 档案和实盘一样（collect.build_state），today / time_left 按 T 算
- 新闻只用 T 之前 7 天内发布的（Google News 日期搜索 + 按发布时间再过滤一次）
- 问 Jev 和实盘一样的两组问题（结果概率 + 信息量；只看规则的规则清晰度 + 争议风险）

结果逐条追加到 records.jsonl，可以断点续跑；--analyze 只分析不调用。

用法（项目根目录）：
  .venv/bin/python research/jev_backtest/backtest_jev.py              # 抽样、回测、分析
  .venv/bin/python research/jev_backtest/backtest_jev.py --analyze    # 只分析已有记录
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import random
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(HERE)))  # 项目根目录

import config  # noqa: E402
from collect import Dossier, build_state, fetch_news  # noqa: E402
from news import NewsFeed  # noqa: E402
from polymarket import Market, Polymarket  # noqa: E402

log = logging.getLogger("backtest")

RECORDS = os.path.join(HERE, "records.jsonl")
RESULTS = os.path.join(HERE, "results.json")
FMT = "%Y-%m-%dT%H:%M:%SZ"
HALF_SPREAD = 0.01  # 历史上只有价格没有盘口，买入时按多付 1¢ 算


def _ts(s: str | None) -> float | None:
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00").replace(" ", "T")).timestamp()


# ---------- 抽样 ----------

async def sample(pm: Polymarket, days: int, max_sports: int, max_others: int, seed: int) -> list[tuple[Market, dict]]:
    now = datetime.now(timezone.utc)
    rows = await pm.raw_markets({
        "closed": "true", "order": "volumeNum", "ascending": "false", "volume_num_min": 20000,
        "end_date_min": (now - timedelta(days=days)).strftime(FMT), "end_date_max": now.strftime(FMT),
    }, pages=21)
    sports, others = [], []
    for raw in rows:
        series = ((raw.get("events") or [{}])[0].get("seriesSlug") or "")
        if "up-or-down" in series or "multi-strikes" in series or "updown" in (raw.get("slug") or ""):
            continue  # 5 / 15 分钟和小时级涨跌盘不归交易台管
        m = Market.from_gamma(raw)
        if not m or m.resolved_a not in (0.0, 1.0) or not raw.get("closedTime"):
            continue
        if (raw.get("feeType") or "").startswith("sports"):
            if raw.get("gameStartTime"):  # 只要知道开赛时间的：事件时间确定，T 才不会落在结果之后
                sports.append((m, raw))
        else:
            others.append((m, raw))
    rng = random.Random(seed)
    rng.shuffle(sports)
    rng.shuffle(others)
    log.info("可用：体育 %d、其他 %d；抽 %d + %d", len(sports), len(others),
             min(max_sports, len(sports)), min(max_others, len(others)))
    return sports[:max_sports] + others[:max_others]


# ---------- 回测一个市场 ----------

# 新闻标题看起来已经在报道结果（新闻站会在原链接上更新标题，发布时间却不变）。只做标记，分析时分组对比
RESULT_WORDS = re.compile(
    r"\b(beat|beats|defeat(s|ed)?|rout(s|ed)?|edges? past|final score|recap|highlights|clinch(es|ed)?|"
    r"eliminat(es|ed)|knocked out|declared (the )?winner|wins? (the )?(election|title|final|race|vote)|"
    r"won|victory over|results?:|seats?, (election|officials?)|landslide)\b", re.I)


async def backtest_one(pm, judge, news_lock: asyncio.Lock, m: Market, raw: dict, hours_before: float) -> dict | None:
    closed = _ts(raw.get("closedTime"))
    game = _ts(raw.get("gameStartTime"))
    if game:   # 体育：开赛前 hours_before 小时
        t, basis = int(game - hours_before * 3600), "game_start"
    else:      # 其他：事件时间不确定，用结算 / 到期前 72 小时，结论只作参考
        t, basis = int(min(m.end_ts, closed) - 72 * 3600), "close_minus_72h"
    history = await pm.price_history(m.token_ids[0], t - 6 * 3600, t, fidelity=30)
    price = history[-1][1] if history else None
    record = {
        "condition_id": m.condition_id, "question": m.question, "outcome_a": m.outcomes[0],
        "category": raw.get("feeType") or "none",
        "fee_rate": m.fee_rate, "t": t, "t_basis": basis, "end_ts": m.end_ts, "closed_ts": closed,
        "price": price, "resolved_a": m.resolved_a, "volume": float(raw.get("volumeNum") or 0),
    }
    if price is None:
        record["skipped"] = "no_price"  # T 之前 6 小时没有成交
        return record
    if not 0.05 <= price <= 0.95:
        record["skipped"] = "price_extreme"  # 已经基本定局，交易台也不会碰
        return record
    async with news_lock:  # 新闻一个一个搜，别被 Google 限速
        async with NewsFeed(now=t, until=t) as feed:
            news = await fetch_news(feed, m)
        await asyncio.sleep(0.3)
    d = Dossier(m, [], 0, build_state(m, t, news))
    j = await judge.judge(d)
    record.update(n_news=len(news), news=news, result_like_news=sum(bool(RESULT_WORDS.search(n)) for n in news),
                  p_jev=j.p_a, info=j.info, rules_clear=j.rules_clear,
                  dispute_risk=j.dispute_risk, model=j.model, input_tokens=j.input_tokens)
    return record


async def run(days: int, max_sports: int, max_others: int, hours_before: float, seed: int, limit: int | None):
    from judge import JevJudge  # 只有真跑回测才需要 key

    done = set()
    if os.path.exists(RECORDS):
        with open(RECORDS) as f:
            done = {json.loads(line)["condition_id"] for line in f if line.strip()}
    pm = Polymarket(concurrency=6)
    judge = JevJudge(concurrency=8)
    news_lock, sem = asyncio.Lock(), asyncio.Semaphore(6)
    try:
        todo = [(m, raw) for m, raw in await sample(pm, days, max_sports, max_others, seed) if m.condition_id not in done]
        todo = todo[:limit] if limit else todo
        log.info("已有 %d 条，这次要跑 %d 个市场", len(done), len(todo))
        finished = 0

        async def task(m, raw):
            nonlocal finished
            async with sem:
                try:
                    rec = await backtest_one(pm, judge, news_lock, m, raw, hours_before)
                except Exception as e:
                    log.warning("失败 %s: %s %s", m.question[:50], type(e).__name__, e)
                    return
            if rec:
                with open(RECORDS, "a") as f:
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            finished += 1
            if finished % 25 == 0:
                log.info("进度 %d / %d", finished, len(todo))

        await asyncio.gather(*(task(m, raw) for m, raw in todo))
    finally:
        await pm.close()
        await judge.close()


# ---------- 分析 ----------

def clip(p: float) -> float:
    return min(max(p, 0.01), 0.99)


def logit(p: float) -> float:
    p = clip(p)
    return math.log(p / (1 - p))


def sigmoid(x: float) -> float:
    return 1 / (1 + math.exp(-x))


def brier(pairs) -> float | None:
    pairs = list(pairs)
    return sum((p - y) ** 2 for p, y in pairs) / len(pairs) if pairs else None


def auc(pairs) -> float | None:
    pos = [p for p, y in pairs if y == 1]
    neg = [p for p, y in pairs if y == 0]
    if not pos or not neg:
        return None
    wins = sum((p > q) + 0.5 * (p == q) for p in pos for q in neg)
    return wins / (len(pos) * len(neg))


def _solve(a: list[list[float]], b: list[float]) -> list[float]:
    """小矩阵高斯消元（逻辑回归的牛顿步用）。"""
    n = len(b)
    m = [row[:] + [b[i]] for i, row in enumerate(a)]
    for c in range(n):
        p = max(range(c, n), key=lambda r: abs(m[r][c]))
        m[c], m[p] = m[p], m[c]
        for r in range(n):
            if r != c and m[c][c]:
                f = m[r][c] / m[c][c]
                m[r] = [x - f * y for x, y in zip(m[r], m[c])]
    return [m[i][n] / m[i][i] for i in range(n)]


def _inverse(a: list[list[float]]) -> list[list[float]]:
    n = len(a)
    cols = [_solve(a, [1.0 if i == j else 0.0 for i in range(n)]) for j in range(n)]
    return [[cols[j][i] for j in range(n)] for i in range(n)]


def logistic(xs: list[list[float]], ys: list[int], ridge: float = 1e-3) -> tuple[list[float], list[float]]:
    """y ~ sigmoid(x·w)，牛顿法；返回系数和标准误。xs 里自己带常数项。"""
    k = len(xs[0])
    w = [0.0] * k
    for _ in range(50):
        grad = [-ridge * wi for wi in w]
        hess = [[-ridge * (i == j) for j in range(k)] for i in range(k)]
        for x, y in zip(xs, ys):
            p = sigmoid(sum(a * b for a, b in zip(w, x)))
            for i in range(k):
                grad[i] += (y - p) * x[i]
                for j in range(k):
                    hess[i][j] -= p * (1 - p) * x[i] * x[j]
        step = _solve(hess, grad)
        w = [wi - si for wi, si in zip(w, step)]
        if max(abs(s) for s in step) < 1e-8:
            break
    cov = _inverse([[-v for v in row] for row in hess])
    return w, [math.sqrt(max(cov[i][i], 0)) for i in range(k)]


def fee(rate: float, p: float) -> float:
    return rate * p * (1 - p)


def simulate(rows: list[dict], fair_of) -> dict:
    """按交易台的规则模拟：中间价 0.15–0.85、软过滤、扣费后每股优势 ≥ MIN_EDGE 才买，每笔 1 股。"""
    trades = []
    for r in rows:
        m = r["price"]
        if not config.PRICE_BAND[0] <= m <= config.PRICE_BAND[1]:
            continue
        if (r["rules_clear"] < config.MIN_RULES_CLEAR or r["dispute_risk"] > config.MAX_DISPUTE_RISK
                or r["info"] < config.MIN_INFO_SCORE):
            continue
        fa = fair_of(r)
        for fair, price, won in ((fa, m, r["resolved_a"]), (1 - fa, 1 - m, 1 - r["resolved_a"])):
            cost = price + HALF_SPREAD + fee(r["fee_rate"], price + HALF_SPREAD)
            if fair - cost >= config.MIN_EDGE:
                trades.append((cost, won))
    if not trades:
        return {"n": 0}
    cost = sum(c for c, _ in trades)
    pnl = sum(w - c for c, w in trades)
    return {"n": len(trades), "win_rate": sum(w for _, w in trades) / len(trades), "roi": pnl / cost}


def analyze() -> dict:
    with open(RECORDS) as f:
        rows = [json.loads(line) for line in f if line.strip()]
    judged = [r for r in rows if "p_jev" in r]
    ys = [int(r["resolved_a"]) for r in judged]
    mk = [r["price"] for r in judged]
    jv = [r["p_jev"] for r in judged]
    shrink = [m + config.SHRINK * (j - m) for m, j in zip(mk, jv)]

    # 逻辑回归：结果 ~ 市场价（logit）+ Jev（logit）。Jev 的系数显著为正，才说明它带来了市场价以外的信息
    xs = [[1.0, logit(m), logit(j)] for m, j in zip(mk, jv)]
    w, se = logistic(xs, ys)
    # 两折交叉验证：用一半数据拟合的组合去预测另一半，Brier 才不偏乐观。
    # 对照组只用市场价（重新校准，不看 Jev）：如果它和“市场 + Jev”一样好，改善就和 Jev 无关
    folds = [i % 2 for i in range(len(judged))]
    xs_mkt = [[1.0, logit(m)] for m in mk]

    def cross_fit(features):
        out, fold_coefs = [0.0] * len(judged), []
        for k in (0, 1):
            idx = [i for i in range(len(judged)) if folds[i] != k]
            wk, _ = logistic([features[i] for i in idx], [ys[i] for i in idx])
            fold_coefs.append(wk)
            for i in range(len(judged)):
                if folds[i] == k:
                    out[i] = sigmoid(sum(a * b for a, b in zip(wk, features[i])))
        return out, fold_coefs

    combo_cv, combo_folds = cross_fit(xs)
    market_cv, _ = cross_fit(xs_mkt)
    w_mkt, se_mkt = logistic(xs_mkt, ys)
    # 交叉验证版的模拟交易：每个市场用“没见过它”的那一折拟合的系数定价，ROI 才不偏乐观
    cv_fair = {r["condition_id"]: p for r, p in zip(judged, combo_cv)}
    cv_market_fair = {r["condition_id"]: p for r, p in zip(judged, market_cv)}

    buckets = []
    for lo in (0.0, 0.2, 0.4, 0.6, 0.8):
        b = [(j, y) for j, y in zip(jv, ys) if lo <= j < lo + 0.2 or (lo == 0.8 and j == 1.0)]
        if b:
            buckets.append({"lo": lo, "n": len(b), "jev": sum(p for p, _ in b) / len(b),
                            "actual": sum(y for _, y in b) / len(b)})
    # Jev 和市场分歧大的时候，谁更接近结果
    disagree = [(abs(j - y) < abs(m - y)) for m, j, y in zip(mk, jv, ys) if abs(j - m) >= 0.10]

    by_cat = defaultdict(list)
    sports_vols = sorted(r["volume"] for r in judged if r["t_basis"] == "game_start")
    vol_median = sports_vols[len(sports_vols) // 2] if sports_vols else 0
    for r in judged:
        by_cat["sports" if r["t_basis"] == "game_start" else "other"].append(r)
        if not r.get("result_like_news"):
            by_cat["sports_clean" if r["t_basis"] == "game_start" else "other_clean"].append(r)
        if r["t_basis"] == "game_start" and r["volume"] >= vol_median:
            by_cat["sports_liquid"].append(r)  # 成交量前一半：T 时刻的价格更不容易是旧价
    groups = {}
    for name, rs in [("all", judged), *sorted(by_cat.items())]:
        # 每个市场 Jev 和市场价的平方误差之差；均值 < 0 = Jev 更准。标准误 = 标准差 / √n
        diffs = [(r["p_jev"] - r["resolved_a"]) ** 2 - (r["price"] - r["resolved_a"]) ** 2 for r in rs]
        mean = sum(diffs) / len(diffs)
        sd = math.sqrt(sum((d - mean) ** 2 for d in diffs) / max(1, len(diffs) - 1))
        groups[name] = {
            "n": len(rs), "diff": mean, "diff_se": sd / math.sqrt(len(diffs)),
            "brier_market": brier((r["price"], r["resolved_a"]) for r in rs),
            "brier_jev": brier((r["p_jev"], r["resolved_a"]) for r in rs),
            "brier_shrink": brier((r["price"] + config.SHRINK * (r["p_jev"] - r["price"]), r["resolved_a"]) for r in rs),
            "auc_market": auc([(r["price"], r["resolved_a"]) for r in rs]),
            "auc_jev": auc([(r["p_jev"], r["resolved_a"]) for r in rs]),
        }

    result = {
        "generated_at": time.time(),
        "records": len(rows), "judged": len(judged),
        "skipped": dict(Counter(r.get("skipped") for r in rows if r.get("skipped"))),
        "with_news": sum(1 for r in judged if r.get("n_news")),
        "groups": groups, "sports_volume_median": vol_median,
        "brier_combo_cv": brier(zip(combo_cv, ys)),
        "brier_market_recal_cv": brier(zip(market_cv, ys)),
        "logistic": {"names": ["const", "logit_market", "logit_jev"], "coef": w, "se": se},
        "logistic_market_only": {"names": ["const", "logit_market"], "coef": w_mkt, "se": se_mkt},
        "jev_buckets": buckets,
        "disagree_n": len(disagree), "disagree_jev_closer": sum(disagree) / len(disagree) if disagree else None,
        "strategy_current": simulate(judged, lambda r: r["price"] + config.SHRINK * (r["p_jev"] - r["price"])),
        "strategy_combo_cv": simulate(judged, lambda r: cv_fair[r["condition_id"]]),
        "strategy_market_recal_cv": simulate(judged, lambda r: cv_market_fair[r["condition_id"]]),
        "strategy_shrink_025": simulate(judged, lambda r: r["price"] + 0.25 * (r["p_jev"] - r["price"])),
        "tokens": sum(r.get("input_tokens", 0) for r in judged),
    }
    with open(RESULTS, "w") as f:
        json.dump(result, f, ensure_ascii=False, indent=1)
    return result


def print_report(res: dict):
    print(f"\n== Jev 历史回测：{res['judged']} 个市场问了 Jev（共 {res['records']} 条；跳过 {res['skipped']}；"
          f"{res['with_news']} 个有新闻）==")
    print(f"{'':14}{'n':>5}{'Brier市场':>11}{'Brier Jev':>11}{'Jev−市场 ± 标准误':>20}{'Brier收缩':>11}{'AUC市场':>9}{'AUC Jev':>9}")
    for name, g in res["groups"].items():
        fmt_auc = lambda x: f"{x:>9.3f}" if x is not None else f"{'-':>9}"
        print(f"{name:14}{g['n']:>5}{g['brier_market']:>11.4f}{g['brier_jev']:>11.4f}"
              f"{g['diff']:>+12.4f} ± {g['diff_se']:.4f}{g['brier_shrink']:>11.4f}{fmt_auc(g['auc_market'])}{fmt_auc(g['auc_jev'])}")
    print(f"（sports_liquid = 体育盘里成交量 ≥ 中位数 ${res['sports_volume_median']:,.0f} 的那一半）")
    lg = res["logistic"]
    print(f"\n逻辑回归 结果 ~ logit(市场) + logit(Jev)：" + "，".join(
        f"{n} {c:+.3f} ± {s:.3f}" for n, c, s in zip(lg["names"], lg["coef"], lg["se"])))
    lm = res["logistic_market_only"]
    print(f"只用市场价重新校准：" + "，".join(f"{n} {c:+.3f} ± {s:.3f}" for n, c, s in zip(lm["names"], lm["coef"], lm["se"])))
    print(f"两折交叉验证 Brier：市场 + Jev {res['brier_combo_cv']:.4f}，只用市场价重新校准 {res['brier_market_recal_cv']:.4f}")
    print(f"Jev 和市场相差 ≥ 10¢ 的 {res['disagree_n']} 个里，Jev 更接近结果的比例 "
          f"{res['disagree_jev_closer']:.0%}" if res["disagree_jev_closer"] is not None else "")
    print("Jev 校准：" + "；".join(f"{b['lo']:.1f}+ n={b['n']} 说 {b['jev']:.2f} 实际 {b['actual']:.2f}"
                                   for b in res["jev_buckets"]))
    labels = {"strategy_current": "现在的定价（收缩 0.5）", "strategy_shrink_025": "收缩 0.25",
              "strategy_combo_cv": "市场 + Jev 组合（交叉验证）", "strategy_market_recal_cv": "只用市场价重新校准（交叉验证，不看 Jev）"}
    for name, label in labels.items():
        s = res[name]
        print(f"{label}：" + (f"{s['n']} 笔，胜率 {s['win_rate']:.0%}，ROI {s['roi']:+.1%}" if s["n"] else "没有交易"))
    print(f"Jev 输入 token {res['tokens']:,}（≈ ${res['tokens'] * config.JEV_PRICE_PER_MTOK / 1e6:.4f}）")


def main():
    parser = argparse.ArgumentParser(description="Jev 历史回测")
    parser.add_argument("--analyze", action="store_true", help="只分析已有的 records.jsonl")
    parser.add_argument("--days", type=int, default=21, help="取最近多少天结算的市场")
    parser.add_argument("--max-sports", type=int, default=400, help="体育盘最多抽多少个（主样本）")
    parser.add_argument("--max-others", type=int, default=200, help="其他类别最多抽多少个（参考样本）")
    parser.add_argument("--hours-before", type=float, default=24, help="体育盘：T = 开赛前多少小时")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--limit", type=int, default=None, help="这次最多跑多少个市场（试跑用）")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)-8s %(message)s", datefmt="%H:%M:%S")
    for noisy in ("httpx", "httpx2", "typesafe_sdk"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    if not args.analyze:
        from main import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(os.path.dirname(HERE)), ".env"))
        asyncio.run(run(args.days, args.max_sports, args.max_others, args.hours_before, args.seed, args.limit))
    print_report(analyze())


if __name__ == "__main__":
    main()
