"""影子模式成绩单：Jev 比市场价更准吗？影子仓位赚钱吗？"""

from __future__ import annotations

import argparse
import math
import sqlite3

import config
from sizing import fair_value


def brier(ps, ys):
    return sum((p - y) ** 2 for p, y in zip(ps, ys)) / len(ps)


def log_loss(ps, ys):
    eps = 1e-6
    return -sum(y * math.log(max(p, eps)) + (1 - y) * math.log(max(1 - p, eps)) for p, y in zip(ps, ys)) / len(ps)


def calibration(db):
    # 每个市场只取第一次判断，避免同一个市场被反复计数
    rows = db.execute(
        "SELECT * FROM judgments j WHERE resolved_a IS NOT NULL"
        " AND id = (SELECT MIN(id) FROM judgments WHERE condition_id = j.condition_id)"
    ).fetchall()
    print(f"\n== 校准（已结算市场 {len(rows)} 个）==")
    if not rows:
        print("还没有已结算的市场。判断记录会在市场到期结算后自动补上结果。")
        return
    ys = [r["resolved_a"] for r in rows]
    series = {
        "市场中间价": [r["mid_a"] for r in rows],
        "Jev 原始": [r["p_a"] for r in rows],
        "交易用的公允价": [fair_value(r["mid_a"], r["p_a"]) for r in rows],
    }
    print(f"{'Brier ↓':>9}  {'LogLoss ↓':>9}")
    for name, ps in series.items():
        print(f"{brier(ps, ys):>9.4f}  {log_loss(ps, ys):>9.4f}  {name}")
    if len(rows) < 50:
        print("样本少于 50 个，先别下结论。")

    print("\nJev 可靠性（预测概率分桶 vs 实际发生频率）")
    buckets: dict[int, list] = {}
    for r in rows:
        buckets.setdefault(min(int(r["p_a"] * 5), 4), []).append((r["p_a"], r["resolved_a"]))
    for i, bucket in sorted(buckets.items()):
        avg_p = sum(p for p, _ in bucket) / len(bucket)
        freq = sum(y for _, y in bucket) / len(bucket)
        print(f"  {i * 0.2:.1f}-{i * 0.2 + 0.2:.1f}  n={len(bucket):<4} 预测 {avg_p:.2f}  实际 {freq:.2f}")


def positions(db):
    closed = db.execute("SELECT * FROM positions WHERE status = 'closed' ORDER BY closed_ts").fetchall()
    opened = db.execute("SELECT * FROM positions WHERE status = 'open' ORDER BY opened_ts").fetchall()
    print(f"\n== 影子仓位（已平 {len(closed)}，持有 {len(opened)}）==")
    if closed:
        pnl = sum(p["pnl"] for p in closed)
        cost = sum(p["shares"] * p["entry_price"] + p["entry_fee"] for p in closed)
        wins = sum(1 for p in closed if p["pnl"] > 0)
        print(f"已实现盈亏 {pnl:+.2f}  投入 {cost:.2f}  回报率 {pnl / cost:+.1%}  胜率 {wins}/{len(closed)}")
        for p in closed[-10:]:
            print(f"  {p['pnl']:+8.2f}  {p['exit_reason']:<13} {p['outcome']:<8} {p['question'][:60]}")
    for p in opened:
        print(f"  持有 {p['shares']:.1f} × {p['outcome']} @ {p['entry_price']:.3f}  公允 {p['fair']:.3f}  {p['question'][:60]}")


def usage(db):
    total, tokens = db.execute("SELECT COUNT(*), COALESCE(SUM(input_tokens), 0) FROM judgments").fetchone()
    print(f"\n== 用量 ==\nJev 判断 {total} 次，输入 {tokens:,} tokens ≈ ${tokens * config.JEV_PRICE_PER_MTOK / 1e6:.4f}")
    for verdict, n in db.execute("SELECT verdict, COUNT(*) FROM judgments GROUP BY verdict ORDER BY 2 DESC"):
        print(f"  {verdict:<16}{n}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=config.DB_PATH)
    args = parser.parse_args()
    db = sqlite3.connect(args.db)
    db.row_factory = sqlite3.Row
    calibration(db)
    positions(db)
    usage(db)


if __name__ == "__main__":
    main()
