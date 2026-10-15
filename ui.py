"""本地看板：影子交易台（持仓、Jev 判断、校准）。

用法：.venv/bin/python ui.py [--port 8765]，然后打开 http://localhost:8765
只读：不下单，也不改 desk.db；只监听 127.0.0.1。
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import config

log = logging.getLogger("ui")

ROOT = os.path.dirname(os.path.abspath(__file__))
STATIC = os.path.join(ROOT, "ui")


def _brier(pairs):
    return sum((p - y) ** 2 for p, y in pairs) / len(pairs) if pairs else None


def desk_summary() -> dict:
    path = os.path.join(ROOT, config.DB_PATH)
    if not os.path.exists(path):
        return {"exists": False}
    db = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        def rows(sql, *args):
            return [dict(r) for r in db.execute(sql, args)]

        realized = db.execute("SELECT COALESCE(SUM(pnl), 0) FROM positions WHERE status = 'closed'").fetchone()[0]
        locked = db.execute(
            "SELECT COALESCE(SUM(shares * entry_price + entry_fee), 0) FROM positions WHERE status = 'open'"
        ).fetchone()[0]
        count, tokens, last_ts = db.execute(
            "SELECT COUNT(*), COALESCE(SUM(input_tokens), 0), MAX(ts) FROM judgments").fetchone()
        resolved = rows(
            "SELECT mid_a, p_a, resolved_a FROM judgments j WHERE resolved_a IS NOT NULL"
            " AND id = (SELECT MIN(id) FROM judgments WHERE condition_id = j.condition_id)")
        calibration = {"n": len(resolved)}
        if resolved:
            ys = [r["resolved_a"] for r in resolved]
            calibration.update(
                market=_brier(list(zip((r["mid_a"] for r in resolved), ys))),
                jev=_brier(list(zip((r["p_a"] for r in resolved), ys))),
                fair=_brier(list(zip((r["mid_a"] + config.SHRINK * (r["p_a"] - r["mid_a"]) for r in resolved), ys))),
            )
        return {
            "exists": True, "bank": config.SHADOW_BANK, "cash": config.SHADOW_BANK + realized - locked,
            "realized": realized, "locked": locked,
            "open": rows("SELECT * FROM positions WHERE status = 'open' ORDER BY opened_ts DESC"),
            "closed": rows("SELECT * FROM positions WHERE status = 'closed' ORDER BY closed_ts DESC LIMIT 30"),
            "judgments": count, "tokens": tokens, "cost": tokens * config.JEV_PRICE_PER_MTOK / 1e6,
            "last_ts": last_ts, "cycle_seconds": config.CYCLE_SECONDS,
            "verdicts": rows("SELECT verdict, COUNT(*) AS n FROM judgments GROUP BY verdict ORDER BY n DESC"),
            "recent": rows("SELECT ts, question, outcome_a, mid_a, p_a, rules_clear, dispute_risk, info, verdict"
                           " FROM judgments ORDER BY id DESC LIMIT 40"),
            "calibration": calibration,
        }
    finally:
        db.close()


ROUTES = {"/api/desk": desk_summary}


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=STATIC, **kwargs)

    def do_GET(self):
        route = ROUTES.get(urlparse(self.path).path)
        if route is None:
            return super().do_GET()
        try:
            body, status = json.dumps(route(), ensure_ascii=False).encode(), 200
        except Exception as e:
            log.exception("api error")
            body, status = json.dumps({"error": str(e)}, ensure_ascii=False).encode(), 502
        self.send_response(status)
        self.send_header("content-type", "application/json; charset=utf-8")
        self.send_header("cache-control", "no-store")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def end_headers(self):
        # 静态文件每次都向服务器确认，改完前端刷新就能看到
        if not self.path.startswith("/api/"):
            self.send_header("cache-control", "no-cache")
        super().end_headers()

    def log_message(self, fmt, *args):
        log.debug(fmt, *args)


def main():
    parser = argparse.ArgumentParser(description="Jev Desk 本地看板")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)-4s %(message)s", datefmt="%H:%M:%S")
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    log.info("看板已启动：http://localhost:%d", args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
