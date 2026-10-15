"""SQLite 账本：Jev 的每次判断（校准用）、影子仓位、冷却名单。"""

from __future__ import annotations

import sqlite3

import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS judgments (
    id INTEGER PRIMARY KEY,
    ts REAL, condition_id TEXT, question TEXT, outcome_a TEXT, end_ts REAL,
    mid_a REAL, p_a REAL, rules_clear REAL, dispute_risk REAL, info REAL,
    model TEXT, input_tokens INTEGER, verdict TEXT,
    resolved_a REAL
);
CREATE INDEX IF NOT EXISTS judgments_cid ON judgments (condition_id);
CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY,
    condition_id TEXT, event_id TEXT, question TEXT, outcome TEXT, side INTEGER, token_id TEXT,
    shares REAL, entry_price REAL, entry_fee REAL, fair REAL, p_jev REAL, opened_ts REAL,
    status TEXT DEFAULT 'open',
    exit_price REAL, exit_fee REAL, exit_reason TEXT, closed_ts REAL, pnl REAL
);
CREATE TABLE IF NOT EXISTS bench (condition_id TEXT PRIMARY KEY, until_ts REAL, reason TEXT);
"""


class Ledger:
    def __init__(self, path: str):
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)

    def _write(self, sql: str, params=()):
        self.db.execute(sql, params)
        self.db.commit()

    # ---- 判断记录 ----
    def log_judgment(self, d, j, verdict: str, now: float):
        m, book = d.market, d.books[0]
        self._write(
            "INSERT INTO judgments (ts, condition_id, question, outcome_a, end_ts, mid_a, p_a,"
            " rules_clear, dispute_risk, info, model, input_tokens, verdict)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (now, m.condition_id, m.question, m.outcomes[0], m.end_ts,
             (book.best_bid + book.best_ask) / 2, j.p_a, j.rules_clear, j.dispute_risk, j.info,
             j.model, j.input_tokens, verdict),
        )

    def unsettled_condition_ids(self, now: float, limit: int = 200) -> list[str]:
        rows = self.db.execute(
            "SELECT DISTINCT condition_id FROM judgments"
            " WHERE resolved_a IS NULL AND end_ts < ? LIMIT ?", (now, limit))
        return [r[0] for r in rows]

    def settle(self, condition_id: str, resolved_a: float):
        self._write("UPDATE judgments SET resolved_a = ? WHERE condition_id = ?", (resolved_a, condition_id))

    # ---- 冷却 ----
    def bench(self, condition_id: str, hours: float, reason: str, now: float):
        self._write("INSERT OR REPLACE INTO bench VALUES (?, ?, ?)", (condition_id, now + hours * 3600, reason))

    def benched(self, now: float) -> set[str]:
        return {r[0] for r in self.db.execute("SELECT condition_id FROM bench WHERE until_ts > ?", (now,))}

    # ---- 仓位 ----
    def open_positions(self) -> list[sqlite3.Row]:
        return self.db.execute("SELECT * FROM positions WHERE status = 'open'").fetchall()

    def held_condition_ids(self) -> set[str]:
        return {p["condition_id"] for p in self.open_positions()}

    def held_event_ids(self) -> set[str]:
        return {p["event_id"] for p in self.open_positions()}

    def cash(self) -> float:
        realized = self.db.execute("SELECT COALESCE(SUM(pnl), 0) FROM positions WHERE status = 'closed'").fetchone()[0]
        locked = self.db.execute(
            "SELECT COALESCE(SUM(shares * entry_price + entry_fee), 0) FROM positions WHERE status = 'open'"
        ).fetchone()[0]
        return config.SHADOW_BANK + realized - locked

    def open_position(self, opp, now: float):
        m = opp.dossier.market
        self._write(
            "INSERT INTO positions (condition_id, event_id, question, outcome, side, token_id,"
            " shares, entry_price, entry_fee, fair, p_jev, opened_ts)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (m.condition_id, m.event_id, m.question, opp.outcome, opp.side, opp.token_id,
             opp.shares, opp.avg_price, opp.fee, opp.fair, opp.judgment.p_a, now),
        )

    def close_position(self, pos: sqlite3.Row, exit_price: float, exit_fee: float, reason: str, now: float) -> float:
        pnl = pos["shares"] * (exit_price - pos["entry_price"]) - pos["entry_fee"] - exit_fee
        self._write(
            "UPDATE positions SET status = 'closed', exit_price = ?, exit_fee = ?, exit_reason = ?,"
            " closed_ts = ?, pnl = ? WHERE id = ?",
            (exit_price, exit_fee, reason, now, pnl, pos["id"]),
        )
        return pnl
