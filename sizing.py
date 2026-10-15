"""公允价、手续费、下单量。全部由代码计算，不交给 Jev。"""

from __future__ import annotations

from dataclasses import dataclass

import config


def fee_per_share(rate: float, price: float) -> float:
    # Polymarket taker 手续费 = 股数 × rate × p × (1 − p)，挂单方不收
    return rate * price * (1 - price)


def fill_summary(fills: list[tuple[float, float]], rate: float) -> tuple[float, float, float]:
    """逐档成交 → (股数, 均价, 手续费)。"""
    shares = sum(s for _, s in fills)
    if shares <= 0:
        return 0.0, 0.0, 0.0
    notional = sum(p * s for p, s in fills)
    fee = sum(s * fee_per_share(rate, p) for p, s in fills)
    return shares, notional / shares, fee


@dataclass
class Opportunity:
    dossier: object
    judgment: object
    side: int          # 0 = 买 outcomes[0]，1 = 买 outcomes[1]
    fair: float        # 这一侧的公允概率
    shares: float
    avg_price: float
    fee: float
    edge: float        # 扣完手续费后每股的优势

    @property
    def outcome(self) -> str:
        return self.dossier.market.outcomes[self.side]

    @property
    def token_id(self) -> str:
        return self.dossier.market.token_ids[self.side]

    @property
    def cost(self) -> float:
        return self.shares * self.avg_price + self.fee


def fair_a(d, j) -> float:
    book = d.books[0]
    mid = (book.best_bid + book.best_ask) / 2
    return mid + config.SHRINK * (j.p_a - mid)


def evaluate(d, j, cash: float) -> Opportunity | None:
    """两边都试一下，返回扣费后优势最大且达标的那一侧。"""
    rate = d.market.fee_rate
    fa = fair_a(d, j)
    best = None
    for side, fair in ((0, fa), (1, 1 - fa)):
        book = d.books[side]
        ask = book.best_ask
        if ask is None or ask >= 1 or fair - ask - fee_per_share(rate, ask) < config.MIN_EDGE:
            continue
        kelly = (fair - ask) / (1 - ask)
        ticket = min(
            cash * config.KELLY_FRACTION * kelly,
            cash * config.MAX_TICKET_PCT_BANK,
            book.depth_usd("ask", config.DEPTH_BAND) * config.MAX_TICKET_PCT_DEPTH,
        )
        if j.info < 2:
            ticket *= config.LOW_INFO_FACTOR
        if ticket < config.MIN_TICKET_USD:
            continue
        shares, avg, fee = fill_summary(book.buy(ticket), rate)
        if shares < d.market.min_order_size:
            continue
        edge = fair - avg - fee / shares
        if edge < config.MIN_EDGE:
            continue
        opp = Opportunity(d, j, side, fair, shares, avg, fee, edge)
        if best is None or opp.edge > best.edge:
            best = opp
    return best
