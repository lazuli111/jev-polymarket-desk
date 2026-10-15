"""把候选整理成 Jev 能读的样子，让它最多挑一个。"""

from __future__ import annotations

import config


async def pick(judge, opps: list) -> tuple[object | None, str, float]:
    labels, candidates = {}, []
    for i, opp in enumerate(opps, 1):
        key, d = f"c{i}", opp.dossier
        labels[key] = f'Buy "{opp.outcome}" on: {d.market.question}'
        candidate = {
            "id": key,
            "question": d.market.question,
            "buy": opp.outcome,
            "resolution_rules": d.market.rules[:1500],
            "time_left": d.state["time_left"],
        }
        if "news" in d.state:
            candidate["news"] = d.state["news"]
        candidates.append(candidate)

    state = {
        "today": opps[0].dossier.state["today"],
        "task": "Choose at most one position to open.",
        "candidates": candidates,
    }
    choice, confidence, _ = await judge.pick(state, labels)
    if choice not in labels or confidence < config.MIN_PICK_CONFIDENCE:
        return None, choice, confidence
    return opps[int(choice[1:]) - 1], choice, confidence
