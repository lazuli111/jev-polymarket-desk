"""Jev 判断层。整个项目里只有这里会用到 TYPESAFE_API_KEY。"""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass

from typesafe_sdk import AsyncTypeSafeClient

import config
from collect import rules_state
from questions import outcome_questions, pick_question, rules_questions


@dataclass
class Judgment:
    p_a: float           # 第一个结果发生的概率（通常就是 Yes）
    rules_clear: float
    dispute_risk: float
    info: float          # 0~3 的期望分
    model: str
    input_tokens: int


class JevJudge:
    def __init__(self, model: str = config.JEV_MODEL, concurrency: int = 8):
        self._client = AsyncTypeSafeClient(model=model)
        self._sem = asyncio.Semaphore(concurrency)

    async def _ask(self, state, questions):
        async with self._sem:
            return await self._client.system_one(state=state, questions=questions)

    async def judge(self, d) -> Judgment:
        # 两次调用：结果判断看完整档案；规则质量只看规则，免得被新闻和结果不确定性带偏
        outcome, rules = await asyncio.gather(
            self._ask(d.state, outcome_questions(*d.market.outcomes)),
            self._ask(rules_state(d.market), rules_questions()),
        )
        return Judgment(
            p_a=outcome.nouls["p_a"].noul,
            rules_clear=rules.nouls["rules_clear"].noul,
            dispute_risk=rules.nouls["dispute_risk"].noul,
            info=outcome.scores["info"].score,
            model=outcome.model,
            input_tokens=(outcome.usage.input_tokens or 0) + (rules.usage.input_tokens or 0),
        )

    async def pick(self, state: dict, labels: dict[str, str]) -> tuple[str, float, int]:
        r = await self._ask(state, pick_question(labels))
        answer = r.choices["pick"]
        return answer.choice, answer.confidence, r.usage.input_tokens or 0

    async def close(self):
        await self._client.aclose()


class MockJudge:
    """不调用 Jev，只用来在没有 key 时跑通整条流程。这里的数字是伪随机的，不是信号。"""

    async def judge(self, d) -> Judgment:
        h = int(hashlib.sha256(d.market.condition_id.encode()).hexdigest(), 16)
        noise = ((h % 1000) / 1000 - 0.5) * 0.4  # ±0.20
        mid = d.market.mid or 0.5
        return Judgment(
            p_a=min(max(mid + noise, 0.01), 0.99),
            rules_clear=0.85, dispute_risk=0.10, info=1.5,
            model="mock", input_tokens=0,
        )

    async def pick(self, state: dict, labels: dict[str, str]) -> tuple[str, float, int]:
        return next(iter(labels)), 0.60, 0

    async def close(self):
        pass
