"""所有给 Jev 的问题。

写法注意（来自 TypeSafe 文档和第三方测试）：Jev 按字面理解问题，别用双重否定；
它不会计数、不会算日期、数字推理不可靠，这些都在代码里算好。
"""

from typesafe_sdk import Choice, Noul, Score


def outcome_questions(outcome_a: str, outcome_b: str) -> dict:
    """看完整档案（含新闻）：结果更可能是哪个，以及信息够不够。"""
    return {
        "p_a": Noul(
            instructions=f'This market will resolve to "{outcome_a}" rather than "{outcome_b}".',
            criteria={
                "true": f'Applied to what actually happens, the resolution rules select "{outcome_a}".',
                "false": f'Applied to what actually happens, the resolution rules select "{outcome_b}".',
            },
        ),
        "info": Score(
            instructions="How much does the state tell you, beyond the question and its rules, about which outcome will happen?",
            criteria=[
                "Nothing beyond the question and its rules.",
                "Only general background that does not point to either outcome.",
                "Specific, recent facts that lean toward one outcome.",
                "Specific, recent facts that nearly settle the outcome.",
            ],
        ),
    }


def rules_questions() -> dict:
    """只看规则（state 里不放新闻）：问的是规则本身写得好不好，跟结果难不难猜无关。
    放在同一次调用里时，Jev 会把“结果不确定”混进这两个分数。"""
    return {
        "rules_clear": Noul(
            instructions="The resolution rules say exactly what counts for each outcome and name the source that decides it.",
        ),
        "dispute_risk": Noul(
            instructions=(
                "Even with every fact about the event known, the wording of the rules could be read "
                "to select different outcomes."
            ),
        ),
    }


def pick_question(labels: dict[str, str]) -> dict:
    criteria = dict(labels)
    criteria["none"] = (
        "None of these: every candidate either has unclear rules "
        "or a forecast that rests on too little information."
    )
    return {
        "pick": Choice(
            instructions=(
                "Pick the one position whose forecast is best supported by the information "
                "in the state and whose resolution rules are the clearest."
            ),
            criteria=criteria,
        ),
    }
