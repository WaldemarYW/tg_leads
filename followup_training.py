from __future__ import annotations

from typing import Dict, List


def _mk(
    step_name: str,
    candidate_signal: str,
    return_type: str,
    cta_target: str,
    phrases: List[str],
    operator_return_gold: str,
) -> List[Dict[str, str]]:
    return [
        {
            "step_name": step_name,
            "candidate_signal": candidate_signal,
            "candidate_phrase_raw": phrase,
            "operator_return_gold": operator_return_gold,
            "return_type": return_type,
            "cta_target": cta_target,
        }
        for phrase in phrases
    ]


RETURN_TRAINING_EXAMPLES: List[Dict[str, str]] = []

RETURN_TRAINING_EXAMPLES += _mk(
    "value_hook",
    "ack",
    "clarify",
    "value_hook_continue",
    ["так", "цікаво", "актуально", "підходить"],
    "Якщо в цілому формат Вам підходить, можемо перейти далі.",
)
RETURN_TRAINING_EXAMPLES += _mk(
    "value_hook",
    "question",
    "clarify",
    "value_hook_continue",
    ["що саме треба робити", "це без дзвінків", "віддалено повністю", "тільки з пк"],
    "Коротко відповів по формату. Якщо в цілому все ок, можемо перейти далі.",
)
RETURN_TRAINING_EXAMPLES += _mk(
    "value_hook",
    "delay",
    "fallback_1",
    "value_hook_continue",
    ["пізніше", "зараз не зручно", "подивлюсь пізніше", "завтра напишу"],
    "Якщо зручно, повернемось з цього місця: підкажете, чи підходить Вам формат у цілому?",
)
RETURN_TRAINING_EXAMPLES += _mk(
    "value_hook",
    "objection",
    "fallback_2",
    "value_hook_continue",
    ["не зрозумів", "є сумнів", "не впевнений", "потрібно подумати"],
    "Якщо залишився сумнів саме по формату, напишіть що саме, і я коротко уточню.",
)

RETURN_TRAINING_EXAMPLES += _mk(
    "shift_close",
    "ack",
    "clarify",
    "shift_select",
    ["ок", "зрозуміло", "добре", "підходить"],
    "Залишилось тільки зафіксувати зміну: денну чи нічну?",
)
RETURN_TRAINING_EXAMPLES += _mk(
    "shift_close",
    "soft_choice",
    "clarify",
    "shift_select",
    ["думаю денна", "скоріше денна", "мабуть денна", "десь денна", "думаю нічна", "скоріше нічна"],
    "Підтверджу лише один момент: яку зміну Вам зручніше зафіксувати - денну чи нічну?",
)
RETURN_TRAINING_EXAMPLES += _mk(
    "shift_close",
    "question",
    "clarify",
    "shift_select",
    ["а вихідні фіксовані", "скільки перерв", "можна міняти зміни", "а графік який", "по вихідних як", "можна совмещать"],
    "Коротко відповів по графіку. Щоб рухатися далі, підкажіть, будь ласка, яку зміну Вам зручніше розглянути: денну чи нічну?",
)
RETURN_TRAINING_EXAMPLES += _mk(
    "shift_close",
    "delay",
    "fallback_1",
    "shift_select",
    ["подумаю", "пізніше напишу", "не зараз", "потім відповім"],
    "Якщо вакансія ще актуальна, залишилось тільки обрати зміну: денну чи нічну.",
)
RETURN_TRAINING_EXAMPLES += _mk(
    "shift_close",
    "objection",
    "fallback_2",
    "shift_select",
    ["не підходить графік", "не дуже зручно", "складний графік", "не зрозумів по змінах"],
    "Якщо сумнів саме в графіку, коротко підкажу. Якщо в цілому ок, напишіть, яку зміну розглядаєте: денну чи нічну.",
)

RETURN_TRAINING_EXAMPLES += _mk(
    "objection_gate",
    "ack",
    "clarify",
    "income_model",
    ["так", "все зрозуміло", "ок", "можемо далі"],
    "Зафіксував Вашу зміну. Якщо по формату роботи все зрозуміло, рухаємось далі до блоку про дохід і навчання.",
)
RETURN_TRAINING_EXAMPLES += _mk(
    "objection_gate",
    "question",
    "clarify",
    "income_model",
    ["чи можна совмещать", "це full time", "наскільки інтенсивно", "а якщо не вийде"],
    "Коротко відповів по формату роботи. Якщо в цілому все зрозуміло, рухаємось далі до блоку про дохід і навчання.",
)
RETURN_TRAINING_EXAMPLES += _mk(
    "objection_gate",
    "delay",
    "fallback_1",
    "income_model",
    ["подумаю", "пізніше", "завтра скажу", "не зараз"],
    "Ми вже зафіксували потрібний етап. Коли буде зручно, перейдемо звідси до блоку про дохід і навчання.",
)
RETURN_TRAINING_EXAMPLES += _mk(
    "objection_gate",
    "objection",
    "fallback_2",
    "income_model",
    ["це складно", "не впевнений по навантаженню", "не зрозумів формат", "сумніваюся"],
    "Якщо сумнів саме в інтенсивності або full-time форматі, коротко уточню. Якщо в цілому ок, рухаємось далі.",
)

RETURN_TRAINING_EXAMPLES += _mk(
    "income_model",
    "ack",
    "clarify",
    "form_handoff",
    ["так", "ок", "зрозуміло", "можемо"],
    "Коротко підсумую: модель доходу прозора, навчання безкоштовне, далі залишився один короткий крок - анкета.",
)
RETURN_TRAINING_EXAMPLES += _mk(
    "income_model",
    "question",
    "clarify",
    "form_handoff",
    ["скільки реально заробляють", "а виплати коли", "без ставки", "як росте дохід", "платне навчання"],
    "Коротко пояснив по доходу та навчанню. Якщо в цілому все зрозуміло, можемо перейти до анкети.",
)
RETURN_TRAINING_EXAMPLES += _mk(
    "income_model",
    "delay",
    "fallback_1",
    "form_handoff",
    ["подумаю", "не зараз", "пізніше відповім"],
    "Коли буде зручно, повернемось з цього місця: після цього блоку залишився лише перехід до анкети.",
)
RETURN_TRAINING_EXAMPLES += _mk(
    "income_model",
    "objection",
    "fallback_2",
    "form_handoff",
    ["без ставки не хочу", "сумніваюсь по оплаті", "не зрозумів модель доходу"],
    "Якщо залишився сумнів по оплаті або навчанню, коротко відповім. Якщо в цілому ок, перейдемо до анкети.",
)

RETURN_TRAINING_EXAMPLES += _mk(
    "form_handoff",
    "ack",
    "clarify",
    "form_submit",
    ["ок", "заповню", "добре", "прийняв"],
    "Залишився останній крок перед передачею тімліду - заповнити анкету.",
)
RETURN_TRAINING_EXAMPLES += _mk(
    "form_handoff",
    "question",
    "clarify",
    "form_submit",
    ["куди надсилати", "що саме заповнити", "а що далі", "скільки часу це займе"],
    "Коротко підкажу: зараз потрібна лише анкета. Це останній крок перед передачею тімліду.",
)
RETURN_TRAINING_EXAMPLES += _mk(
    "form_handoff",
    "delay",
    "fallback_1",
    "form_submit",
    ["пізніше заповню", "зараз не можу", "зроблю потім"],
    "Коли буде зручно, просто поверніться до анкети. Це останній крок перед передачею тімліду.",
)


def get_return_examples(step_name: str, candidate_signal: str, return_type: str) -> List[Dict[str, str]]:
    return [
        item
        for item in RETURN_TRAINING_EXAMPLES
        if item["step_name"] == step_name
        and item["candidate_signal"] == candidate_signal
        and item["return_type"] == return_type
    ]
