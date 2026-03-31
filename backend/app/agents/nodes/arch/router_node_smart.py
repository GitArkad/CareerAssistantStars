from langchain_core.messages import AIMessage, HumanMessage
from app.agents.state import AgentState


# =========================================================
# 🎯 TRIGGERS
# =========================================================

GREETING_TRIGGERS = [
    "привет", "здравствуй", "hello", "hi",
    "как дела", "как ты", "что делаешь", "что нового"
]

ANALYSIS_TRIGGERS = [
    "резюме",
    "анализ",
    "оцен",
    "ваканс",
    "работ",
    "подбери",
    "найди"
]


# =========================================================
# 🧠 HELPERS
# =========================================================

def normalize(text: str) -> str:
    return text.lower().strip()


def is_greeting(text: str) -> bool:
    return any(trigger in text for trigger in GREETING_TRIGGERS)


def is_analysis(text: str) -> bool:
    return any(trigger in text for trigger in ANALYSIS_TRIGGERS)


# =========================================================
# 🚀 ROUTER NODE
# =========================================================

def router_node(state: AgentState):
    print("\n--- [ROUTER NODE] ---")

    messages = state.get("messages", [])

    # =========================================================
    # 👋 FIRST RUN (авто-приветствие)
    # =========================================================

    if not messages:
        return {
            "messages": [
                AIMessage(content=(
                    "Привет! 👋\n\n"
                    "Я помогу тебе:\n"
                    "— проанализировать резюме\n"
                    "— подобрать вакансии\n"
                    "— подготовиться к интервью\n\n"
                    "Можешь загрузить резюме или задать вопрос 🙂"
                ))
            ],
            "stage": "chat"
        }

    # =========================================================
    # 📩 LAST USER MESSAGE
    # =========================================================

    last_user_message = (
        messages[-1].content
        if isinstance(messages[-1], HumanMessage)
        else ""
    )

    text = normalize(last_user_message)

    # =========================================================
    # 👋 GREETING
    # =========================================================

    if is_greeting(text):
        return {
            "messages": [
                AIMessage(content="Привет! 👋 Чем могу помочь?")
            ],
            "stage": "chat"
        }

    # =========================================================
    # 🔍 ANALYSIS (только по явному запросу)
    # =========================================================

    if is_analysis(text):
        candidate = state.get("candidate", {})

        # если нет резюме → не пускаем в анализ
        if not candidate or not candidate.get("skills"):
            return {
                "messages": [
                    AIMessage(content="Сначала загрузи резюме или вставь текст")
                ],
                "stage": "chat"
            }

        return {
            "stage": "analysis"
        }

    # =========================================================
    # 💬 DEFAULT CHAT
    # =========================================================

    return {
        "stage": "chat"
    }