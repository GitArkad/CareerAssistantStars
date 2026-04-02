# app/agents/tools/parse_resume_node.py
from langgraph import Node
from app.agents.services.parser import ResumeParser
from app.agents.services.utils import get_extraction_chain
from app.agents.services.state import AgentState

class ParseResumeNode(Node):
    """
    Узел LangGraph для парсинга резюме.
    Берет текст или файл, парсит через LLM и кладет результат в state.candidate.
    """
    def run(self, state: AgentState, user_text: str = None, file_bytes=None):
        if file_bytes:
            raw_text = ResumeParser.parse(file_bytes)
        else:
            raw_text = user_text or ""

        state.raw_file_content = file_bytes or b""
        state.user_input = user_text

        # Парсим через GROQ/Llama
        candidate_profile = get_extraction_chain(raw_text)
        state.candidate = candidate_profile
        return state