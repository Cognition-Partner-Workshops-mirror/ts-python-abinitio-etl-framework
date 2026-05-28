"""
nodes.py - Node functions that form the vertices of the LangGraph workflow.

Each function below is a "node" in the graph. A node receives the current
state, performs work, and returns updates to the state.

GRAPH LAYOUT:
                     ┌───────────────────┐
                     │   router_node     │  ← Classifies the question
                     └────────┬──────────┘
                              │
            ┌─────────────────┼─────────────────┐
            │ "research"      │ "math"           │ "general"
            ▼                 ▼                  ▼
    ┌───────────────┐ ┌──────────────┐ ┌────────────────┐
    │ research_node │ │  math_node   │ │  general_node  │
    └───────┬───────┘ └──────┬───────┘ └────────┬───────┘
            │                │                   │
            └─────────────────┼──────────────────┘
                              ▼
                   ┌─────────────────────┐
                   │  synthesizer_node   │  ← Produces the final answer
                   └─────────────────────┘
"""

from langchain_openai import ChatOpenAI

from config.settings import OPENAI_API_KEY, MODEL_NAME, TEMPERATURE
from workflow.state import AgentState
from workflow.tools import search_wikipedia, calculate, get_current_datetime


def _get_llm() -> ChatOpenAI:
    """Create a configured ChatOpenAI instance (shared by all nodes)."""
    return ChatOpenAI(
        model=MODEL_NAME,
        temperature=TEMPERATURE,
        api_key=OPENAI_API_KEY,
    )


# ──────────────────────────────────────────────────────────────────────
# Node 1: ROUTER — Classifies the question into a category
# ──────────────────────────────────────────────────────────────────────

def router_node(state: AgentState) -> dict:
    """Classify the user's question to decide which tool node to run next.

    The LLM reads the question and returns exactly one word:
    "research", "math", or "general". This word becomes the routing key.

    Args:
        state: Current graph state containing the user's question.

    Returns:
        Dict with "route" key set to the classification result.
    """
    llm = _get_llm()

    # Prompt the LLM to classify the question into one of three categories
    classification_prompt = (
        "You are a question classifier. Classify the following question "
        "into exactly ONE of these categories:\n\n"
        "- 'research'  → Factual questions about people, places, events, "
        "  science, history, technology (anything that Wikipedia could answer)\n"
        "- 'math'      → Math calculations, arithmetic, algebra, conversions\n"
        "- 'general'   → Greetings, opinions, date/time, advice, or anything "
        "  that doesn't fit the other two categories\n\n"
        f"Question: {state['question']}\n\n"
        "Reply with ONLY one word: research, math, or general"
    )

    response = llm.invoke(classification_prompt)
    route = response.content.strip().lower()

    # Validate the response — default to "general" if unexpected
    if route not in ("research", "math", "general"):
        route = "general"

    return {"route": route}


# ──────────────────────────────────────────────────────────────────────
# Node 2a: RESEARCH — Searches Wikipedia for factual information
# ──────────────────────────────────────────────────────────────────────

def research_node(state: AgentState) -> dict:
    """Look up the user's question on Wikipedia.

    Uses the Wikipedia API to fetch a concise summary of the topic.

    Args:
        state: Current graph state containing the user's question.

    Returns:
        Dict with "tool_output" set to the Wikipedia summary.
    """
    result = search_wikipedia(state["question"])
    return {"tool_output": result}


# ──────────────────────────────────────────────────────────────────────
# Node 2b: MATH — Evaluates mathematical expressions
# ──────────────────────────────────────────────────────────────────────

def math_node(state: AgentState) -> dict:
    """Extract a math expression from the question and compute the result.

    The LLM extracts the mathematical expression from the natural-language
    question, then the calculate() tool evaluates it safely.

    Args:
        state: Current graph state containing the user's question.

    Returns:
        Dict with "tool_output" set to the calculation result.
    """
    llm = _get_llm()

    # Ask the LLM to extract just the math expression from the question
    extract_prompt = (
        "Extract ONLY the mathematical expression from this question. "
        "Return just the expression, nothing else.\n\n"
        f"Question: {state['question']}\n\n"
        "Examples:\n"
        "  'What is 25 * 48 + 137?' → '25 * 48 + 137'\n"
        "  'Calculate the square root of 144' → 'sqrt(144)'\n"
        "  'What is 15% of 200?' → '0.15 * 200'\n"
    )

    response = llm.invoke(extract_prompt)
    expression = response.content.strip()

    result = calculate(expression)
    return {"tool_output": result}


# ──────────────────────────────────────────────────────────────────────
# Node 2c: GENERAL — Handles greetings, date/time, and general queries
# ──────────────────────────────────────────────────────────────────────

def general_node(state: AgentState) -> dict:
    """Handle general questions, date/time requests, and greetings.

    Checks if the question is about date/time (uses the DateTime tool),
    otherwise uses the LLM to generate a direct response.

    Args:
        state: Current graph state containing the user's question.

    Returns:
        Dict with "tool_output" set to the response text.
    """
    question_lower = state["question"].lower()

    # Check if the user is asking about date or time
    time_keywords = ["time", "date", "today", "day", "month", "year"]
    if any(keyword in question_lower for keyword in time_keywords):
        result = get_current_datetime()
        return {"tool_output": result}

    # For general questions, use the LLM directly
    llm = _get_llm()
    response = llm.invoke(
        f"Answer this question concisely and helpfully:\n\n{state['question']}"
    )
    return {"tool_output": response.content}


# ──────────────────────────────────────────────────────────────────────
# Node 3: SYNTHESIZER — Produces the final polished answer
# ──────────────────────────────────────────────────────────────────────

def synthesizer_node(state: AgentState) -> dict:
    """Compose a polished final answer from the tool output.

    Takes the raw tool output and the original question, then uses the
    LLM to create a well-formatted, conversational response.

    Args:
        state: Current graph state with question, route, and tool_output.

    Returns:
        Dict with "final_answer" set to the polished response.
    """
    llm = _get_llm()

    synthesis_prompt = (
        "You are a helpful assistant. Using the information below, "
        "write a clear and concise answer to the user's question.\n\n"
        f"User's question: {state['question']}\n\n"
        f"Information gathered (via {state['route']} tool):\n"
        f"{state['tool_output']}\n\n"
        "Instructions:\n"
        "- Answer the question directly and conversationally\n"
        "- If the information came from Wikipedia, mention the source\n"
        "- If it was a calculation, show the expression and result clearly\n"
        "- Keep the answer concise but informative\n"
    )

    response = llm.invoke(synthesis_prompt)
    return {"final_answer": response.content}
