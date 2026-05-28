"""
tools.py - Tool functions that the agent can use during its workflow.

Each tool is a plain Python function that performs a specific task.
The LangGraph nodes in nodes.py call these tools based on the
routing decision made by the router node.

Tools available:
  1. search_wikipedia  — Look up factual information on Wikipedia
  2. calculate         — Evaluate a mathematical expression safely
  3. get_current_datetime — Return the current date and time
"""

import datetime
import wikipedia


def search_wikipedia(query: str, sentences: int = 3) -> str:
    """Search Wikipedia and return a short summary.

    Args:
        query:     The topic to search for on Wikipedia.
        sentences: Number of summary sentences to return (default 3).

    Returns:
        A brief summary from Wikipedia, or an error message if not found.
    """
    try:
        # Fetch a concise summary from Wikipedia
        result = wikipedia.summary(query, sentences=sentences)
        return f"Wikipedia result for '{query}':\n{result}"
    except wikipedia.DisambiguationError as e:
        # Multiple matches found — use the first suggestion
        first_option = e.options[0]
        result = wikipedia.summary(first_option, sentences=sentences)
        return f"Wikipedia result for '{first_option}' (closest match):\n{result}"
    except wikipedia.PageError:
        return f"No Wikipedia page found for '{query}'."
    except Exception as e:
        return f"Wikipedia search error: {str(e)}"


def calculate(expression: str) -> str:
    """Safely evaluate a mathematical expression.

    Uses Python's eval with a restricted namespace that only allows
    basic math operations and built-in math functions.

    Args:
        expression: A math expression string, e.g. "25 * 48 + 137".

    Returns:
        The result as a string, or an error message if evaluation fails.
    """
    try:
        # Restricted namespace — only safe math builtins are available
        import math
        allowed_names = {
            "abs": abs, "round": round, "min": min, "max": max,
            "pow": pow, "sum": sum, "int": int, "float": float,
            "pi": math.pi, "e": math.e,
            "sqrt": math.sqrt, "log": math.log, "log10": math.log10,
            "sin": math.sin, "cos": math.cos, "tan": math.tan,
            "ceil": math.ceil, "floor": math.floor,
        }
        result = eval(expression, {"__builtins__": {}}, allowed_names)
        return f"Calculation: {expression} = {result}"
    except Exception as e:
        return f"Calculation error for '{expression}': {str(e)}"


def get_current_datetime() -> str:
    """Return the current date and time in a human-readable format.

    Returns:
        A formatted string with the current date, time, and day of the week.
    """
    now = datetime.datetime.now()
    return (
        f"Current date and time: {now.strftime('%A, %B %d, %Y at %I:%M %p')}"
    )
