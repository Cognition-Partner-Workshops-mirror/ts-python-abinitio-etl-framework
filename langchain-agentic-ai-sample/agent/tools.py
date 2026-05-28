"""
tools.py - Custom tools that the AI agent can use.

In an agentic workflow, "tools" are functions the AI can call
on its own to gather information or perform actions. The agent
decides WHICH tool to use and WHEN based on the user's question.

This file defines three simple tools:
  1. Wikipedia Search  - Look up factual information
  2. Calculator        - Solve math problems
  3. Current DateTime  - Get the current date and time
"""

from datetime import datetime

from langchain_core.tools import tool


@tool
def search_wikipedia(query: str) -> str:
    """Search Wikipedia for factual information about a topic.

    Args:
        query: The topic to search for on Wikipedia.

    Returns:
        A summary from Wikipedia or an error message.
    """
    # Import wikipedia inside the function to keep startup fast
    import wikipedia

    try:
        # Get a short summary (max 3 sentences) from Wikipedia
        result = wikipedia.summary(query, sentences=3)
        return result
    except wikipedia.DisambiguationError as e:
        # Topic is ambiguous - return the first suggestion
        return f"Ambiguous topic. Did you mean one of: {', '.join(e.options[:5])}?"
    except wikipedia.PageError:
        # No Wikipedia page found for this topic
        return f"No Wikipedia page found for '{query}'."
    except Exception as e:
        return f"Search error: {str(e)}"


@tool
def calculator(expression: str) -> str:
    """Evaluate a math expression and return the result.

    Args:
        expression: A math expression like '2 + 2' or '(10 * 5) / 3'.

    Returns:
        The calculated result as a string.
    """
    try:
        # Use Python's eval with restricted builtins for safety
        # Only allow basic math operations - no file access or imports
        allowed_names = {"__builtins__": {}}
        result = eval(expression, allowed_names)
        return str(result)
    except Exception as e:
        return f"Could not calculate '{expression}'. Error: {str(e)}"


@tool
def get_current_datetime() -> str:
    """Get the current date and time.

    Returns:
        The current date and time in a readable format.
    """
    now = datetime.now()
    return now.strftime("%A, %B %d, %Y at %I:%M %p")


def get_all_tools() -> list:
    """Return a list of all available tools for the agent.

    This is the single place to register tools. Adding a new tool
    is as simple as defining it above and adding it to this list.
    """
    return [search_wikipedia, calculator, get_current_datetime]
