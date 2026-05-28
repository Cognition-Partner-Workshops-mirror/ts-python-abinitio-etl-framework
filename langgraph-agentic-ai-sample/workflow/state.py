"""
state.py - Defines the shared state that flows through the LangGraph workflow.

In LangGraph, every node reads from and writes to a shared state object.
This TypedDict defines the "shape" of that state — what fields exist
and what types they hold.

Think of state as a shared whiteboard that every node can read and update.

FLOW:
  User types a question
       ↓
  State["question"] is set
       ↓
  Router node reads the question, sets State["route"]
       ↓
  The correct tool node runs, sets State["tool_output"]
       ↓
  Synthesizer reads everything, sets State["final_answer"]
"""

from typing import TypedDict, Literal


class AgentState(TypedDict):
    """Shared state passed between all nodes in the workflow graph.

    Attributes:
        question:     The user's original question (set at the start).
        route:        The category chosen by the router node. One of
                      "research", "math", or "general".
        tool_output:  The raw output produced by the tool node that ran.
        final_answer: The polished response composed by the synthesizer node.
    """

    # The user's original question — set once at graph entry
    question: str

    # Category chosen by the router: determines which tool node runs next
    route: Literal["research", "math", "general"]

    # Raw output from the tool node (Wikipedia result, calculation, or LLM reply)
    tool_output: str

    # Final polished answer assembled by the synthesizer node
    final_answer: str
