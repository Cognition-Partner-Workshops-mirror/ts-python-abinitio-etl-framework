"""
graph.py - Compiles the LangGraph StateGraph that defines the agentic workflow.

This is where all the pieces come together. We:
  1. Create a StateGraph with our AgentState schema
  2. Add nodes (router, research, math, general, synthesizer)
  3. Add edges (including conditional edges for routing)
  4. Compile the graph into a runnable workflow

VISUAL REPRESENTATION OF THE GRAPH:

         ┌─────────┐
         │  START   │
         └────┬─────┘
              │
              ▼
       ┌──────────────┐
       │  router_node │  ← LLM classifies the question
       └──────┬───────┘
              │
     ┌────────┼────────┐     (conditional edges)
     │        │        │
     ▼        ▼        ▼
 ┌────────┐ ┌──────┐ ┌─────────┐
 │research│ │ math │ │ general │  ← Tool nodes
 └───┬────┘ └──┬───┘ └────┬────┘
     │         │          │
     └─────────┼──────────┘
               ▼
      ┌────────────────┐
      │  synthesizer   │  ← Polishes the final answer
      └───────┬────────┘
              │
              ▼
         ┌─────────┐
         │   END   │
         └─────────┘

KEY LANGGRAPH CONCEPTS DEMONSTRATED:
  - StateGraph:        Graph where nodes share a typed state dictionary
  - add_node():        Registers a function as a vertex in the graph
  - add_edge():        Connects two nodes with a fixed transition
  - add_conditional_edges(): Routes to different nodes based on a function's return value
  - START / END:       Special built-in nodes marking entry and exit points
  - compile():         Converts the graph definition into an executable workflow
"""

from langgraph.graph import StateGraph, START, END

from workflow.state import AgentState
from workflow.nodes import (
    router_node,
    research_node,
    math_node,
    general_node,
    synthesizer_node,
)


def route_question(state: AgentState) -> str:
    """Routing function — returns the name of the next node to execute.

    LangGraph calls this function after the router_node completes.
    The return value must match one of the keys in the conditional
    edge mapping (see below).

    Args:
        state: Current graph state (contains the "route" set by router_node).

    Returns:
        One of "research", "math", or "general" — the next node name.
    """
    return state["route"]


def build_graph() -> StateGraph:
    """Build and compile the LangGraph agentic workflow.

    Returns:
        A compiled StateGraph ready to be invoked with a question.
    """

    # Step 1: Create a StateGraph with the AgentState schema
    # AgentState defines which fields exist in the shared state
    graph = StateGraph(AgentState)

    # Step 2: Add nodes (each node is a function from nodes.py)
    graph.add_node("router", router_node)         # Classifies the question
    graph.add_node("research", research_node)      # Searches Wikipedia
    graph.add_node("math", math_node)              # Evaluates math expressions
    graph.add_node("general", general_node)        # Handles general queries
    graph.add_node("synthesizer", synthesizer_node)  # Polishes the final answer

    # Step 3: Add edges to define the flow

    # START → router (every question begins at the router)
    graph.add_edge(START, "router")

    # router → (conditional) → research / math / general
    # The route_question function reads state["route"] and returns
    # the name of the next node to execute
    graph.add_conditional_edges(
        source="router",
        path=route_question,
        path_map={
            "research": "research",
            "math": "math",
            "general": "general",
        },
    )

    # All tool nodes → synthesizer (converge to a single exit point)
    graph.add_edge("research", "synthesizer")
    graph.add_edge("math", "synthesizer")
    graph.add_edge("general", "synthesizer")

    # synthesizer → END (workflow complete)
    graph.add_edge("synthesizer", END)

    # Step 4: Compile the graph into a runnable workflow
    compiled_graph = graph.compile()

    return compiled_graph


def run_workflow(question: str) -> dict:
    """Execute the full agentic workflow for a given question.

    This is the main entry point for running the graph. It:
      1. Builds and compiles the graph
      2. Invokes it with the user's question
      3. Returns the final state (including the answer)

    Args:
        question: The user's question to process.

    Returns:
        The final AgentState dict with all fields populated.
    """
    # Build the graph
    compiled_graph = build_graph()

    # Run the workflow with the user's question as initial state
    result = compiled_graph.invoke({"question": question})

    return result
