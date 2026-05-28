"""
agent.py - LangChain Agentic AI workflow setup.

This is the core of the agentic workflow. It wires together:
  1. An LLM (OpenAI GPT) as the "brain"
  2. Tools the agent can use (Wikipedia, Calculator, DateTime)
  3. A prompt that tells the agent how to behave

HOW IT WORKS (Agentic Loop):
  User asks a question
       |
       v
  Agent (LLM) reads the question
       |
       v
  Agent DECIDES which tool to call (or answers directly)
       |
       v
  Tool runs and returns a result
       |
       v
  Agent reads the tool result and decides next step
       |
       v
  Agent either calls another tool OR gives a final answer
       |
       v
  Final answer is returned to the user

This "decide -> act -> observe -> repeat" loop is what makes
the workflow "agentic" - the AI autonomously plans its actions.
"""

from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.agents import create_tool_calling_agent, AgentExecutor

from config.settings import OPENAI_API_KEY, MODEL_NAME, TEMPERATURE
from agent.tools import get_all_tools


def create_agent() -> AgentExecutor:
    """Create and return a fully configured LangChain agent.

    Returns:
        AgentExecutor: A ready-to-use agent that can answer questions
                       using its tools (Wikipedia, Calculator, DateTime).
    """

    # Step 1: Initialize the LLM (Large Language Model)
    # This is the "brain" of the agent that understands and generates text
    llm = ChatOpenAI(
        model=MODEL_NAME,
        temperature=TEMPERATURE,
        api_key=OPENAI_API_KEY,
    )

    # Step 2: Get the list of tools the agent can use
    tools = get_all_tools()

    # Step 3: Define the agent's system prompt
    # This tells the agent WHO it is and HOW to behave
    prompt = ChatPromptTemplate.from_messages([
        (
            "system",
            "You are a helpful research assistant. "
            "Use your tools to answer questions accurately. "
            "Always cite your sources when using Wikipedia. "
            "For math questions, use the calculator tool. "
            "If you don't know something, say so honestly.",
        ),
        # Placeholder for conversation history (enables multi-turn chat)
        MessagesPlaceholder(variable_name="chat_history", optional=True),
        # The user's current question
        ("human", "{input}"),
        # Placeholder where the agent's reasoning steps are stored
        MessagesPlaceholder(variable_name="agent_scratchpad"),
    ])

    # Step 4: Create the agent by combining LLM + tools + prompt
    # create_tool_calling_agent uses the LLM's native function-calling
    agent = create_tool_calling_agent(llm, tools, prompt)

    # Step 5: Wrap in AgentExecutor which manages the agentic loop
    # verbose=True prints the agent's thought process to the console
    agent_executor = AgentExecutor(
        agent=agent,
        tools=tools,
        verbose=True,      # Set to False to hide thinking steps
        max_iterations=5,  # Safety limit to prevent infinite loops
        handle_parsing_errors=True,  # Gracefully handle malformed LLM output
    )

    return agent_executor
