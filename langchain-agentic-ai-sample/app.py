"""
app.py - Streamlit UI for the LangChain Agentic AI workflow.

This file creates an interactive chat interface where users can
ask questions and see the AI agent work in real time. The agent
autonomously decides which tools to use (Wikipedia, Calculator,
or DateTime) to answer each question.

Run with: streamlit run app.py
"""

import streamlit as st
from agent.agent import create_agent
from config.settings import OPENAI_API_KEY

# ──────────────────────────────────────────────
# Page Configuration
# ──────────────────────────────────────────────
st.set_page_config(
    page_title="Agentic AI Assistant",
    page_icon="🤖",
    layout="centered",
)

# ──────────────────────────────────────────────
# Header Section
# ──────────────────────────────────────────────
st.title("🤖 Agentic AI Assistant")
st.caption("Powered by LangChain + OpenAI | Tools: Wikipedia, Calculator, DateTime")

# ──────────────────────────────────────────────
# Sidebar - Show available tools and sample prompts
# ──────────────────────────────────────────────
with st.sidebar:
    st.header("🛠️ Available Tools")
    st.markdown("""
    The agent can autonomously use these tools:

    - **📚 Wikipedia Search** — Look up facts
    - **🔢 Calculator** — Solve math problems
    - **📅 DateTime** — Get current date/time
    """)

    st.divider()
    st.header("💡 Try These Prompts")
    st.markdown("""
    - *"What is LangChain?"*
    - *"What is 25 * 48 + 137?"*
    - *"What day is it today?"*
    - *"Who invented Python and when?"*
    - *"What is the population of Japan times 2?"*
    """)

# ──────────────────────────────────────────────
# API Key Validation
# ──────────────────────────────────────────────
if not OPENAI_API_KEY:
    st.error(
        "⚠️ OpenAI API key not found! "
        "Please set OPENAI_API_KEY in your .env file. "
        "See .env.example for instructions."
    )
    st.stop()

# ──────────────────────────────────────────────
# Initialize Chat History in Streamlit Session State
# Session state persists data across Streamlit reruns
# ──────────────────────────────────────────────
if "messages" not in st.session_state:
    # Start with a welcome message from the assistant
    st.session_state.messages = [
        {
            "role": "assistant",
            "content": (
                "Hello! I'm your AI research assistant. "
                "I can search Wikipedia, do calculations, and tell you the date. "
                "Ask me anything!"
            ),
        }
    ]

# ──────────────────────────────────────────────
# Display Chat History
# Render all previous messages in the chat UI
# ──────────────────────────────────────────────
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# ──────────────────────────────────────────────
# Chat Input and Agent Execution
# ──────────────────────────────────────────────
if user_input := st.chat_input("Ask me anything..."):

    # Display the user's message in the chat UI
    with st.chat_message("user"):
        st.markdown(user_input)

    # Save user message to history
    st.session_state.messages.append({"role": "user", "content": user_input})

    # Run the agent and display its response
    with st.chat_message("assistant"):
        # Show a spinner while the agent is thinking
        with st.spinner("🧠 Thinking..."):
            try:
                # Create the agent and invoke it with the user's question
                agent = create_agent()
                response = agent.invoke({"input": user_input})

                # Extract the final answer from the agent's response
                answer = response.get("output", "Sorry, I could not generate a response.")

            except Exception as e:
                answer = f"❌ Error: {str(e)}"

        # Display the agent's answer
        st.markdown(answer)

    # Save assistant response to history
    st.session_state.messages.append({"role": "assistant", "content": answer})
