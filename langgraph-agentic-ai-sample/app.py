"""
app.py - Streamlit UI for the LangGraph Agentic AI Workflow.

This file creates an interactive chat interface where users can ask
questions. Behind the scenes, a LangGraph workflow:
  1. Routes the question to the right tool (Wikipedia, Calculator, or LLM)
  2. Executes the tool
  3. Synthesizes a polished answer

Run with:  streamlit run app.py
"""

import streamlit as st

from config.settings import OPENAI_API_KEY
from workflow.graph import run_workflow, build_graph


# ──────────────────────────────────────────────────────────────────────
# Page Configuration
# ──────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="LangGraph Agentic AI Assistant",
    page_icon="🤖",
    layout="centered",
)


# ──────────────────────────────────────────────────────────────────────
# Header
# ──────────────────────────────────────────────────────────────────────
st.title("🤖 LangGraph Agentic AI Assistant")
st.caption(
    "Powered by LangGraph + LangChain + OpenAI  |  "
    "Routes questions through an intelligent graph workflow"
)


# ──────────────────────────────────────────────────────────────────────
# Sidebar — Graph Info, Tools, and Sample Prompts
# ──────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("📊 Workflow Graph")
    st.markdown(
        """
        ```
        START
          │
          ▼
        Router ──→ classifies question
          │
        ┌─┼─────────┐
        │ │         │
        ▼ ▼         ▼
      Research  Math  General
        │ │         │
        └─┼─────────┘
          ▼
       Synthesizer ──→ final answer
          │
          ▼
         END
        ```
        """
    )

    st.divider()
    st.header("🛠️ Available Tools")
    st.markdown(
        """
        - **📚 Wikipedia Search** — Factual look-ups
        - **🔢 Calculator** — Math expressions
        - **📅 DateTime** — Current date and time
        - **💬 LLM Direct** — General conversation
        """
    )

    st.divider()
    st.header("💡 Try These Prompts")
    st.markdown(
        """
        - *"What is LangGraph?"*
        - *"What is 25 * 48 + 137?"*
        - *"What day is it today?"*
        - *"Who invented Python?"*
        - *"Explain quantum computing briefly"*
        """
    )


# ──────────────────────────────────────────────────────────────────────
# API Key Check
# ──────────────────────────────────────────────────────────────────────
if not OPENAI_API_KEY:
    st.error(
        "⚠️ **OpenAI API key not found!** "
        "Please set `OPENAI_API_KEY` in your `.env` file. "
        "See `.env.example` for instructions."
    )
    st.stop()


# ──────────────────────────────────────────────────────────────────────
# Chat History (persisted across Streamlit reruns via session_state)
# ──────────────────────────────────────────────────────────────────────
if "messages" not in st.session_state:
    st.session_state.messages = [
        {
            "role": "assistant",
            "content": (
                "Hello! I'm your AI assistant powered by **LangGraph**. "
                "I can search Wikipedia, solve math problems, and answer "
                "general questions. Try asking me something!"
            ),
        }
    ]

# Render all previous messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])


# ──────────────────────────────────────────────────────────────────────
# Chat Input and Workflow Execution
# ──────────────────────────────────────────────────────────────────────
if user_input := st.chat_input("Ask me anything..."):

    # Display the user's message
    with st.chat_message("user"):
        st.markdown(user_input)
    st.session_state.messages.append({"role": "user", "content": user_input})

    # Run the LangGraph workflow and display the response
    with st.chat_message("assistant"):
        with st.spinner("🧠 Running LangGraph workflow..."):
            try:
                # Execute the full graph workflow
                result = run_workflow(user_input)

                # Extract the final answer from the workflow result
                answer = result.get("final_answer", "Sorry, I could not process that.")
                route_used = result.get("route", "unknown")

                # Show which route was taken (for transparency)
                route_labels = {
                    "research": "📚 Wikipedia Search",
                    "math": "🔢 Calculator",
                    "general": "💬 General / DateTime",
                }
                route_label = route_labels.get(route_used, "❓ Unknown")

                # Display the answer with route info
                st.markdown(answer)
                st.caption(f"Route taken: {route_label}")

            except Exception as e:
                answer = f"❌ Error: {str(e)}"
                st.error(answer)

    # Save to chat history
    st.session_state.messages.append({"role": "assistant", "content": answer})
