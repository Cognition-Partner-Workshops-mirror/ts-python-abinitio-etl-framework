# 🤖 LangChain Agentic AI - Sample Project

A **complete, minimal, and easy-to-understand** sample project demonstrating how to build an **Agentic AI workflow** using [LangChain](https://www.langchain.com/) with a [Streamlit](https://streamlit.io/) chat UI.

---

## 📌 What Is an Agentic AI Workflow?

An **agentic workflow** is an AI system that can **autonomously decide** which actions to take to answer a question. Unlike a simple chatbot that only generates text, an agent can:

1. **Reason** about the user's question
2. **Choose** the right tool (e.g., search, calculator)
3. **Execute** the tool and read the result
4. **Repeat** until it has enough information
5. **Respond** with a final answer

```
User Question → Agent (LLM) → Picks a Tool → Reads Result → Final Answer
                     ↑                              |
                     └──────── loops if needed ──────┘
```

---

## 🗂️ Project Structure

```
langchain-agentic-ai-sample/
├── README.md              ← You are here
├── requirements.txt       ← Python dependencies
├── .env.example           ← Template for API keys
├── app.py                 ← Streamlit chat UI (entry point)
├── agent/
│   ├── __init__.py
│   ├── agent.py           ← LangChain agent setup (the brain)
│   └── tools.py           ← Tools the agent can use
└── config/
    ├── __init__.py
    └── settings.py        ← Configuration & env variable loading
```

| File               | Purpose                                                  |
|--------------------|----------------------------------------------------------|
| `app.py`           | Streamlit UI — chat interface to interact with the agent |
| `agent/agent.py`   | Creates the LangChain agent (LLM + Tools + Prompt)      |
| `agent/tools.py`   | Defines 3 tools: Wikipedia, Calculator, DateTime         |
| `config/settings.py` | Loads `.env` and provides config constants             |

---

## 🛠️ Tools Available to the Agent

| Tool              | What It Does                              |
|-------------------|-------------------------------------------|
| **Wikipedia Search** | Searches Wikipedia for factual info     |
| **Calculator**       | Evaluates math expressions              |
| **Current DateTime** | Returns the current date and time       |

---

## 🚀 Local Setup Instructions

### Prerequisites

- **Python 3.10+** installed ([download](https://www.python.org/downloads/))
- **OpenAI API key** ([get one here](https://platform.openai.com/api-keys))

### Step 1: Navigate to the Project Folder

```bash
cd langchain-agentic-ai-sample
```

### Step 2: Create a Virtual Environment (Recommended)

```bash
# Create a virtual environment
python -m venv venv

# Activate it
# On macOS/Linux:
source venv/bin/activate

# On Windows:
venv\Scripts\activate
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Configure Your API Key

```bash
# Copy the example environment file
cp .env.example .env

# Edit .env and paste your OpenAI API key
# Replace "your-openai-api-key-here" with your actual key
```

### Step 5: Run the Application

```bash
streamlit run app.py
```

The app will open in your browser at **http://localhost:8501**.

---

## 💡 Sample Prompts to Try

| Prompt                                     | Tools Used          |
|--------------------------------------------|---------------------|
| "What is LangChain?"                       | Wikipedia Search    |
| "What is 25 * 48 + 137?"                   | Calculator          |
| "What day is it today?"                    | DateTime            |
| "Who invented Python and when was it released?" | Wikipedia Search |
| "What is the population of Japan times 2?" | Wikipedia + Calculator |

---

## 🖥️ Configure and Run in IntelliJ IDEA

### Step 1: Open the Project

1. Open IntelliJ IDEA
2. Go to **File → Open**
3. Select the `langchain-agentic-ai-sample` folder
4. Click **OK**

### Step 2: Configure Python Interpreter

1. Go to **File → Project Structure → SDKs**
2. Click **+ → Add Python SDK → Virtual Environment → New**
3. Set the base interpreter to **Python 3.10+**
4. Click **OK**

### Step 3: Install Dependencies

1. Open the **Terminal** tab at the bottom of IntelliJ
2. Run:
   ```bash
   pip install -r requirements.txt
   ```

### Step 4: Set Up Environment Variables

1. Copy `.env.example` to `.env`
2. Edit `.env` and add your OpenAI API key

### Step 5: Create a Run Configuration

1. Go to **Run → Edit Configurations**
2. Click **+ → Shell Script** (or **Python**)
3. Configure as:
   - **Name**: `Run Streamlit App`
   - **Script**: `streamlit`
   - **Script parameters**: `run app.py`
   - **Working directory**: `path/to/langchain-agentic-ai-sample`
   - **Environment variables**: Add `OPENAI_API_KEY=your-key` (or rely on `.env`)
4. Click **OK**
5. Press the **▶ Run** button

Alternatively, just use the IntelliJ Terminal:
```bash
streamlit run app.py
```

---

## 📦 Downloadable ZIP Structure

To package this project as a ZIP for distribution:

```bash
# From the parent directory of the project
zip -r langchain-agentic-ai-sample.zip langchain-agentic-ai-sample/ \
    -x "langchain-agentic-ai-sample/venv/*" \
    -x "langchain-agentic-ai-sample/__pycache__/*" \
    -x "langchain-agentic-ai-sample/.env"
```

This excludes the virtual environment, cache files, and your secret `.env` file.

---

## 📖 How the Code Works (Quick Walkthrough)

### 1. `config/settings.py` — Loads Configuration
Reads `OPENAI_API_KEY` from the `.env` file using `python-dotenv`.

### 2. `agent/tools.py` — Defines Agent Tools
Each tool is a Python function decorated with `@tool`. LangChain
automatically generates a description for the LLM from the docstring.

### 3. `agent/agent.py` — Builds the Agent
Combines the LLM (OpenAI GPT), tools, and a system prompt into an
`AgentExecutor`. The executor manages the **decide → act → observe** loop.

### 4. `app.py` — Streamlit Chat UI
Creates a chat interface. When the user sends a message, it invokes the
agent and displays the response. Chat history is stored in Streamlit's
session state.

---

## ⚙️ Key LangChain Concepts Demonstrated

| Concept             | Where                | What It Means                                      |
|---------------------|----------------------|----------------------------------------------------|
| **LLM**            | `agent.py` line 40   | The AI model (GPT-4o-mini) that powers reasoning   |
| **Tools**          | `tools.py`           | Functions the agent can call autonomously           |
| **Prompt Template** | `agent.py` line 45  | Instructions telling the agent how to behave        |
| **Agent**          | `agent.py` line 63   | The LLM + tools combination that can reason & act   |
| **AgentExecutor**  | `agent.py` line 67   | Runs the agentic loop (reason → act → observe)     |
| **Tool Calling**   | Automatic            | LLM natively decides which tool to invoke           |

---

## 🔧 Extending the Project

Want to add a new tool? It's simple:

1. Open `agent/tools.py`
2. Add a new function with the `@tool` decorator:
   ```python
   @tool
   def my_new_tool(param: str) -> str:
       """Description of what this tool does."""
       return "result"
   ```
3. Add it to the `get_all_tools()` list
4. Restart the app — the agent will automatically discover and use it!

---

## 📋 Troubleshooting

| Issue                          | Solution                                      |
|--------------------------------|-----------------------------------------------|
| `OPENAI_API_KEY not found`     | Make sure `.env` file exists with your key     |
| `ModuleNotFoundError`          | Run `pip install -r requirements.txt`          |
| Agent gives wrong answers      | Try setting `TEMPERATURE=0.0` in `settings.py` |
| Streamlit won't start          | Check Python version: `python --version` (need 3.10+) |
