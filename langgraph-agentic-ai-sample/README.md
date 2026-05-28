# 🤖 LangGraph Agentic AI — Sample Project

A **complete, simple, and easy-to-understand** sample project demonstrating how to build an **Agentic AI workflow** using [LangGraph](https://langchain-ai.github.io/langgraph/) with a [Streamlit](https://streamlit.io/) chat UI.

---

## 📌 What Is an Agentic AI Workflow?

An **agentic workflow** is an AI system that can **autonomously decide** which actions to take to answer a question. Unlike a simple chatbot that only generates text, an agent can:

1. **Reason** about the user's question
2. **Route** to the appropriate tool or action
3. **Execute** the tool and gather results
4. **Synthesize** a polished final answer

---

## 📌 What Is LangGraph?

**LangGraph** is a library by LangChain for building **stateful, graph-based** AI workflows. It extends LangChain by letting you define your agentic logic as a **directed graph**:

- **Nodes** = Functions that do work (e.g., classify, search, calculate)
- **Edges** = Transitions between nodes (fixed or conditional)
- **State** = A shared dictionary passed between all nodes

This graph-based approach gives you **full control** over the agent's decision flow, unlike a black-box agent loop.

### Why LangGraph over a simple LangChain Agent?

| Feature                | LangChain Agent          | LangGraph                    |
|------------------------|--------------------------|------------------------------|
| Flow control           | Implicit (agent decides) | Explicit (you define graph)  |
| Visibility             | Black-box loop           | Visual, inspectable graph    |
| Conditional branching  | Limited                  | First-class support          |
| State management       | Manual                   | Built-in TypedDict state     |
| Debugging              | Harder (hidden steps)    | Easier (step-by-step nodes)  |

---

## 🗂️ Project Structure

```
langgraph-agentic-ai-sample/
├── README.md                ← You are here
├── requirements.txt         ← Python dependencies
├── .env.example             ← Template for API keys
├── .gitignore               ← Files to exclude from git
├── app.py                   ← Streamlit chat UI (entry point)
├── workflow/
│   ├── __init__.py
│   ├── state.py             ← Shared state schema (TypedDict)
│   ├── tools.py             ← Tool functions (Wikipedia, Calculator, DateTime)
│   ├── nodes.py             ← Graph node functions (Router, Research, Math, General, Synthesizer)
│   └── graph.py             ← LangGraph StateGraph definition and compilation
└── config/
    ├── __init__.py
    └── settings.py          ← Environment variable loading
```

### File Descriptions

| File                  | Purpose                                                           |
|-----------------------|-------------------------------------------------------------------|
| `app.py`              | Streamlit chat UI — the visual frontend you interact with         |
| `workflow/state.py`   | Defines `AgentState` (the shared data passed between nodes)       |
| `workflow/tools.py`   | Tool functions: `search_wikipedia`, `calculate`, `get_current_datetime` |
| `workflow/nodes.py`   | Node functions: `router_node`, `research_node`, `math_node`, `general_node`, `synthesizer_node` |
| `workflow/graph.py`   | Builds the `StateGraph`, adds nodes/edges, and compiles it        |
| `config/settings.py`  | Loads `.env` file and exposes config constants                    |

---

## 📊 Workflow Graph Explained

```
         ┌─────────┐
         │  START   │
         └────┬─────┘
              │
              ▼
       ┌──────────────┐
       │  router_node │  ← LLM classifies the question
       └──────┬───────┘
              │
     ┌────────┼────────┐     (conditional routing)
     │        │        │
     ▼        ▼        ▼
 ┌────────┐ ┌──────┐ ┌─────────┐
 │research│ │ math │ │ general │  ← Tool nodes execute
 └───┬────┘ └──┬───┘ └────┬────┘
     │         │          │
     └─────────┼──────────┘
               ▼
      ┌────────────────┐
      │  synthesizer   │  ← LLM polishes the final answer
      └───────┬────────┘
              │
              ▼
         ┌─────────┐
         │   END   │
         └─────────┘
```

### How It Works Step-by-Step

1. **User** types a question in the Streamlit UI
2. **Router Node** — The LLM classifies the question as `research`, `math`, or `general`
3. **Conditional Edge** — LangGraph routes to the appropriate tool node:
   - `research` → **Research Node** (searches Wikipedia)
   - `math` → **Math Node** (extracts and evaluates a math expression)
   - `general` → **General Node** (checks for date/time or uses LLM directly)
4. **Synthesizer Node** — Takes the tool output and the original question, then produces a polished answer
5. **Result** is displayed in the Streamlit chat UI

---

## 🛠️ Tools Available to the Agent

| Tool                    | What It Does                                   | Used By        |
|-------------------------|------------------------------------------------|----------------|
| `search_wikipedia()`   | Searches Wikipedia for factual information     | Research Node  |
| `calculate()`          | Safely evaluates math expressions              | Math Node      |
| `get_current_datetime()`| Returns current date and time                 | General Node   |
| LLM Direct              | Uses ChatGPT for general conversation         | General Node   |

---

## 🚀 Local Setup Instructions

### Prerequisites

- **Python 3.10+** installed ([download here](https://www.python.org/downloads/))
- **OpenAI API key** ([get one here](https://platform.openai.com/api-keys))
- **pip** (comes with Python)

### Step 1: Clone or Download the Project

```bash
# If using git:
git clone <repository-url>
cd langgraph-agentic-ai-sample

# Or download and extract the ZIP file (see ZIP section below)
```

### Step 2: Create a Virtual Environment (Recommended)

```bash
# Create a virtual environment
python -m venv venv

# Activate it:
# macOS / Linux:
source venv/bin/activate

# Windows (Command Prompt):
venv\Scripts\activate

# Windows (PowerShell):
venv\Scripts\Activate.ps1
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Configure Your API Key

```bash
# Copy the example env file
cp .env.example .env

# Edit .env and replace the placeholder with your actual OpenAI API key:
# OPENAI_API_KEY=sk-your-actual-key-here
```

### Step 5: Run the Application

```bash
streamlit run app.py
```

The app will open in your browser at **http://localhost:8501**.

---

## 💡 Sample Prompts to Try

| Prompt                                          | Route Used        | Tools Invoked              |
|-------------------------------------------------|-------------------|----------------------------|
| "What is LangGraph?"                            | 📚 Research       | Wikipedia Search           |
| "What is 25 * 48 + 137?"                        | 🔢 Math           | Calculator                 |
| "What day is it today?"                          | 💬 General        | DateTime                   |
| "Who invented Python and when was it released?" | 📚 Research       | Wikipedia Search           |
| "Calculate the square root of 144"               | 🔢 Math           | Calculator                 |
| "Tell me a joke"                                 | 💬 General        | LLM Direct                 |

---

## 🖥️ Configure and Run in IntelliJ IDEA

### Step 1: Open the Project

1. Launch **IntelliJ IDEA** (Community or Ultimate)
2. Go to **File → Open**
3. Navigate to and select the `langgraph-agentic-ai-sample` folder
4. Click **OK** to open the project

### Step 2: Install the Python Plugin (Community Edition Only)

1. Go to **File → Settings → Plugins** (or **IntelliJ IDEA → Preferences → Plugins** on macOS)
2. Search for **"Python Community Edition"** in the Marketplace tab
3. Click **Install** and restart IntelliJ if prompted

> **Note:** IntelliJ Ultimate has Python support built-in.

### Step 3: Configure Python Interpreter

1. Go to **File → Project Structure** (or press `Ctrl+Alt+Shift+S`)
2. Under **Platform Settings → SDKs**, click the **+** button
3. Select **Add Python SDK → Virtualenv Environment → New environment**
4. Set **Base interpreter** to your Python 3.10+ installation
5. Set **Location** to `<project-root>/venv`
6. Click **OK** to create the virtual environment

### Step 4: Install Dependencies

1. Open the **Terminal** tab at the bottom of IntelliJ IDEA
2. Verify the virtual environment is active (you should see `(venv)` in the prompt)
3. Run:
   ```bash
   pip install -r requirements.txt
   ```

### Step 5: Set Up Environment Variables

1. In the Project view, right-click `.env.example` → **Copy**
2. Right-click the project root → **Paste** and rename to `.env`
3. Open `.env` and replace `your-openai-api-key-here` with your actual key

### Step 6: Create a Run Configuration

1. Go to **Run → Edit Configurations**
2. Click **+** and select **Shell Script**
3. Configure as follows:

| Field                  | Value                                         |
|------------------------|-----------------------------------------------|
| **Name**               | `Run Streamlit App`                           |
| **Execute**            | `Script text`                                 |
| **Script text**        | `streamlit run app.py`                        |
| **Working directory**  | `<full-path-to>/langgraph-agentic-ai-sample`  |
| **Interpreter path**   | `<project-root>/venv/bin/python` (Linux/macOS) or `<project-root>\venv\Scripts\python.exe` (Windows) |

4. Click **OK**

### Step 7: Run the Application

1. Select **Run Streamlit App** from the configuration dropdown in the toolbar
2. Click the **▶ Run** button (or press `Shift+F10`)
3. The Streamlit app will start and open at **http://localhost:8501**

### Alternative: Run from IntelliJ Terminal

Open the IntelliJ terminal and run:
```bash
streamlit run app.py
```

---

## 📦 Downloadable ZIP File Structure

To create a downloadable ZIP of this project:

### On macOS / Linux:

```bash
# Navigate to the parent directory
cd ..

# Create a ZIP file (excluding virtual env, .env, and IDE files)
zip -r langgraph-agentic-ai-sample.zip langgraph-agentic-ai-sample/ \
  -x "langgraph-agentic-ai-sample/venv/*" \
  -x "langgraph-agentic-ai-sample/.env" \
  -x "langgraph-agentic-ai-sample/__pycache__/*" \
  -x "langgraph-agentic-ai-sample/.idea/*"
```

### On Windows (PowerShell):

```powershell
Compress-Archive -Path langgraph-agentic-ai-sample -DestinationPath langgraph-agentic-ai-sample.zip
```

### ZIP Contents (What Recipients Get):

```
langgraph-agentic-ai-sample.zip
└── langgraph-agentic-ai-sample/
    ├── README.md              ← Setup guide (this file)
    ├── requirements.txt       ← Dependencies to install
    ├── .env.example           ← Template — recipient fills in API key
    ├── .gitignore             ← Git ignore rules
    ├── app.py                 ← Main entry point
    ├── workflow/
    │   ├── __init__.py
    │   ├── state.py           ← State schema
    │   ├── tools.py           ← Tool functions
    │   ├── nodes.py           ← Graph node logic
    │   └── graph.py           ← Graph definition
    └── config/
        ├── __init__.py
        └── settings.py        ← Config loader
```

After extracting, recipients follow Steps 2–5 from the **Local Setup Instructions** above.

---

## 🧩 Key LangGraph Concepts Used

| Concept                  | Where Used        | What It Does                                              |
|--------------------------|-------------------|-----------------------------------------------------------|
| `StateGraph`             | `graph.py`        | Creates a graph with typed state                          |
| `AgentState` (TypedDict) | `state.py`        | Defines the shape of the shared state object              |
| `add_node()`             | `graph.py`        | Registers a function as a node in the graph               |
| `add_edge()`             | `graph.py`        | Connects two nodes with a fixed transition                |
| `add_conditional_edges()`| `graph.py`        | Routes to different nodes based on state values           |
| `START` / `END`          | `graph.py`        | Built-in entry and exit points of the graph               |
| `compile()`              | `graph.py`        | Converts the graph definition into a runnable workflow    |
| `invoke()`               | `graph.py`        | Executes the compiled graph with initial state            |

---

## 🔧 Extending This Project

Here are ideas for extending this sample:

1. **Add more tools** — Add a web search tool (e.g., DuckDuckGo), a weather API, or a database query tool in `workflow/tools.py`
2. **Add memory** — Use LangGraph's checkpointing to persist conversation history across sessions
3. **Add human-in-the-loop** — Add a node that asks for user confirmation before executing certain actions
4. **Add parallel nodes** — Use LangGraph's `Send` API to run multiple tool nodes in parallel
5. **Add error handling nodes** — Create a fallback node that handles tool failures gracefully
6. **Swap the LLM** — Replace OpenAI with an open-source model (e.g., Ollama + Llama) by changing `config/settings.py`

---

## 📚 Further Reading

- [LangGraph Documentation](https://langchain-ai.github.io/langgraph/)
- [LangGraph Tutorials](https://langchain-ai.github.io/langgraph/tutorials/)
- [LangChain Documentation](https://python.langchain.com/)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [OpenAI API Reference](https://platform.openai.com/docs/api-reference)

---

## 📝 License

This sample project is provided for **educational and demonstration purposes**. All libraries used are open-source.
