"""
settings.py - Application configuration and environment variable loading.

Loads API keys and model settings from a .env file so that
sensitive values are never hard-coded in source code.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file (if present)
load_dotenv()

# OpenAI API key — required for LLM calls
OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")

# Model to use for all LLM calls (GPT-4o-mini is fast and cost-effective)
MODEL_NAME: str = os.getenv("MODEL_NAME", "gpt-4o-mini")

# Temperature controls randomness: 0 = deterministic, 1 = creative
TEMPERATURE: float = float(os.getenv("TEMPERATURE", "0"))
