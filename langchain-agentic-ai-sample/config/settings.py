"""
settings.py - Application configuration management.

Loads environment variables from a .env file and provides
a central place to manage all configuration values.
"""

import os
from dotenv import load_dotenv

# Load variables from .env file into the environment
load_dotenv()

# OpenAI API key - required for the LLM (Large Language Model)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# Model name - gpt-4o-mini is fast, cheap, and good enough for demos
MODEL_NAME = "gpt-4o-mini"

# Temperature controls randomness: 0 = deterministic, 1 = creative
TEMPERATURE = 0.0
