#!/usr/bin/env python3
"""
Convenience entry point: python run.py <command>

Loads .env file if present, then runs the pipeline.
"""
import sys
from pathlib import Path

# Load .env if python-dotenv is available
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
        print(f"Loaded environment from {env_path}")
except ImportError:
    pass

from src.pipeline import main

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run.py <command>")
        print("")
        print("Commands:")
        print("  seed     - Load the 140 existing papers into MongoDB")
        print("  run      - Fetch new papers from PubMed, analyze with OpenAI, store in MongoDB")
        print("  full     - seed + run (first-time setup)")
        print("  rebuild  - Rebuild aggregate collections (categories, drugs, sub-mechanisms)")
        print("  stats    - Show database statistics")
        sys.exit(1)
    main()
