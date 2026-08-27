"""Vercel entrypoint — re-exports FastAPI app from src/api.py"""
from src.api import app  # noqa: F401
