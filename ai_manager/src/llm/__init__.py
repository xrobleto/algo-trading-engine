"""LLM client for AI Investment Manager."""

from .claude_client import ClaudeClient, generate_email_content

__all__ = ["ClaudeClient", "generate_email_content"]
