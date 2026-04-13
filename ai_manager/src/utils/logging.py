"""Structured logging utilities for AI Investment Manager."""

import logging
import os
import re
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional


# Patterns for redacting sensitive data
SENSITIVE_PATTERNS = [
    (re.compile(r'api[_-]?key["\s:=]+[A-Za-z0-9_\-]{10,}', re.IGNORECASE), 'api_key=REDACTED'),
    (re.compile(r'secret[_-]?key["\s:=]+[A-Za-z0-9_\-]{10,}', re.IGNORECASE), 'secret_key=REDACTED'),
    (re.compile(r'password["\s:=]+[^\s,}"\']{3,}', re.IGNORECASE), 'password=REDACTED'),
    (re.compile(r'Bearer\s+[A-Za-z0-9_\-\.]{20,}', re.IGNORECASE), 'Bearer REDACTED'),
    (re.compile(r'sk-ant-[A-Za-z0-9_\-]{20,}'), 'sk-ant-REDACTED'),
    (re.compile(r'\b[A-Za-z0-9]{32,}\b'), '[REDACTED_TOKEN]'),
]


class RedactingFormatter(logging.Formatter):
    """Formatter that redacts sensitive information from log messages."""

    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)
        for pattern, replacement in SENSITIVE_PATTERNS:
            message = pattern.sub(replacement, message)
        return message


class ColoredFormatter(RedactingFormatter):
    """Colored formatter for console output."""

    COLORS = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[35m',  # Magenta
    }
    RESET = '\033[0m'

    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)
        color = self.COLORS.get(record.levelname, self.RESET)
        return f"{color}{message}{self.RESET}"


def setup_logging(
    log_dir: str = "logs",
    level: str = "INFO",
    json_format: bool = False,
    max_bytes: int = 10_485_760,
    backup_count: int = 5
) -> None:
    """
    Setup application-wide logging.

    Args:
        log_dir: Directory for log files
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        json_format: Use JSON structured logging
        max_bytes: Max log file size before rotation
        backup_count: Number of backup files to keep
    """
    # Create log directory
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))

    # Clear existing handlers
    root_logger.handlers.clear()

    # Console handler with colors
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    if sys.stdout.isatty():
        console_format = ColoredFormatter(
            '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
            datefmt='%H:%M:%S'
        )
    else:
        console_format = RedactingFormatter(
            '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
            datefmt='%H:%M:%S'
        )
    console_handler.setFormatter(console_format)
    root_logger.addHandler(console_handler)

    # File handler with rotation
    today = datetime.now().strftime("%Y-%m-%d")
    log_file = log_path / f"ai_investment_manager_{today}.log"

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)

    if json_format:
        file_format = RedactingFormatter(
            '{"timestamp": "%(asctime)s", "level": "%(levelname)s", '
            '"logger": "%(name)s", "message": "%(message)s"}',
            datefmt='%Y-%m-%dT%H:%M:%S'
        )
    else:
        file_format = RedactingFormatter(
            '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    file_handler.setFormatter(file_format)
    root_logger.addHandler(file_handler)

    # Suppress noisy third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("anthropic").setLevel(logging.WARNING)
    logging.getLogger("watchdog").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the given name.

    Args:
        name: Logger name (typically __name__ of the module)

    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)


def redact_sensitive(text: str) -> str:
    """
    Redact sensitive information from text.

    Args:
        text: Text that may contain sensitive data

    Returns:
        Text with sensitive data redacted
    """
    result = text
    for pattern, replacement in SENSITIVE_PATTERNS:
        result = pattern.sub(replacement, result)
    return result
