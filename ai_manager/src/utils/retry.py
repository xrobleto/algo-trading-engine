"""Retry and rate limiting utilities for AI Investment Manager."""

import functools
import threading
import time
from collections import deque
from datetime import datetime, timedelta
from typing import Callable, TypeVar, Optional, Type, Tuple, Any

from .logging import get_logger

logger = get_logger(__name__)

T = TypeVar('T')


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    on_retry: Optional[Callable[[Exception, int], None]] = None
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator for retrying functions with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay between retries (seconds)
        max_delay: Maximum delay between retries (seconds)
        exponential_base: Base for exponential backoff
        exceptions: Tuple of exception types to catch and retry
        on_retry: Optional callback function(exception, attempt_number)

    Returns:
        Decorated function with retry logic
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e

                    if attempt == max_retries:
                        logger.error(
                            f"{func.__name__} failed after {max_retries + 1} attempts: {e}"
                        )
                        raise

                    # Calculate delay with exponential backoff
                    delay = min(
                        base_delay * (exponential_base ** attempt),
                        max_delay
                    )

                    logger.warning(
                        f"{func.__name__} attempt {attempt + 1}/{max_retries + 1} failed: {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )

                    if on_retry:
                        on_retry(e, attempt + 1)

                    time.sleep(delay)

            # Should never reach here, but just in case
            if last_exception:
                raise last_exception
            raise RuntimeError(f"{func.__name__} failed unexpectedly")

        return wrapper
    return decorator


class RateLimiter:
    """
    Rate limiter using sliding window algorithm.

    Thread-safe implementation for limiting API calls.
    """

    def __init__(self, calls_per_minute: int, name: str = "default"):
        """
        Initialize rate limiter.

        Args:
            calls_per_minute: Maximum calls allowed per minute
            name: Name for logging purposes
        """
        self.calls_per_minute = calls_per_minute
        self.name = name
        self.window_seconds = 60
        self._timestamps: deque = deque()
        self._lock = threading.Lock()

    def acquire(self, block: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Acquire a rate limit slot.

        Args:
            block: If True, block until slot is available
            timeout: Maximum time to wait (seconds). None = wait forever.

        Returns:
            True if slot acquired, False if timeout reached
        """
        start_time = time.time()

        while True:
            with self._lock:
                now = time.time()
                cutoff = now - self.window_seconds

                # Remove timestamps outside the window
                while self._timestamps and self._timestamps[0] < cutoff:
                    self._timestamps.popleft()

                # Check if we can make a call
                if len(self._timestamps) < self.calls_per_minute:
                    self._timestamps.append(now)
                    return True

                # Calculate wait time until oldest timestamp expires
                wait_time = self._timestamps[0] - cutoff

            if not block:
                return False

            # Check timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    logger.warning(
                        f"RateLimiter[{self.name}]: Timeout after {timeout:.1f}s"
                    )
                    return False
                wait_time = min(wait_time, timeout - elapsed)

            logger.debug(
                f"RateLimiter[{self.name}]: Rate limit hit, waiting {wait_time:.1f}s"
            )
            time.sleep(min(wait_time + 0.1, 5.0))

    def __enter__(self) -> 'RateLimiter':
        """Context manager entry."""
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        pass

    @property
    def current_usage(self) -> int:
        """Get current number of calls in the window."""
        with self._lock:
            now = time.time()
            cutoff = now - self.window_seconds
            while self._timestamps and self._timestamps[0] < cutoff:
                self._timestamps.popleft()
            return len(self._timestamps)

    @property
    def available_calls(self) -> int:
        """Get number of calls available in current window."""
        return max(0, self.calls_per_minute - self.current_usage)


class CachedResult:
    """Wrapper for cached results with expiration."""

    def __init__(self, value: Any, ttl_seconds: int):
        self.value = value
        self.expires_at = datetime.now() + timedelta(seconds=ttl_seconds)

    @property
    def is_expired(self) -> bool:
        return datetime.now() >= self.expires_at


class TTLCache:
    """
    Simple thread-safe TTL cache.

    Useful for caching API responses to reduce redundant calls.
    """

    def __init__(self, default_ttl_seconds: int = 300):
        """
        Initialize cache.

        Args:
            default_ttl_seconds: Default time-to-live for entries (5 minutes)
        """
        self.default_ttl = default_ttl_seconds
        self._cache: dict = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found/expired
        """
        with self._lock:
            if key not in self._cache:
                return None

            cached = self._cache[key]
            if cached.is_expired:
                del self._cache[key]
                return None

            return cached.value

    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        """
        Set value in cache.

        Args:
            key: Cache key
            value: Value to cache
            ttl_seconds: Optional TTL override
        """
        ttl = ttl_seconds if ttl_seconds is not None else self.default_ttl
        with self._lock:
            self._cache[key] = CachedResult(value, ttl)

    def delete(self, key: str) -> bool:
        """
        Delete value from cache.

        Args:
            key: Cache key

        Returns:
            True if key existed
        """
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def clear(self) -> None:
        """Clear all cached values."""
        with self._lock:
            self._cache.clear()

    def cleanup_expired(self) -> int:
        """
        Remove all expired entries.

        Returns:
            Number of entries removed
        """
        removed = 0
        with self._lock:
            expired_keys = [
                k for k, v in self._cache.items() if v.is_expired
            ]
            for key in expired_keys:
                del self._cache[key]
                removed += 1
        return removed


def cached(ttl_seconds: int = 300, key_func: Optional[Callable[..., str]] = None):
    """
    Decorator for caching function results.

    Args:
        ttl_seconds: Time-to-live for cached results
        key_func: Optional function to generate cache key from args

    Returns:
        Decorated function with caching
    """
    cache = TTLCache(ttl_seconds)

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            # Generate cache key
            if key_func:
                key = key_func(*args, **kwargs)
            else:
                key = f"{func.__name__}:{str(args)}:{str(sorted(kwargs.items()))}"

            # Check cache
            cached_value = cache.get(key)
            if cached_value is not None:
                logger.debug(f"Cache hit for {func.__name__}")
                return cached_value

            # Call function and cache result
            result = func(*args, **kwargs)
            cache.set(key, result)
            return result

        # Expose cache for manual control
        wrapper.cache = cache  # type: ignore
        return wrapper

    return decorator
