
"""
retry.py - Exponential backoff retry utilities
"""
import time
import random
import functools
from typing import Callable, Tuple, Type

from utils.logger import get_logger

logger = get_logger(__name__)


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    jitter: bool = True,
):
    """
    Decorator that retries the wrapped function with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts.
        base_delay: Base delay in seconds (doubles each retry).
        exceptions: Tuple of exception types to catch and retry on.
        jitter: Add random jitter to delay to avoid thundering herd.
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    if attempt == max_retries:
                        logger.error(f"[retry] {func.__name__} failed after {max_retries} retries: {e}")
                        raise
                    delay = base_delay * (2 ** attempt)
                    if jitter:
                        delay += random.uniform(0, delay * 0.3)
                    logger.warning(
                        f"[retry] {func.__name__} attempt {attempt + 1}/{max_retries} failed: {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    time.sleep(delay)
            raise last_exc
        return wrapper
    return decorator


def random_delay(min_secs: float = 2.0, max_secs: float = 7.0):
    """Sleep for a random duration between min and max seconds."""
    delay = random.uniform(min_secs, max_secs)
    logger.debug(f"[delay] Sleeping {delay:.2f}s")
    time.sleep(delay)
