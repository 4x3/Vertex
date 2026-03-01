"""
Stealth & Proxy Management

Handles user-agent rotation, delay randomization, and proxy routing
to bypass anti-bot protections.
"""

import random
import time
import os
import logging
from typing import Optional, Callable
from functools import wraps

import httpx

logger = logging.getLogger(__name__)

# Modern, high-market-share user agents to prevent fingerprinting
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:124.0) Gecko/20100101 Firefox/124.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0',
]

_free_proxy_cache = []
_free_proxy_last_fetch = 0
_FREE_PROXY_TTL = 300


def random_user_agent() -> str:
    """Return a random modern browser user agent."""
    return random.choice(USER_AGENTS)


def random_delay(min_seconds: float = 1.5, max_seconds: float = 4.5):
    """Sleep for a random duration to mimic human behavior."""
    time.sleep(random.uniform(min_seconds, max_seconds))


def _fetch_free_proxies() -> list:
    """Fetch free proxies from fp library, using caching to avoid spamming."""
    global _free_proxy_cache, _free_proxy_last_fetch

    if _free_proxy_cache and (time.time() - _free_proxy_last_fetch) < _FREE_PROXY_TTL:
        return _free_proxy_cache

    try:
        from fp.fp import FreeProxy
        proxies = []
        # Attempt to grab a small pool of working free proxies
        for _ in range(5):
            try:
                p = FreeProxy(timeout=1, rand=True, anonym=True).get()
                if p:
                    proxies.append(p)
            except Exception:
                continue

        if proxies:
            _free_proxy_cache = proxies
            _free_proxy_last_fetch = time.time()
            logger.info(f"Fetched {len(proxies)} free proxies")
            return proxies
    except ImportError:
        logger.debug("fp-freeproxy not installed. Skipping free proxies.")
    except Exception as e:
        logger.debug(f"Free proxy fetch failed: {e}")

    return []


def get_proxy() -> Optional[str]:
    """Retrieve the current configured proxy string."""
    proxy = os.environ.get('VERTEX_PROXY')
    if proxy:
        return proxy

    proxy_file = os.environ.get('VERTEX_PROXY_FILE')
    if proxy_file and os.path.exists(proxy_file):
        with open(proxy_file, 'r') as f:
            proxies = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        if proxies:
            return random.choice(proxies)

    if os.environ.get('VERTEX_FREE_PROXY', '').lower() in ('1', 'true', 'yes'):
        free_proxies = _fetch_free_proxies()
        if free_proxies:
            return random.choice(free_proxies)

    return None


def get_httpx_proxy() -> Optional[str]:
    """Format proxy specifically for httpx string configurations."""
    proxy = get_proxy()
    if not proxy:
        return None
    if not proxy.startswith('http'):
        proxy = f'http://{proxy}'
    return proxy


def get_requests_proxies() -> Optional[dict]:
    """Format proxy as a dictionary for httpx/requests proxies arguments."""
    proxy = get_proxy()
    if not proxy:
        return None
    if not proxy.startswith('http'):
        proxy = f'http://{proxy}'
    return {'http://': proxy, 'https://': proxy}


def proxy_status() -> str:
    """Return a string indicating the current proxy mode."""
    if os.environ.get('VERTEX_PROXY'):
        return 'custom'
    if os.environ.get('VERTEX_PROXY_FILE'):
        return 'file'
    if os.environ.get('VERTEX_FREE_PROXY', '').lower() in ('1', 'true', 'yes'):
        return 'free'
    return 'none'


def test_proxy() -> bool:
    """Test if current proxy is actually routing traffic successfully."""
    proxy = get_requests_proxies()
    if not proxy:
        return True

    try:
        with httpx.Client(proxies=proxy, timeout=10) as client:
            r = client.get('https://httpbin.org/ip')
            return r.status_code == 200
    except Exception as e:
        logger.debug(f"Proxy test failed: {e}")
        return False


def retry_request(max_retries: int = 3, delay: float = 2.0):
    """
    Decorator to retry failed network requests.
    Updated to catch modern httpx exceptions instead of requests exceptions.
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except httpx.ProxyError as e:
                    last_error = e
                    logger.warning(f"Proxy error (attempt {attempt + 1}/{max_retries})")
                except httpx.TimeoutException as e:
                    last_error = e
                    logger.warning(f"Timeout (attempt {attempt + 1}/{max_retries})")
                except httpx.ConnectError as e:
                    last_error = e
                    logger.warning(f"Connection error (attempt {attempt + 1}/{max_retries})")
                except httpx.HTTPError as e:
                    last_error = e
                    logger.warning(f"HTTP error (attempt {attempt + 1}/{max_retries})")
                
                if attempt < max_retries - 1:
                    time.sleep(delay)
                    
            logger.error(f"All {max_retries} attempts failed: {last_error}")
            return None
        return wrapper
    return decorator