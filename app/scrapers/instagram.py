"""
Instagram Profile Scraper

Scrapes public Instagram profiles by parsing the HTML page directly.
Uses mobile user agents, advanced stealth headers, and rotating proxies.

Features:
- Extracts follower/following counts, bio, verification status, etc.
- Prioritizes embedded JSON extraction with optimized regex fallbacks
- Fast httpx networking with automatic retries and proxy rotation
- No authentication required (public profiles only)
"""

import httpx
from typing import Dict, Optional
import logging
import re
import random
import time
import json

from app.scrapers.stealth import get_requests_proxies
from app.scrapers.utils import extract_email, extract_phone, parse_abbreviated_number

logger = logging.getLogger(__name__)

# Modern, high-market-share mobile user agents to blend in
MOBILE_USER_AGENTS = [
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 13; Pixel 7 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Mobile Safari/537.36',
]

def _is_page_not_found(html: str) -> bool:
    """Check if the HTML indicates the profile does not exist or is banned."""
    not_found_signals = [
        "Page Not Found",
        "Sorry, this page isn",
        "The link you followed may be broken",
        "Profile isn\\'t available",
        "profile may have been removed",
        '"HttpErrorPage"',
    ]
    # Check only the first 10k characters to save memory
    snippet = html[:10000]
    return any(signal in snippet for signal in not_found_signals)

def scrape_profile_no_login(username: str, max_retries: int = 3) -> Optional[Dict]:
    """Scrape Instagram profile using mobile web HTML parsing (no API)."""
    url = f'https://www.instagram.com/{username}/'

    for attempt in range(max_retries):
        proxies = get_requests_proxies()
        
        # Advanced stealth headers to mimic a real mobile browser
        headers = {
            'User-Agent': random.choice(MOBILE_USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
        }

        try:
            with httpx.Client(proxies=proxies, verify=False, timeout=20.0, follow_redirects=True) as client:
                r = client.get(url, headers=headers)

                if r.status_code == 404:
                    return None

                if r.status_code == 429:
                    raise RuntimeError("Rate limited by Instagram (429). Wait a few minutes before scraping again.")

                if r.status_code != 200:
                    logger.debug(f"HTTP {r.status_code} for @{username}, attempt {attempt + 1}/{max_retries}")
                    if attempt < max_retries - 1:
                        time.sleep(random.uniform(1.5, 3.0))
                        continue
                    return None

                html = r.text

                if _is_page_not_found(html):
                    return None

                # Detect login wall redirects
                if '/accounts/login' in str(r.url) or ('login' in html[:5000].lower() and 'password' in html[:5000].lower()):
                    logger.debug(f"Hit Instagram login wall for @{username}")
                    if attempt < max_retries - 1:
                        time.sleep(random.uniform(2.0, 4.0))
                        continue
                    return None

                data = _extract_profile_from_html(html, username)

                if data:
                    return data

                if attempt < max_retries - 1:
                    logger.debug(f"Extraction failed for @{username}, retrying ({attempt + 1}/{max_retries})")
                    time.sleep(random.uniform(1.0, 2.5))
                    continue

                return None

        except httpx.TimeoutException:
            logger.debug(f"Timeout for @{username}, attempt {attempt + 1}/{max_retries}")
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            return None
        except RuntimeError:
            raise
        except Exception as e:
            if '429' in str(e):
                raise RuntimeError("Rate limited by Instagram (429). Wait a few minutes before scraping again.")
            logger.debug(f"Error scraping @{username}: {e}, attempt {attempt + 1}/{max_retries}")
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            return None

    return None

def _clean_unicode(text: str) -> str:
    """Helper to cleanly decode Instagram's escaped unicode strings."""
    try:
        # First attempt standard decode
        decoded = text.encode('utf-8').decode('unicode_escape')
        return decoded.encode('utf-16', 'surrogatepass').decode('utf-16')
    except Exception:
        return text.replace('\\/', '/').replace('\\u', '')

def _extract_profile_from_html(html: str, username: str) -> Optional[Dict]:
    """Extract profile data from Instagram HTML page."""
    results = {}

    # 1. Attempt to extract from embedded JSON first (Faster and more accurate if present)
    json_match = re.search(r'"user":\s*(\{.*?"username":"' + re.escape(username) + r'".*?\})', html, re.IGNORECASE)
    if json_match:
        try:
            # We found a potential user object block, try to parse it safely
            user_data_str = json_match.group(1)
            # Find the closing brace of the user object to make it valid JSON
            brace_count = 0
            end_idx = -1
            for i, char in enumerate(user_data_str):
                if char == '{': brace_count += 1
                elif char == '}': brace_count -= 1
                if brace_count == 0:
                    end_idx = i + 1
                    break
            
            if end_idx != -1:
                user_obj = json.loads(user_data_str[:end_idx])
                results['username'] = user_obj.get('username', username)
                results['full_name'] = user_obj.get('full_name', '')
                results['biography'] = user_obj.get('biography', '')
                results['follower_count'] = user_obj.get('edge_followed_by', {}).get('count') or user_obj.get('follower_count')
                results['following_count'] = user_obj.get('edge_follow', {}).get('count') or user_obj.get('following_count')
                results['media_count'] = user_obj.get('edge_owner_to_timeline_media', {}).get('count') or user_obj.get('media_count')
                results['is_verified'] = user_obj.get('is_verified', False)
                results['is_private'] = user_obj.get('is_private', False)
                results['is_business'] = user_obj.get('is_business_account', False)
                results['external_url'] = user_obj.get('external_url', '')
        except Exception:
            pass # Fallback to regex if JSON parsing fails

    # 2. Regex Fallbacks (If JSON wasn't found or was incomplete)
    
    if 'username' not in results:
        for pattern in [r'"username":"([^"]+)"', r'"owner":\{"username":"([^"]+)"']:
            if match := re.search(pattern, html):
                if match.group(1).lower() == username.lower():
                    results['username'] = match.group(1)
                    break

    if 'full_name' not in results:
        for pattern in [r'"full_name":"([^"]*)"', r'<title>([^(<]+)\s*\(@']:
            if match := re.search(pattern, html, re.IGNORECASE):
                results['full_name'] = _clean_unicode(match.group(1).strip())
                break

    if 'biography' not in results:
        for pattern in [r'"biography":"([^"]*)"', r'"description":"([^"]*)"']:
            if match := re.search(pattern, html):
                results['biography'] = _clean_unicode(match.group(1))
                break

    if 'follower_count' not in results:
        for pattern in [r'"follower_count":(\d+)', r'"edge_followed_by":\{"count":(\d+)\}']:
            if match := re.search(pattern, html):
                results['follower_count'] = int(match.group(1))
                break

    if 'following_count' not in results:
        for pattern in [r'"following_count":(\d+)', r'"edge_follow":\{"count":(\d+)\}']:
            if match := re.search(pattern, html):
                results['following_count'] = int(match.group(1))
                break

    if 'media_count' not in results:
        if match := re.search(r'"edge_owner_to_timeline_media":\{"count":(\d+)\}', html):
            results['media_count'] = int(match.group(1))

    # Fallback for stats using meta tags
    if not all(k in results for k in ['follower_count', 'following_count', 'media_count']):
        meta_patterns = [
            r'content="([\d.,]+[KMB]?)\s*Followers?,\s*([\d.,]+[KMB]?)\s*Following,\s*([\d.,]+[KMB]?)\s*Posts?',
            r'([\d.,]+[KMB]?)\s*Followers?\s*[,·]\s*([\d.,]+[KMB]?)\s*Following\s*[,·]\s*([\d.,]+[KMB]?)\s*Posts?'
        ]
        for pattern in meta_patterns:
            if meta_match := re.search(pattern, html, re.IGNORECASE):
                results.setdefault('follower_count', parse_abbreviated_number(meta_match.group(1)))
                results.setdefault('following_count', parse_abbreviated_number(meta_match.group(2)))
                results.setdefault('media_count', parse_abbreviated_number(meta_match.group(3)))
                break

    if 'is_verified' not in results:
        if match := re.search(r'"is_verified":(true|false)', html):
            results['is_verified'] = match.group(1) == 'true'

    if 'is_private' not in results:
        if match := re.search(r'"is_private":(true|false)', html):
            results['is_private'] = match.group(1) == 'true'

    if 'external_url' not in results:
        if match := re.search(r'"external_url":"([^"]+)"', html):
            results['external_url'] = _clean_unicode(match.group(1))

    # Validation: If we couldn't even find a follower count, the page layout completely changed or blocked us
    if not results.get('follower_count'):
        return None

    bio = results.get('biography', '')

    return {
        'username': results.get('username', username),
        'full_name': results.get('full_name', ''),
        'bio': bio,
        'follower_count': results.get('follower_count', 0),
        'following_count': results.get('following_count', 0),
        'post_count': results.get('media_count', 0),
        'is_verified': results.get('is_verified', False),
        'is_private': results.get('is_private', False),
        'is_business': results.get('is_business', False),
        'website': results.get('external_url', ''),
        'email': extract_email(bio),
        'phone': extract_phone(bio),
        'platform': 'instagram',
        'profile_url': f'https://www.instagram.com/{username}/',
    }

_parse_abbreviated_number = parse_abbreviated_number
_extract_email = extract_email
_extract_phone = extract_phone