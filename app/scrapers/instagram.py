"""
Instagram Profile Scraper

Scrapes public Instagram profiles by parsing the HTML page directly.
Uses mobile user agents and rotating proxies to avoid rate limits.

Features:
- Extracts follower/following counts, bio, verification status, etc.
- Tries embedded JSON first, then regex fallbacks
- Automatic retry with proxy rotation on extraction failures
- No authentication required (public profiles only)
"""

import json
import logging
import random
import re
import time
from typing import Dict, Optional

import httpx

from app.scrapers.stealth import make_client
from app.scrapers.utils import extract_email, extract_phone, parse_abbreviated_number

logger = logging.getLogger(__name__)

MOBILE_USER_AGENTS = [
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 12; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36',
]


def _is_page_not_found(html: str) -> bool:
    """Check if the HTML indicates the profile does not exist."""
    not_found_signals = [
        "Page Not Found",
        "Sorry, this page isn",
        "The link you followed may be broken",
        "Profile isn\\'t available",
        "profile may have been removed",
        '"HttpErrorPage"',
    ]
    snippet = html[:10000]
    return any(signal in snippet for signal in not_found_signals)


def scrape_profile_no_login(username: str, max_retries: int = 3) -> Optional[Dict]:
    """Scrape Instagram profile using mobile web HTML parsing (no API)."""
    url = f'https://www.instagram.com/{username}/'

    for attempt in range(max_retries):
        headers = {
            'User-Agent': random.choice(MOBILE_USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
        }

        try:
            with make_client() as client:
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

                if '/accounts/login' in str(r.url) or (
                    'login' in html[:5000].lower() and 'password' in html[:5000].lower()
                ):
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
            err = str(e)
            if '429' in err:
                raise RuntimeError("Rate limited by Instagram (429). Wait a few minutes before scraping again.")
            logger.debug(f"Error scraping @{username}: {e}, attempt {attempt + 1}/{max_retries}")
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            return None

    return None


def _clean_unicode(text: str) -> str:
    try:
        decoded = text.encode('utf-8').decode('unicode_escape')
        return decoded.encode('utf-16', 'surrogatepass').decode('utf-16')
    except (UnicodeDecodeError, UnicodeEncodeError):
        return text.replace('\\/', '/').replace('\\u', '')


def _extract_json_user(html: str, username: str) -> dict:
    """Pull a user object out of the page if Instagram embedded one."""
    needle = f'"username":"{username}"'
    idx = html.lower().find(needle.lower())
    if idx == -1:
        needle = f'"username": "{username}"'
        idx = html.lower().find(needle.lower())
    if idx == -1:
        return {}

    start = html.rfind('{', 0, idx)
    if start == -1:
        return {}

    depth = 0
    for i, char in enumerate(html[start:], start):
        if char == '{':
            depth += 1
        elif char == '}':
            depth -= 1
            if depth == 0:
                try:
                    obj = json.loads(html[start:i + 1])
                except json.JSONDecodeError:
                    return {}
                if str(obj.get('username', '')).lower() != username.lower():
                    return {}
                return obj
    return {}


def _extract_profile_from_html(html: str, username: str) -> Optional[Dict]:
    """Extract profile data from Instagram HTML page."""
    results = {}

    user_obj = _extract_json_user(html, username)
    if user_obj:
        results['username'] = user_obj.get('username', username)
        results['full_name'] = user_obj.get('full_name', '')
        results['biography'] = user_obj.get('biography', '')
        results['follower_count'] = (
            (user_obj.get('edge_followed_by') or {}).get('count')
            or user_obj.get('follower_count')
        )
        results['following_count'] = (
            (user_obj.get('edge_follow') or {}).get('count')
            or user_obj.get('following_count')
        )
        results['media_count'] = (
            (user_obj.get('edge_owner_to_timeline_media') or {}).get('count')
            or user_obj.get('media_count')
        )
        results['is_verified'] = user_obj.get('is_verified', False)
        results['is_private'] = user_obj.get('is_private', False)
        results['is_business'] = user_obj.get('is_business_account', False)
        results['external_url'] = user_obj.get('external_url', '') or ''

    if 'username' not in results:
        username_patterns = [
            r'"username":"([^"]+)"',
            r'"owner":\{"username":"([^"]+)"',
            r'instagram\.com/([a-zA-Z0-9_.]+)/"\s*>',
        ]
        for pattern in username_patterns:
            match = re.search(pattern, html)
            if match and match.group(1).lower() == username.lower():
                results['username'] = match.group(1)
                break

    if 'full_name' not in results or not results.get('full_name'):
        name_patterns = [
            r'"full_name":"([^"]*)"',
            r'"name":"([^"]*)"',
            r'<title>([^(<]+)\s*\(@' + re.escape(username) + r'\)',
        ]
        for pattern in name_patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                results['full_name'] = _clean_unicode(match.group(1).strip())
                break

    if 'biography' not in results:
        bio_patterns = [
            r'"biography":"([^"]*)"',
            r'"bio":"([^"]*)"',
            r'"description":"([^"]*)"',
        ]
        for pattern in bio_patterns:
            match = re.search(pattern, html)
            if match:
                results['biography'] = _clean_unicode(match.group(1))
                break

    if not results.get('follower_count'):
        follower_patterns = [
            r'"follower_count":(\d+)',
            r'"edge_followed_by":\{"count":(\d+)\}',
            r'"userInteractionCount":"?(\d+)"?.*?[Ff]ollow',
            r'followers["\s:]+(\d+)',
        ]
        for pattern in follower_patterns:
            match = re.search(pattern, html)
            if match:
                results['follower_count'] = int(match.group(1))
                break

    if not results.get('following_count'):
        following_patterns = [
            r'"following_count":(\d+)',
            r'"edge_follow":\{"count":(\d+)\}',
        ]
        for pattern in following_patterns:
            match = re.search(pattern, html)
            if match:
                results['following_count'] = int(match.group(1))
                break

    if not results.get('media_count'):
        media_patterns = [
            r'"media_count":(\d+)',
            r'"edge_owner_to_timeline_media":\{"count":(\d+)\}',
        ]
        for pattern in media_patterns:
            match = re.search(pattern, html)
            if match:
                results['media_count'] = int(match.group(1))
                break

    if not all(results.get(k) for k in ('follower_count', 'following_count', 'media_count')):
        meta_patterns = [
            r'content="([\d.,]+[KMB]?)\s*Followers?,\s*([\d.,]+[KMB]?)\s*Following,\s*([\d.,]+[KMB]?)\s*Posts?',
            r'([\d.,]+[KMB]?)\s*Followers?\s*[,·]\s*([\d.,]+[KMB]?)\s*Following\s*[,·]\s*([\d.,]+[KMB]?)\s*Posts?',
        ]
        for pattern in meta_patterns:
            meta_match = re.search(pattern, html, re.IGNORECASE)
            if meta_match:
                results.setdefault('follower_count', parse_abbreviated_number(meta_match.group(1)))
                results.setdefault('following_count', parse_abbreviated_number(meta_match.group(2)))
                results.setdefault('media_count', parse_abbreviated_number(meta_match.group(3)))
                break

    if 'is_verified' not in results:
        match = re.search(r'"is_verified":(true|false)', html)
        if match:
            results['is_verified'] = match.group(1) == 'true'

    if 'is_private' not in results:
        match = re.search(r'"is_private":(true|false)', html)
        if match:
            results['is_private'] = match.group(1) == 'true'

    if 'is_business' not in results:
        match = re.search(r'"is_business_account":(true|false)', html)
        if match:
            results['is_business'] = match.group(1) == 'true'

    if not results.get('external_url'):
        url_patterns = [
            r'"external_url":"([^"]+)"',
            r'"website":"([^"]+)"',
        ]
        for pattern in url_patterns:
            match = re.search(pattern, html)
            if match:
                results['external_url'] = _clean_unicode(match.group(1).replace('\\/', '/'))
                break

    if not results.get('follower_count'):
        return None

    bio = results.get('biography', '') or ''

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
