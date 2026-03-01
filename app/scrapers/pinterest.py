"""
Pinterest Profile Scraper

Scrapes public Pinterest user profiles by parsing HTML page data.
Extracts follower counts, pin counts, bio, and website links.

Features:
- Uses httpx for fast, proxied network requests
- Safe iterative DFS parsing of embedded __PWS_DATA__ JSON
- Extracts email and phone numbers from bio text
- No authentication required
"""

import httpx
from typing import Dict, Optional
import logging
import re
import json

from app.scrapers.stealth import random_user_agent, get_requests_proxies
from app.scrapers.utils import extract_email, extract_phone

logger = logging.getLogger(__name__)

def scrape_profile(username: str) -> Optional[Dict]:
    """Fetch Pinterest profile data."""
    username = username.strip().lower()
    url = f'https://www.pinterest.com/{username}/'

    headers = {
        'User-Agent': random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
    }

    proxies = get_requests_proxies()

    try:
        with httpx.Client(proxies=proxies, verify=False, timeout=15.0, follow_redirects=True) as client:
            r = client.get(url, headers=headers)

            if r.status_code == 404:
                logger.debug(f"Pinterest user {username} not found (404)")
                return None

            if r.status_code != 200:
                logger.error(f"Pinterest error {r.status_code} for {username}")
                return None

            html = r.text

            if 'User not found' in html or "This page isn't available" in html:
                logger.debug(f"Pinterest user {username} account missing/banned")
                return None

            return _extract_profile_data(html, username)

    except httpx.TimeoutException:
        logger.error(f"Timeout fetching Pinterest profile {username}")
        return None
    except Exception as e:
        logger.error(f"Error fetching Pinterest profile {username}: {e}")
        return None


def _extract_profile_data(html: str, username: str) -> Optional[Dict]:
    """Extract profile data from Pinterest HTML using JSON DFS and regex fallbacks."""
    results = {}

    # 1. Attempt to extract from the hydrated PWS JSON state
    pws_match = re.search(r'<script[^>]*id="__PWS_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if pws_match:
        try:
            pws_data = json.loads(pws_match.group(1))
            if user_data := _find_user_in_pws(pws_data, username):
                results.update(user_data)
        except json.JSONDecodeError:
            logger.debug(f"Failed to decode __PWS_DATA__ for {username}")

    # 2. Regex Fallbacks (if JSON is missing or structure changed)
    if 'full_name' not in results:
        if name_match := re.search(r'"full_name":"([^"]+)"', html):
            results['full_name'] = _decode_unicode(name_match.group(1))

    if 'follower_count' not in results:
        if followers_match := re.search(r'"follower_count":(\d+)', html):
            results['follower_count'] = int(followers_match.group(1))

    if 'following_count' not in results:
        if following_match := re.search(r'"following_count":(\d+)', html):
            results['following_count'] = int(following_match.group(1))

    if 'bio' not in results:
        if bio_match := re.search(r'"about":"([^"]*)"', html):
            results['bio'] = _decode_unicode(bio_match.group(1))

    if 'website' not in results:
        if website_match := re.search(r'"website_url":"([^"]+)"', html):
            results['website'] = website_match.group(1).replace('\\/', '/')

    if 'pin_count' not in results:
        if pin_match := re.search(r'"pin_count":(\d+)', html):
            results['pin_count'] = int(pin_match.group(1))

    if 'board_count' not in results:
        if board_match := re.search(r'"board_count":(\d+)', html):
            results['board_count'] = int(board_match.group(1))

    results['verified'] = bool(re.search(r'"is_verified_merchant":true', html))

    # Validation: Ensure we actually scraped a profile
    if 'full_name' not in results and 'follower_count' not in results:
        return None

    bio = results.get('bio', '')

    return {
        'username': username,
        'full_name': results.get('full_name', ''),
        'bio': bio,
        'email': extract_email(bio),
        'phone': extract_phone(bio),
        'website': results.get('website', ''),
        'follower_count': results.get('follower_count', 0),
        'following_count': results.get('following_count', 0),
        'pin_count': results.get('pin_count', 0),
        'board_count': results.get('board_count', 0),
        'verified': results.get('verified', False),
        'platform': 'pinterest',
        'profile_url': f'https://pinterest.com/{username}/',
    }


def _find_user_in_pws(data: dict, username: str) -> Optional[Dict]:
    """
    Iterative Depth-First Search (DFS) to find user data in the massive PWS JSON.
    Replaces recursive approach to prevent RecursionError and save memory.
    """
    stack = [data]
    username_lower = username.lower()

    while stack:
        current = stack.pop()
        
        if isinstance(current, dict):
            # Check if this node is the target user object
            if current.get('username', '').lower() == username_lower and 'follower_count' in current:
                return {
                    'full_name': current.get('full_name', ''),
                    'bio': current.get('about', ''),
                    'follower_count': current.get('follower_count', 0),
                    'following_count': current.get('following_count', 0),
                    'website': current.get('website_url', ''),
                    'pin_count': current.get('pin_count', 0),
                    'board_count': current.get('board_count', 0),
                }
            
            # Push nested dicts/lists to stack
            for value in current.values():
                if isinstance(value, (dict, list)):
                    stack.append(value)
                    
        elif isinstance(current, list):
            # Push list items to stack
            for item in current:
                if isinstance(item, (dict, list)):
                    stack.append(item)

    return None


def _decode_unicode(text: str) -> str:
    """Safely decode escaped unicode characters."""
    try:
        return text.encode('utf-8').decode('unicode_escape')
    except (UnicodeDecodeError, UnicodeEncodeError):
        return text