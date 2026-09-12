"""
Pinterest Profile Scraper

Scrapes public Pinterest user profiles by parsing HTML page data.
Extracts follower counts, pin counts, bio, and website links.

Features:
- Parses embedded JSON from page source
- No authentication required
- Extracts email and phone from bio text
"""

import json
import logging
import re
from typing import Dict, Optional

import httpx

from app.scrapers.stealth import random_user_agent, make_client
from app.scrapers.utils import extract_email, extract_phone

logger = logging.getLogger(__name__)


def scrape_profile(username: str) -> Optional[Dict]:
    """
    Fetch Pinterest profile data.

    Args:
        username: Pinterest username
    """
    username = username.strip().lower()
    url = f'https://www.pinterest.com/{username}/'

    headers = {
        'User-Agent': random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    try:
        with make_client() as client:
            r = client.get(url, headers=headers)

            if r.status_code == 404:
                logger.error(f"Pinterest user {username} not found")
                return None

            if r.status_code != 200:
                logger.error(f"Pinterest error {r.status_code} for {username}")
                return None

            html = r.text

            if 'User not found' in html or "This page isn't available" in html:
                logger.error(f"Pinterest user {username} not found")
                return None

            return _extract_profile_data(html, username)

    except httpx.TimeoutException:
        logger.error(f"Timeout fetching Pinterest profile {username}")
        return None
    except httpx.RequestError as e:
        logger.error(f"Error fetching Pinterest profile {username}: {e}")
        return None


def _extract_profile_data(html: str, username: str) -> Optional[Dict]:
    """Extract profile data from Pinterest HTML."""
    results = {}

    pws_match = re.search(r'<script[^>]*id="__PWS_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if pws_match:
        try:
            pws_data = json.loads(pws_match.group(1))
            user_data = _find_user_in_pws(pws_data, username)
            if user_data:
                results.update(user_data)
        except json.JSONDecodeError:
            pass

    if 'full_name' not in results:
        name_match = re.search(r'"full_name":"([^"]+)"', html)
        if name_match:
            results['full_name'] = _decode_unicode(name_match.group(1))

    if 'follower_count' not in results:
        followers_match = re.search(r'"follower_count":(\d+)', html)
        if followers_match:
            results['follower_count'] = int(followers_match.group(1))

    if 'following_count' not in results:
        following_match = re.search(r'"following_count":(\d+)', html)
        if following_match:
            results['following_count'] = int(following_match.group(1))

    if 'bio' not in results:
        bio_match = re.search(r'"about":"([^"]*)"', html)
        if bio_match:
            results['bio'] = _decode_unicode(bio_match.group(1))

    if 'website' not in results:
        website_match = re.search(r'"website_url":"([^"]+)"', html)
        if website_match:
            results['website'] = website_match.group(1).replace('\\/', '/')

    if 'pin_count' not in results:
        pin_match = re.search(r'"pin_count":(\d+)', html)
        if pin_match:
            results['pin_count'] = int(pin_match.group(1))

    if 'board_count' not in results:
        board_match = re.search(r'"board_count":(\d+)', html)
        if board_match:
            results['board_count'] = int(board_match.group(1))

    verified_match = re.search(r'"is_verified_merchant":true', html)
    results['verified'] = bool(verified_match)

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
    """Walk the PWS JSON without recursing into it."""
    stack = [data]
    username_lower = username.lower()

    while stack:
        current = stack.pop()

        if isinstance(current, dict):
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
            for value in current.values():
                if isinstance(value, (dict, list)):
                    stack.append(value)

        elif isinstance(current, list):
            for item in current:
                if isinstance(item, (dict, list)):
                    stack.append(item)

    return None


def _decode_unicode(text: str) -> str:
    try:
        return text.encode('utf-8').decode('unicode_escape')
    except (UnicodeDecodeError, UnicodeEncodeError):
        return text


_extract_email = extract_email
