"""
TikTok Profile Scraper

Scrapes public TikTok user profiles by parsing the rehydration JSON data
embedded in the page HTML.

Features:
- Extracts follower/following/like counts, bio, and verification status
- Clean JSON extraction instead of messy HTML parsing
- Properly rotates proxies per request (no global client leaking)
"""

import json
import logging
import re
from typing import Dict, Optional

import httpx

from app.scrapers.stealth import random_user_agent, get_requests_proxies
from app.scrapers.utils import extract_email, extract_phone

logger = logging.getLogger(__name__)

def _build_headers() -> dict:
    return {
        'User-Agent': random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-Dest': 'document',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Upgrade-Insecure-Requests': '1',
    }

def scrape_tiktok_profile(username: str) -> Optional[Dict]:
    """Scrape TikTok profile. Returns profile dict or None."""
    username = username.lstrip('@').strip()
    url = f'https://www.tiktok.com/@{username}'
    
    proxies = get_requests_proxies()

    try:
        # Create a fresh client for every request to ensure proper proxy rotation
        with httpx.Client(proxies=proxies, verify=False, timeout=20.0, follow_redirects=True) as client:
            resp = client.get(url, headers=_build_headers())
            
            if resp.status_code == 404:
                logger.debug(f"TikTok user @{username} not found")
                return None
                
            resp.raise_for_status()
            html = resp.text

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error for @{username}: {e.response.status_code}")
        return None
    except httpx.RequestError as e:
        logger.error(f"Request error for @{username}: {e}")
        return None

    # Extract the massive JSON payload TikTok uses to hydrate the page
    match = re.search(
        r'<script\s+id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>(.*?)</script>',
        html,
        re.DOTALL,
    )
    
    if not match:
        # Sometimes TikTok serves a captcha or radically different layout to bots
        logger.warning(f"Could not find rehydration data for @{username}. Proxy might be flagged.")
        return None

    try:
        data = json.loads(match.group(1))
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error for @{username}: {e}")
        return None

    try:
        # Navigate the JSON tree to find the user profile block
        user_detail = data.get('__DEFAULT_SCOPE__', {}).get('webapp.user-detail', {})
        if not user_detail:
            # TikTok occasionally changes the scope name
            logger.debug(f"TikTok JSON structure changed or missing for @{username}")
            return None
            
        user_info = user_detail.get('userInfo', {})
        user = user_info.get('user', {})
        stats = user_info.get('stats', {})
    except (KeyError, TypeError) as e:
        logger.error(f"Unexpected JSON structure for @{username}: {e}")
        return None

    if not user:
        return None

    bio = user.get('signature', '')

    return {
        'platform': 'tiktok',
        'username': user.get('uniqueId', username),
        'full_name': user.get('nickname', ''),
        'bio': bio,
        'email': extract_email(bio),
        'phone': extract_phone(bio),
        'profile_url': url,
        'is_verified': user.get('verified', False),
        'follower_count': stats.get('followerCount', 0),
        'following_count': stats.get('followingCount', 0),
        'likes_count': stats.get('heartCount', 0),
        'video_count': stats.get('videoCount', 0),
    }