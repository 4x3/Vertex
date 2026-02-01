"""
LinkedIn Profile Scraper

Uses an authenticated session cookie (li_at) to query LinkedIn's internal
Voyager API. Extracts profile details, current company, and network stats.

Features:
- Dynamically fetches CSRF tokens (JSESSIONID) required for API calls
- Routes all authenticated traffic through proxies to protect the cookie
- Deep JSON traversal to pull Current Company and Follower Count
"""

import os
import json
import logging
from typing import Dict, Optional

import httpx

from app.scrapers.stealth import random_user_agent, get_requests_proxies
from app.scrapers.utils import extract_email

logger = logging.getLogger(__name__)

def _get_li_cookie() -> Optional[str]:
    """Retrieve and validate the length of the LinkedIn authentication cookie."""
    cookie = os.environ.get('LINKEDIN_COOKIE', '').strip()
    if not cookie:
        return None
    if len(cookie) < 50:
        logger.warning("LinkedIn cookie seems too short - it may be invalid.")
    return cookie

def get_linkedin_session(proxies: dict) -> tuple[Optional[httpx.Client], Optional[str]]:
    """
    Initialize a clean, proxied HTTP client and extract the required CSRF token.
    This replaces the unsafe global caching approach.
    """
    cookie = _get_li_cookie()
    if not cookie:
        return None, None

    # Always wrap the authenticated session in a proxy to prevent IP bans
    client = httpx.Client(proxies=proxies, verify=False, timeout=20.0, follow_redirects=True)
    client.cookies.set('li_at', cookie, domain='.linkedin.com')

    try:
        # Hit the feed to force LinkedIn to generate the JSESSIONID (CSRF token)
        client.get('https://www.linkedin.com/feed/', headers={'User-Agent': random_user_agent()})
        
        csrf = None
        for c in client.cookies.jar:
            if c.name == 'JSESSIONID':
                csrf = c.value.strip('"')
                break
                
        if not csrf:
            logger.error("Could not extract CSRF token from LinkedIn. Cookie may be expired.")
            return client, None
            
        return client, csrf
    except Exception as e:
        logger.error(f"Error initializing LinkedIn session: {e}")
        return client, None

def scrape_linkedin_profile(username: str) -> Optional[Dict]:
    """Scrape LinkedIn profile using the internal Voyager API."""
    proxies = get_requests_proxies()
    client, csrf = get_linkedin_session(proxies)
    
    if not client or not csrf:
        if client: client.close()
        return None

    # Required headers to mimic a genuine frontend API request
    headers = {
        'csrf-token': csrf,
        'Accept': 'application/vnd.linkedin.normalized+json+2.1',
        'x-li-lang': 'en_US',
        'x-restli-protocol-version': '2.0.0',
        'User-Agent': random_user_agent()
    }

    url = f'https://www.linkedin.com/voyager/api/identity/dash/profiles?q=memberIdentity&memberIdentity={username}'

    try:
        resp = client.get(url, headers=headers)
        
        if resp.status_code == 403:
            logger.error(f"Profile {username} is restricted or out of your network reach.")
            return None
        if resp.status_code == 401:
            logger.error("LinkedIn cookie expired. You need to grab a fresh li_at cookie.")
            return None
        if resp.status_code != 200:
            logger.error(f"HTTP {resp.status_code} failed for {username}")
            return None

        data = resp.json()
        
        profile_data = {}
        current_company = ""
        location = ""
        follower_count = 0
        
        # The Voyager API returns a flat array of 'included' entities. 
        # We must iterate and match the internal $type to extract the data.
        for item in data.get('included', []):
            
            # 1. Main Profile Block
            if 'firstName' in item and 'lastName' in item:
                profile_data = item
            
            # 2. Experience Block (Crucial for lead enrichment)
            if item.get('$type') == 'com.linkedin.voyager.dash.profile.Position' and not current_company:
                if not item.get('dateRange', {}).get('end'): # No end date means they currently work here
                    current_company = item.get('companyName', '')
            
            # 3. Location Block
            if item.get('$type') == 'com.linkedin.voyager.dash.profile.Profile':
                location = item.get('locationName', '')
                
            # 4. Network Info Block
            if item.get('$type') == 'com.linkedin.voyager.dash.identity.profile.ProfileNetworkInfo':
                follower_count = item.get('followersCount', 0)

        if not profile_data:
            return None

        summary = profile_data.get('summary', '')
        if not summary:
            multi = profile_data.get('multiLocaleSummary', {})
            summary = multi.get('en_US', '') if isinstance(multi, dict) else ''

        websites = [w['url'] for w in profile_data.get('websites', []) if isinstance(w, dict) and w.get('url')]

        return {
            'platform': 'linkedin',
            'username': profile_data.get('publicIdentifier', username),
            'full_name': f"{profile_data.get('firstName', '')} {profile_data.get('lastName', '')}".strip(),
            'headline': profile_data.get('headline', ''),
            'bio': summary,
            'company': current_company,
            'location': location,
            'follower_count': follower_count,
            'profile_url': f"https://www.linkedin.com/in/{profile_data.get('publicIdentifier', username)}/",
            'is_verified': profile_data.get('showVerificationBadge', False),
            'is_premium': profile_data.get('premium', False),
            'website': websites[0] if websites else '',
            'email': extract_email(summary),
        }

    except Exception as e:
        logger.error(f"Error extracting LinkedIn data for {username}: {e}")
        return None
    finally:
        client.close()