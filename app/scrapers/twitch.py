"""
Twitch Profile Scraper

Fetches public Twitch user profiles via the Twitch GQL API.
Extracts follower counts, bio, stream status, and social links.

Features:
- Uses public GraphQL endpoint (no auth required)
- Uses httpx with a smart proxy fallback to direct connection
- Extracts social links from channel panels
"""

import httpx
from typing import Dict, Optional
import logging

from app.scrapers.stealth import random_user_agent, get_requests_proxies
from app.scrapers.utils import extract_email, extract_phone

logger = logging.getLogger(__name__)

# Twitch's public, hardcoded client ID for their frontend GQL
CLIENT_ID = 'kimne78kx3ncx6brgo4mv6wki5h1ko'

def scrape_profile(username: str) -> Optional[Dict]:
    """Fetch Twitch channel data using GQL API."""
    username = username.lower().strip()

    headers = {
        'Client-ID': CLIENT_ID,
        'User-Agent': random_user_agent(),
        'Accept': 'application/json',
    }

    # GQL query to fetch the user, their roles, and their linked social media
    query = """
    query {
        user(login: "%s") {
            id
            login
            displayName
            description
            followers {
                totalCount
            }
            roles {
                isPartner
                isAffiliate
            }
            channel {
                socialMedias {
                    name
                    url
                }
            }
        }
    }
    """ % username

    proxies = get_requests_proxies()
    payload = {'query': query}
    
    try:
        # First attempt: Use rotating proxy
        try:
            with httpx.Client(proxies=proxies, verify=False, timeout=20.0) as client:
                r = client.post('https://gql.twitch.tv/gql', headers=headers, json=payload)
        
        except (httpx.ProxyError, httpx.ConnectError) as e:
            if proxies:
                logger.warning(f"Proxy failed for Twitch (@{username}), retrying direct: {e}")
                # Second attempt: Fallback to direct connection if proxy is dead
                # Twitch GQL rarely IP bans, so this is a safe fallback
                with httpx.Client(verify=False, timeout=20.0) as client:
                    r = client.post('https://gql.twitch.tv/gql', headers=headers, json=payload)
            else:
                raise

        if r.status_code != 200:
            logger.error(f"Twitch API error {r.status_code} for {username}")
            return None

        data = r.json()

        if 'errors' in data:
            logger.error(f"Twitch GQL error for {username}: {data['errors']}")
            return None

        user_data = data.get('data', {}).get('user')
        if not user_data:
            logger.debug(f"Twitch user {username} not found")
            return None

        return _format_profile(user_data, username)

    except httpx.TimeoutException:
        logger.error(f"Timeout fetching Twitch profile {username}")
        return None
    except Exception as e:
        logger.error(f"Error fetching/parsing Twitch profile {username}: {e}")
        return None


def _format_profile(data: dict, username: str) -> Dict:
    """Format Twitch API response into the standard Vertex profile format."""
    bio = data.get('description', '') or ''

    followers = data.get('followers', {})
    follower_count = followers.get('totalCount', 0) if followers else 0

    roles = data.get('roles', {}) or {}
    is_partner = roles.get('isPartner', False)
    is_affiliate = roles.get('isAffiliate', False)

    links = []
    channel = data.get('channel', {}) or {}
    social_medias = channel.get('socialMedias', []) or []
    
    for social in social_medias:
        if url := social.get('url', ''):
            links.append(url)

    return {
        'username': data.get('login', username),
        'full_name': data.get('displayName', ''),
        'bio': bio,
        'email': extract_email(bio),
        'phone': extract_phone(bio),
        'follower_count': follower_count,
        'is_partner': is_partner,
        'is_affiliate': is_affiliate,
        'links': links[:5], # Keep top 5 links
        'website': links[0] if links else '',
        'platform': 'twitch',
        'profile_url': f'https://twitch.tv/{data.get("login", username)}',
    }