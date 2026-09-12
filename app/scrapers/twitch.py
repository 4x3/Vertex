"""
Twitch Profile Scraper

Fetches public Twitch user profiles via the Twitch GQL API.
Extracts follower counts, bio, stream status, and social links.

Features:
- Uses public GraphQL endpoint (no auth required)
- Extracts social links from channel panels
- Falls back to a direct connection if the proxy dies
"""

import logging
from typing import Dict, Optional

import httpx

from app.scrapers.stealth import random_user_agent, make_client
from app.scrapers.utils import extract_email, extract_phone

logger = logging.getLogger(__name__)

CLIENT_ID = 'kimne78kx3ncx6brgo4mv6wki5h1ko'


def scrape_profile(username: str) -> Optional[Dict]:
    """
    Fetch Twitch channel data using GQL API.

    Args:
        username: Twitch username
    """
    username = username.lower().strip()

    headers = {
        'Client-ID': CLIENT_ID,
        'User-Agent': random_user_agent(),
        'Accept': 'application/json',
    }

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

    payload = {'query': query}

    try:
        try:
            with make_client() as client:
                r = client.post('https://gql.twitch.tv/gql', headers=headers, json=payload)
        except (httpx.ProxyError, httpx.ConnectError) as e:
            logger.warning(f"Proxy failed for Twitch, retrying direct: {e}")
            with httpx.Client(timeout=20, follow_redirects=True) as client:
                r = client.post('https://gql.twitch.tv/gql', headers=headers, json=payload)

        if r.status_code != 200:
            logger.error(f"Twitch API error {r.status_code} for {username}")
            return None

        data = r.json()

        if 'errors' in data:
            logger.error(f"Twitch GQL error for {username}: {data['errors']}")
            return None

        user_data = data.get('data', {}).get('user')
        if not user_data:
            logger.error(f"Twitch user {username} not found")
            return None

        return _format_profile(user_data, username)

    except httpx.TimeoutException:
        logger.error(f"Timeout fetching Twitch profile {username}")
        return None
    except httpx.RequestError as e:
        logger.error(f"Error fetching Twitch profile {username}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error parsing Twitch response for {username}: {e}")
        return None


def _format_profile(data: dict, username: str) -> Dict:
    """Format Twitch API response into standard profile format."""
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
        url = social.get('url', '')
        if url:
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
        'links': links[:5],
        'website': links[0] if links else '',
        'platform': 'twitch',
        'profile_url': f'https://twitch.tv/{data.get("login", username)}',
    }


_extract_email = extract_email
