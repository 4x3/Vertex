"""
GitHub Profile Scraper

Fetches public GitHub user profiles via the GitHub REST API.
Extracts bio, follower counts, repos, company, location, and website.

Features:
- Dynamically uses GITHUB_TOKEN for 5,000 req/hr limit (falls back to 60/hr)
- Extracts hidden emails from recent public commit payloads
- Uses httpx for faster network requests
"""

import os
import httpx
import logging
import re
from typing import Dict, Optional

from app.scrapers.stealth import get_requests_proxies, random_user_agent
from app.scrapers.utils import extract_email

logger = logging.getLogger(__name__)

def _get_hidden_commit_email(username: str, headers: dict, proxies: dict = None) -> Optional[str]:
    """
    Sneaky workaround: Fetch the user's recent events and rip their email 
    from the raw commit payloads if their profile email is hidden.
    """
    events_url = f'https://api.github.com/users/{username}/events/public'
    try:
        with httpx.Client(proxies=proxies, verify=False, timeout=10) as client:
            r = client.get(events_url, headers=headers)
            if r.status_code != 200:
                return None

            events = r.json()
            for event in events:
                if event.get('type') == 'PushEvent':
                    commits = event.get('payload', {}).get('commits', [])
                    for commit in commits:
                        author_email = commit.get('author', {}).get('email', '')
                        # Skip GitHub's automated privacy emails
                        if author_email and 'noreply.github.com' not in author_email:
                            return author_email
    except Exception as e:
        logger.debug(f"Failed to extract commit email for {username}: {e}")
    
    return None

def scrape_profile(username: str) -> Optional[Dict]:
    """Fetch GitHub profile data for a username."""
    url = f'https://api.github.com/users/{username}'

    headers = {
        'Accept': 'application/vnd.github.v3+json',
        'User-Agent': random_user_agent(),
    }

    # Drastically increase rate limits if token is provided
    token = os.environ.get('GITHUB_TOKEN', '').strip()
    if token:
        headers['Authorization'] = f'Bearer {token}'

    proxies = get_requests_proxies()

    try:
        with httpx.Client(proxies=proxies, verify=False, timeout=15) as client:
            r = client.get(url, headers=headers)

            if r.status_code == 404:
                logger.error(f"GitHub user @{username} not found")
                return None

            if r.status_code in (403, 429):
                limit_type = "5,000/hr" if token else "60/hr"
                logger.error(f"GitHub API rate limit exceeded ({limit_type}).")
                raise RuntimeError("Rate limit exceeded")

            if r.status_code != 200:
                logger.error(f"GitHub API error {r.status_code} for @{username}")
                return None

            data = r.json()
            bio = data.get('bio') or ''

            # If the profile is essentially a ghost town, skip it
            if not any([
                data.get('name'), bio, data.get('email'), data.get('blog'),
                data.get('company'), data.get('twitter_username')
            ]):
                logger.info(f"GitHub user @{username} has no usable profile data")
                return None

            # Primary email extraction (Profile -> Bio -> Commit History)
            email = data.get('email') or extract_email(bio)
            if not email:
                email = _get_hidden_commit_email(username, headers, proxies) or ''

            return {
                'username': data.get('login', username),
                'full_name': data.get('name') or '',
                'bio': bio,
                'email': email,
                'company': (data.get('company') or '').lstrip('@'),
                'location': data.get('location') or '',
                'website': data.get('blog') or '',
                'socials': {
                    'twitter': data.get('twitter_username') or ''
                },
                'follower_count': data.get('followers', 0),
                'following_count': data.get('following', 0),
                'public_repos': data.get('public_repos', 0),
                'is_hireable': data.get('hireable') or False,
                'platform': 'github',
                'profile_url': data.get('html_url', f'https://github.com/{username}'),
            }

    except httpx.TimeoutException:
        logger.error(f"Timeout fetching GitHub profile @{username}")
        return None
    except RuntimeError:
        # Re-raise so VertexApp knows to pause/stop due to rate limits
        raise
    except Exception as e:
        logger.error(f"Error fetching GitHub profile @{username}: {e}")
        return None