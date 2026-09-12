"""
GitHub Profile Scraper

Fetches public GitHub user profiles via the GitHub REST API.
Extracts bio, follower counts, repos, company, location, and website.

Features:
- Uses GITHUB_TOKEN when set (5000 req/hour vs 60 unauthenticated)
- Falls back to emails from recent public commits if the profile hides it
- Skips empty profiles (no repos, no bio, no activity)
"""

import logging
import os
from typing import Dict, Optional

import httpx

from app.scrapers.stealth import make_client, random_user_agent
from app.scrapers.utils import extract_email

logger = logging.getLogger(__name__)


def _commit_email(username: str, headers: dict) -> str:
    """Look through recent public events for a real author email."""
    url = f'https://api.github.com/users/{username}/events/public'
    try:
        with make_client(timeout=10) as client:
            r = client.get(url, headers=headers)
            if r.status_code != 200:
                return ''

            for event in r.json():
                if event.get('type') != 'PushEvent':
                    continue
                for commit in event.get('payload', {}).get('commits', []):
                    email = (commit.get('author') or {}).get('email') or ''
                    if email and 'noreply.github.com' not in email.lower():
                        return email
    except Exception as e:
        logger.debug(f"Commit email lookup failed for {username}: {e}")

    return ''


def scrape_profile(username: str) -> Optional[Dict]:
    """Fetch GitHub profile data for a username."""
    url = f'https://api.github.com/users/{username}'

    headers = {
        'Accept': 'application/vnd.github.v3+json',
        'User-Agent': random_user_agent(),
    }

    token = os.environ.get('GITHUB_TOKEN', '').strip()
    if token:
        headers['Authorization'] = f'Bearer {token}'

    try:
        with make_client(timeout=15) as client:
            r = client.get(url, headers=headers)

            if r.status_code == 404:
                logger.error(f"GitHub user @{username} not found")
                return None

            if r.status_code in (403, 429):
                cap = "5000/hour" if token else "60/hour"
                logger.error(f"GitHub API rate limit exceeded ({cap})")
                raise RuntimeError("GitHub API rate limit exceeded")

            if r.status_code != 200:
                logger.error(f"GitHub API error {r.status_code} for @{username}")
                return None

            data = r.json()
            bio = data.get('bio') or ''

            if not any([
                data.get('name'),
                data.get('bio'),
                data.get('email'),
                data.get('blog'),
                data.get('company'),
                data.get('twitter_username'),
            ]):
                logger.info(f"GitHub user @{username} has no profile data")
                return None

            email = data.get('email') or extract_email(bio)
            if not email:
                email = _commit_email(username, headers)

            return {
                'username': data.get('login', username),
                'full_name': data.get('name') or '',
                'bio': bio,
                'email': email or '',
                'company': (data.get('company') or '').lstrip('@'),
                'location': data.get('location') or '',
                'website': data.get('blog') or '',
                'twitter': data.get('twitter_username') or '',
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
        raise
    except Exception as e:
        logger.error(f"Error fetching GitHub profile @{username}: {e}")
        return None


_extract_email = extract_email
