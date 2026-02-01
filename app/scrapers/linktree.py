"""
Link-in-Bio Scraper

Scrapes public link-in-bio pages across multiple platforms.

Supported platforms:
- Linktree (linktr.ee)
- Stan Store (stan.store)
- Linkr (linkr.bio)
- Bio.link (bio.link)

Features:
- Extracts all social links from profile pages
- Discovers email and phone from linked content
- Uses httpx and rotating proxies to bypass WAFs
- Rips hydrated JSON state directly from Next.js frameworks
"""

import httpx
from typing import Dict, Optional, List
import logging
import re
import json

from app.scrapers.stealth import random_user_agent, get_requests_proxies
from app.scrapers.utils import extract_email as _shared_extract_email

logger = logging.getLogger(__name__)

PLATFORMS = {
    'linktree': 'https://linktr.ee/{username}',
    'stan': 'https://stan.store/{username}',
    'linkr': 'https://linkr.bio/{username}',
    'biolink': 'https://bio.link/{username}',
}

def scrape_linktree(username: str) -> Optional[Dict]:
    return _scrape_profile(username, 'linktree')

def scrape_stan(username: str) -> Optional[Dict]:
    return _scrape_profile(username, 'stan')

def scrape_linkr(username: str) -> Optional[Dict]:
    return _scrape_profile(username, 'linkr')

def scrape_biolink(username: str) -> Optional[Dict]:
    return _scrape_profile(username, 'biolink')

def scrape_all(username: str) -> Optional[Dict]:
    """Try all link-in-bio platforms for a username."""
    for platform in PLATFORMS.keys():
        if result := _scrape_profile(username, platform):
            return result
    return None

def _scrape_profile(username: str, platform: str) -> Optional[Dict]:
    """Fetch link-in-bio profile data using a proxied httpx client."""
    username = username.lstrip('@').strip().lower()

    if platform not in PLATFORMS:
        logger.error(f"Unknown platform: {platform}")
        return None

    url = PLATFORMS[platform].format(username=username)

    headers = {
        'User-Agent': random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Upgrade-Insecure-Requests': '1',
    }

    proxies = get_requests_proxies()

    try:
        with httpx.Client(proxies=proxies, verify=False, follow_redirects=True, timeout=15.0) as client:
            r = client.get(url, headers=headers)

            if r.status_code == 404:
                logger.debug(f"{platform} user {username} not found")
                return None

            if r.status_code != 200:
                logger.error(f"{platform} error {r.status_code} for {username}")
                return None

            html = r.text

            if platform == 'linktree':
                return _parse_linktree(html, username)
            elif platform == 'stan':
                return _parse_stan(html, username)
            else:
                return _parse_generic(html, username, platform)

    except httpx.TimeoutException:
        logger.error(f"Timeout fetching {platform} profile {username}")
        return None
    except Exception as e:
        logger.error(f"Error fetching {platform} profile {username}: {e}")
        return None


def _parse_linktree(html: str, username: str) -> Optional[Dict]:
    """Parse Linktree page by extracting the Next.js hydration payload."""
    data_match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if not data_match:
        return _parse_generic(html, username, 'linktree')

    try:
        data = json.loads(data_match.group(1))
        account = data.get('props', {}).get('pageProps', {}).get('account', {})

        if not account:
            return None

        links = [{'title': link.get('title', ''), 'url': link.get('url', '')} 
                 for link in account.get('links', []) if link.get('url')]

        bio = account.get('description', '')

        return {
            'username': username,
            'full_name': account.get('pageTitle', ''),
            'bio': bio,
            'email': _extract_email_from_links(links) or _extract_email(bio),
            'follower_count': 0,
            'website': _extract_website(links),
            'links': links,
            'link_count': len(links),
            'socials': _extract_socials(links),
            'platform': 'linktree',
            'profile_url': f'https://linktr.ee/{username}',
        }

    except json.JSONDecodeError:
        return _parse_generic(html, username, 'linktree')


def _parse_stan(html: str, username: str) -> Optional[Dict]:
    """Parse Stan.store page utilizing JSON extraction where possible."""
    links = []
    
    # Stan also uses Next.js, check for __NEXT_DATA__ first
    data_match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if data_match:
        try:
            data = json.loads(data_match.group(1))
            store_data = data.get('props', {}).get('pageProps', {}).get('store', {})
            
            full_name = store_data.get('name', '')
            bio = store_data.get('description', '')
            
            for product in store_data.get('products', []):
                if url := product.get('externalUrl'):
                    links.append({'title': product.get('title', ''), 'url': url})
                    
            # If we successfully parsed JSON, return early
            if full_name or links:
                return _build_response_dict(username, full_name, bio, links, 'stan')
        except json.JSONDecodeError:
            pass

    # Fallback to Regex if JSON extraction fails
    link_matches = set(re.findall(r'href="(https?://[^"]+)"', html))
    for url in link_matches:
        if 'stan.store' not in url:
            links.append({'title': '', 'url': url})

    name_match = re.search(r'"name":"([^"]+)"', html)
    full_name = name_match.group(1) if name_match else ''

    bio_match = re.search(r'"description":"([^"]*)"', html)
    bio = bio_match.group(1) if bio_match else ''

    if not full_name and not links:
        return None

    return _build_response_dict(username, full_name, bio, links, 'stan')


def _parse_generic(html: str, username: str, platform: str) -> Optional[Dict]:
    """Generic Regex parser for biolink and linkr pages."""
    links = []
    link_matches = re.findall(r'href="(https?://[^"]+)"', html)

    seen = set()
    for url in link_matches:
        if url not in seen and not any(skip in url for skip in ['favicon', 'static', 'assets', '.css', '.js']):
            links.append({'title': '', 'url': url})
            seen.add(url)

    title_match = re.search(r'<title>([^<]+)</title>', html)
    full_name = title_match.group(1).split('|')[0].strip() if title_match else ''

    bio = ''
    if meta_desc := re.search(r'<meta[^>]*name="description"[^>]*content="([^"]*)"', html, re.IGNORECASE):
        bio = meta_desc.group(1)

    if not links and not full_name:
        return None

    return _build_response_dict(username, full_name, bio, links, platform)


def _build_response_dict(username: str, full_name: str, bio: str, links: List[Dict], platform: str) -> Dict:
    """Helper to standardize the response dictionary."""
    base_url = PLATFORMS.get(platform, '').format(username=username)
    return {
        'username': username,
        'full_name': full_name,
        'bio': bio,
        'email': _extract_email_from_links(links) or _extract_email(bio),
        'follower_count': 0,
        'website': _extract_website(links),
        'links': links[:20],
        'link_count': len(links),
        'socials': _extract_socials(links),
        'platform': platform,
        'profile_url': base_url,
    }


def _extract_socials(links: List[Dict]) -> Dict[str, str]:
    """Extract social media handles from a list of URLs."""
    socials = {}
    patterns = {
        'instagram': r'instagram\.com/([^/?]+)',
        'twitter': r'(?:twitter|x)\.com/([^/?]+)',
        'tiktok': r'tiktok\.com/@?([^/?]+)',
        'youtube': r'youtube\.com/(?:@|c/|channel/)?([^/?]+)',
        'twitch': r'twitch\.tv/([^/?]+)',
        'github': r'github\.com/([^/?]+)',
        'linkedin': r'linkedin\.com/in/([^/?]+)',
        'discord': r'discord\.(?:gg|com/invite)/([^/?]+)',
        'spotify': r'open\.spotify\.com/(?:artist|user)/([^/?]+)',
        'soundcloud': r'soundcloud\.com/([^/?]+)',
    }

    for link in links:
        url = link.get('url', '')
        for platform, pattern in patterns.items():
            if platform not in socials:
                if match := re.search(pattern, url, re.IGNORECASE):
                    handle = match.group(1)
                    if handle.lower() not in ['share', 'intent', 'post']:
                        socials[platform] = handle

    return socials


def _extract_website(links: List[Dict]) -> str:
    """Extract a personal website URL from links, skipping social media."""
    social_domains = {
        'instagram.com', 'twitter.com', 'x.com', 'tiktok.com',
        'youtube.com', 'twitch.tv', 'github.com', 'linkedin.com',
        'discord.gg', 'discord.com', 'spotify.com', 'soundcloud.com',
        'facebook.com', 'pinterest.com', 'snapchat.com', 'reddit.com',
        'stan.store', 'linktr.ee', 'linkr.bio', 'bio.link', 'wa.me'
    }
    
    for link in links:
        url = link.get('url', '').lower()
        if url.startswith('http') and not url.startswith('mailto:'):
            if not any(domain in url for domain in social_domains):
                return link.get('url') # Return original casing
    return ''


def _extract_email_from_links(links: List[Dict]) -> str:
    """Extract email from mailto links."""
    for link in links:
        url = link.get('url', '')
        if url.lower().startswith('mailto:'):
            return url[7:].split('?')[0].strip()
    return ''


_extract_email = _shared_extract_email