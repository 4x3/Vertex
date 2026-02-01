"""
YouTube Channel Scraper

Scrapes public YouTube channel pages by parsing the embedded ytInitialData JSON.
Extracts subscriber counts, video counts, channel description, and links.

Features:
- Parses ytInitialData JSON for highly accurate data extraction
- Uses httpx and rotating proxies for thread-safe bulk scraping
- Handles @handle, /channel/ ID, and custom URL formats
- No API key required
"""

import httpx
from typing import Dict, Optional
import logging
import re
import json
from urllib.parse import unquote

from app.scrapers.stealth import random_user_agent, get_requests_proxies
from app.scrapers.utils import extract_email, extract_phone, parse_abbreviated_number

logger = logging.getLogger(__name__)

def scrape_channel(channel_identifier: str) -> Optional[Dict]:
    """Fetch YouTube channel data."""
    channel_identifier = channel_identifier.strip()
    
    if channel_identifier.startswith('@'):
        url = f'https://www.youtube.com/{channel_identifier}'
    elif channel_identifier.startswith('UC') and len(channel_identifier) == 24:
        url = f'https://www.youtube.com/channel/{channel_identifier}'
    else:
        url = f'https://www.youtube.com/@{channel_identifier}'

    headers = {
        'User-Agent': random_user_agent(),
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Upgrade-Insecure-Requests': '1',
    }
    
    # YouTube's consent cookie bypasses the EU consent interstitial wall
    cookies = {
        'CONSENT': 'PENDING+999',
    }

    proxies = get_requests_proxies()

    try:
        with httpx.Client(proxies=proxies, verify=False, timeout=20.0, follow_redirects=True) as client:
            r = client.get(url, headers=headers, cookies=cookies)

            if r.status_code == 404:
                logger.debug(f"YouTube channel {channel_identifier} not found")
                return None

            if r.status_code != 200:
                logger.error(f"YouTube error {r.status_code} for {channel_identifier}")
                return None

            return _extract_channel_data(r.text, channel_identifier)

    except httpx.TimeoutException:
        logger.error(f"Timeout fetching YouTube channel {channel_identifier}")
        return None
    except Exception as e:
        logger.error(f"Error fetching YouTube channel {channel_identifier}: {e}")
        return None


def _extract_channel_data(html: str, identifier: str) -> Optional[Dict]:
    """Extract channel data from YouTube page by parsing ytInitialData."""
    results = {}

    # 1. Extract the master JSON payload
    data_match = re.search(r'var ytInitialData = (\{.*?\});</script>', html)
    
    if data_match:
        try:
            yt_data = json.loads(data_match.group(1))
            
            # Navigate to the channel metadata block
            metadata = yt_data.get('metadata', {}).get('channelMetadataRenderer', {})
            if metadata:
                results['channel_name'] = metadata.get('title', '')
                results['description'] = metadata.get('description', '')
                results['handle'] = metadata.get('vanityChannelUrl', '').split('/')[-1]
                results['channel_id'] = metadata.get('externalId', '')

            # Navigate to the header block for subscriber counts
            header = yt_data.get('header', {}).get('c4TabbedHeaderRenderer', {})
            if not header:
                header = yt_data.get('header', {}).get('pageHeaderRenderer', {}) # Newer UI layout

            if header:
                # Subscriber extraction
                sub_text = header.get('subscriberCountText', {}).get('simpleText', '')
                if not sub_text: # Fallback for accessibility label
                    sub_text = header.get('subscriberCountText', {}).get('accessibility', {}).get('accessibilityData', {}).get('label', '')
                
                if sub_match := re.search(r'([\d.,]+[KMB]?)\s*subscribers?', sub_text, re.IGNORECASE):
                    results['subscriber_count'] = parse_abbreviated_number(sub_match.group(1))

        except json.JSONDecodeError:
            logger.debug(f"Failed to parse ytInitialData JSON for {identifier}")

    # 2. Regex fallbacks if JSON parsing failed or YouTube changes structure
    if 'channel_name' not in results:
        if name_match := re.search(r'"channelMetadataRenderer":\{"title":"([^"]+)"', html):
            results['channel_name'] = name_match.group(1)

    if 'description' not in results:
        if desc_match := re.search(r'"description":"([^"]*)"', html):
            results['description'] = _decode_unicode(desc_match.group(1))

    if 'subscriber_count' not in results:
        sub_patterns = [
            r'"subscriberCountText":\{"simpleText":"([\d.,]+[KMB]?) subscribers?"',
            r'"subscriberCountText":\{"accessibility":\{"accessibilityData":\{"label":"([\d.,]+[KMB]?) subscribers?"',
        ]
        for pattern in sub_patterns:
            if match := re.search(pattern, html, re.IGNORECASE):
                results['subscriber_count'] = parse_abbreviated_number(match.group(1))
                break

    if 'handle' not in results:
        if handle_match := re.search(r'"canonicalChannelUrl":"https://www\.youtube\.com/@([^"]+)"', html):
            results['handle'] = handle_match.group(1)

    if 'channel_id' not in results:
        if channel_id_match := re.search(r'"channelId":"(UC[a-zA-Z0-9_-]{22})"', html):
            results['channel_id'] = channel_id_match.group(1)

    # 3. Extract Links
    links = []
    link_pattern = r'"urlEndpoint":\{"url":"(https?://[^"]+)"'
    for match in re.finditer(link_pattern, html):
        link = match.group(1)
        if 'youtube.com' not in link and 'google.com' not in link:
            clean_link = _clean_redirect_url(link)
            if clean_link and clean_link not in links:
                links.append(clean_link)
    results['links'] = links[:5]

    if 'channel_name' not in results:
        return None

    handle = results.get('handle') or identifier.lstrip('@')
    desc = results.get('description', '')

    return {
        'username': handle,
        'full_name': results.get('channel_name', ''),
        'bio': desc,
        'email': extract_email(desc),
        'phone': extract_phone(desc),
        'follower_count': results.get('subscriber_count', 0),
        'website': results['links'][0] if results.get('links') else '',
        'links': results.get('links', []),
        'channel_id': results.get('channel_id', ''),
        'platform': 'youtube',
        'profile_url': f'https://www.youtube.com/@{handle}',
    }


def _clean_redirect_url(url: str) -> str:
    """Extract actual URL from YouTube redirect wrapper."""
    if 'youtube.com/redirect' in url:
        if q_match := re.search(r'[?&]q=([^&]+)', url):
            return unquote(q_match.group(1))
    return url


def _decode_unicode(text: str) -> str:
    """Safely decode escaped unicode characters."""
    try:
        return text.encode('utf-8').decode('unicode_escape')
    except (UnicodeDecodeError, UnicodeEncodeError):
        return text