"""
YouTube Channel Scraper

Scrapes public YouTube channel pages by parsing HTML and embedded JSON.
Extracts subscriber counts, video counts, channel description, and links.

Features:
- Parses ytInitialData JSON from page source
- Handles both @handle and /channel/ URL formats
- No API key required
"""

import json
import logging
import re
from typing import Dict, Optional
from urllib.parse import unquote

import httpx

from app.scrapers.stealth import random_user_agent, make_client
from app.scrapers.utils import extract_email, extract_phone, parse_abbreviated_number

logger = logging.getLogger(__name__)


def scrape_channel(channel_identifier: str) -> Optional[Dict]:
    """
    Fetch YouTube channel data.

    Args:
        channel_identifier: Can be @handle, channel ID, or custom URL name
    """
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
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    }
    cookies = {
        'CONSENT': 'PENDING+999',
    }

    try:
        with make_client() as client:
            r = client.get(url, headers=headers, cookies=cookies)

            if r.status_code == 404:
                logger.error(f"YouTube channel {channel_identifier} not found")
                return None

            if r.status_code != 200:
                logger.error(f"YouTube error {r.status_code} for {channel_identifier}")
                return None

            result = _extract_channel_data(r.text, channel_identifier)
            if result:
                return result

            headers['User-Agent'] = random_user_agent()
            r = client.get(url, headers=headers, cookies=cookies)
            if r.status_code == 200:
                return _extract_channel_data(r.text, channel_identifier)
            return None

    except httpx.TimeoutException:
        logger.error(f"Timeout fetching YouTube channel {channel_identifier}")
        return None
    except httpx.RequestError as e:
        logger.error(f"Error fetching YouTube channel {channel_identifier}: {e}")
        return None


def _extract_channel_data(html: str, identifier: str) -> Optional[Dict]:
    """Extract channel data from YouTube page HTML."""
    results = {}

    data_match = re.search(r'var ytInitialData = (\{.*?\});</script>', html)
    if data_match:
        try:
            yt_data = json.loads(data_match.group(1))
            metadata = (yt_data.get('metadata') or {}).get('channelMetadataRenderer', {})
            if metadata:
                results['channel_name'] = metadata.get('title', '')
                results['description'] = metadata.get('description', '')
                vanity = metadata.get('vanityChannelUrl', '')
                if vanity:
                    results['handle'] = vanity.rstrip('/').split('/')[-1]
                results['channel_id'] = metadata.get('externalId', '')

            header = (yt_data.get('header') or {}).get('c4TabbedHeaderRenderer') or {}
            if not header:
                header = (yt_data.get('header') or {}).get('pageHeaderRenderer') or {}

            sub_text = ''
            if header:
                sub = header.get('subscriberCountText') or {}
                sub_text = sub.get('simpleText', '') or ''
                if not sub_text:
                    sub_text = (
                        (sub.get('accessibility') or {})
                        .get('accessibilityData', {})
                        .get('label', '')
                    )

            if sub_text:
                sub_match = re.search(r'([\d.,]+[KMB]?)\s*subscribers?', sub_text, re.IGNORECASE)
                if sub_match:
                    results['subscriber_count'] = parse_abbreviated_number(sub_match.group(1))
        except json.JSONDecodeError:
            logger.debug(f"Failed to parse ytInitialData for {identifier}")

    if 'channel_name' not in results:
        name_match = re.search(r'"channelMetadataRenderer":\{"title":"([^"]+)"', html)
        if name_match:
            results['channel_name'] = name_match.group(1)

    if 'description' not in results:
        desc_match = re.search(r'"description":"([^"]*)"', html)
        if desc_match:
            results['description'] = _decode_unicode(desc_match.group(1))

    if 'subscriber_count' not in results:
        sub_patterns = [
            r'"subscriberCountText":\{"simpleText":"([\d.,]+[KMB]?) subscribers?"',
            r'"subscriberCountText":\{"accessibility":\{"accessibilityData":\{"label":"([\d.,]+[KMB]?) subscribers?"',
        ]
        for pattern in sub_patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                results['subscriber_count'] = parse_abbreviated_number(match.group(1))
                break

    if 'handle' not in results:
        handle_match = re.search(r'"canonicalChannelUrl":"https://www\.youtube\.com/@([^"]+)"', html)
        if handle_match:
            results['handle'] = handle_match.group(1)

    if 'channel_id' not in results:
        channel_id_match = re.search(r'"channelId":"(UC[a-zA-Z0-9_-]{22})"', html)
        if channel_id_match:
            results['channel_id'] = channel_id_match.group(1)

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
        q_match = re.search(r'[?&]q=([^&]+)', url)
        if q_match:
            return unquote(q_match.group(1))
    return url


def _decode_unicode(text: str) -> str:
    try:
        return text.encode('utf-8').decode('unicode_escape')
    except (UnicodeDecodeError, UnicodeEncodeError):
        return text


_parse_count = parse_abbreviated_number
_extract_email = extract_email
