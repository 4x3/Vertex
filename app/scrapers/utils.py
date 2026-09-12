"""
Shared scraper helpers.

- extract_email() — first usable email in a blob of text
- extract_phone() — first phone-looking number
- parse_abbreviated_number() — "11.5K" / "2.3M" to int
"""

import re


EMAIL_BLACKLIST = {
    'example.com', 'test.com', 'email.com', 'youremail.com', 'sentry.io',
    'wixpress.com', 'googleapis.com', 'w3.org', 'schema.org', 'gravatar.com',
    'wordpress.com', 'sentry.wixpress.com', 'domain.com',
}

FILE_EXT_BLACKLIST = (
    '.png', '.jpg', '.jpeg', '.gif', '.css', '.js',
    '.svg', '.webp', '.ico', '.mp4', '.mov',
)


def extract_email(text: str) -> str:
    """Extract first email address from text, skipping placeholders and assets."""
    if not text:
        return ''

    pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    matches = re.findall(pattern, text)

    for match in matches:
        lower = match.lower()
        domain = lower.split('@')[-1]
        if domain in EMAIL_BLACKLIST:
            continue
        if lower.endswith(FILE_EXT_BLACKLIST):
            continue
        return match

    return ''


def extract_phone(text: str) -> str:
    """Extract first phone number (10-15 digits) from text."""
    if not text:
        return ''

    visible = re.sub(r'<(script|style)[^>]*>.*?</\1>', ' ', text, flags=re.DOTALL)
    visible = re.sub(r'<[^>]+>', ' ', visible)
    visible = re.sub(r'\s+', ' ', visible)

    patterns = [
        r'\+1[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',
        r'\+?\d{1,3}[-.\s]\(?\d{3}\)[-.\s]?\d{3}[-.\s]?\d{4}',
        r'\(\d{3}\)[-.\s]?\d{3}[-.\s]?\d{4}',
        r'\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b',
    ]

    for pattern in patterns:
        for phone in re.findall(pattern, visible):
            clean = re.sub(r'[^\d+]', '', phone)
            if 10 <= len(clean) <= 15:
                return phone.strip()

    return ''


def parse_abbreviated_number(s: str) -> int:
    """Parse abbreviated numbers like 11M, 7.5K, 1.2B into integers."""
    if not s or not isinstance(s, str):
        return 0

    s = s.strip().replace(',', '').replace(' ', '').upper()
    multipliers = {'K': 1_000, 'M': 1_000_000, 'B': 1_000_000_000}

    for suffix, mult in multipliers.items():
        if s.endswith(suffix):
            try:
                return int(float(s[:-1]) * mult)
            except (ValueError, IndexError):
                return 0

    try:
        return int(float(s))
    except ValueError:
        return 0
