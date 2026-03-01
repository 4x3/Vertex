"""
Shared Scraper Utilities

Common functions used across all platform scrapers.

Provides:
- extract_email() — regex-based email extraction with blacklist filtering
- extract_phone() — robust phone number extraction with length validation
- parse_abbreviated_number() — safely converts "11.5K", "2.3M" to integers
"""

import re
from typing import Optional

# Shared blacklists to prevent scrapers from pulling garbage data
EMAIL_BLACKLIST = {
    'example.com', 'test.com', 'email.com', 'youremail.com', 'sentry.io',
    'wixpress.com', 'googleapis.com', 'w3.org', 'schema.org', 'gravatar.com', 
    'wordpress.com', 'sentry.wixpress.com', 'domain.com'
}

FILE_EXT_BLACKLIST = (
    '.png', '.jpg', '.jpeg', '.gif', '.css', '.js', 
    '.svg', '.webp', '.ico', '.mp4', '.mov'
)


def extract_email(text: str) -> str:
    """Extract first valid email address from text, filtering out noise."""
    if not text:
        return ''
        
    pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    matches = re.findall(pattern, text)
    
    for match in matches:
        lower_match = match.lower()
        domain = lower_match.split('@')[-1]
        
        # Skip dummy domains and file extensions masquerading as emails
        if domain in EMAIL_BLACKLIST:
            continue
        if lower_match.endswith(FILE_EXT_BLACKLIST):
            continue
            
        return match  # Return the first cleanly validated email
        
    return ''


def extract_phone(text: str) -> str:
    """Extract first valid phone number (10-15 digits) from text."""
    if not text:
        return ''
        
    # Strip basic HTML tags if they snuck in to prevent fusing numbers
    visible = re.sub(r'<(script|style)[^>]*>.*?</\1>', ' ', text, flags=re.DOTALL)
    visible = re.sub(r'<[^>]+>', ' ', visible)
    visible = re.sub(r'\s+', ' ', visible)

    patterns = [
        r'\+1[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',       # US/Canada with +1
        r'\+?\d{1,3}[-.\s]\(?\d{3}\)[-.\s]?\d{3}[-.\s]?\d{4}', # International
        r'\(\d{3}\)[-.\s]?\d{3}[-.\s]?\d{4}',                   # Standard (123) 456-7890
        r'\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b',                   # Strict boundary 123-456-7890
    ]
    
    for pattern in patterns:
        for phone in re.findall(pattern, visible):
            # Clean all formatting to count pure digits
            clean = re.sub(r'[^\d+]', '', phone)
            
            # Standard phone numbers are between 10 and 15 digits (E.164 standard)
            if 10 <= len(clean) <= 15:
                return phone.strip()
                
    return ''


def parse_abbreviated_number(s: str) -> int:
    """Safely parse abbreviated numbers like 11M, 7.5K, 1.2B into integers."""
    if not s or not isinstance(s, str):
        return 0
        
    # Remove commas and spaces (e.g., "1.5 M" -> "1.5M")
    s = s.strip().replace(',', '').replace(' ', '').upper()
    multipliers = {'K': 1_000, 'M': 1_000_000, 'B': 1_000_000_000}

    for suffix, mult in multipliers.items():
        if s.endswith(suffix):
            try:
                return int(float(s[:-1]) * mult)
            except ValueError:
                return 0

    try:
        # Handle cases where it's just a raw number string
        return int(float(s))
    except ValueError:
        return 0