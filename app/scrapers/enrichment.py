import re
import dns.resolver
import smtplib
import socket
import logging
import threading
from typing import Optional, Dict, List
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx

from app.scrapers.stealth import random_user_agent, random_delay

logger = logging.getLogger(__name__)

EMAIL_RE = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
EMAIL_BLACKLIST = {
    'example.com', 'test.com', 'email.com', 'youremail.com', 'sentry.io',
    'wixpress.com', 'googleapis.com', 'w3.org', 'schema.org', 'gravatar.com', 
    'wordpress.com', 'sentry.wixpress.com'
}
FILE_EXT_BLACKLIST = ('.png', '.jpg', '.jpeg', '.gif', '.css', '.js', '.svg', '.webp', '.ico')

class LeadEnricher:
    def __init__(self, hunter_api_key: Optional[str] = None):
        self.hunter_api_key = hunter_api_key
        self._domain_pattern_cache: Dict[str, Optional[str]] = {}
        self._cache_lock = threading.Lock()
        
        # Use a persistent client for connection pooling
        self.http_client = httpx.Client(
            timeout=10.0,
            follow_redirects=True,
            verify=False  # Ignore SSL errors on sketchy lead sites
        )

    def __del__(self):
        try:
            self.http_client.close()
        except Exception:
            pass

    def enrich_lead(self, lead_data: Dict) -> Dict:
        enriched = lead_data.copy()
        email_candidates = []

        # 1. Check Bio
        bio_contacts = self._extract_from_text(lead_data.get('bio', ''))
        if bio_contacts['email']: email_candidates.append((bio_contacts['email'], 'bio'))
        if bio_contacts['phone']: enriched['phone'] = bio_contacts['phone']

        # 2. Check Website
        website = lead_data.get('website', '')
        site_emails = []
        useless_domains = {
            'youtube.com', 'youtu.be', 'instagram.com', 'tiktok.com', 'twitter.com', 
            'x.com', 'facebook.com', 'linktr.ee', 'stan.store', 'beacons.ai', 'bit.ly'
        }
        website_is_useful = website and not any(d in website.lower() for d in useless_domains)

        if website_is_useful:
            site_info = self._deep_scrape_website(website)
            site_emails = site_info.get('all_emails', [])
            if site_info['email']: email_candidates.append((site_info['email'], 'website'))
            if site_info['phone'] and not enriched.get('phone'): enriched['phone'] = site_info['phone']

        # 3. Check Company Domain
        company_domain = None
        if not website_is_useful or not site_emails:
            company_domain = self._find_company_domain(lead_data)
            if company_domain:
                enriched['company_domain'] = company_domain
                if not website_is_useful:
                    site_info = self._deep_scrape_website('https://' + company_domain)
                    site_emails = site_info.get('all_emails', [])
                    if site_info['email']: email_candidates.append((site_info['email'], 'website'))
                    if site_info['phone'] and not enriched.get('phone'): enriched['phone'] = site_info['phone']

        work_domain = company_domain or (self._extract_domain(website) if website_is_useful else None)

        # 4. Pattern Guessing (leveraging cache)
        full_name = lead_data.get('full_name')
        if full_name and work_domain:
            pattern_email = self._predict_email_from_pattern(full_name, work_domain, site_emails)
            if pattern_email:
                email_candidates.append((pattern_email, 'pattern'))
            
            # 5. SMTP Brute Force (if no valid candidates yet)
            if not email_candidates:
                candidates = self._generate_email_candidates(full_name, work_domain)
                for c in candidates[:5]:
                    smtp = self._verify_email_smtp(c)
                    if smtp['exists'] and not smtp['accept_all']:
                        email_candidates.append((c, 'smtp_guess'))
                        break

            # 6. Hunter.io Fallback
            if self.hunter_api_key:
                hunter_email = self._find_with_hunter(full_name, work_domain)
                if hunter_email: email_candidates.append((hunter_email, 'hunter.io'))

        # 7. Check Link-in-Bios
        bio_links = self._extract_bio_links(lead_data.get('bio', ''))
        for link in bio_links[:3]:
            link_info = self._scrape_link_page(link)
            if link_info['email']: email_candidates.append((link_info['email'], 'bio_link'))
            if link_info['phone'] and not enriched.get('phone'): enriched['phone'] = link_info['phone']

        # Score & Finalize Best Candidate
        if email_candidates:
            unique_candidates = {email.lower(): source for email, source in email_candidates}
            best_scored = None

            for email, source in unique_candidates.items():
                scored = self._score_and_verify_email(
                    email, source, 
                    pattern_match=(source == 'pattern'), 
                    site_emails_count=len(site_emails)
                )
                if not best_scored or scored['score'] > best_scored['score']:
                    best_scored = scored

            if best_scored:
                enriched['email'] = best_scored['email']
                enriched['email_score'] = best_scored['score']
                enriched['email_source'] = best_scored['source']
                enriched['email_verified'] = best_scored['verified']

        if not enriched.get('email') and full_name and work_domain:
            enriched['possible_emails'] = self._generate_email_candidates(full_name, work_domain)

        enriched['lead_score'] = self._calculate_lead_score(enriched)
        return enriched

    def _extract_from_text(self, text: str) -> Dict[str, Optional[str]]:
        result = {'email': None, 'phone': None}
        if not text: return result

        valid_emails = [e for e in re.findall(EMAIL_RE, text) if self._is_valid_email(e)]
        if valid_emails: result['email'] = valid_emails[0]

        if phone := self._extract_phone_from_text(text):
            result['phone'] = phone

        return result

    def _is_valid_email(self, email: str) -> bool:
        lower = email.lower()
        if lower.split('@')[-1] in EMAIL_BLACKLIST: return False
        if lower.endswith(FILE_EXT_BLACKLIST): return False
        return True

    def _extract_phone_from_text(self, text: str) -> Optional[str]:
        # Clean HTML carefully to avoid fusing words
        visible = re.sub(r'<(script|style)[^>]*>.*?</\1>', ' ', text, flags=re.DOTALL)
        visible = re.sub(r'<[^>]+>', ' ', visible)
        visible = re.sub(r'\s+', ' ', visible)

        patterns = [
            r'\+1[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',
            r'\+?\d{1,3}[-.\s]\(?\d{3}\)[-.\s]?\d{3}[-.\s]?\d{4}',
            r'\(\d{3}\)[-.\s]?\d{3}[-.\s]?\d{4}',
        ]

        for pattern in patterns:
            for phone in re.findall(pattern, visible):
                clean = re.sub(r'[^\d+]', '', phone)
                if 10 <= len(clean) <= 15:
                    return phone.strip()
        return None

    def _fetch_page(self, url: str) -> Optional[str]:
        try:
            if not url.startswith('http'): url = 'https://' + url
            self.http_client.headers.update({'User-Agent': random_user_agent()})
            resp = self.http_client.get(url)
            if resp.status_code == 200: return resp.text
        except Exception as e:
            logger.debug(f"Fetch failed {url}: {e}")
        return None

    def _deep_scrape_website(self, website: str) -> Dict:
        result = {'email': None, 'phone': None, 'all_emails': []}
        if not website.startswith('http'): website = 'https://' + website
        
        base = website.rstrip('/')
        pages = [base, f"{base}/contact", f"{base}/contact-us", f"{base}/about", f"{base}/about-us"]
        all_emails = set()

        for url in pages:
            if html := self._fetch_page(url):
                valid_emails = [e for e in re.findall(EMAIL_RE, html) if self._is_valid_email(e)]
                all_emails.update(valid_emails)

                if not result['email'] and valid_emails: result['email'] = valid_emails[0]
                if not result['phone']:
                    if phone := self._extract_phone_from_text(html): result['phone'] = phone
                
                if result['email'] and result['phone']: break
                random_delay(0.2, 0.5)

        result['all_emails'] = list(all_emails)
        return result

    def _predict_email_from_pattern(self, full_name: str, domain: str, site_emails: List[str]) -> Optional[str]:
        parts = full_name.lower().strip().split()
        if len(parts) < 2: return None
        first, last = parts[0], parts[-1]

        # Check Cache
        with self._cache_lock:
            cached_pattern = self._domain_pattern_cache.get(domain)
        
        if not cached_pattern:
            domain_emails = [e.lower() for e in site_emails if e.lower().endswith(f'@{domain}')]
            if domain_emails:
                local = domain_emails[0].split('@')[0]
                cached_pattern = self._detect_pattern(local)
                with self._cache_lock:
                    self._domain_pattern_cache[domain] = cached_pattern

        if cached_pattern:
            return self._apply_pattern(cached_pattern, first, last, domain)
        return None

    def _detect_pattern(self, local: str) -> Optional[str]:
        if '.' in local:
            parts = local.split('.')
            if len(parts) == 2: return 'f.last' if len(parts[0]) == 1 else 'first.last'
        if re.match(r'^[a-z]+$', local): return 'first'
        if re.match(r'^[a-z]\.[a-z]+$', local): return 'f.last'
        return None

    def _apply_pattern(self, pattern: str, first: str, last: str, domain: str) -> Optional[str]:
        templates = {
            'first.last': f'{first}.{last}@{domain}',
            'first': f'{first}@{domain}',
            'f.last': f'{first[0]}.{last}@{domain}',
            'flast': f'{first[0]}{last}@{domain}',
            'firstlast': f'{first}{last}@{domain}',
        }
        return templates.get(pattern)

    def _verify_email_smtp(self, email: str) -> Dict:
        parts = email.split('@')
        result = {'exists': False, 'accept_all': False, 'score': 0}
        if len(parts) != 2 or not parts[1]: return result
        domain = parts[1]

        try:
            mx_records = dns.resolver.resolve(domain, 'MX')
            mx_host = str(sorted(mx_records, key=lambda x: x.preference)[0].exchange).rstrip('.')
            result['score'] += 10
        except Exception:
            return result

        try:
            # Shortened timeout to prevent hangs
            with smtplib.SMTP(timeout=5) as smtp:
                smtp.connect(mx_host, 25)
                smtp.helo('vertex-verify.local')
                smtp.mail('verify@vertex-verify.local')
                
                if smtp.rcpt(email)[0] == 250:
                    result['exists'] = True
                    result['score'] += 80

                # Check for Catch-All
                if smtp.rcpt(f'zzznonexistent999@{domain}')[0] == 250:
                    result['accept_all'] = True
                    result['score'] = max(result['score'] - 40, 30)

        except Exception as e:
            logger.debug(f"SMTP verify failed (Port 25 likely blocked) for {email}: {e}")
            result['score'] += 20 # Give benefit of the doubt if we can't connect

        return result

    def _score_and_verify_email(self, email: str, source: str, pattern_match: bool = False, site_emails_count: int = 0) -> Dict:
        score_map = {
            'bio': 90, 'hunter.io': 80, 'website': 70, 
            'smtp_guess': 70, 'bio_link': 65, 'pattern': 40
        }
        score = score_map.get(source, 50)

        if source == 'pattern':
            if site_emails_count >= 3: score += 15
            elif site_emails_count >= 1: score += 10

        smtp_result = self._verify_email_smtp(email)
        if smtp_result['exists']: score += 10
        if smtp_result['accept_all']: score -= 20

        return {
            'email': email,
            'score': min(max(score, 0), 100),
            'source': source,
            'verified': smtp_result['exists'],
            'accept_all': smtp_result['accept_all'],
        }

    def _extract_domain(self, website: str) -> Optional[str]:
        try:
            if not website.startswith('http'): website = 'https://' + website
            return (urlparse(website).netloc or website).replace('www.', '')
        except Exception:
            return None

    def _find_company_domain(self, lead_data: Dict) -> Optional[str]:
        company = lead_data.get('company', '')
        headline = lead_data.get('headline', '')
        company_names = [company] if company else []

        if headline:
            for marker in [' at ', ' @ ', ' - ']:
                if marker in headline:
                    company_names.append(headline.split(marker)[-1].strip().rstrip('.'))

            for p in [r'(?:CEO|CTO|COO|Founder|Owner|Director|Partner)\s+(?:of|at|@|-)\s+(.+?)(?:\s*[|,.]|$)', r'(?:at|@)\s+(.+?)(?:\s*[|,.]|$)']:
                if m := re.search(p, headline, re.IGNORECASE):
                    company_names.append(m.group(1).strip())

        seen = set()
        unique_companies = []
        for name in company_names:
            clean = re.sub(r'[^\w\s]', '', name).strip()
            if clean and len(clean) > 2 and clean.lower() not in seen:
                seen.add(clean.lower())
                unique_companies.append(clean)

        for name in unique_companies[:3]:
            if domain := self._guess_domain(name): return domain
        return None

    def _guess_domain(self, company_name: str) -> Optional[str]:
        clean = re.sub(r'\s+(inc|llc|ltd|co|corp|group|holdings)\.?$', '', company_name.lower().strip(), flags=re.IGNORECASE)
        slug = re.sub(r'[^a-z0-9]', '', clean)
        
        guesses = [f'{slug}.com', f'{slug}.io', f'{slug}.co']
        if ' ' in clean:
            parts = clean.split()
            if len(parts) >= 2: guesses.append(f'{parts[0]}{parts[1]}.com')

        for domain in guesses:
            try:
                dns.resolver.resolve(domain, 'MX')
                return domain
            except Exception:
                pass
        return None

    def _generate_email_candidates(self, full_name: str, domain: str) -> List[str]:
        parts = full_name.lower().strip().split()
        if len(parts) < 2: return []
        first, last = parts[0], parts[-1]
        
        return [
            f'{first}.{last}@{domain}', f'{first}@{domain}', f'{first}{last}@{domain}',
            f'{first[0]}{last}@{domain}', f'{first[0]}.{last}@{domain}', f'contact@{domain}'
        ]

    def _extract_bio_links(self, bio: str) -> List[str]:
        if not bio: return []
        links = re.findall(r'https?://[^\s<>"{}|\\^`\[\]]+|(?:linktr\.ee|stan\.store|beacons\.ai)/[^\s<>"{}|\\^`\[\]]+', bio)
        return [l if l.startswith('http') else f'https://{l}'.rstrip('.,;:!?)') for l in links]

    def _scrape_link_page(self, url: str) -> Dict[str, Optional[str]]:
        result = {'email': None, 'phone': None}
        if html := self._fetch_page(url):
            valid_emails = [e for e in re.findall(EMAIL_RE, html) if self._is_valid_email(e)]
            if valid_emails: result['email'] = valid_emails[0]
            if phone := self._extract_phone_from_text(html): result['phone'] = phone
        return result

    def _find_with_hunter(self, full_name: Optional[str], domain: Optional[str]) -> Optional[str]:
        if not self.hunter_api_key or not full_name or not domain: return None
        try:
            parts = full_name.split()
            if len(parts) < 2: return None

            resp = self.http_client.get('https://api.hunter.io/v2/email-finder', params={
                'domain': domain, 'first_name': parts[0], 'last_name': parts[-1], 'api_key': self.hunter_api_key
            })
            if email := resp.json().get('data', {}).get('email'): return email
        except Exception as e:
            logger.debug(f"Hunter.io error: {e}")
        return None

    def _calculate_lead_score(self, lead_data: Dict) -> int:
        score = sum([
            30 if lead_data.get('email') else 0,
            5 if lead_data.get('email_source') == 'hunter.io' else 0,
            30 if lead_data.get('phone') else 0,
            10 if lead_data.get('is_verified') else 0,
            10 if lead_data.get('website') else 0
        ])

        followers = lead_data.get('follower_count', 0)
        if followers >= 5000 and followers <= 50000: score += 15
        elif followers > 1000: score += 10
        elif followers > 0: score += 5

        bio = (lead_data.get('bio') or '').lower()
        if any(k in bio for k in ['coach', 'consultant', 'ceo', 'founder', 'agency', 'owner']):
            score += 5

        return min(score, 100)

    def enrich_bulk(self, leads: List[Dict], max_workers: int = 5) -> List[Dict]:
        enriched = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.enrich_lead, lead): lead for lead in leads}
            for future in as_completed(futures):
                try:
                    enriched.append(future.result())
                except Exception as e:
                    logger.error(f"Enrichment error: {e}")
                    enriched.append(futures[future])
        return enriched

def enrich_lead(lead_data: Dict, hunter_api_key: Optional[str] = None) -> Dict:
    enricher = LeadEnricher(hunter_api_key=hunter_api_key)
    return enricher.enrich_lead(lead_data)