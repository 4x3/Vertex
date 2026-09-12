# Vertex

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

Fork of [Scout](https://github.com/kiryano/Scout) by [kiryano](https://github.com/kiryano). Same idea, a bit more reliable on the scrape side.

CLI for pulling public profiles off Instagram, TikTok, LinkedIn, GitHub, YouTube, Twitch, Pinterest, and link-in-bio pages. Emails get pulled from bios and sites, checked over SMTP, and written out to CSV.

![Vertex CLI](screenshot.png)

## Install

```bash
git clone https://github.com/4x3/Vertex.git
cd Vertex
pip install -r requirements.txt
cp .env.example .env
```

## Usage

```bash
python vertex.py
python vertex.py --verbose   # debug logging
python vertex.py --version
python vertex.py --help
```

Run it with no args for the menu. From there you can scrape one platform, dump a username list, look at past exports, or change proxy/delay settings.

## Supported platforms

| Platform | Auth | What you get |
|----------|------|----------------|
| Instagram | None | profile, bio, followers, email, phone, links |
| TikTok | None | profile, bio, followers, likes, email |
| LinkedIn | Session cookie | profile, headline, company, location, email |
| GitHub | Optional token | profile, bio, repos, email (including from public commits), website |
| YouTube | None | channel name, description, subscribers, email, links |
| Twitch | None | profile, bio, followers, partner/affiliate, social links |
| Pinterest | None | profile, bio, followers, pins, website |
| Linktree | None | Linktree, Stan, Linkr, Bio.link |

## Features

- Eight scrapers, same output shape
- Email enrichment with SMTP verification (no paid API required)
- Company domain guess from headline/bio
- Email pattern detection (`first.last@company.com` and friends)
- Lead score 0–100
- Website contact/about scrape
- Bio link scrape (Linktree, WhatsApp, `tel:` links)
- CSV export under `exports/`
- Bulk scrape from a username list (CSV or TXT)
- Proxy rotation and user-agent rotation
- Configurable scrape delay
- Optional Hunter.io lookups

## LinkedIn

1. Log into LinkedIn in Chrome
2. DevTools (F12) > Application > Cookies > linkedin.com
3. Copy `li_at`
4. Add to `.env`:

```
LINKEDIN_COOKIE=your_li_at_cookie_value
```

You can also paste it from the Settings menu.

## GitHub

Unauthenticated requests cap out at 60/hour. A personal access token bumps that to 5,000/hour. Set `GITHUB_TOKEN` in `.env` or from Settings. If the profile email is hidden, Vertex will look through recent public commits for a real one.

## Proxy

```
# Single proxy
VERTEX_PROXY=http://user:pass@host:port

# Rotating proxies from file (one per line)
VERTEX_PROXY_FILE=proxies.txt

# Free anonymous proxies (no config needed, not very reliable)
VERTEX_FREE_PROXY=true
```

Proxies are optional. Twitch falls back to a direct connection if the proxy fails.

## Enrichment

After a scrape, Vertex tries to fill in contact info:

1. Email/phone from the bio
2. The lead's website, `/contact`, and `/about`
3. Company name from the headline (e.g. "CEO at CompanyX")
4. Company domain via DNS MX
5. Email pattern from existing addresses on the site
6. Candidate generation (`first.last@`, `first@`, etc.)
7. SMTP check against the mail server
8. Confidence score 0–100

Hunter.io is optional if you want extra coverage.

## Limitations

- Instagram may need retries depending on region/IP
- TikTok can serve CAPTCHAs
- LinkedIn cookies expire
- GitHub API is 60 req/hour without a token
- SMTP checks get blocked by some mail servers
- Free proxies are hit or miss

## Credits

This is a fork of [Scout](https://github.com/kiryano/Scout), originally written by [kiryano](https://github.com/kiryano). The scrapers, enrichment pipeline, and CLI layout started there. Vertex keeps that structure and changes the bits that were flaky or missing.

## License

MIT. Original copyright remains with Scout; this fork adds 4x3.
