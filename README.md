# Vertex

<div align="center">

[![Stars](https://img.shields.io/github/stars/4x3/Vertex?style=for-the-badge&color=00f2fe)](https://github.com/4x3/Vertex/stargazers)
[![Forks](https://img.shields.io/github/forks/4x3/Vertex?style=for-the-badge&color=00f2fe)](https://github.com/4x3/Vertex/network/members)
[![Issues](https://img.shields.io/github/issues/4x3/Vertex?style=for-the-badge&color=00f2fe)](https://github.com/4x3/Vertex/issues)
[![Version](https://img.shields.io/badge/version-1.0.0-00f2fe?style=for-the-badge)](#)
[![Python](https://img.shields.io/badge/python-3.8+-blue.svg?style=for-the-badge&color=007b80)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/github/license/4x3/Vertex?style=for-the-badge&color=007b80)](https://opensource.org/licenses/MIT)

<br>

<img src="screenshot.png" alt="Vertex CLI Preview" width="800">

</div>

<br>

**Vertex is a high-performance, multi-platform OSINT and lead generation CLI that extracts, bypasses, and enriches social media profiles at scale.** Built with a thread-safe `httpx` architecture, it extracts hidden contact information from social media profiles, bypasses rate limits using automated proxy rotation, and enriches leads via Hunter.io.

## Features

* **Multi-Platform Support:** Seamlessly scrape Instagram, TikTok, LinkedIn, GitHub, YouTube, Twitch, Pinterest, and Link-in-Bio pages (Linktree, Stan Store, etc.).
* **WAF & Rate Limit Bypass:** Utilizes modern mobile user agents, specific `Sec-Fetch` headers, and automated rotating proxies to evade Web Application Firewalls.
* **Deep JSON Extraction:** Bypasses brittle HTML parsing by directly extracting hydrated state objects (Next.js `__NEXT_DATA__`, TikTok `__UNIVERSAL_DATA_FOR_REHYDRATION__`, Pinterest `__PWS_DATA__`).
* **Advanced Lead Enrichment:** Waterfall enrichment pipeline (Bio -> Website -> Domain -> Pattern Guessing -> SMTP Verification -> Hunter.io) to maximize email and phone number discovery.
* **Beautiful CLI:** Interactive terminal UI powered by `rich`, featuring gradient branding, live progress spinners, and profile data cards.
* **Smart Exporting:** Automatically flattens complex data structures and exports clean, enriched datasets directly to CSV.

## Installation

Clone the repository and install the required dependencies:

    git clone https://github.com/4x3/Vertex.git
    cd Vertex
    pip install -r requirements.txt

## Configuration

Vertex runs out of the box, but configuring your `.env` file unlocks its full potential. You can set these manually in a `.env` file in the root directory, or use the interactive Settings menu inside the CLI.

    # Proxy Configuration (Highly Recommended)
    VERTEX_PROXY=http://user:pass@host:port
    VERTEX_PROXY_FILE=proxies.txt
    VERTEX_FREE_PROXY=false
    
    # Scraping Delays (in seconds)
    VERTEX_DELAY_MIN=1.0
    VERTEX_DELAY_MAX=3.0
    
    # API Keys & Authentication
    HUNTER_API_KEY=your_hunter_api_key_here
    GITHUB_TOKEN=your_github_personal_access_token_here
    LINKEDIN_COOKIE=your_li_at_cookie_value_here

### Note on Authentication
* **LinkedIn:** Requires your `li_at` session cookie to access the internal Voyager API. Use the in-app settings menu to save it. 
* **GitHub:** Unauthenticated limits are 60 requests/hr. Add a free Personal Access Token to hit 5,000 requests/hr.
* **All other platforms:** Completely unauthenticated.

## Usage

Start the interactive CLI menu:

    python vertex.py

From the menu, you can select specific platforms to scrape manually, trigger a bulk scrape from a `.csv` or `.txt` file, view your past exports, or configure your proxy and delay settings.

To enable verbose debug logging (useful for troubleshooting proxy connections or JSON parsing issues):

    python vertex.py --verbose

## Acknowledgments

Vertex is an upgraded and heavily expanded iteration of [Scout](https://github.com/kiryano/scout), originally created by [kiryano](https://github.com/kiryano). Huge thanks to him for the foundational architecture and initial concept that made this project possible!

## Disclaimer

Vertex is developed for educational and research purposes. Scraping social media platforms may violate their respective Terms of Service. The developers of Vertex are not responsible for any misuse, account bans, or IP blacklisting that may occur. Please use responsibly and ensure you have permission to extract data from target domains.

---
<div align="center">
  <b>Built by <a href="https://github.com/4x3">4x3</a></b>
</div>
