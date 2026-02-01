#!/usr/bin/env python3

import sys
import csv
import time
import os
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import Callable, List, Optional, Tuple

if sys.platform == 'win32':
    os.system('chcp 65001 >nul 2>&1')
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')

sys.path.insert(0, str(Path(__file__).parent))

from app import __version__

# --- RICH UI IMPORTS ---
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich import box
from rich.text import Text
from rich.theme import Theme
from rich.rule import Rule

# --- SCRAPER IMPORTS ---
from app.scrapers.stealth import random_delay, proxy_status
from app.scrapers.enrichment import LeadEnricher

# Clean facade imports
from app.scrapers import (
    scrape_instagram, scrape_tiktok, scrape_linkedin, 
    scrape_github, scrape_youtube, scrape_twitch, 
    scrape_linktree, scrape_pinterest
)

# --- BRANDING & CONSTANTS ---
APP_NAME = "Vertex"
REPO_URL = "https://github.com/4x3/Vertex"

# Sleek Cyan/Blue Gradient
ACCENT = "#00f2fe"
ACCENT_DIM = "#007b80"
_GRAD_START = (0, 242, 254)
_GRAD_END = (79, 172, 254)

_LOGO_LINES = [
    "██    ██ ███████ ██████  ████████ ███████ ██   ██",
    "██    ██ ██      ██   ██    ██    ██       ██ ██ ",
    "██    ██ █████   ██████     ██    █████     ███  ",
    " ██  ██  ██      ██   ██    ██    ██       ██ ██ ",
    "  ████   ███████ ██   ██    ██    ███████ ██   ██",
]

# --- CONFIGURATION MANAGER ---
class ConfigManager:
    """Handles all environment variables and configuration state."""
    def __init__(self):
        self.env_file = Path(__file__).parent / '.env'
        self._load_env()

    def _load_env(self):
        if self.env_file.exists():
            with open(self.env_file) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, _, val = line.partition('=')
                        os.environ.setdefault(key.strip(), val.strip())

    def update(self, key: str, value: str):
        lines = []
        found = False
        if self.env_file.exists():
            with open(self.env_file) as f:
                for line in f:
                    if line.strip().startswith(key + '='):
                        lines.append(f'{key}={value}\n')
                        found = True
                    else:
                        lines.append(line)
        if not found:
            lines.append(f'{key}={value}\n')
            
        with open(self.env_file, 'w') as f:
            f.writelines(lines)
        os.environ[key] = value

    def get_delay_range(self) -> Tuple[float, float]:
        try:
            d_min = float(os.environ.get('VERTEX_DELAY_MIN', '1.0'))
            d_max = float(os.environ.get('VERTEX_DELAY_MAX', '2.5'))
            return (d_min, d_max) if d_max >= d_min >= 0 else (1.0, 2.5)
        except ValueError:
            return (1.0, 2.5)

# --- PLATFORM REGISTRY ---
@dataclass
class Platform:
    id: str
    name: str
    target_type: str
    scraper_func: Callable
    delay_range: Tuple[float, float]
    strip_at: bool = False
    requires_cookie: str = None
    input_prompt: str = "Username"

class ScraperRegistry:
    """Centralized registry for all scraping modules."""
    def __init__(self):
        self.platforms = {}

    def register(self, p: Platform):
        self.platforms[p.id] = p

    def get(self, p_id: str) -> Optional[Platform]:
        return self.platforms.get(p_id)

    def get_all(self) -> List[Platform]:
        return list(self.platforms.values())

# --- MAIN APPLICATION ---
class VertexApp:
    def __init__(self):
        self.config = ConfigManager()
        self.stats = {"scraped": 0}
        
        custom_theme = Theme({"prompt.choices": ACCENT, "prompt.default": "dim"})
        self.console = Console(theme=custom_theme)
        Prompt.prompt_suffix = " "
        Confirm.prompt_suffix = " "
        
        self.registry = self._initialize_registry()

    def _initialize_registry(self) -> ScraperRegistry:
        registry = ScraperRegistry()
        
        registry.register(Platform("1", "Instagram", "profiles", scrape_instagram, (1.5, 4.0), True))
        registry.register(Platform("2", "TikTok", "profiles", scrape_tiktok, (2.0, 5.0), True))
        registry.register(Platform("3", "LinkedIn", "cookie", scrape_linkedin, (3.0, 6.0), False, "LINKEDIN_COOKIE", "Username/URL"))
        registry.register(Platform("4", "GitHub", "profiles", scrape_github, (0.5, 1.5), False))
        registry.register(Platform("5", "YouTube", "channels", scrape_youtube, (1.0, 2.5), False, None, "Channel"))
        registry.register(Platform("6", "Twitch", "streamers", scrape_twitch, (0.5, 1.5), False))
        registry.register(Platform("7", "Linktree", "link-in-bio", scrape_linktree, (0.5, 1.5), False))
        registry.register(Platform("8", "Pinterest", "profiles", scrape_pinterest, (1.0, 2.5), False))
        
        return registry

    # --- UI HELPERS ---
    def _gradient_text(self, text: str, progress_shift: float = 0.0) -> Text:
        t = Text()
        length = len(text.rstrip())
        for j, char in enumerate(text):
            if j < length and char != ' ':
                progress = max(0.0, min(1.0, (j / max(length - 1, 1)) * 0.6 + progress_shift * 0.4))
                r = int(_GRAD_START[0] + (_GRAD_END[0] - _GRAD_START[0]) * progress)
                g = int(_GRAD_START[1] + (_GRAD_END[1] - _GRAD_START[1]) * progress)
                b = int(_GRAD_START[2] + (_GRAD_END[2] - _GRAD_START[2]) * progress)
                t.append(char, style=f"bold #{r:02x}{g:02x}{b:02x}")
            else:
                t.append(char)
        return t

    def show_header(self):
        self.console.print()
        bar = Text("▀" * min(self.console.width, 80), style=ACCENT)
        self.console.print(bar, justify="center")
        self.console.print()

        for i, line in enumerate(_LOGO_LINES):
            t = self._gradient_text(line, i / max(len(_LOGO_LINES) - 1, 1))
            self.console.print(t, justify="center")

        self.console.print(f"\n[dim]v{__version__}[/dim]  [white]Advanced Lead Generation[/white]\n", justify="center")
        self.console.print(bar, justify="center")

        ps = proxy_status()
        proxy_str = {"custom": "[green]● custom[/green]", "file": "[green]● rotating[/green]", "free": "[yellow]● free[/yellow]"}.get(ps, "[dim]○ off[/dim]")
        
        status = f"[dim]{REPO_URL.replace('https://', '')}[/dim]  ·  [dim]Proxy:[/dim] {proxy_str}"
        if self.stats["scraped"] > 0:
            status += f"  ·  [dim]Leads:[/dim] [white]{self.stats['scraped']}[/white]"
        self.console.print(status, justify="center")
        self.console.print()

    def _print_rule(self, title: str, subtitle: str = ""):
        self.console.print()
        text = f"[bold white]{title}[/bold white]"
        if subtitle:
            text += f"  [dim]{subtitle}[/dim]"
        self.console.print(Rule(text, style=ACCENT_DIM, align="left"))
        self.console.print()

    # --- CORE LOGIC ---
    def collect_inputs(self, prompt_label: str, strip_at: bool = False) -> List[str]:
        self.console.print(f"[white]Enter {prompt_label}s (one per line)[/white]")
        self.console.print("[dim]Press Enter on empty line when done[/dim]\n")
        
        items = []
        while True:
            entry = Prompt.ask(prompt_label, default="").strip()
            if not entry: break
            if strip_at: entry = entry.replace('@', '')
            
            if '/in/' in entry: entry = entry.split('/in/')[-1].strip('/')
            
            if entry:
                items.append(entry)
                self.console.print(f"[green]✓[/green] Added {entry}")
        return items

    def run_scraper_loop(self, platform: Platform, items: List[str]) -> List[dict]:
        profiles = []
        delay = self.config.get_delay_range() or platform.delay_range
        prefix = "@" if platform.strip_at else ""

        for i, item in enumerate(items, 1):
            with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=self.console, transient=True) as progress:
                task = progress.add_task(f"[white]Scraping {prefix}{item}  [dim]({i}/{len(items)})[/dim]", total=None)
                try:
                    prev_level = logging.getLogger().level
                    if '-V' not in sys.argv and '--verbose' not in sys.argv:
                        logging.getLogger().setLevel(logging.CRITICAL)
                        
                    profile = platform.scraper_func(item)
                    
                    logging.getLogger().setLevel(prev_level)

                    progress.stop()
                    if profile:
                        profiles.append(profile)
                        self._render_profile_card(profile)
                    else:
                        self.console.print(f"  [red]✗[/red] [dim]{prefix}{item} — not found[/dim]")
                        
                except Exception as e:
                    logging.getLogger().setLevel(prev_level)
                    progress.stop()
                    err_msg = "rate limited" if isinstance(e, RuntimeError) else str(e)[:80]
                    style = "bold red" if isinstance(e, RuntimeError) else "red"
                    self.console.print(f"  [{style}]✗ {prefix}{item} — {err_msg}[/{style}]")
                    if isinstance(e, RuntimeError): break

            if i < len(items):
                random_delay(*delay)

        return profiles

    def _render_profile_card(self, profile: dict):
        self.stats["scraped"] += 1
        lines = []

        if profile.get('full_name'):
            lines.append(f"[bold white]{profile['full_name'][:50]}[/bold white]")

        stats = []
        if profile.get('follower_count'): stats.append(f"[white]{profile['follower_count']:,}[/white] [dim]followers[/dim]")
        if profile.get('following_count'): stats.append(f"[white]{profile['following_count']:,}[/white] [dim]following[/dim]")
        if stats: lines.append("  ·  ".join(stats))

        email = profile.get('email', '')
        if bio := profile.get('bio'):
            bio = bio.replace('\n', ' ')
            if email: bio = bio.replace(email, '').strip(' |·-,')
            lines.append(f"[dim]{bio[:80] + '...' if len(bio) > 80 else bio}[/dim]")

        if email: lines.append(f"[bold {ACCENT}]{email}[/bold {ACCENT}]")
        if profile.get('website'): lines.append(f"[dim]{profile['website']}[/dim]")
        if profile.get('phone'): lines.append(f"[dim]{profile['phone']}[/dim]")

        self.console.print(Panel(
            "\n".join(lines) if lines else "[dim]No data[/dim]",
            title=f"[bold white]@{profile.get('username', '?')}[/bold white]",
            title_align="left",
            border_style=ACCENT_DIM,
            box=box.ROUNDED,
            padding=(0, 2),
            width=64
        ))

    def handle_enrichment_and_export(self, profiles: List[dict], platform_name: str):
        if not profiles:
            self.console.print("\n  [yellow]No profiles were scraped successfully[/yellow]\n")
            return

        self.console.print(f"\n  [green]OK[/green] [white]Successfully extracted {len(profiles)} leads.[/white]\n")

        if Confirm.ask("\n[white][+] Enrich leads with Hunter.io contact info?[/white]", default=True):
            profiles = self._enrich_profiles(profiles)

        if Confirm.ask("[+] Export to CSV?", default=True):
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            filename = f"{platform_name.lower()}_export_{timestamp}.csv"
            
            flat_profiles = []
            for p in profiles:
                flat = {k: v for k, v in p.items() if k not in ['links', 'socials']}
                if p.get('socials'):
                    for plat, handle in p['socials'].items():
                        flat[f'social_{plat}'] = handle
                flat_profiles.append(flat)

            with open(filename, 'w', newline='', encoding='utf-8') as f:
                all_keys = {k for p in flat_profiles for k in p.keys()}
                writer = csv.DictWriter(f, fieldnames=sorted(all_keys))
                writer.writeheader()
                writer.writerows(flat_profiles)
                
            self.console.print(f"  [green]Saved[/green] [white]{filename}[/white]\n")

    def _enrich_profiles(self, profiles: List[dict]) -> List[dict]:
        hunter_key = os.environ.get('HUNTER_API_KEY', '').strip() or None
        enricher = LeadEnricher(hunter_api_key=hunter_key)
        
        emails_found, phones_found = 0, 0
        enriched = []

        with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=self.console) as progress:
            task = progress.add_task("[white]Enriching leads...", total=len(profiles))
            for p in profiles:
                had_email, had_phone = bool(p.get('email')), bool(p.get('phone'))
                result = enricher.enrich_lead(p)
                enriched.append(result)
                
                if result.get('email') and not had_email: emails_found += 1
                if result.get('phone') and not had_phone: phones_found += 1
                progress.advance(task)

        self._print_rule("Enrichment Results")
        if emails_found: self.console.print(f"  [green]{emails_found} new emails found[/green]")
        if phones_found: self.console.print(f"  [green]{phones_found} new phones found[/green]")
        if not (emails_found or phones_found): self.console.print("  [yellow]No new contact info found[/yellow]")

        scores = [e.get('lead_score', 0) for e in enriched]
        self.console.print(f"  [white]Avg lead score: {sum(scores) // len(scores) if scores else 0}/100[/white]\n")
        return enriched

    # --- COMMAND ROUTERS ---
    def execute_platform_scrape(self, platform_id: str):
        platform = self.registry.get(platform_id)
        if not platform: return

        if platform.requires_cookie and not os.environ.get(platform.requires_cookie, '').strip():
            self._print_rule(f"{platform.name} Authentication Required", style="yellow")
            self.console.print(f"  [white]Please set [bold]{platform.requires_cookie}[/bold] in your .env or Settings menu.[/white]\n")
            return

        self._print_rule(platform.name, f"Targeting {platform.target_type}")
        items = self.collect_inputs(platform.input_prompt, platform.strip_at)
        
        if not items: return
        self.console.print()
        
        profiles = self.run_scraper_loop(platform, items)
        self.handle_enrichment_and_export(profiles, platform.name)

    def execute_bulk_scrape(self):
        self._print_rule("Bulk Scrape", "From file")
        filename = Prompt.ask("[>] Enter filename", default="usernames.txt").strip()
        
        try:
            items = []
            if filename.lower().endswith('.csv'):
                with open(filename, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        username = row.get('username', '') or row.get('Username', '') or row.get('handle', '') or row.get('Handle', '')
                        if not username and row:
                            username = list(row.values())[0]
                        username = username.strip().replace('@', '')
                        if username: items.append(username)
            else:
                with open(filename, 'r', encoding='utf-8') as f:
                    items = [line.strip().replace('@', '') for line in f if line.strip()]
            
            if not items:
                self.console.print(f"\n[red]✗ No targets found in {filename}[/red]")
                return
                
            self.console.print(f"\n[white]Found {len(items)} targets in {filename}[/white]\n")
            
            platforms = self.registry.get_all()
            for p in platforms:
                self.console.print(f"  [{ACCENT}][{p.id}][/{ACCENT}] {p.name}")
            self.console.print()
            
            choice = Prompt.ask(f"[{ACCENT}]>[/{ACCENT}] Select Platform", choices=[p.id for p in platforms], default="1")
            platform = self.registry.get(choice)
            
            if platform.requires_cookie and not os.environ.get(platform.requires_cookie, '').strip():
                self.console.print(f"\n[red]✗ {platform.requires_cookie} not set. Check Settings.[/red]")
                return
                
            if not Confirm.ask(f"Scrape {len(items)} targets on {platform.name}?", default=True): return
            
            self.console.print()
            profiles = self.run_scraper_loop(platform, items)
            self.handle_enrichment_and_export(profiles, platform.name)
            
        except FileNotFoundError:
            self.console.print(f"\n[red]✗ Error: File '{filename}' not found![/red]")
        except Exception as e:
            self.console.print(f"\n[red]✗ Error: {e}[/red]")

    def view_exports(self):
        self._print_rule("Exports", "Your scraped data")
        csv_files = sorted(Path('.').glob('*_export_*.csv'), key=lambda x: x.stat().st_mtime, reverse=True)

        if not csv_files:
            self.console.print("  [yellow]No exports found yet.[/yellow]\n")
            return

        table = Table(box=box.MINIMAL_HEAVY_HEAD, border_style="dim", padding=(0, 1))
        table.add_column("#", style="dim", width=4)
        table.add_column("Filename", style="white")
        table.add_column("Size", style="white", justify="right")
        table.add_column("Date", style="dim")

        for i, file in enumerate(csv_files[:10], 1):
            size = file.stat().st_size / 1024
            mtime = time.strftime("%Y-%m-%d %H:%M", time.localtime(file.stat().st_mtime))
            table.add_row(str(i), file.name, f"{size:.1f} KB", mtime)

        self.console.print(table)
        self.console.print(f"  [dim]{len(csv_files)} exports total[/dim]\n")

    def settings_menu(self):
        self._print_rule("Settings", "Configure Application")
        
        self.console.print(f"  [dim]Proxy:[/dim]     {proxy_status()}")
        self.console.print(f"  [dim]Delay:[/dim]     [white]{os.environ.get('VERTEX_DELAY_MIN', '1.0')}s - {os.environ.get('VERTEX_DELAY_MAX', '2.5')}s[/white]")
        self.console.print(f"  [dim]LinkedIn:[/dim]  {'[green]cookie set[/green]' if os.environ.get('LINKEDIN_COOKIE') else '[dim]not configured[/dim]'}\n")
        
        self.console.print(f"  [{ACCENT}][1][/{ACCENT}]  Set Proxy URL")
        self.console.print(f"  [{ACCENT}][2][/{ACCENT}]  Set Scrape Delays")
        self.console.print(f"  [{ACCENT}][3][/{ACCENT}]  Set LinkedIn Cookie")
        self.console.print(f"  [{ACCENT}][4][/{ACCENT}]  Clear All Exports")
        self.console.print(f"  [{ACCENT}][0][/{ACCENT}]  Back\n")
        
        choice = Prompt.ask(f"[{ACCENT}]>[/{ACCENT}]", choices=["0", "1", "2", "3", "4"], default="0")
        
        if choice == '1':
            url = Prompt.ask("[>] Proxy URL").strip()
            if url: self.config.update('VERTEX_PROXY', url)
        elif choice == '2':
            min_d = Prompt.ask("[>] Min delay (sec)", default=os.environ.get('VERTEX_DELAY_MIN', '1.0'))
            max_d = Prompt.ask("[>] Max delay (sec)", default=os.environ.get('VERTEX_DELAY_MAX', '2.5'))
            self.config.update('VERTEX_DELAY_MIN', min_d)
            self.config.update('VERTEX_DELAY_MAX', max_d)
        elif choice == '3':
            cookie = Prompt.ask("[>] LinkedIn li_at cookie").strip()
            if cookie: self.config.update('LINKEDIN_COOKIE', cookie)
        elif choice == '4':
            files = list(Path('.').glob('*_export_*.csv'))
            if Confirm.ask(f"[yellow]Delete {len(files)} export files?[/yellow]", default=False):
                for f in files: f.unlink()
                self.console.print("[green]✓ Cleared.[/green]")

    def run(self):
        self.console.clear()
        self.show_header()

        while True:
            self._print_rule("Platforms")
            
            platforms = self.registry.get_all()
            table = Table(show_header=False, box=None, padding=(0, 0), pad_edge=False, expand=False)
            table.add_column("", width=35)
            table.add_column("", width=35)
            
            half = (len(platforms) + 1) // 2
            left_col, right_col = platforms[:half], platforms[half:]
            
            for i in range(half):
                left = f"  [{ACCENT}][{left_col[i].id}][/{ACCENT}]  [bold white]{left_col[i].name}[/bold white]"
                right = f"  [{ACCENT}][{right_col[i].id}][/{ACCENT}]  [bold white]{right_col[i].name}[/bold white]" if i < len(right_col) else ""
                table.add_row(left, right)
            
            self.console.print(table)
            
            self._print_rule("Tools")
            self.console.print(f"  [{ACCENT}][9][/{ACCENT}]  Bulk File Scrape")
            self.console.print(f"  [{ACCENT}][10][/{ACCENT}] View Exports")
            self.console.print(f"  [{ACCENT}][11][/{ACCENT}] Settings")
            self.console.print(f"  [{ACCENT}][0][/{ACCENT}]  Exit\n")

            valid_choices = [p.id for p in platforms] + ["9", "10", "11", "0"]
            choice = Prompt.ask(f"[{ACCENT}]>[/{ACCENT}]", choices=valid_choices, default="1", show_choices=False)

            if choice == '0':
                self.console.print(f"\n  [bold {ACCENT}]Session Closed.[/bold {ACCENT}]  [dim]★ {REPO_URL}[/dim]\n")
                break

            self.console.clear()

            try:
                if choice in [p.id for p in platforms]:
                    self.execute_platform_scrape(choice)
                elif choice == '9':
                    self.execute_bulk_scrape()
                elif choice == '10':
                    self.view_exports()
                elif choice == '11':
                    self.settings_menu()
            except KeyboardInterrupt:
                pass
            except Exception as e:
                self.console.print(f"\n[red]✗ System Error: {e}[/red]")

            Prompt.ask("\n[dim]Press Enter to return to menu[/dim]", default="")
            self.console.clear()
            self.show_header()

if __name__ == '__main__':
    _verbose = '--verbose' in sys.argv or '-V' in sys.argv
    logging.basicConfig(level=logging.DEBUG if _verbose else logging.WARNING, format='%(name)s: %(message)s')
    
    args = [a for a in sys.argv[1:] if a not in ('--verbose', '-V')]
    if args and args[0].lower() in ('--version', '-v', 'version'):
        print(f"{APP_NAME} v{__version__}")
        sys.exit(0)
        
    app = VertexApp()
    app.run()