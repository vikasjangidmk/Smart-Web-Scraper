"""
tradeindia.py - TradeIndia B2B Marketplace Crawler (Optimized)
================================================================
Optimized with request interception, reduced timeouts, early stopping.
"""
import re
import random
import time
import yaml  # type: ignore
from pathlib import Path
from urllib.parse import urlparse, urljoin
from playwright.sync_api import (  # type: ignore
    sync_playwright, Page, Browser, BrowserContext,
    TimeoutError as PWTimeout, Route,
)

from utils.logger import get_logger  # type: ignore
from utils.retry import random_delay  # type: ignore

logger = get_logger(__name__)
CONFIG_PATH = Path(__file__).parent.parent / "config" / "settings.yaml"


def _load_config() -> dict:
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def _get_random_ua(config: dict) -> str:
    agents = config.get("user_agents", [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ])
    return random.choice(agents)


def _build_search_url(keyword: str, city: str, page_num: int = 1) -> str:
    """Build TradeIndia search URL."""
    keyword_esc = keyword.strip().replace(" ", "+")
    city_esc = city.strip().lower().replace(" ", "+")
    url = f"https://www.tradeindia.com/search.html?keyword={keyword_esc}&city={city_esc}"
    if page_num > 1:
        url += f"&page={page_num}"
    return url


# ─── REQUEST INTERCEPTION ─────────────────────────────────────────────────────
BLOCKED_RESOURCE_TYPES = {"image", "media", "font", "stylesheet"}
BLOCKED_URL_PATTERNS = [
    ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico",
    ".woff", ".woff2", ".ttf", ".eot",
    ".css", ".mp4", ".mp3",
    "google-analytics", "googletagmanager", "facebook",
    "doubleclick", "analytics", "tracking",
]


def _intercept_route(route: Route) -> None:
    """Block unnecessary resources."""
    req = route.request
    if req.resource_type in BLOCKED_RESOURCE_TYPES:
        route.abort()
        return
    url_lower = req.url.lower()
    for pattern in BLOCKED_URL_PATTERNS:
        if pattern in url_lower:
            route.abort()
            return
    route.continue_()


# ─── URL CLASSIFICATION ──────────────────────────────────────────────────────
EXCLUDE_URL_PATTERNS = [
    "/login", "/register", "/join_now",
    "javascript:", "#", "/advertise",
    "/help/", "/about/", "/sitemap",
    "/blog/", "/newsletter/",
    "/search.html",
]

COMPANY_URL_PATTERNS = [
    re.compile(r"https?://(?:www\.)?tradeindia\.com/fp\d+/", re.IGNORECASE),
    re.compile(r"https?://(?:www\.)?tradeindia\.com/Ede\d+/", re.IGNORECASE),
]


def _is_company_profile_url(url: str) -> bool:
    if not url:
        return False
    url_lower = url.lower()
    for pat in EXCLUDE_URL_PATTERNS:
        if pat.lower() in url_lower:
            return False
    for pattern in COMPANY_URL_PATTERNS:
        if pattern.match(url):
            return True
    return False


def _normalise_company_url(url: str) -> str | None:
    if not url or "tradeindia.com" not in url.lower():
        return None
    url_lower = url.lower()
    for pat in EXCLUDE_URL_PATTERNS:
        if pat.lower() in url_lower:
            return None
    try:
        parsed = urlparse(url)
        host = parsed.hostname or ""
    except Exception:
        return None
    if host not in ("www.tradeindia.com", "tradeindia.com"):
        return None
    path = parsed.path.strip("/")
    if not path:
        return None
    segments = path.split("/")
    if not segments or not segments[0]:
        return None
    first_segment = segments[0]
    if re.match(r"^(fp|Ede)\d+$", first_segment, re.IGNORECASE):
        return f"https://www.tradeindia.com/{first_segment}/"
    return None


def _extract_company_links_from_search(page: Page) -> list[str]:
    """Extract company profile URLs from search results."""
    raw_urls: set[str] = set()
    
    selectors = [
        "a[href*='/fp']", "a[href*='/Ede']",
        ".company-name a", ".comp-name a",
        "h2 a[href*='tradeindia.com']", "h3 a[href*='tradeindia.com']",
        ".product-card a[href*='tradeindia.com/fp']",
        ".srp-card a[href*='tradeindia.com']",
        ".listing a[href*='tradeindia.com/fp']",
        "[class*='company'] a", "[class*='supplier'] a",
    ]
    for sel in selectors:
        try:
            for el in page.query_selector_all(sel):
                href = el.get_attribute("href") or ""
                if href:
                    full = href if href.startswith("http") else urljoin("https://www.tradeindia.com/", href)
                    raw_urls.add(full.split("?")[0])
        except Exception:
            pass
    
    try:
        content = page.content()
        fp_matches = re.findall(
            r'https?://(?:www\.)?tradeindia\.com/(fp\d+)/?',
            content, re.IGNORECASE
        )
        for fp in fp_matches:
            raw_urls.add(f"https://www.tradeindia.com/{fp}/")
    except Exception:
        pass
    
    company_urls: set[str] = set()
    for url in raw_urls:
        norm = _normalise_company_url(url)
        if norm:
            company_urls.add(norm)
    
    result = list(company_urls)
    logger.info(f"[tradeindia] Extracted {len(result)} company profiles")
    return result


def _detect_captcha(page: Page) -> bool:
    try:
        content = page.content().lower()
    except Exception:
        return False
    signals = ["captcha", "recaptcha", "i am not a robot",
               "verify you are human", "access denied", "blocked"]
    return any(s in content for s in signals)


def _scroll_page(page: Page, steps: int = 3) -> None:
    try:
        for i in range(steps):
            page.evaluate(f"window.scrollTo(0, document.body.scrollHeight * {(i+1)/steps})")
            page.wait_for_timeout(200)
    except Exception:
        pass


def crawl_tradeindia(
    max_companies: int = 30,
    max_pages: int = 2,
    visited_urls: list[str] | None = None,
) -> list[tuple[str, str]]:
    """Crawl TradeIndia with request interception and early stopping."""
    config = _load_config()
    crawler_cfg = config.get("crawler", {})
    search_queries = config.get("search_queries", [])
    sources_cfg = config.get("sources", {})
    ti_cfg = sources_cfg.get("tradeindia", {})
    
    if not ti_cfg.get("enabled", True):
        logger.info("[tradeindia] Disabled in config")
        return []
    
    ti_max_pages = ti_cfg.get("max_pages", max_pages)
    visited: set[str] = set(visited_urls or [])
    results: list[tuple[str, str]] = []
    
    headless = crawler_cfg.get("headless", True)
    timeout = crawler_cfg.get("timeout", 10000)
    
    if not search_queries:
        logger.error("[tradeindia] No search queries configured!")
        return []
    
    with sync_playwright() as p:
        ua = _get_random_ua(config)
        browser: Browser = p.chromium.launch(
            headless=headless,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-extensions",
                "--disable-background-networking",
                "--no-first-run",
            ],
        )
        context: BrowserContext = browser.new_context(
            user_agent=ua,
            viewport={
                "width": crawler_cfg.get("viewport_width", 1366),
                "height": crawler_cfg.get("viewport_height", 768),
            },
            locale="en-IN",
            timezone_id="Asia/Kolkata",
            extra_http_headers={
                "Accept-Language": "en-IN,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Referer": "https://www.google.com/",
            },
        )
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.chrome = { runtime: {} };
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        """)

        # Enable request interception
        context.route("**/*", _intercept_route)
        
        # ── Phase 1: Collect company URLs ─────────────────────────────────
        all_company_urls: set[str] = set()
        
        for query in search_queries:
            kw = str(query.get("keyword", ""))
            cities = query.get("cities", [])
            
            for city_name in cities:
                city_str = str(city_name)
                if len(all_company_urls) >= max_companies:
                    break
                
                logger.info(f"[tradeindia] 🔍 '{kw}' in {city_str}")
                
                for page_num in range(1, ti_max_pages + 1):
                    if len(all_company_urls) >= max_companies:
                        break
                    
                    search_url = _build_search_url(kw, city_str, page_num)
                    page = None
                    try:
                        page = context.new_page()  # type: ignore
                        page.goto(search_url, wait_until="domcontentloaded", timeout=timeout)
                        random_delay(0.2, 0.5)
                        
                        if _detect_captcha(page):
                            continue
                        
                        _scroll_page(page, steps=2)
                        page.wait_for_timeout(300)
                        
                        links = _extract_company_links_from_search(page)
                        new_count: int = 0
                        for link in links:
                            if link not in visited and link not in all_company_urls:
                                all_company_urls.add(link)
                                new_count = new_count + 1  # type: ignore
                        
                        logger.info(f"[tradeindia] Page {page_num}: +{new_count} (total: {len(all_company_urls)})")
                        if new_count == 0 and page_num >= 2:
                            break
                    except Exception as e:
                        logger.warning(f"[tradeindia] Search failed: {e}")
                    finally:
                        if page:
                            try:
                                page.close()
                            except Exception:
                                pass
                    
                    random_delay(0.2, 0.5)
        
        logger.info(f"[tradeindia] Total unique URLs: {len(all_company_urls)}")
        
        # ── Phase 2: Visit each company profile ──────────────────────────
        url_list = list(all_company_urls)
        random.shuffle(url_list)
        
        for url in url_list:
            if len(results) >= max_companies:
                break
            if url in visited:
                continue
            
            page = None
            try:
                page = context.new_page()  # type: ignore
                page.goto(url, wait_until="domcontentloaded", timeout=timeout)
                random_delay(0.2, 0.5)
                
                if _detect_captcha(page):
                    visited.add(url)
                    continue
                
                _scroll_page(page, steps=2)
                
                html = page.content() or ""
                if len(html) > 500:
                    results.append((url, html))
                    visited.add(url)
                    logger.info(f"[tradeindia] ✓ Collected ({len(results)}): {url}")
                else:
                    visited.add(url)
            except PWTimeout:
                visited.add(url)
            except Exception as e:
                logger.warning(f"[tradeindia] Error: {e}")
                visited.add(url)
            finally:
                if page:
                    try:
                        page.close()
                    except Exception:
                        pass
            
            random_delay(0.2, 0.5)
        
        browser.close()
    
    logger.info(f"[tradeindia] Crawl complete. Collected {len(results)} pages.")
    return results
