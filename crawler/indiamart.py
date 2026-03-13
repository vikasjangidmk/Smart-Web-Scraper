"""
indiamart.py - IndiaMART B2B Marketplace Crawler (Rewritten)
=============================================================
Key fixes vs previous version:
  1. Searches each city SEPARATELY (IndiaMART doesn't support multi-city cq)
  2. Uses multiple keyword variants to maximise unique company discovery
  3. Only collects COMPANY ROOT profile pages (filters out /product, /testimonial sub-pages)
  4. Normalises company URLs to root profile to avoid duplicates
  5. Extracts company links from the dir.indiamart.com listing page cards
"""
import re
import random
import time
import yaml  # type: ignore
from pathlib import Path
from urllib.parse import urlparse
from playwright.sync_api import (  # type: ignore
    sync_playwright, Page, Browser, BrowserContext,
    TimeoutError as PWTimeout
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
    """Build IndiaMART search URL — one city at a time."""
    keyword_esc = keyword.strip().replace(" ", "+")
    city_esc = city.strip().replace(" ", "+")
    url = f"https://dir.indiamart.com/search.mp?ss={keyword_esc}&cq={city_esc}"
    if page_num > 1:
        url += f"&page={page_num}"
    return url


# ─── URL CLASSIFICATION ──────────────────────────────────────────────────────

# These patterns indicate non-company pages
EXCLUDE_URL_PATTERNS = [
    "corporate.indiamart", "help.indiamart", "buyer.indiamart",
    "seller.indiamart", "my.indiamart", "m.indiamart",
    "shopping.indiamart", "lens.indiamart", "hindi.indiamart",
    "export.indiamart", "paywith.indiamart", "shipwith.indiamart",
    "/search.mp", "/impcat/", "/city/", "/messages/",
    "#", "javascript:", "login", "register",
]

# Product/testimonial sub-pages — we want ONLY the company root profile
SUBPAGE_PATTERNS = [
    "/testimonial", "/proddetail", "/aboutus", "/contactus",
    "/profile", "/catalogue", "/certificate",
]


def _normalise_to_company_root(url: str) -> str | None:
    """
    Given any IndiaMART company URL, return the company ROOT profile URL.
    
    Examples:
        https://www.indiamart.com/jjenterprisegujarat/air-bubble-roll
        → https://www.indiamart.com/jjenterprisegujarat/
        
        https://www.indiamart.com/applpackaging/testimonial
        → https://www.indiamart.com/applpackaging/
        
        https://www.indiamart.com/starpackinnovation/
        → https://www.indiamart.com/starpackinnovation/
    """
    if not url or "indiamart.com" not in url.lower():
        return None

    # Reject known non-company URLs
    url_lower = url.lower()
    for pat in EXCLUDE_URL_PATTERNS:
        if pat in url_lower:
            return None

    try:
        parsed = urlparse(url)
        host = parsed.hostname or ""
    except Exception:
        return None

    # Pattern A: subdomain company pages like  starpack.indiamart.com
    # → convert to https://www.indiamart.com/starpack/
    if host.endswith(".indiamart.com") and host not in (
        "www.indiamart.com", "dir.indiamart.com", "m.indiamart.com"
    ):
        subdomain = host.replace(".indiamart.com", "")
        # Skip known non-company subdomains
        skip_subs = {
            "corporate", "help", "buyer", "seller", "my", "shopping",
            "lens", "hindi", "export", "paywith", "shipwith", "img",
            "utils", "api", "login", "s", "cdn",
        }
        if subdomain in skip_subs:
            return None
        return f"https://www.indiamart.com/{subdomain}/"

    # Pattern B: www.indiamart.com/company-name/...
    if host in ("www.indiamart.com", "indiamart.com"):
        path = parsed.path.strip("/")
        if not path:
            return None
        segments = path.split("/")
        if not segments or not segments[0]:
            return None
        company_slug = segments[0]
        # Reject if slug looks like a category or utility page
        non_company_slugs = {
            "search", "impcat", "city", "proddetail", "messages",
            "help", "about", "contact", "privacy", "terms",
            "html", "xml", "js", "css",
        }
        if company_slug.lower() in non_company_slugs:
            return None
        return f"https://www.indiamart.com/{company_slug}/"

    # Pattern C: dir.indiamart.com — this is a search/listing page, skip
    return None


def _extract_company_links_from_search(page: Page) -> list[str]:
    """
    Extract all unique company profile ROOT URLs from an IndiaMART search results page.
    Uses multiple strategies and normalises all URLs to company root.
    """
    raw_urls: set[str] = set()

    # Strategy 1: CSS selectors for company name links
    selectors = [
        "a.lcname",                             # main company name link
        ".organic-card a",                      # organic listing cards
        ".lst-crd-cnt a",                       # list card content
        ".company-name a",
        "h3.company-name a",
        ".sup-name a",
        ".supplierName a",
        "h3 a[href*='indiamart']",
        ".cname a",
        "[class*='company'] a[href*='indiamart']",
        "[class*='supplier'] a[href*='indiamart']",
        ".cardlinks a",
        ".flx99 a",
    ]
    for sel in selectors:
        try:
            for el in page.query_selector_all(sel):
                href = el.get_attribute("href") or ""
                if href:
                    raw_urls.add(href.split("?")[0])
        except Exception:
            pass

    # Strategy 2: All anchors on the page pointing to indiamart
    try:
        all_anchors = page.query_selector_all("a[href*='indiamart.com']")
        for anchor in all_anchors:
            try:
                href = anchor.get_attribute("href") or ""
                raw_urls.add(href.split("?")[0].rstrip("/") + "/")
            except Exception:
                continue
    except Exception:
        pass

    # Strategy 3: Regex extraction from raw page HTML
    try:
        content = page.content()
        # Match subdomain pattern: company.indiamart.com
        subdomain_matches = re.findall(
            r'https?://([a-z0-9\-]+)\.indiamart\.com',
            content, re.IGNORECASE
        )
        for sub in subdomain_matches:
            raw_urls.add(f"https://{sub}.indiamart.com/")

        # Match path pattern: www.indiamart.com/company-slug/
        path_matches = re.findall(
            r'https?://(?:www\.)?indiamart\.com/([a-z0-9\-]+)/?',
            content, re.IGNORECASE
        )
        for slug in path_matches:
            raw_urls.add(f"https://www.indiamart.com/{slug}/")
    except Exception:
        pass

    # Normalise all URLs to company root profiles
    company_roots: set[str] = set()
    for url in raw_urls:
        root = _normalise_to_company_root(url)
        if root:
            company_roots.add(root)

    result = list(company_roots)
    logger.info(f"[indiamart] Extracted {len(result)} unique company root profiles")
    return result


def _click_view_mobile(page: Page) -> None:
    """Click 'View Mobile Number' buttons to reveal hidden phone numbers."""
    btn_selectors = [
        "button:has-text('View Mobile Number')",
        "span:has-text('View Mobile Number')",
        "a:has-text('View Mobile Number')",
        "button:has-text('Contact Supplier')",
        ".view-mobile-number",
        "[class*='mobile-number']",
        "button:has-text('Get Phone')",
        "[data-show-phone]",
    ]
    for sel in btn_selectors:
        try:
            btns = page.query_selector_all(sel)
            for btn in btns:
                if btn.is_visible():
                    btn.scroll_into_view_if_needed()
                    page.wait_for_timeout(500)
                    btn.click(force=True, timeout=3000)
                    page.wait_for_timeout(2000)
                    logger.debug(f"[indiamart] Revealed phone: {sel}")
        except Exception:
            pass


def _detect_captcha(page: Page) -> bool:
    """Return True if CAPTCHA/bot detection is present."""
    try:
        content = page.content().lower()
    except Exception:
        return False
    signals = ["captcha", "recaptcha", "i am not a robot",
               "verify you are human", "hcaptcha", "access denied"]
    return any(s in content for s in signals)


def _scroll_page(page: Page, steps: int = 6) -> None:
    """Scroll page incrementally to trigger lazy-loaded content."""
    try:
        for i in range(steps):
            page.evaluate(f"window.scrollTo(0, document.body.scrollHeight * {(i+1)/steps})")
            page.wait_for_timeout(700)
    except Exception:
        pass


def crawl_indiamart(
    keyword: str | None = None,
    city: str | None = None,
    max_companies: int = 120,
    max_pages: int = 5,
    visited_urls: list[str] | None = None,
) -> list[tuple[str, str]]:
    """
    Crawl IndiaMART using MULTIPLE keyword + city combinations.
    
    Reads search_queries from settings.yaml and runs each combo individually.
    This is critical because IndiaMART's cq param only works with ONE city.
    
    Returns:
        List of (company_url, html_content) tuples.
    """
    config = _load_config()
    crawler_cfg = config.get("crawler", {})
    search_queries = config.get("search_queries", [])
    sources_cfg = config.get("sources", {})
    im_max_pages = sources_cfg.get("indiamart", {}).get("max_pages", max_pages)

    visited: set[str] = set(visited_urls or [])
    results: list[tuple[str, str]] = []

    min_delay = crawler_cfg.get("min_delay", 3)
    max_delay = crawler_cfg.get("max_delay", 8)
    headless = crawler_cfg.get("headless", True)
    timeout = crawler_cfg.get("timeout", 45000)

    # If no search_queries in config, fall back to legacy keyword/city args
    if not search_queries:
        if keyword and city:
            search_queries = [{"keyword": keyword, "cities": [city]}]
        else:
            logger.error("[indiamart] No search queries configured!")
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

        # Mask automation signals
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.chrome = { runtime: {} };
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        """)

        # ── Phase 1: Collect company URLs from all search queries ─────────
        all_company_urls: set[str] = set()

        for query in search_queries:
            kw = str(query.get("keyword", ""))
            cities = query.get("cities", [])
            
            for city_name in cities:
                city_str = str(city_name)
                if len(all_company_urls) >= max_companies:
                    break

                logger.info(f"\n[indiamart] 🔍 Searching: '{kw}' in {city_str}")

                for page_num in range(1, im_max_pages + 1):
                    if len(all_company_urls) >= max_companies:
                        break

                    search_url = _build_search_url(kw, city_str, page_num)
                    logger.info(f"[indiamart] Search page {page_num}/{im_max_pages}: {search_url}")

                    page_found_new = False
                    for attempt in range(3):
                        page = None
                        try:
                            page = context.new_page()
                            page.goto(search_url, wait_until="domcontentloaded", timeout=timeout)
                            random_delay(2, 4)

                            if _detect_captcha(page):
                                logger.warning(f"[indiamart] CAPTCHA (attempt {attempt+1})")
                                page.close()
                                page = None
                                time.sleep(5 + attempt * 3)
                                continue

                            # Scroll heavily to load all lazy results
                            _scroll_page(page, steps=10)
                            page.wait_for_timeout(2000)

                            links = _extract_company_links_from_search(page)
                            new_count = 0
                            for link in links:
                                if link not in visited and link not in all_company_urls:
                                    all_company_urls.add(link)
                                    new_count += 1

                            logger.info(
                                f"[indiamart] Page {page_num}: +{new_count} new "
                                f"(total unique: {len(all_company_urls)})"
                            )
                            page_found_new = new_count > 0
                            break  # Success — exit retry loop
                        except Exception as e:
                            logger.warning(f"[indiamart] Attempt {attempt+1}/3 failed: {e}")
                            time.sleep(3 + attempt * 2)
                        finally:
                            if page is not None:
                                try:
                                    page.close()
                                except Exception:
                                    pass

                    # If no new links found on this page, skip remaining pages for this query
                    if not page_found_new and page_num >= 2:
                        logger.info(f"[indiamart] No new results for '{kw}' in {city_name}, moving on")
                        break

                    random_delay(min_delay, max_delay)

        logger.info(f"\n[indiamart] ════ Total unique company URLs: {len(all_company_urls)} ════")

        # ── Phase 2: Visit each company profile page ──────────────────────
        url_list = list(all_company_urls)
        random.shuffle(url_list)  # Randomise visit order to reduce detection

        for url in url_list:
            if len(results) >= max_companies:
                break
            if url in visited:
                continue

            logger.info(f"[indiamart] Visiting: {url}")

            page = None
            for attempt in range(2):
                try:
                    page = context.new_page()
                    page.goto(url, wait_until="domcontentloaded", timeout=timeout)
                    break
                except Exception as e:
                    logger.warning(f"[indiamart] Visit attempt {attempt+1}: {e}")
                    if page is not None:
                        try:
                            page.close()
                        except Exception:
                            pass
                        page = None
                    time.sleep(3)

            if page is None:
                visited.add(url)
                continue

            try:
                random_delay(1, 3)

                if _detect_captcha(page):
                    logger.warning(f"[indiamart] CAPTCHA on: {url}")
                    visited.add(url)
                    continue

                # Try to reveal hidden phone numbers
                _click_view_mobile(page)

                # Scroll to load lazy content
                _scroll_page(page, steps=4)
                page.wait_for_timeout(1000)

                html = page.content() or ""
                if len(html) > 500:  # Minimum viable page
                    results.append((url, html))
                    visited.add(url)
                    logger.info(f"[indiamart] ✓ Collected ({len(results)}): {url}")
                else:
                    logger.warning(f"[indiamart] Page too small: {url}")
                    visited.add(url)

            except PWTimeout:
                logger.warning(f"[indiamart] Timeout: {url}")
                visited.add(url)
            except Exception as e:
                logger.error(f"[indiamart] Error on {url}: {e}")
                visited.add(url)
            finally:
                try:
                    if page:
                        page.close()
                except Exception:
                    pass

            random_delay(min_delay, max_delay)

        browser.close()

    logger.info(f"[indiamart] Crawl complete. Collected {len(results)} company pages.")
    return results
