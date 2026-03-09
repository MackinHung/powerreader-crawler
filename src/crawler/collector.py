"""
Stage A — Title & URL Collection from RSS feeds and JSON APIs.
Copyright (C) 2026 @MackinHung (https://github.com/MackinHung)

Collects article metadata (title, url, source, published_at, summary)
without fetching full article content.
"""

import hashlib
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

import feedparser
import requests

from .config import (
    API_SOURCES,
    BOT_USER_AGENT,
    HEADERS,
    REQUEST_TIMEOUT,
    RSS_SOURCES,
    SITEMAP_SOURCES,
    TW_TZ,
)


# ------------------------------------------------------------------
# Data types
# ------------------------------------------------------------------

def make_article_meta(
    *,
    source: str,
    title: str,
    url: str,
    published_at: datetime | None,
    summary: str,
    feed_category: str = "綜合",
) -> dict:
    """Create an immutable article metadata record."""
    clean_title = re.sub(r"\s+", "", title.strip())
    return {
        "source": source,
        "title": title.strip(),
        "url": url.strip(),
        "published_at": published_at,
        "summary": summary[:500] if summary else "",
        "feed_category": feed_category,
        "title_hash": hashlib.sha256(
            clean_title.encode("utf-8")
        ).hexdigest()[:16],
    }


# ------------------------------------------------------------------
# Date parsing
# ------------------------------------------------------------------

_DATE_FORMATS = [
    "%a, %d %b %Y %H:%M:%S %z",
    "%a, %d %b %Y %H:%M:%S %Z",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
    "%Y-%m-%d",
]


def parse_pub_time(entry) -> datetime | None:
    """Extract publication time from a feedparser entry."""
    # Try parsed struct first
    for attr in ("published_parsed", "updated_parsed"):
        struct = getattr(entry, attr, None)
        if struct is not None:
            try:
                import calendar
                ts = calendar.timegm(struct)
                return datetime.fromtimestamp(ts, tz=timezone.utc)
            except (ValueError, OverflowError):
                continue

    # Try raw string
    raw = getattr(entry, "published", None) or getattr(entry, "updated", None)
    if not raw:
        return None

    raw = raw.strip()

    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(raw, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=TW_TZ)
            return dt
        except (ValueError, TypeError):
            continue

    # RFC 2822 fallback
    try:
        return parsedate_to_datetime(raw)
    except Exception:
        return None


def parse_datetime_str(dt_str: str) -> datetime | None:
    """Parse a date string (for API responses)."""
    if not dt_str:
        return None

    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(dt_str.strip(), fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=TW_TZ)
            return dt
        except (ValueError, TypeError):
            continue

    return None


# ------------------------------------------------------------------
# RSS collection
# ------------------------------------------------------------------

def _clean_html(text: str) -> str:
    """Strip HTML tags from text."""
    return re.sub(r"<[^>]+>", "", text).strip()


def _extract_entry_category(entry) -> str | None:
    """Extract first category tag from a feedparser entry.

    RSS <category> tags are parsed by feedparser into entry.tags:
      [{'term': '政治', 'scheme': None, 'label': None}, ...]
    Returns the first term, or None if no tags.
    """
    tags = getattr(entry, "tags", None)
    if tags and isinstance(tags, list) and len(tags) > 0:
        term = tags[0].get("term", "")
        if term and len(term) <= 20:  # Skip garbage/encoded tags
            return term
    return None


def collect_rss(source: dict) -> list[dict]:
    """Collect articles from RSS feeds for a single source.

    Returns a list of article metadata dicts.
    """
    articles = []
    seen_urls: set[str] = set()
    fix_prefix = source.get("fix_relative_urls", "")
    url_replace = source.get("url_replace")

    for feed_item in source["feeds"]:
        # Support both old string format and new {url, category} dict
        if isinstance(feed_item, str):
            feed_url = feed_item
            feed_category = "綜合"
        else:
            feed_url = feed_item["url"]
            feed_category = feed_item.get("category", "綜合")

        try:
            feed = feedparser.parse(feed_url, agent=BOT_USER_AGENT)

            if feed.bozo and not feed.entries:
                print(f"    [WARN] Parse error: {feed_url}")
                continue

            for entry in feed.entries:
                title = getattr(entry, "title", "") or ""
                link = getattr(entry, "link", "") or ""

                if not title.strip() or not link.strip():
                    continue

                # Fix relative URLs (Storm Media)
                if not link.startswith("http"):
                    if fix_prefix:
                        link = fix_prefix + link
                    else:
                        continue

                # Replace broken domain in feed URLs (CTV)
                if url_replace:
                    link = link.replace(url_replace[0], url_replace[1])

                # Strip tracking params
                link = link.split("?utm_")[0]

                # Fix Storm Media article URLs: /NNNNN -> /article/NNNNN
                if "storm.mg/" in link:
                    link = re.sub(
                        r"(storm\.mg)/(\d+)$",
                        r"\1/article/\2",
                        link,
                    )

                if link in seen_urls:
                    continue
                seen_urls.add(link)

                pub_time = parse_pub_time(entry)
                summary = _clean_html(
                    getattr(entry, "summary", "") or ""
                )

                # Article-level category from RSS <category> tag
                # overrides feed-level category for mixed feeds
                entry_cat = _extract_entry_category(entry)
                category = entry_cat if entry_cat else feed_category

                articles.append(make_article_meta(
                    source=source["key"],
                    title=title,
                    url=link,
                    published_at=pub_time,
                    summary=summary,
                    feed_category=category,
                ))

        except Exception as e:
            print(f"    [ERROR] {feed_url}: {e}")

    return articles


# ------------------------------------------------------------------
# UDN API collection (with pagination)
# ------------------------------------------------------------------

def collect_udn_api(source: dict) -> list[dict]:
    """Collect articles from UDN JSON API with pagination.

    UDN returns max 20 articles per page.
    """
    articles = []
    seen_urls: set[str] = set()
    max_pages = source.get("max_pages", 5)
    url_prefix = source.get("url_prefix", "https://udn.com")
    source_category = source.get("category", "綜合")

    for page in range(max_pages):
        try:
            params = {
                "page": str(page),
                "id": "",
                "totalRecNo": "100",
                **source["params"],
            }

            resp = requests.get(
                source["api_url"],
                params=params,
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT,
            )

            if resp.status_code != 200:
                print(f"    [WARN] Page {page}: HTTP {resp.status_code}")
                break

            data = resp.json()
            items = data.get("lists", [])

            if not items:
                break

            for item in items:
                title = item.get("title", "")
                title_link = item.get("titleLink", "")

                if not title or not title_link:
                    continue

                clean_path = title_link.split("?")[0]
                url = f"{url_prefix}{clean_path}"

                if url in seen_urls:
                    continue
                seen_urls.add(url)

                time_obj = item.get("time", {})
                time_str = time_obj.get("date", "") if isinstance(time_obj, dict) else ""
                pub_time = parse_datetime_str(time_str)

                # UDN API cate_name field as article-level category
                api_cat = item.get("cate_name", "")
                category = api_cat if api_cat else source_category

                articles.append(make_article_meta(
                    source=source["key"],
                    title=title,
                    url=url,
                    published_at=pub_time,
                    summary="",
                    feed_category=category,
                ))

            if data.get("end", False):
                break

            if page < max_pages - 1:
                time.sleep(1)

        except Exception as e:
            print(f"    [ERROR] API page {page}: {e}")
            break

    return articles


# ------------------------------------------------------------------
# Sitemap collection (Google News Sitemap XML)
# ------------------------------------------------------------------

def collect_sitemap(source: dict) -> list[dict]:
    """Collect articles from a Google News Sitemap XML.

    Parses <url> elements with <news:news> children to extract
    title, link, and publication date.
    """
    import xml.etree.ElementTree as ET

    try:
        resp = requests.get(
            source["sitemap_url"],
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
        )

        if resp.status_code != 200:
            print(f"    [WARN] Sitemap HTTP {resp.status_code}")
            return []

        root = ET.fromstring(resp.content)

        ns = {
            "sm": "http://www.sitemaps.org/schemas/sitemap/0.9",
            "news": "http://www.google.com/schemas/sitemap-news/0.9",
        }

        articles = []
        seen_urls: set[str] = set()

        for url_elem in root.findall("sm:url", ns):
            loc = (url_elem.findtext("sm:loc", "", ns) or "").strip()
            if not loc or loc in seen_urls:
                continue
            seen_urls.add(loc)

            news_elem = url_elem.find("news:news", ns)
            title = ""
            pub_date = None

            if news_elem is not None:
                title = (
                    news_elem.findtext("news:title", "", ns) or ""
                ).strip()
                date_str = (
                    news_elem.findtext(
                        "news:publication_date", "", ns
                    ) or ""
                ).strip()
                pub_date = parse_datetime_str(date_str)

                # Sitemap dates may lack time precision (e.g. "2026-03-08").
                # Default midnight causes premature freshness expiry.
                # Bump to noon so articles stay fresh 04:00-20:00 TW time.
                if pub_date and pub_date.hour == 0 and pub_date.minute == 0:
                    if len(date_str) <= 10:  # "YYYY-MM-DD" only
                        pub_date = pub_date.replace(hour=12)

            if not title or not loc:
                continue

            articles.append(make_article_meta(
                source=source["key"],
                title=title,
                url=loc,
                published_at=pub_date,
                summary="",
                feed_category=source.get("category", "綜合"),
            ))

        return articles

    except Exception as e:
        print(f"    [ERROR] Sitemap {source['key']}: {e}")
        return []


# ------------------------------------------------------------------
# Collect all sources
# ------------------------------------------------------------------

def collect_all() -> list[dict]:
    """Collect article metadata from all configured sources.

    Returns a deduplicated list of article metadata dicts.
    """
    all_articles: list[dict] = []
    source_counts: dict[str, int] = {}

    # RSS sources
    for src in RSS_SOURCES:
        print(f"  [{src['key']}] {src['name']}")
        time.sleep(1)

        articles = collect_rss(src)
        source_counts[src["key"]] = len(articles)
        all_articles.extend(articles)
        print(f"    -> {len(articles)} articles")

    # API sources
    for src in API_SOURCES:
        print(f"  [{src['key']}] {src['name']}")
        time.sleep(1)

        articles = collect_udn_api(src)
        source_counts[src["key"]] = len(articles)
        all_articles.extend(articles)
        print(f"    -> {len(articles)} articles")

    # Sitemap sources
    for src in SITEMAP_SOURCES:
        print(f"  [{src['key']}] {src['name']}")
        time.sleep(1)

        articles = collect_sitemap(src)
        source_counts[src["key"]] = len(articles)
        all_articles.extend(articles)
        print(f"    -> {len(articles)} articles")

    # Deduplicate by URL
    seen: set[str] = set()
    unique = []
    for art in all_articles:
        if art["url"] not in seen:
            seen.add(art["url"])
            unique.append(art)

    dup_count = len(all_articles) - len(unique)
    if dup_count > 0:
        print(f"\n  Removed {dup_count} URL duplicates")

    print(f"  Total unique: {len(unique)} articles from {len(source_counts)} sources")
    return unique
