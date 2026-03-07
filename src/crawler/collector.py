"""
Stage A — Title & URL Collection from RSS feeds and JSON APIs.

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
    HEADERS,
    REQUEST_TIMEOUT,
    RSS_SOURCES,
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
) -> dict:
    """Create an immutable article metadata record."""
    clean_title = re.sub(r"\s+", "", title.strip())
    return {
        "source": source,
        "title": title.strip(),
        "url": url.strip(),
        "published_at": published_at,
        "summary": summary[:500] if summary else "",
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


def collect_rss(source: dict) -> list[dict]:
    """Collect articles from RSS feeds for a single source.

    Returns a list of article metadata dicts.
    """
    articles = []
    seen_urls: set[str] = set()
    fix_prefix = source.get("fix_relative_urls", "")

    for feed_url in source["feeds"]:
        try:
            feed = feedparser.parse(feed_url)

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

                articles.append(make_article_meta(
                    source=source["key"],
                    title=title,
                    url=link,
                    published_at=pub_time,
                    summary=summary,
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

                articles.append(make_article_meta(
                    source=source["key"],
                    title=title,
                    url=url,
                    published_at=pub_time,
                    summary="",
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
