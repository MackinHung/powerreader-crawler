"""
robots.txt compliance checker with per-domain caching.
Copyright (C) 2026 @MackinHung (https://github.com/MackinHung)

Checks robots.txt before crawling any article page.
Respects Crawl-delay directives.

Standard behavior:
  - If robots.txt returns 404 or is unreachable → ALLOW (standard practice)
  - If robots.txt disallows our UA → BLOCK
  - Crawl-delay from robots.txt overrides default rate limiting
"""

import time
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

from .config import BOT_USER_AGENT, BOT_NAME, ROBOTS_TXT_TIMEOUT, ROBOTS_CACHE_TTL


class RobotsChecker:
    """Per-domain robots.txt checker with caching."""

    def __init__(self):
        self._cache: dict[str, _CacheEntry] = {}
        self._stats = {"checked": 0, "allowed": 0, "blocked": 0, "errors": 0}

    def can_fetch(self, url: str) -> bool:
        """Check if our bot is allowed to fetch this URL.

        Returns True if allowed, False if disallowed by robots.txt.
        """
        self._stats["checked"] += 1
        parser = self._get_parser(url)

        if parser is None:
            # robots.txt unreachable → allow (standard practice)
            self._stats["allowed"] += 1
            return True

        allowed = parser.can_fetch(BOT_USER_AGENT, url)

        # Also check wildcard if specific UA not found
        if not allowed:
            allowed = parser.can_fetch("*", url)

        if allowed:
            self._stats["allowed"] += 1
        else:
            self._stats["blocked"] += 1

        return allowed

    def get_crawl_delay(self, url: str) -> float | None:
        """Get Crawl-delay for our bot from robots.txt.

        Returns seconds to wait between requests, or None if not specified.
        """
        parser = self._get_parser(url)
        if parser is None:
            return None

        # Try our specific UA first, then wildcard
        delay = parser.crawl_delay(BOT_USER_AGENT)
        if delay is None:
            delay = parser.crawl_delay("*")

        return float(delay) if delay is not None else None

    def prefetch_domains(self, urls: list[str]) -> dict[str, bool]:
        """Pre-fetch robots.txt for all unique domains in the URL list.

        Returns dict mapping domain -> whether robots.txt was loaded.
        """
        domains_seen: set[str] = set()
        results: dict[str, bool] = {}

        for url in urls:
            domain = self._domain_key(url)
            if domain not in domains_seen:
                domains_seen.add(domain)
                parser = self._get_parser(url)
                results[domain] = parser is not None

        return results

    @property
    def stats(self) -> dict:
        return dict(self._stats)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _domain_key(self, url: str) -> str:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"

    def _get_parser(self, url: str) -> RobotFileParser | None:
        """Get cached robots.txt parser for the domain of the given URL."""
        domain = self._domain_key(url)

        # Check cache
        entry = self._cache.get(domain)
        if entry is not None and not entry.is_expired():
            return entry.parser

        # Fetch and parse robots.txt
        robots_url = f"{domain}/robots.txt"
        parser = self._fetch_robots(robots_url)

        self._cache[domain] = _CacheEntry(parser=parser)
        return parser

    def _fetch_robots(self, robots_url: str) -> RobotFileParser | None:
        """Fetch and parse a robots.txt file.

        Returns None if unreachable (404, timeout, etc.).
        """
        try:
            req = Request(
                robots_url,
                headers={"User-Agent": BOT_USER_AGENT},
                method="GET",
            )

            with urlopen(req, timeout=ROBOTS_TXT_TIMEOUT) as resp:
                if resp.status != 200:
                    return None

                content = resp.read().decode("utf-8", errors="replace")

            parser = RobotFileParser()
            parser.parse(content.splitlines())
            return parser

        except HTTPError as e:
            if e.code in (404, 410):
                # No robots.txt → everything allowed
                # Return a permissive parser
                parser = RobotFileParser()
                parser.parse([])  # Empty = allow all
                return parser
            self._stats["errors"] += 1
            return None

        except (URLError, TimeoutError, OSError):
            self._stats["errors"] += 1
            return None

        except Exception:
            self._stats["errors"] += 1
            return None


class _CacheEntry:
    """Cached robots.txt parse result with TTL."""

    def __init__(self, parser: RobotFileParser | None):
        self.parser = parser
        self.fetched_at = time.monotonic()

    def is_expired(self) -> bool:
        return (time.monotonic() - self.fetched_at) > ROBOTS_CACHE_TTL
