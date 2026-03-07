"""
Stage F — Push articles to PowerReader API.

Sends cleaned and deduplicated articles to the PowerReader
Cloudflare Workers endpoint via POST /api/v1/articles/batch.

Authentication: Bearer token (POWERREADER_API_KEY env var).
Batch size: max 50 articles per request (API limit).

Usage:
    from .pusher import push_articles
    result = push_articles(articles)
"""

import json
import os
import time
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------

BATCH_SIZE = 50  # API maximum per request
REQUEST_TIMEOUT = 30  # seconds
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds (doubles each retry)

# Default API URL — override with POWERREADER_API_URL env var
DEFAULT_API_URL = "https://api.powerreader.dev/api/v1"


def _get_config() -> tuple[str, str]:
    """Get API URL and API key from environment."""
    api_url = os.environ.get("POWERREADER_API_URL", DEFAULT_API_URL)
    api_key = os.environ.get("POWERREADER_API_KEY", "")
    return api_url.rstrip("/"), api_key


# ------------------------------------------------------------------
# Article formatter
# ------------------------------------------------------------------

def _format_article(article: dict) -> dict:
    """Format a pipeline article dict to match API schema.

    Maps internal field names to the API contract defined in
    T01_SYSTEM_ARCHITECTURE/API_ROUTES.md.
    """
    dedup = article.get("dedup_metadata", {})

    return {
        "article_id": article["article_id"],
        "content_hash": article["content_hash"],
        "title": article["title"],
        "summary": article.get("summary", ""),
        "author": article.get("author"),
        "content_markdown": article["content_markdown"],
        "char_count": article["char_count"],
        "source": article["source"],
        "primary_url": article["primary_url"],
        "duplicate_urls": dedup.get("duplicate_urls", []),
        "published_at": article.get("published_at", ""),
        "crawled_at": article.get("crawled_at", ""),
        "filter_score": article.get("filter_score", 0.0),
        "matched_topic": article.get("matched_topic", ""),
        "dedup_metadata": {
            "article_type": dedup.get("article_type", "original"),
            "max_similarity": dedup.get("max_similarity", 0.0),
            "cluster_size": dedup.get("cluster_size", 1),
            "duplicate_urls": dedup.get("duplicate_urls", []),
        },
        "status": "deduplicated",
    }


# ------------------------------------------------------------------
# HTTP push with retry
# ------------------------------------------------------------------

def _post_batch(
    url: str,
    api_key: str,
    articles: list[dict],
) -> dict:
    """POST a batch of articles with retry logic.

    Returns:
        {"accepted": int, "rejected": int, "errors": list}
    """
    payload = json.dumps({"articles": articles}, ensure_ascii=False).encode("utf-8")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = Request(url, data=payload, headers=headers, method="POST")
            with urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                body = json.loads(resp.read().decode("utf-8"))

                if body.get("success"):
                    return body["data"]
                else:
                    error_msg = body.get("error", {}).get("message", "Unknown error")
                    return {
                        "accepted": 0,
                        "rejected": len(articles),
                        "errors": [{"reason": error_msg}],
                    }

        except HTTPError as e:
            last_error = f"HTTP {e.code}: {e.reason}"
            # Don't retry on 4xx (client errors)
            if 400 <= e.code < 500:
                return {
                    "accepted": 0,
                    "rejected": len(articles),
                    "errors": [{"reason": last_error}],
                }

        except (URLError, TimeoutError, OSError) as e:
            last_error = str(e)

        if attempt < MAX_RETRIES:
            delay = RETRY_DELAY * (2 ** (attempt - 1))
            print(f"    Retry {attempt}/{MAX_RETRIES} in {delay}s ({last_error})")
            time.sleep(delay)

    return {
        "accepted": 0,
        "rejected": len(articles),
        "errors": [{"reason": f"All {MAX_RETRIES} retries failed: {last_error}"}],
    }


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------

def push_articles(
    articles: list[dict],
    *,
    dry_run: bool = False,
) -> dict:
    """Push articles to PowerReader API in batches.

    Args:
        articles: List of pipeline article dicts (post-dedup).
        dry_run: If True, format articles but don't send.

    Returns:
        {
            "total": int,
            "accepted": int,
            "rejected": int,
            "batches": int,
            "errors": list,
        }
    """
    if not articles:
        return {
            "total": 0,
            "accepted": 0,
            "rejected": 0,
            "batches": 0,
            "errors": [],
        }

    api_url, api_key = _get_config()
    batch_url = f"{api_url}/articles/batch"

    # Format all articles
    formatted = [_format_article(a) for a in articles]

    if dry_run:
        print(f"  [DRY RUN] Would push {len(formatted)} articles to {batch_url}")
        return {
            "total": len(formatted),
            "accepted": len(formatted),
            "rejected": 0,
            "batches": (len(formatted) + BATCH_SIZE - 1) // BATCH_SIZE,
            "errors": [],
        }

    if not api_key:
        print("  [SKIP] POWERREADER_API_KEY not set — skipping push")
        return {
            "total": len(formatted),
            "accepted": 0,
            "rejected": len(formatted),
            "batches": 0,
            "errors": [{"reason": "POWERREADER_API_KEY not set"}],
        }

    # Split into batches of BATCH_SIZE
    batches = [
        formatted[i:i + BATCH_SIZE]
        for i in range(0, len(formatted), BATCH_SIZE)
    ]

    total_accepted = 0
    total_rejected = 0
    all_errors: list[dict] = []

    for batch_idx, batch in enumerate(batches):
        batch_num = batch_idx + 1
        print(f"  Batch {batch_num}/{len(batches)}: {len(batch)} articles...")

        result = _post_batch(batch_url, api_key, batch)

        total_accepted += result.get("accepted", 0)
        total_rejected += result.get("rejected", 0)

        errors = result.get("errors", [])
        if errors:
            all_errors.extend(errors)
            for err in errors[:3]:  # Show max 3 errors per batch
                print(f"    Error: {err.get('article_id', '?')}: {err.get('reason', '?')}")

        print(f"    -> {result.get('accepted', 0)} accepted, {result.get('rejected', 0)} rejected")

    return {
        "total": len(formatted),
        "accepted": total_accepted,
        "rejected": total_rejected,
        "batches": len(batches),
        "errors": all_errors,
    }
