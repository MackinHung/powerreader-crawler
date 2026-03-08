"""
Stage B — Full text extraction via markdown.new with trafilatura fallback.
Copyright (C) 2026 @MackinHung (https://github.com/MackinHung)

Routes each article URL to the appropriate extraction method based on
source configuration.
"""

import time

import requests

from .config import (
    HEADERS,
    MARKDOWN_NEW_API,
    MARKDOWN_NEW_TIMEOUT,
    REQUEST_TIMEOUT,
    RSS_SOURCES,
    API_SOURCES,
    SITEMAP_SOURCES,
)

# Build a lookup: source_key -> extractor method name
_SOURCE_EXTRACTOR: dict[str, str] = {}
for _src in RSS_SOURCES + API_SOURCES + SITEMAP_SOURCES:
    _SOURCE_EXTRACTOR[_src["key"]] = _src.get("extractor", "markdown_new")


# ------------------------------------------------------------------
# markdown.new extraction
# ------------------------------------------------------------------

def extract_markdown_new(url: str) -> dict:
    """Extract article content via markdown.new API.

    Returns:
        {
            "success": bool,
            "content": str,       # raw markdown
            "title": str | None,  # from API response
            "tokens": int | None,
            "error": str | None,
        }
    """
    try:
        resp = requests.post(
            MARKDOWN_NEW_API,
            json={"url": url},
            headers={"Content-Type": "application/json"},
            timeout=MARKDOWN_NEW_TIMEOUT,
        )

        if resp.status_code != 200:
            return {
                "success": False,
                "content": "",
                "title": None,
                "tokens": None,
                "error": f"HTTP {resp.status_code}",
            }

        try:
            data = resp.json()
            content = data.get("content", "") or data.get("markdown", "") or ""
            return {
                "success": bool(content),
                "content": content,
                "title": data.get("title"),
                "tokens": data.get("tokens"),
                "error": None if content else "Empty content",
            }
        except (ValueError, KeyError):
            # Plain text response
            text = resp.text
            return {
                "success": bool(text),
                "content": text,
                "title": None,
                "tokens": None,
                "error": None if text else "Empty response",
            }

    except requests.Timeout:
        return {
            "success": False,
            "content": "",
            "title": None,
            "tokens": None,
            "error": "Timeout",
        }
    except Exception as e:
        return {
            "success": False,
            "content": "",
            "title": None,
            "tokens": None,
            "error": str(e),
        }


# ------------------------------------------------------------------
# trafilatura extraction (fallback for CNA .aspx)
# ------------------------------------------------------------------

def extract_trafilatura(url: str) -> dict:
    """Extract article content via trafilatura.

    Fallback chain:
      L1: trafilatura with academic UA
      L2: trafilatura with enhanced browser headers
    """
    try:
        import trafilatura

        # L1: Standard trafilatura
        config = trafilatura.settings.use_config()
        config.set(
            "DEFAULT", "USER_AGENT",
            "Mozilla/5.0 (compatible; PowerReader/1.0; academic research)"
        )

        downloaded = trafilatura.fetch_url(url, config=config)

        if not downloaded:
            # L2: Try with browser-like headers
            try:
                enhanced_headers = {
                    **HEADERS,
                    "Referer": "https://www.cna.com.tw/",
                    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
                    "Accept": "text/html,application/xhtml+xml",
                }
                resp = requests.get(
                    url, headers=enhanced_headers, timeout=REQUEST_TIMEOUT
                )
                if resp.status_code == 200:
                    downloaded = resp.text
            except Exception:
                pass

        if not downloaded:
            return {
                "success": False,
                "content": "",
                "title": None,
                "tokens": None,
                "error": "Failed to download page",
            }

        text = trafilatura.extract(
            downloaded,
            include_comments=False,
            include_tables=False,
            no_fallback=False,
            favor_precision=True,
            deduplicate=True,
        )

        if not text:
            return {
                "success": False,
                "content": "",
                "title": None,
                "tokens": None,
                "error": "trafilatura extracted empty content",
            }

        # Extract title from HTML if possible
        title = None
        metadata = trafilatura.extract(
            downloaded,
            output_format="json",
            include_comments=False,
        )
        if metadata:
            try:
                import json
                meta_dict = json.loads(metadata)
                title = meta_dict.get("title")
            except (ValueError, TypeError):
                pass

        return {
            "success": True,
            "content": text,
            "title": title,
            "tokens": None,
            "error": None,
        }

    except ImportError:
        return {
            "success": False,
            "content": "",
            "title": None,
            "tokens": None,
            "error": "trafilatura not installed (pip install trafilatura)",
        }
    except Exception as e:
        return {
            "success": False,
            "content": "",
            "title": None,
            "tokens": None,
            "error": str(e),
        }


# ------------------------------------------------------------------
# Routing
# ------------------------------------------------------------------

_EXTRACTORS = {
    "markdown_new": extract_markdown_new,
    "trafilatura": extract_trafilatura,
}


def extract_content(url: str, source_key: str) -> dict:
    """Route article URL to the correct extractor.

    Uses source config to determine primary method, falls back to
    trafilatura if markdown.new fails.
    """
    method_name = _SOURCE_EXTRACTOR.get(source_key, "markdown_new")
    extractor = _EXTRACTORS[method_name]

    result = extractor(url)

    # If markdown.new fails, try trafilatura as fallback
    if not result["success"] and method_name == "markdown_new":
        print(f"      [FALLBACK] markdown.new failed, trying trafilatura")
        result = extract_trafilatura(url)

    return result
