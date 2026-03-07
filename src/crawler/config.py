"""
Crawler pipeline configuration — source definitions and constants.
Copyright (C) 2026 @MackinHung (https://github.com/MackinHung)
Single source of truth for all crawler settings.
"""

from datetime import timezone, timedelta

# ------------------------------------------------------------------
# Global settings
# ------------------------------------------------------------------

TW_TZ = timezone(timedelta(hours=8))

MARKDOWN_NEW_API = "https://markdown.new/"

# Rate limiting
SAME_DOMAIN_DELAY_MIN = 2.0   # seconds
SAME_DOMAIN_DELAY_MAX = 5.0
CROSS_DOMAIN_DELAY_MIN = 0.5
CROSS_DOMAIN_DELAY_MAX = 1.0
CIRCUIT_BREAKER_THRESHOLD = 3  # consecutive failures to block domain

# Content quality thresholds
MIN_ARTICLE_CHARS = 100
MIN_PARAGRAPH_COUNT = 2
MAX_ARTICLE_CHARS = 50000

# HTTP
REQUEST_TIMEOUT = 20  # seconds
MARKDOWN_NEW_TIMEOUT = 30

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

# ------------------------------------------------------------------
# RSS source definitions
# ------------------------------------------------------------------

RSS_SOURCES = [
    {
        "key": "LIBERTY_TIMES",
        "name": "Liberty Times",
        "feeds": [
            "https://news.ltn.com.tw/rss/politics.xml",
            "https://news.ltn.com.tw/rss/society.xml",
            "https://news.ltn.com.tw/rss/life.xml",
            "https://news.ltn.com.tw/rss/business.xml",
        ],
        "extractor": "markdown_new",
    },
    {
        "key": "CNA",
        "name": "Central News Agency",
        "feeds": [
            "https://feeds.feedburner.com/rsscna/politics",
            "https://feeds.feedburner.com/rsscna/finance",
            "https://feeds.feedburner.com/rsscna/social",
            "https://feeds.feedburner.com/rsscna/lifehealth",
        ],
        "extractor": "trafilatura",  # markdown.new blocks .aspx
    },
    {
        "key": "PTS",
        "name": "Public Television Service",
        "feeds": ["https://about.pts.org.tw/rss/XML/newsfeed.xml"],
        "extractor": "markdown_new",
    },
    {
        "key": "THE_NEWS_LENS",
        "name": "The News Lens",
        "feeds": ["https://feeds.feedburner.com/TheNewsLens"],
        "extractor": "markdown_new",
    },
    {
        "key": "THE_REPORTER",
        "name": "The Reporter",
        "feeds": ["https://www.twreporter.org/a/rss2.xml"],
        "extractor": "markdown_new",
    },
    {
        "key": "TECHNEWS",
        "name": "TechNews",
        "feeds": ["https://technews.tw/feed/"],
        "extractor": "markdown_new",
    },
    {
        "key": "ITHOME",
        "name": "iThome",
        "feeds": ["https://www.ithome.com.tw/rss"],
        "extractor": "markdown_new",
    },
    {
        "key": "STORM_MEDIA",
        "name": "Storm Media",
        "feeds": ["https://www.storm.mg/api/getRss/channel_id/2"],
        "extractor": "markdown_new",
        "fix_relative_urls": "https://www.storm.mg",
    },
]

# ------------------------------------------------------------------
# API source definitions (UDN family)
# ------------------------------------------------------------------

API_SOURCES = [
    {
        "key": "UNITED_DAILY_NEWS",
        "name": "United Daily News",
        "api_url": "https://udn.com/api/more",
        "params": {
            "channelId": "1",
            "cate_id": "0",
            "type": "breaknews",
        },
        "url_prefix": "https://udn.com",
        "max_pages": 5,
        "extractor": "markdown_new",
    },
    {
        "key": "ECONOMIC_DAILY_NEWS",
        "name": "Economic Daily News",
        "api_url": "https://udn.com/api/more",
        "params": {
            "channelId": "2",
            "cate_id": "0",
            "type": "breaknews",
        },
        "url_prefix": "https://money.udn.com",
        "max_pages": 5,
        "extractor": "markdown_new",
    },
]

# Frontmatter fields we extract
FRONTMATTER_FIELDS = ["title", "description", "image", "author", "date"]
