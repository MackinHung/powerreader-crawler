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

# Bot identity — honest, identifiable user-agent
BOT_NAME = "PowerReaderCrawler"
BOT_VERSION = "1.0"
BOT_URL = "https://github.com/MackinHung/powerreader-crawler"
BOT_USER_AGENT = f"{BOT_NAME}/{BOT_VERSION} (+{BOT_URL})"

HEADERS = {
    "User-Agent": BOT_USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
}

# robots.txt
ROBOTS_TXT_TIMEOUT = 10  # seconds
ROBOTS_CACHE_TTL = 3600  # 1 hour

# ------------------------------------------------------------------
# RSS source definitions
# ------------------------------------------------------------------

RSS_SOURCES = [
    {
        "key": "LIBERTY_TIMES",
        "name": "Liberty Times",
        "feeds": [
            {"url": "https://news.ltn.com.tw/rss/politics.xml", "category": "政治"},
            {"url": "https://news.ltn.com.tw/rss/society.xml", "category": "社會"},
            {"url": "https://news.ltn.com.tw/rss/life.xml", "category": "生活"},
            {"url": "https://news.ltn.com.tw/rss/business.xml", "category": "財經"},
        ],
        "extractor": "markdown_new",
    },
    {
        "key": "CNA",
        "name": "Central News Agency",
        "feeds": [
            {"url": "https://feeds.feedburner.com/rsscna/politics", "category": "政治"},
            {"url": "https://feeds.feedburner.com/rsscna/finance", "category": "財經"},
            {"url": "https://feeds.feedburner.com/rsscna/social", "category": "社會"},
            {"url": "https://feeds.feedburner.com/rsscna/lifehealth", "category": "生活"},
        ],
        "extractor": "trafilatura",  # markdown.new blocks .aspx
    },
    {
        "key": "PTS",
        "name": "Public Television Service",
        "feeds": [{"url": "https://about.pts.org.tw/rss/XML/newsfeed.xml", "category": "綜合"}],
        "extractor": "markdown_new",
    },
    {
        "key": "THE_NEWS_LENS",
        "name": "The News Lens",
        "feeds": [{"url": "https://feeds.feedburner.com/TheNewsLens", "category": "綜合"}],
        "extractor": "markdown_new",
    },
    {
        "key": "THE_REPORTER",
        "name": "The Reporter",
        "feeds": [{"url": "https://www.twreporter.org/a/rss2.xml", "category": "綜合"}],
        "extractor": "markdown_new",
    },
    {
        "key": "TECHNEWS",
        "name": "TechNews",
        "feeds": [{"url": "https://technews.tw/feed/", "category": "科技"}],
        "extractor": "markdown_new",
    },
    {
        "key": "ITHOME",
        "name": "iThome",
        "feeds": [{"url": "https://www.ithome.com.tw/rss", "category": "科技"}],
        "extractor": "markdown_new",
    },
    # Storm Media: RSS API broken since ~2026-03 (1 garbled entry,
    # Big5/UTF-8 encoding mismatch). No RSSHub route available.
    # Disabled until alternative source found.
    # {
    #     "key": "STORM_MEDIA",
    #     "name": "Storm Media",
    #     "feeds": [{"url": "https://www.storm.mg/api/getRss/channel_id/2", "category": "綜合"}],
    #     "extractor": "markdown_new",
    #     "fix_relative_urls": "https://www.storm.mg",
    # },
    {
        "key": "ETTODAY",
        "name": "ETtoday",
        "feeds": [
            {"url": "https://feeds.feedburner.com/ettoday/news", "category": "綜合"},
            {"url": "https://feeds.feedburner.com/ettoday/society", "category": "社會"},
            {"url": "https://feeds.feedburner.com/ettoday/lifestyle", "category": "生活"},
            {"url": "https://feeds.feedburner.com/ettoday/finance", "category": "財經"},
        ],
        "extractor": "markdown_new",
    },
    {
        "key": "SETN",
        "name": "SET News",
        "feeds": [{"url": "https://rsshub.rssforever.com/setn", "category": "綜合"}],
        "extractor": "trafilatura",  # setn.com uses .aspx URLs
    },
    {
        "key": "EBC",
        "name": "EBC News",
        "feeds": [
            {"url": "https://rsshub.rssforever.com/ebc/realtime", "category": "綜合"},
            {"url": "https://rsshub.rssforever.com/ebc/realtime/society", "category": "社會"},
            {"url": "https://rsshub.rssforever.com/ebc/realtime/living", "category": "生活"},
        ],
        "extractor": "markdown_new",
    },
    {
        "key": "NEWTALK",
        "name": "Newtalk",
        "feeds": [
            {"url": "https://newtalk.tw/rss/category/2", "category": "政治"},
            {"url": "https://newtalk.tw/rss/category/14", "category": "國際"},
        ],
        "extractor": "markdown_new",
    },
    {
        "key": "MIRROR_MEDIA",
        "name": "Mirror Media",
        "feeds": [
            {"url": "https://www.mirrormedia.mg/rss/political.xml", "category": "政治"},
            {"url": "https://www.mirrormedia.mg/rss/rss.xml", "category": "綜合"},
        ],
        "extractor": "markdown_new",
    },
    {
        "key": "CNEWS",
        "name": "CNEWS",
        "feeds": [{"url": "https://cnews.com.tw/feed/", "category": "綜合"}],
        "extractor": "markdown_new",
    },
    {
        "key": "TTV",
        "name": "TTV News",
        "feeds": [{"url": "https://www.ttv.com.tw/rss/RSSHandler.ashx?d=news", "category": "綜合"}],
        "extractor": "markdown_new",
    },
    {
        "key": "CTV",
        "name": "CTV News",
        "feeds": [{"url": "https://www.ctv.com.tw/rss", "category": "綜合"}],
        "extractor": "trafilatura",
        "url_replace": ["http://new.ctv.com.tw", "https://www.ctv.com.tw"],
    },
]

# ------------------------------------------------------------------
# API source definitions (UDN family)
# ------------------------------------------------------------------

API_SOURCES = [
    {
        "key": "UNITED_DAILY_NEWS",
        "name": "United Daily News",
        "category": "綜合",
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
        "category": "財經",
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

# ------------------------------------------------------------------
# Sitemap source definitions (Google News Sitemap XML)
# ------------------------------------------------------------------

SITEMAP_SOURCES = [
    {
        "key": "CTS",
        "name": "CTS News",
        "category": "綜合",
        "sitemap_url": "https://news.cts.com.tw/sitemap_cts_google.xml",
        "extractor": "trafilatura",
    },
]

# Frontmatter fields we extract
FRONTMATTER_FIELDS = ["title", "description", "image", "author", "date"]
