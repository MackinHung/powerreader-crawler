"""
Stage C — Content cleaning and structured data extraction.
Copyright (C) 2026 @MackinHung (https://github.com/MackinHung)

Multi-phase cleaning pipeline:
  Phase 1: parse_frontmatter() — extract YAML-like frontmatter
  Phase 2: extract_article_body() — locate article within full-page markdown
  Phase 3: clean_content() — remove remaining noise from article body
  Phase 4: validate_quality() — check article meets minimum standards
"""

import hashlib
import re
from datetime import datetime

from .config import (
    FRONTMATTER_FIELDS,
    MAX_ARTICLE_CHARS,
    MIN_ARTICLE_CHARS,
    MIN_PARAGRAPH_COUNT,
    TW_TZ,
)


# ------------------------------------------------------------------
# Frontmatter parsing
# ------------------------------------------------------------------

def parse_frontmatter(raw_markdown: str) -> tuple[dict, str]:
    """Parse YAML-like frontmatter from markdown.new output.

    markdown.new typically returns:
        ---
        title: ...
        description: ...
        image: ...
        ---
        (content body)

    Returns:
        (metadata_dict, content_body)
    """
    metadata: dict = {}
    content = raw_markdown

    if not raw_markdown.startswith("---"):
        return metadata, content

    end_idx = raw_markdown.find("---", 3)
    if end_idx == -1:
        return metadata, content

    frontmatter_block = raw_markdown[3:end_idx].strip()
    content = raw_markdown[end_idx + 3:].strip()

    for line in frontmatter_block.split("\n"):
        line = line.strip()
        if not line or ":" not in line:
            continue

        key, _, value = line.partition(":")
        key = key.strip().lower()
        value = value.strip()

        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]

        if key in FRONTMATTER_FIELDS and value:
            metadata[key] = value

    return metadata, content


# ------------------------------------------------------------------
# Article body extraction — locates article within full-page markdown
# ------------------------------------------------------------------

# Strong markers — always terminate regardless of line length.
# These never appear naturally in article text.
_STRONG_TERMINATION = [
    "不用抽 不用搶",
    "不用抽不用搶",
    "點我下載APP",
    "按我活動辦法",
    "上一則",
    "下一則",
    "此網頁已閒置",
    "```json",
    "```JSON",
    '{"@context"',            # JSON-LD schema blocks
    '{"@type"',               # JSON-LD schema blocks
    "（相關報導：",            # Storm article-end related links
    "（相關報導:",
    "張貼文章或下標籤",        # UDN comment ToS
    # --- 2026-03-09 Round 2: new patterns from raw analysis ---
    "### ‧",                  # ETTODAY sidebar news ticker
    "今日排行榜",              # EBC ranking section
    "往下看更多",              # EBC "see more" section
    "廣告 / 請繼續往下閱讀",   # EBC ad break
    "## 書籍介紹",             # TNL book promo section
    "責任編輯：",              # Editor attribution (article end)
    "責任編輯:",
    "核稿編輯：",              # TNL review editor
    "核稿編輯:",
    "封面示意圖",              # EBC cover image line (after article)
    # --- 2026-03-09 Round 3: per-source tail patterns ---
    "## 熱門新聞",             # Generic section — truncate all below
    "## 專題報導",             # Generic section — truncate all below
    "※本文版權所有",           # Copyright boilerplate — truncate all below
    "更多CNEWS",              # CNEWS related articles
    "更多匯流新聞網報導",      # CNEWS alt wording
    "All rights Reserved",    # English copyright footer
    "All Rights Reserved",
    "收看中視新聞",            # CTV channel promo
    "請鎖定新聞台",            # CTV/generic channel promo
    "新聞授權",                # News authorization footer — truncate all below
    "經授權轉載",              # Authorized reprint footer — truncate all below
    # --- 2026-03-10 Round 4: user-reported patterns ---
    "文章轉載請註明出處",      # Reprint notice footer — truncate all below
    "延伸閱讀",                # "Extended reading" section (upgraded from weak)
    "深度求真",                # THE_REPORTER slogan — truncate all below
    "訂閱中視新聞",            # CTV subscription CTA — truncate all below
]

# Weak markers — only terminate on short lines (< 80 chars)
# to avoid false positives when these phrases appear in article text.
_WEAK_TERMINATION = [
    "今日熱門",
    "熱門新訊",
    "熱門新聞",
    "大家都關注",
    "看更多！請加入",
    "看更多!請加入",
    "推薦閱讀",
    "你可能也想看",
    "更多新聞",
    "相關報導",
    "相關新聞",
    "則留言",          # UDN comment section: "共 0 則留言"
    "[發布]",          # UDN comment button
    # --- 2026-03-09 Round 2 ---
    "政治熱門新聞",     # ETTODAY section header
    "讀者迴響",         # ETTODAY comment section
    "最夯影音",         # ETTODAY video section
    "熱門快報",         # ETTODAY breaking news
    "更多熱門",         # ETTODAY more button
    "關鍵字：",         # ETTODAY/TECHNEWS tag section
    "關鍵字:",
]

# Byline pattern: YYYY/MM/DD HH:MM or YYYY-MM-DD HH:MM followed by reporter
_BYLINE_RE = re.compile(
    r"(\d{4}[/-]\d{2}[/-]\d{2}\s+\d{2}:\d{2})\s*(.*)"
)
_REPORTER_RE = re.compile(r"\u8a18\u8005\s*(.+?)\s*[\uff0f/]")  # 記者XXX／


def extract_article_body(content: str) -> tuple[str, dict]:
    """Locate and extract article body from full-page markdown.

    For markdown.new output that includes the entire rendered page
    (navigation, ads, footer, JSON-LD), finds the actual article
    bounded by the H1 title heading and termination markers.

    Returns:
        (article_body_text, extracted_metadata)
    """
    metadata: dict = {}

    if not _is_full_page(content):
        return content, metadata

    lines = content.split("\n")

    # Step 1: Find H1 heading — the article title
    h1_idx = _find_h1(lines)

    if h1_idx is None:
        return _strip_html(content), metadata

    metadata["h1_title"] = lines[h1_idx].strip().lstrip("#").strip()

    # Step 2: Find byline (date + reporter) after H1
    start_idx = h1_idx + 1
    next_idx = _next_non_empty(lines, start_idx)

    if next_idx is not None:
        byline_match = _BYLINE_RE.match(lines[next_idx].strip())
        if byline_match:
            metadata["byline_date"] = byline_match.group(1)
            reporter_match = _REPORTER_RE.search(byline_match.group(2))
            if reporter_match:
                metadata["author"] = reporter_match.group(1).strip()
            start_idx = next_idx + 1

    # Step 3: Find termination point
    end_idx = _find_end(lines, start_idx)

    # Step 4: Join body lines
    body = "\n".join(lines[start_idx:end_idx])

    return body, metadata


def _is_full_page(content: str) -> bool:
    """Detect if content is a full-page dump vs. article-only content."""
    head = content[:600]

    # HTML doctype or tag at start
    if head.lstrip().startswith("<!DOCTYPE") or head.lstrip().startswith("<html"):
        return True

    # Many markdown links in first 20 non-empty lines (navigation menus)
    non_empty = [l for l in content.split("\n") if l.strip()][:20]
    link_count = sum(1 for l in non_empty if re.search(r"\[.*?\]\(.*?\)", l))
    if link_count >= 5:
        return True

    return False


def _find_h1(lines: list[str]) -> int | None:
    """Find first H1 heading with enough Chinese text to be a title."""
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("# ") and not stripped.startswith("## "):
            chinese = len(re.findall(r"[\u4e00-\u9fff]", stripped))
            if chinese >= 3:
                return i
    return None


def _next_non_empty(lines: list[str], start: int) -> int | None:
    """Find next non-empty line within 5 lines of start."""
    for i in range(start, min(start + 5, len(lines))):
        if lines[i].strip():
            return i
    return None


def _find_end(lines: list[str], start: int) -> int:
    """Find line where article body ends (first termination marker)."""
    for i in range(start, len(lines)):
        stripped = lines[i].strip()
        # Strong markers: always check regardless of line length
        for marker in _STRONG_TERMINATION:
            if marker in stripped:
                return i
        # Weak markers: only check on short lines to avoid false positives
        if len(stripped) < 80:
            for marker in _WEAK_TERMINATION:
                if marker in stripped:
                    return i
    return len(lines)


def _strip_html(content: str) -> str:
    """Remove HTML artifacts from content."""
    content = re.sub(r"<!DOCTYPE[^>]*>", "", content, flags=re.IGNORECASE)
    content = re.sub(
        r"<script[^>]*>.*?</script>", "", content,
        flags=re.DOTALL | re.IGNORECASE,
    )
    content = re.sub(
        r"<style[^>]*>.*?</style>", "", content,
        flags=re.DOTALL | re.IGNORECASE,
    )
    content = re.sub(r"<[^>]+>", "", content)
    return content


# ------------------------------------------------------------------
# Content cleaning — removes noise within extracted article body
# ------------------------------------------------------------------

_LINE_NOISE = [
    # Empty image refs: [ ](url) or [  ](url)
    re.compile(r"^\[\s*\]\(.*?\)\s*$"),
    # Standalone link lines: [text](url) alone on a line
    re.compile(r"^\s*\[.*?\]\(https?://.*?\)\s*$"),
    # Bulleted link lists: * [text](url)
    re.compile(r"^\*\s+\[.*?\]\(.*?\)\s*$"),
    # Author link lines: [name](/author/...) — Storm, PTS (standalone or combined)
    re.compile(r"^\s*\[.*?\]\(/author/.*?\)\s*$"),
    # Combined author lines: [author1](/author/N) [author2](/author/M) / reporter
    re.compile(r"^\s*(\[.*?\]\(/author/\d+\)\s*)+"),
    # Read-more prompts
    re.compile(r"請繼續往下閱讀"),
    # Ad tracking URLs
    re.compile(r"pv\d+\.\w+\.com\.tw/click"),
    re.compile(r"javascript:\s*void"),
    # CTA / Membership prompts
    re.compile(r"加入.*會員"),        # 加入公視會員, 加入免費會員 etc.
    re.compile(r"按讚收藏"),
    re.compile(r"登入會員"),
    re.compile(r"下次再說"),
    re.compile(r"訂閱.*會員"),        # 訂閱 風傳媒VIP會員
    re.compile(r"訂閱.*VIP"),
    re.compile(r"零廣告閱讀"),
    re.compile(r"^顯示全部$"),        # Storm "Show all"
    re.compile(r"^我們想讓你知道的是$"),  # TNL editorial note
    # Age gate / Content rating
    re.compile(r"您即將進入之新聞內容"),
    re.compile(r"電腦網路內容分級處理辦法"),
    re.compile(r"台灣網站分級推廣基金會"),
    re.compile(r"未滿18歲"),
    re.compile(r"我同意.*已年滿"),
    re.compile(r"此篇文章含有成人內容"),
    re.compile(r"我已滿 18 歲"),
    re.compile(r"我未滿 18 歲"),
    re.compile(r"禁止酒駕"),
    re.compile(r"未滿十八歲禁止飲酒"),
    # CNA agency boilerplate (appears in TNL syndicated articles)
    re.compile(r"中央通訊社是中華民國的國家通訊社"),
    # H6 headings (site chrome, not article content)
    re.compile(r"^#{6}\s"),
    # Heading links: ### [title](url) — related articles (UDN etc.)
    re.compile(r"^#{2,4}\s*\[.*?\]\(.*?\)"),
    # Empty heading links: ### [](#)
    re.compile(r"^#{2,4}\s*\[.*?\]\(#\)"),
    # Breadcrumb navigation
    re.compile(r"\[首頁\].*?\\>"),
    # UDN comment ToS boilerplate
    re.compile(r"對於明知不實"),
    re.compile(r"凡「暱稱」涉及"),
    re.compile(r"不同意上述規範者"),
    # Standalone "More" button
    re.compile(r"^More$"),
    # TNL CTA buttons
    re.compile(r"^收藏文章$"),
    # CNA copyright boilerplate
    re.compile(r"本網站之文字、圖片及影音.*不得轉載"),
    # Liberty Times YouTube subscription CTA
    re.compile(r"點我訂閱.*頻道"),
    # PTS bullet artifacts: "* ..."
    re.compile(r"^\*\s*\.\.\.\s*$"),
    # Storm separator + related article links: ****‧ [title](url)
    re.compile(r"^\*{3,}‧?\s*\["),
    # Storm standalone datetime lines at article start (YYYY-MM-DD HH:MM)
    re.compile(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}\s*$"),
    # TNL share buttons: Share : [ ](mailto:...)
    re.compile(r"^Share\s*:\s*\["),
    # --- 2026-03-09 Round 2: new noise patterns ---
    # Photo/image credits
    re.compile(r"^Photo Credit:"),
    re.compile(r"^圖片來自"),
    # EBC image caption markers
    re.compile(r"^▼"),
    # Cover/source image attribution: （封面圖...）or （圖／...）
    re.compile(r"^（封面"),
    re.compile(r"^（圖[/／╱]"),
    # Affiliate/ad disclosures
    re.compile(r"透過以上連結購書"),
    re.compile(r"導購分潤收入"),
    re.compile(r"獲得分潤收益"),
    # TNL editorial note prefix (standalone)
    re.compile(r"^TNL精選書籍"),
    # Generic "report by editor" pattern at end
    re.compile(r"^[A-Za-z\u4e00-\u9fff]{1,4}[/／]\s*編輯\s*$"),
    # Tag section: ## #TagName
    re.compile(r"^##\s+#"),
    # EBC inline ad links
    re.compile(r"^\[來就送"),
    # Navigation breadcrumbs: [首頁](/) > [分類]
    re.compile(r"^\[首頁\]\(/"),
    # Standalone share/copy buttons
    re.compile(r"^分享\s+分享\s+複製連結"),
    # Standalone "聽新聞"
    re.compile(r"^聽新聞$"),
    # PTS publish/update time lines
    re.compile(r"^發布時間：\d{4}"),
    re.compile(r"^更新時間：\d{4}"),
    # Combined PTS time line
    re.compile(r"^發布時間：.*更新時間："),
    # --- Round 3: per-source noise ---
    # Copyright notices
    re.compile(r"^※"),                        # ※ prefix lines (copyright/disclaimer)
    re.compile(r"All [Rr]ights [Rr]eserved"),
    # CNEWS related article links
    re.compile(r"^更多CNEWS"),
    re.compile(r"^更多匯流新聞網"),
    # CTV channel promos
    re.compile(r"收看中視新聞"),
    re.compile(r"請鎖定新聞台"),
    # Generic news footer: 【...報導】or 新聞授權
    re.compile(r"^【[^】]*授權[^】]*】"),
    re.compile(r"新聞授權"),
    # --- Round 4: THE_REPORTER + CTV specific ---
    # THE_REPORTER SVG artifacts
    re.compile(r"^Fill 1$"),
    re.compile(r"^Fill$"),
    # Photographer/illustrator credits: （攝影／...）（繪圖／...）
    re.compile(r"^（攝影"),
    re.compile(r"^（繪圖"),
    # Standalone bracket tags: [深度專題], [最新], [更多](/search/tags/...) etc.
    # Matches [2-8 chars] optionally followed by (url), entire line
    re.compile(r"^\s*\[[^\]]{2,8}\](\([^\)]*\))?\s*$"),
    # Book review heading (single line removal, not truncation)
    re.compile(r"^## 書評$"),
]

# Detect lines that are mostly markdown links (navigation bars)
_LINK_RE = re.compile(r"\[.*?\]\(.*?\)")


# Vue/Angular template syntax — always noise in news articles
_TEMPLATE_RE = re.compile(r"\{\{.*?\}\}")

# Late termination markers — applied as fallback in clean_content()
# These patterns trigger truncation even if extract_article_body() missed them.
_LATE_TERMINATION = [
    "```json",
    '{"@context"',
    '{"@type"',
    "登入裝置已達上限",       # TECHNEWS auth popup
    "親愛的會員",             # TECHNEWS auth popup
    "請您暫停使用AD Block",   # TECHNEWS ad block notice
    "### VIP",               # TECHNEWS footer sidebar
    "### 財訊快報",           # TECHNEWS footer sidebar
    "### 編輯精選",           # TECHNEWS footer sidebar
    "### FB 粉絲團",          # TECHNEWS footer sidebar
    "付費訂閱",               # TNL paywall CTA
    "參與議題",               # TNL discussion CTA
    "你認同本文的觀點嗎",     # TNL feedback prompt
    "收藏文章",               # TNL save article button
    "AD Block",              # TECHNEWS ad block (short form)
    "本網站之文字、圖片及影音", # CNA copyright
    "點我訂閱",               # Liberty Times YouTube CTA
    "一手掌握經濟脈動",        # Liberty Times footer CTA
    "到您有啟用",              # TECHNEWS truncated ad block notice
    "我們偵測",                # TECHNEWS ad block detect line start
    "更多風傳媒獨家內幕",      # Storm related content CTA
    "張貼文章或下標籤",        # UDN comment ToS (late fallback)
    "則留言",                  # UDN comment count (late fallback)
    "科技新知，時時更新",      # TECHNEWS slogan/tagline
    "關鍵字:",                 # TECHNEWS tag links section
    "留給我們的話",            # TECHNEWS comment section heading
    "請喝咖啡",               # TECHNEWS donation link
    "總金額共新臺幣",          # TECHNEWS donation counter
    "咖啡贊助",               # TECHNEWS donation CTA
    "每杯咖啡",               # TECHNEWS donation price line
    "想請我們喝",              # TECHNEWS donation CTA heading
    # --- 2026-03-09 Round 2: late fallback ---
    "### ‧",                  # ETTODAY sidebar news ticker
    "今日排行榜",              # EBC ranking
    "往下看更多",              # EBC see more
    "## 書籍介紹",             # TNL book promo
    "責任編輯：",              # Editor attribution
    "責任編輯:",
    "核稿編輯：",
    "核稿編輯:",
    "廣告 / 請繼續往下閱讀",   # EBC ad break
    "## 相關新聞",             # Related news section heading
    "## 相關報導",
    "## #",                   # Tag sections (## #標籤)
    "封面示意圖",              # Cover image attribution
    "政治熱門新聞",            # ETTODAY section
    "讀者迴響",                # ETTODAY comments
    "最夯影音",                # ETTODAY video
    "更多熱門",                # ETTODAY more
    "熱門快報",                # ETTODAY breaking
    # --- Round 3: per-source tail ---
    "## 熱門新聞",
    "## 專題報導",
    "※本文版權所有",
    "更多CNEWS",
    "更多匯流新聞網報導",
    "All rights Reserved",
    "All Rights Reserved",
    "收看中視新聞",
    "請鎖定新聞台",
    "新聞授權",                # News authorization footer
    "經授權轉載",              # Authorized reprint footer
    # --- Round 4 ---
    "文章轉載請註明出處",      # Reprint notice
    "延伸閱讀",                # Extended reading section
    "深度求真",                # THE_REPORTER slogan
    "訂閱中視新聞",            # CTV subscription CTA
]


def _truncate_at_late_markers(lines: list[str]) -> list[str]:
    """Truncate at late termination markers (fallback for missed full-page detection)."""
    for i, line in enumerate(lines):
        stripped = line.strip()
        for marker in _LATE_TERMINATION:
            if marker in stripped:
                return lines[:i]
    return lines


def clean_content(raw_content: str) -> str:
    """Clean article body by removing remaining noise.

    Handles: orphan links, read-more prompts, image placeholders,
    breadcrumbs, age gates, ad tracking, and navigation lines.
    Also truncates at termination markers as a fallback.
    """
    lines = raw_content.split("\n")
    cleaned: list[str] = []

    for line in lines:
        stripped = line.strip()

        # Skip empty lines at start
        if not cleaned and not stripped:
            continue

        # Skip lines matching noise patterns
        if _is_noise_line(stripped):
            continue

        # Skip link-heavy lines (3+ links, little other text)
        if _is_link_heavy(stripped):
            continue

        # Skip lines with short bracket tags and little real text
        if _is_bracket_tag_line(stripped):
            continue

        # Skip very short non-punctuation lines (UI elements)
        if stripped and len(stripped) < 4 and not any(
            c in stripped for c in ".,;:!?\u3002\uff0c\uff1b\uff1a\uff01\uff1f\u3001"
        ):
            continue

        # Skip Vue/Angular template lines: {{ ... }}
        if _TEMPLATE_RE.search(stripped) and len(stripped) < 120:
            continue

        cleaned.append(line)

    # Fallback termination scan — truncate at late markers
    cleaned = _truncate_at_late_markers(cleaned)

    # Remove trailing empty lines
    while cleaned and not cleaned[-1].strip():
        cleaned.pop()

    result = "\n".join(cleaned)

    # Normalize paragraph separators
    # trafilatura returns single \n between paragraphs (no \n\n)
    if "\n\n" not in result and "\n" in result:
        result = result.replace("\n", "\n\n")

    # Collapse 3+ newlines to 2
    result = re.sub(r"\n{3,}", "\n\n", result)

    # Strip remaining HTML artifacts
    result = _strip_html(result)

    # Truncate if exceeding max
    if len(result) > MAX_ARTICLE_CHARS:
        result = result[:MAX_ARTICLE_CHARS]

    return result.strip()


def _is_noise_line(stripped: str) -> bool:
    """Check if a line matches any noise pattern."""
    for pattern in _LINE_NOISE:
        if pattern.search(stripped):
            return True
    return False


def _is_link_heavy(stripped: str) -> bool:
    """Check if a line is mostly markdown links (navigation bar)."""
    if not stripped:
        return False
    links = _LINK_RE.findall(stripped)
    if len(links) < 3:
        return False
    # Remove all link syntax and check what's left
    text_only = _LINK_RE.sub("", stripped).strip()
    return len(text_only) < 20


def _is_bracket_tag_line(stripped: str) -> bool:
    """Check if line contains markdown links with little real text around them.

    Detects navigation/category/tag lines like:
      [醫療](url) • [精選書摘](url)
      _損失金額(元)_ [更多新聞](url)
      > [iThome Security](https://www.facebook.com/...)

    Protects article text with inline links like:
      根據[中央社](url)的報導指出，政府已經宣布...
    """
    if "](" not in stripped:
        return False

    # Remove all markdown links [text](url)
    remaining = _LINK_RE.sub("", stripped)

    # Count Chinese characters in remaining text
    chinese = len(re.findall(r"[\u4e00-\u9fff]", remaining))

    return chinese < 10


# ------------------------------------------------------------------
# Quality validation
# ------------------------------------------------------------------

def validate_quality(content: str) -> tuple[bool, str]:
    """Validate article content quality.

    Returns:
        (is_valid, reason)
    """
    if not content:
        return False, "Empty content"

    if len(content) < MIN_ARTICLE_CHARS:
        return False, f"Too short ({len(content)} < {MIN_ARTICLE_CHARS} chars)"

    chinese_chars = len(re.findall(r"[\u4e00-\u9fff]", content))
    if chinese_chars < 20:
        return False, f"Insufficient Chinese ({chinese_chars} chars)"

    paragraphs = [p for p in content.split("\n\n") if p.strip()]
    if len(paragraphs) < MIN_PARAGRAPH_COUNT:
        return False, f"Too few paragraphs ({len(paragraphs)} < {MIN_PARAGRAPH_COUNT})"

    return True, "OK"


# ------------------------------------------------------------------
# Full cleaning pipeline
# ------------------------------------------------------------------

def clean_article(
    raw_markdown: str,
    *,
    article_meta: dict,
    extractor_result: dict,
) -> dict | None:
    """Process raw markdown into a structured article record.

    Pipeline:
      1. parse_frontmatter() — extract any YAML frontmatter
      2. extract_article_body() — locate article within full page
      3. clean_content() — remove remaining noise
      4. validate_quality() — check minimum standards

    Returns None if article fails quality validation.
    """
    now = datetime.now(TW_TZ)

    # Phase 1: Parse frontmatter
    frontmatter, content_body = parse_frontmatter(raw_markdown)

    # Phase 2: Extract article body from full-page content
    extracted_body, body_meta = extract_article_body(content_body)

    # Phase 3: Clean content
    cleaned_content = clean_content(extracted_body)

    # Phase 4: Validate quality
    is_valid, reason = validate_quality(cleaned_content)
    if not is_valid:
        return None

    # Determine title (priority: RSS > frontmatter > body H1 > extractor)
    title = (
        article_meta.get("title")
        or frontmatter.get("title")
        or body_meta.get("h1_title")
        or extractor_result.get("title")
        or ""
    )

    # Determine summary (priority: frontmatter > RSS > first paragraph)
    summary = (
        frontmatter.get("description")
        or article_meta.get("summary")
        or cleaned_content[:200]
    )

    # Determine author (priority: frontmatter > body byline > RSS)
    author = (
        frontmatter.get("author")
        or body_meta.get("author")
        or article_meta.get("author")
    )

    # Generate content hash
    content_hash = hashlib.sha256(
        cleaned_content.encode("utf-8")
    ).hexdigest()

    # Generate article ID from URL
    article_id = hashlib.sha256(
        article_meta["url"].encode("utf-8")
    ).hexdigest()

    # Build published_at ISO string
    pub_at = article_meta.get("published_at")
    published_iso = pub_at.isoformat() if pub_at else None

    return {
        "article_id": article_id,
        "content_hash": content_hash,
        "title": title,
        "summary": summary[:500],
        "author": author,
        "content_markdown": cleaned_content,
        "char_count": len(cleaned_content),
        "source": article_meta["source"],
        "primary_url": article_meta["url"],
        "published_at": published_iso,
        "crawled_at": now.isoformat(),
        "feed_category": article_meta.get("feed_category", "綜合"),
        "filter_score": None,      # ALBERT classifier (Stage D)
        "matched_topic": None,     # ALBERT keep category (Stage D)
        "dedup_metadata": None,    # dedup info (Stage E)
    }
