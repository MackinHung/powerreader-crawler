"""
Pipeline Runner — orchestrates Stage A + D + B + C + E + F with rate limiting.
Copyright (C) 2026 @MackinHung (https://github.com/MackinHung)

Usage:
    python -m src.crawler.runner [--limit N] [--source KEY] [--dry-run]
                                 [--no-filter] [--threshold 0.55]
                                 [--no-push]

Streaming pipeline: processes articles one-by-one through all stages.

Stages:
  A: Collect article metadata from RSS/API sources
  D: Topic filter via fine-tuned ALBERT classifier (keep political only)
  B: Extract full text via markdown.new / trafilatura
  C: Clean and validate content quality
  E: Content dedup (SHA256 exact + bge-small-zh semantic)
  F: Push to PowerReader API (POST /api/v1/articles/batch)
"""

import argparse
import json
import random
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from urllib.parse import urlparse

# Fix Windows console encoding for CJK characters
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from .cleaner import clean_article
from .collector import collect_all
from .config import (
    CIRCUIT_BREAKER_THRESHOLD,
    CROSS_DOMAIN_DELAY_MAX,
    CROSS_DOMAIN_DELAY_MIN,
    SAME_DOMAIN_DELAY_MAX,
    SAME_DOMAIN_DELAY_MIN,
    TW_TZ,
)
from .extractor import extract_content


# ------------------------------------------------------------------
# Rate limiter with circuit breaker
# ------------------------------------------------------------------

class RateLimiter:
    """Domain-aware rate limiter with circuit breaker."""

    def __init__(self):
        self._domain_last: dict[str, float] = {}
        self._domain_failures: dict[str, int] = defaultdict(int)
        self._blocked: set[str] = set()
        self._prev_domain: str | None = None

    def wait(self, url: str) -> bool:
        """Wait appropriate delay before requesting. Returns False if blocked."""
        domain = urlparse(url).netloc.lower()

        if domain in self._blocked:
            return False

        if self._prev_domain is not None:
            if domain == self._prev_domain:
                delay = random.uniform(
                    SAME_DOMAIN_DELAY_MIN, SAME_DOMAIN_DELAY_MAX
                )
            else:
                delay = random.uniform(
                    CROSS_DOMAIN_DELAY_MIN, CROSS_DOMAIN_DELAY_MAX
                )

            # Ensure minimum gap for same domain
            last = self._domain_last.get(domain)
            if last is not None:
                elapsed = time.monotonic() - last
                if elapsed < SAME_DOMAIN_DELAY_MIN:
                    delay = max(delay, SAME_DOMAIN_DELAY_MIN - elapsed)

            time.sleep(delay)

        self._domain_last[domain] = time.monotonic()
        self._prev_domain = domain
        return True

    def record_success(self, url: str) -> None:
        domain = urlparse(url).netloc.lower()
        self._domain_failures[domain] = 0

    def record_failure(self, url: str) -> None:
        domain = urlparse(url).netloc.lower()
        self._domain_failures[domain] += 1

        if self._domain_failures[domain] >= CIRCUIT_BREAKER_THRESHOLD:
            self._blocked.add(domain)
            print(f"    [BLOCKED] {domain} after {CIRCUIT_BREAKER_THRESHOLD} consecutive failures")

    @property
    def blocked_domains(self) -> set[str]:
        return set(self._blocked)


# ------------------------------------------------------------------
# Round-robin scheduler
# ------------------------------------------------------------------

def schedule_round_robin(articles: list[dict]) -> list[dict]:
    """Interleave articles by domain to avoid hammering same domain."""
    domain_buckets: dict[str, list[dict]] = defaultdict(list)

    for art in articles:
        domain = urlparse(art["url"]).netloc.lower()
        domain_buckets[domain].append(art)

    schedule: list[dict] = []
    buckets = list(domain_buckets.values())

    while buckets:
        next_round = []
        for bucket in buckets:
            schedule.append(bucket.pop(0))
            if bucket:
                next_round.append(bucket)
        buckets = next_round

    return schedule


# ------------------------------------------------------------------
# Pipeline runner
# ------------------------------------------------------------------

def run_pipeline(
    *,
    limit: int = 0,
    source_filter: str | None = None,
    dry_run: bool = False,
    freshness_hours: int = 3,
    skip_filter: bool = False,
    filter_threshold: float = 0.55,
    skip_push: bool = False,
) -> dict:
    """Run the complete Stage A -> D -> B -> C -> E -> F pipeline.

    Args:
        limit: Max articles to process in Stage B+C (0 = all)
        source_filter: Only process this source key
        dry_run: Only run Stage A (collect), skip extraction
        freshness_hours: Only process articles within this window
        skip_filter: Skip topic filtering (Stage D)
        filter_threshold: Cosine similarity threshold for topic filter
    """
    now = datetime.now(TW_TZ)
    start_time = time.monotonic()

    print("=" * 60)
    print(f"PowerReader Crawler Pipeline")
    print(f"Time: {now.isoformat()}")
    print(f"Mode: {'DRY RUN' if dry_run else 'FULL'}")
    if limit:
        print(f"Limit: {limit} articles")
    if source_filter:
        print(f"Source filter: {source_filter}")
    if skip_filter:
        print(f"Topic filter: DISABLED")
    else:
        print(f"Topic filter: threshold={filter_threshold}")
    print("=" * 60)

    # ---- Stage A: Collect ----
    print("\n[Stage A] Collecting article metadata...")
    print("-" * 40)

    all_meta = collect_all()

    # Apply source filter
    if source_filter:
        all_meta = [a for a in all_meta if a["source"] == source_filter]
        print(f"\n  Filtered to {len(all_meta)} articles from {source_filter}")

    # Apply freshness filter
    cutoff = now - timedelta(hours=freshness_hours)
    fresh = [
        a for a in all_meta
        if a["published_at"] is not None and a["published_at"] >= cutoff
    ]
    no_time = [a for a in all_meta if a["published_at"] is None]

    # Include articles without timestamp (can't determine freshness)
    candidates = fresh + no_time
    print(f"\n  Fresh ({freshness_hours}h): {len(fresh)} | No timestamp: {len(no_time)} | Total candidates: {len(candidates)}")

    if dry_run:
        elapsed = time.monotonic() - start_time
        print(f"\n[DRY RUN] Stage A complete in {elapsed:.1f}s")
        return {
            "stage": "A",
            "total_collected": len(all_meta),
            "candidates": len(candidates),
            "elapsed_s": elapsed,
        }

    # ---- Stage D: Topic Filter ----
    filter_stats = {"before": len(candidates), "kept": 0, "skipped": 0}

    if not skip_filter:
        print(f"\n[Stage D] Topic filtering {len(candidates)} articles...")
        print("-" * 40)

        filter_start = time.monotonic()

        from .topic_filter import TopicFilter
        topic_filter = TopicFilter(threshold=filter_threshold)

        # Batch classify for efficiency
        filter_results = topic_filter.classify_batch(candidates)

        # Attach filter results to metadata and filter
        filtered_candidates = []
        topic_counts: dict[str, int] = defaultdict(int)

        for meta, fr in zip(candidates, filter_results):
            meta["_filter_score"] = fr["score"]
            meta["_matched_topic"] = fr["topic"]

            if fr["keep"]:
                filter_stats["kept"] += 1
                topic_counts[fr["topic"]] += 1
                filtered_candidates.append(meta)
            else:
                filter_stats["skipped"] += 1

        filter_elapsed = time.monotonic() - filter_start
        candidates = filtered_candidates

        # Print summary by topic
        for topic, count in sorted(topic_counts.items(), key=lambda x: -x[1]):
            print(f"  {topic}: {count} articles")
        print(f"\n  Kept {filter_stats['kept']}/{filter_stats['before']} ({filter_elapsed:.1f}s)")
    else:
        filter_stats["kept"] = len(candidates)
        # Set defaults for unfiltered articles
        for meta in candidates:
            meta["_filter_score"] = None
            meta["_matched_topic"] = None

    # ---- Cross-source dedup by title_hash ----
    seen_hashes: set[str] = set()
    deduped: list[dict] = []
    dup_count = 0
    for meta in candidates:
        h = meta.get("title_hash", "")
        if h and h in seen_hashes:
            dup_count += 1
            continue
        if h:
            seen_hashes.add(h)
        deduped.append(meta)

    if dup_count:
        print(f"  Cross-source dedup: removed {dup_count} duplicates")
    candidates = deduped

    # Schedule round-robin first (interleave sources),
    # then apply limit to preserve source diversity
    scheduled = schedule_round_robin(candidates)

    if limit and len(scheduled) > limit:
        scheduled = scheduled[:limit]
        print(f"  Limited to {limit} articles")

    # ---- Stage B + C: Extract & Clean ----
    print(f"\n[Stage B+C] Extracting and cleaning {len(scheduled)} articles...")
    print("-" * 40)

    rate_limiter = RateLimiter()
    results: list[dict] = []
    stats = {
        "total_collected": filter_stats["before"],
        "filter_kept": filter_stats["kept"],
        "filter_skipped": filter_stats["skipped"],
        "total": len(scheduled),
        "extracted": 0,
        "cleaned": 0,
        "failed_extract": 0,
        "failed_quality": 0,
        "blocked": 0,
    }

    for i, meta in enumerate(scheduled):
        progress = f"[{i + 1}/{len(scheduled)}]"
        source = meta["source"]
        title_preview = meta["title"][:35]

        # Check rate limit / circuit breaker
        if not rate_limiter.wait(meta["url"]):
            stats["blocked"] += 1
            continue

        print(f"  {progress} [{source}] {title_preview}...")

        # Stage B: Extract
        ext_result = extract_content(meta["url"], source)

        if not ext_result["success"]:
            print(f"    -> FAIL: {ext_result['error']}")
            rate_limiter.record_failure(meta["url"])
            stats["failed_extract"] += 1
            continue

        rate_limiter.record_success(meta["url"])
        stats["extracted"] += 1

        # Stage C: Clean
        article = clean_article(
            ext_result["content"],
            article_meta=meta,
            extractor_result=ext_result,
        )

        if article is None:
            print(f"    -> SKIP: quality check failed")
            stats["failed_quality"] += 1
            continue

        # Attach topic filter metadata
        article["filter_score"] = meta.get("_filter_score")
        article["matched_topic"] = meta.get("_matched_topic")

        stats["cleaned"] += 1
        results.append(article)
        print(f"    -> OK: {article['char_count']} chars")

    # ---- Stage E: Content Dedup ----
    dedup_stats = {"before": len(results), "after": 0, "exact_removed": 0}

    if len(results) >= 2:
        print(f"\n[Stage E] Deduplicating {len(results)} articles...")
        print("-" * 40)

        dedup_start = time.monotonic()

        from .dedup import Deduplicator

        # TopicFilter now uses ALBERT (not SentenceTransformer),
        # so Deduplicator loads its own bge-small-zh model.
        deduplicator = Deduplicator()
        deduped_results = deduplicator.deduplicate(results)

        # Summarize dedup results
        type_counts: dict[str, int] = defaultdict(int)
        for art in deduped_results:
            dm = art.get("dedup_metadata", {})
            type_counts[dm.get("article_type", "unknown")] += 1

        dedup_elapsed = time.monotonic() - dedup_start
        dedup_stats["after"] = len(deduped_results)
        dedup_stats["exact_removed"] = deduped_results[0]["dedup_metadata"]["layer1_exact_removed"] if deduped_results else 0

        for atype, count in sorted(type_counts.items(), key=lambda x: -x[1]):
            print(f"  {atype}: {count}")
        print(f"\n  {dedup_stats['before']} → {dedup_stats['after']} articles ({dedup_elapsed:.1f}s)")

        results = deduped_results
    else:
        dedup_stats["after"] = len(results)

    # ---- Summary ----
    elapsed = time.monotonic() - start_time

    print("\n" + "=" * 60)
    print(f"Pipeline Complete")
    print("=" * 60)

    filter_line = ""
    if not skip_filter:
        filter_line = f"  Stage D filter: {stats['filter_kept']} kept / {stats['filter_skipped']} skipped\n"

    dedup_line = ""
    if dedup_stats["before"] >= 2:
        dedup_line = f"  Stage E dedup:  {dedup_stats['after']} unique / {dedup_stats['before'] - dedup_stats['after']} removed\n"

    print(f"""
  Elapsed:         {elapsed:.1f}s
  Stage A:         {stats['total_collected']} candidates
{filter_line}  Stage B extract: {stats['extracted']} success / {stats['failed_extract']} failed
  Stage C clean:   {stats['cleaned']} passed / {stats['failed_quality']} quality rejected
{dedup_line}  Blocked:         {stats['blocked']}
  Final output:    {len(results)} articles
""")

    if rate_limiter.blocked_domains:
        print(f"  Blocked domains: {', '.join(rate_limiter.blocked_domains)}")

    # ---- Stage F: Push to PowerReader ----
    push_stats = {"total": 0, "accepted": 0, "rejected": 0}

    if results and not skip_push:
        print(f"\n[Stage F] Pushing {len(results)} articles to PowerReader...")
        print("-" * 40)

        from .pusher import push_articles
        push_result = push_articles(results, dry_run=dry_run)

        push_stats["total"] = push_result["total"]
        push_stats["accepted"] = push_result["accepted"]
        push_stats["rejected"] = push_result["rejected"]

        if push_result["errors"]:
            print(f"  Push errors: {len(push_result['errors'])}")

        print(f"  -> {push_stats['accepted']}/{push_stats['total']} accepted")
    elif skip_push:
        print(f"\n  [SKIP] Push disabled (--no-push)")

    # Save results
    output_path = f"output/pipeline_run_{now.strftime('%Y%m%d_%H%M%S')}.json"
    _save_results(results, output_path, stats, elapsed)

    return {
        "stage": "complete",
        "stats": {**stats, "push": push_stats},
        "elapsed_s": elapsed,
        "output_path": output_path,
        "articles": results,
    }


def _save_results(
    articles: list[dict],
    output_path: str,
    stats: dict,
    elapsed: float,
) -> None:
    """Save pipeline results to JSON file."""
    import os
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    report = {
        "run_time": datetime.now(TW_TZ).isoformat(),
        "stats": stats,
        "elapsed_s": round(elapsed, 1),
        "articles": articles,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    print(f"  Output: {output_path}")


# ------------------------------------------------------------------
# CLI entry point
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="PowerReader Crawler Pipeline")
    parser.add_argument("--limit", type=int, default=0, help="Max articles to extract (0=all)")
    parser.add_argument("--source", type=str, default=None, help="Filter by source key")
    parser.add_argument("--dry-run", action="store_true", help="Only collect metadata (Stage A)")
    parser.add_argument("--freshness", type=int, default=6, help="Freshness window in hours")
    parser.add_argument("--no-filter", action="store_true", help="Skip topic filtering (Stage D)")
    parser.add_argument("--threshold", type=float, default=0.55, help="Topic filter threshold")
    parser.add_argument("--no-push", action="store_true", help="Skip pushing to PowerReader API (Stage F)")
    args = parser.parse_args()

    run_pipeline(
        limit=args.limit,
        source_filter=args.source,
        dry_run=args.dry_run,
        freshness_hours=args.freshness,
        skip_filter=args.no_filter,
        filter_threshold=args.threshold,
        skip_push=args.no_push,
    )


if __name__ == "__main__":
    main()
