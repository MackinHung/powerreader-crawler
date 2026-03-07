"""
Stage E — Content deduplication (SHA256 exact + bge-small-zh semantic).

Two-layer dedup within a single pipeline batch:
  Layer 1: SHA256 content_hash exact match (already computed in cleaner)
  Layer 2: bge-small-zh cosine similarity classification + clustering

Article type classification:
  < 0.50  → original        (unique content)
  0.50-0.80 → different_angle (same event, different reporting)
  0.80-0.95 → rewrite        (highly similar wording)
  > 0.95  → duplicate        (near-verbatim copy)
"""

import numpy as np
from sentence_transformers import SentenceTransformer

# ------------------------------------------------------------------
# Thresholds (aligned with shared/config.js ANALYSIS section)
# ------------------------------------------------------------------

THRESHOLD_ORIGINAL = 0.50       # < 0.50 = original
THRESHOLD_DIFFERENT_ANGLE = 0.80  # 0.50-0.80 = different_angle
THRESHOLD_DUPLICATE = 0.95      # > 0.95 = duplicate
# Between 0.80 and 0.95 = rewrite

MIN_CHARS_FOR_SEMANTIC = 200    # Skip semantic dedup for very short articles


def classify_similarity(score: float) -> str:
    """Classify article relationship by cosine similarity score."""
    if score >= THRESHOLD_DUPLICATE:
        return "duplicate"
    if score >= THRESHOLD_DIFFERENT_ANGLE:
        return "rewrite"
    if score >= THRESHOLD_ORIGINAL:
        return "different_angle"
    return "original"


# ------------------------------------------------------------------
# Deduplicator class
# ------------------------------------------------------------------

class Deduplicator:
    """Two-layer dedup: SHA256 exact + bge-small-zh semantic."""

    def __init__(
        self,
        *,
        model: SentenceTransformer | None = None,
        model_name: str = "BAAI/bge-small-zh-v1.5",
    ):
        # Reuse existing model instance if provided (from TopicFilter)
        if model is not None:
            self._model = model
        else:
            self._model = SentenceTransformer(model_name)

    def deduplicate(self, articles: list[dict]) -> list[dict]:
        """Deduplicate a batch of articles in-place.

        Args:
            articles: List of cleaned article dicts (with content_hash,
                      content_markdown).

        Returns:
            Filtered list with duplicates removed and dedup_metadata populated.
            Original list is NOT mutated — new dicts are returned.
        """
        if len(articles) <= 1:
            return [
                {**art, "dedup_metadata": _solo_metadata()}
                for art in articles
            ]

        # ---- Layer 1: SHA256 exact dedup ----
        unique_by_hash: dict[str, dict] = {}
        hash_groups: dict[str, list[str]] = {}  # content_hash → [article_ids]

        for art in articles:
            h = art.get("content_hash", "")
            if h in unique_by_hash:
                # Exact duplicate — keep the longer one
                existing = unique_by_hash[h]
                if art.get("char_count", 0) > existing.get("char_count", 0):
                    unique_by_hash[h] = art
                hash_groups[h].append(art["article_id"])
            else:
                unique_by_hash[h] = art
                hash_groups[h] = [art["article_id"]]

        layer1_unique = list(unique_by_hash.values())
        layer1_removed = len(articles) - len(layer1_unique)

        if len(layer1_unique) <= 1:
            return [
                {
                    **art,
                    "dedup_metadata": {
                        "article_type": "original",
                        "max_similarity": 0.0,
                        "cluster_size": len(hash_groups.get(art.get("content_hash", ""), [])),
                        "duplicate_urls": [],
                        "layer1_exact_removed": layer1_removed,
                    },
                }
                for art in layer1_unique
            ]

        # ---- Layer 2: bge-small-zh cosine semantic ----
        # Encode articles
        texts = []
        encodable_indices = []

        for i, art in enumerate(layer1_unique):
            content = art.get("content_markdown", "")
            if len(content) >= MIN_CHARS_FOR_SEMANTIC:
                texts.append(content[:2000])  # Cap at 2000 chars for efficiency
                encodable_indices.append(i)

        # Compute embeddings
        if len(texts) >= 2:
            embeddings = self._model.encode(
                texts, normalize_embeddings=True, batch_size=32
            )

            # Pairwise cosine similarity matrix
            sim_matrix = np.dot(embeddings, embeddings.T)

            # Build similarity pairs
            pairs: list[tuple[int, int, float]] = []
            for i in range(len(encodable_indices)):
                for j in range(i + 1, len(encodable_indices)):
                    sim = float(sim_matrix[i][j])
                    if sim >= THRESHOLD_ORIGINAL:
                        pairs.append((
                            encodable_indices[i],
                            encodable_indices[j],
                            sim,
                        ))
        else:
            pairs = []

        # Build max similarity per article
        max_sim: dict[int, float] = {}
        most_similar: dict[int, int] = {}

        for idx_a, idx_b, sim in pairs:
            if idx_a not in max_sim or sim > max_sim[idx_a]:
                max_sim[idx_a] = sim
                most_similar[idx_a] = idx_b
            if idx_b not in max_sim or sim > max_sim[idx_b]:
                max_sim[idx_b] = sim
                most_similar[idx_b] = idx_a

        # Union-Find clustering
        clusters = _union_find_cluster(pairs, len(layer1_unique))

        # Select primary per cluster (longest content)
        primary_map = _select_primary(clusters, layer1_unique)

        # Build results
        results = []
        for i, art in enumerate(layer1_unique):
            sim_score = max_sim.get(i, 0.0)
            article_type = classify_similarity(sim_score)

            # Collect duplicate URLs in same cluster
            cluster_id = clusters[i]
            cluster_members = [
                j for j in range(len(layer1_unique))
                if clusters[j] == cluster_id and j != i
            ]
            dup_urls = [layer1_unique[j]["primary_url"] for j in cluster_members]

            is_primary = primary_map.get(i, True)

            # Skip semantic duplicates (> 0.95) that are not primary
            if article_type == "duplicate" and not is_primary:
                continue

            results.append({
                **art,
                "dedup_metadata": {
                    "article_type": article_type,
                    "max_similarity": round(sim_score, 4),
                    "cluster_size": len(cluster_members) + 1,
                    "duplicate_urls": dup_urls,
                    "is_primary": is_primary,
                    "layer1_exact_removed": layer1_removed,
                },
            })

        return results


# ------------------------------------------------------------------
# Union-Find
# ------------------------------------------------------------------

def _union_find_cluster(
    pairs: list[tuple[int, int, float]],
    n: int,
) -> list[int]:
    """Cluster articles using Union-Find on similarity pairs."""
    parent = list(range(n))
    rank = [0] * n

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]  # path compression
            x = parent[x]
        return x

    def union(x: int, y: int) -> None:
        rx, ry = find(x), find(y)
        if rx == ry:
            return
        if rank[rx] < rank[ry]:
            parent[rx] = ry
        elif rank[rx] > rank[ry]:
            parent[ry] = rx
        else:
            parent[ry] = rx
            rank[rx] += 1

    for idx_a, idx_b, _ in pairs:
        union(idx_a, idx_b)

    return [find(i) for i in range(n)]


def _select_primary(
    clusters: list[int],
    articles: list[dict],
) -> dict[int, bool]:
    """Select primary article per cluster (longest content wins)."""
    cluster_groups: dict[int, list[int]] = {}
    for i, c in enumerate(clusters):
        cluster_groups.setdefault(c, []).append(i)

    primary: dict[int, bool] = {}
    for members in cluster_groups.values():
        if len(members) == 1:
            primary[members[0]] = True
            continue

        # Sort by char_count descending
        sorted_members = sorted(
            members,
            key=lambda i: articles[i].get("char_count", 0),
            reverse=True,
        )
        for j, idx in enumerate(sorted_members):
            primary[idx] = (j == 0)

    return primary


def _solo_metadata() -> dict:
    """Default dedup_metadata for single-article batches."""
    return {
        "article_type": "original",
        "max_similarity": 0.0,
        "cluster_size": 1,
        "duplicate_urls": [],
        "is_primary": True,
        "layer1_exact_removed": 0,
    }
