"""
Stage D — Topic filtering via bge-small-zh-v1.5 embeddings.

Filters articles by semantic similarity to predefined topic categories.
Only social/political articles pass through; entertainment, sports (non-political),
lifestyle, etc. are discarded.

Usage:
    filter = TopicFilter()
    result = filter.classify(title="...", summary="...")
    if result["keep"]:
        article["filter_score"] = result["score"]
        article["matched_topic"] = result["topic"]
"""

import numpy as np
from sentence_transformers import SentenceTransformer

# ------------------------------------------------------------------
# Topic reference definitions
# ------------------------------------------------------------------

# Each topic has multiple reference texts to improve matching coverage.
# bge-small-zh-v1.5 is optimized for Chinese semantic similarity.

TOPIC_REFS: dict[str, list[str]] = {
    "政治動態": [
        "總統府立法院行政院政策施政",
        "選舉候選人政黨競選民調",
        "立法委員質詢法案審議修法",
        "政治人物發言記者會聲明",
    ],
    "社會議題": [
        "社會問題弱勢族群權益保障",
        "抗議示威遊行社會運動",
        "貧富差距居住正義房價問題",
        "性別平等歧視人權議題",
    ],
    "經濟政策": [
        "財政預算稅制改革經濟發展",
        "央行利率貨幣政策通膨物價",
        "產業政策半導體供應鏈貿易",
        "勞工薪資就業失業基本工資",
    ],
    "國防外交": [
        "兩岸關係台海軍事國防安全",
        "外交邦交國際關係台美關係",
        "軍購國防預算軍事演習",
        "中國大陸對台政策統獨議題",
    ],
    "司法人權": [
        "司法改革法院判決審判訴訟",
        "檢察官偵辦起訴貪污弊案",
        "死刑廢死人權公約轉型正義",
        "言論自由新聞自由集會自由",
    ],
    "環境能源": [
        "能源政策核能再生能源減碳",
        "環境污染空氣品質氣候變遷",
        "國土規劃都市計畫土地徵收",
        "食品安全公共衛生防疫政策",
    ],
    "教育文化": [
        "教育改革課綱大學入學制度",
        "文化政策文化資產保存母語",
        "原住民客家族群多元文化",
        "媒體識讀假新聞資訊素養",
    ],
}

# Default similarity threshold — articles below this are discarded.
# Calibrated via testing: 0.55 catches most relevant articles
# while filtering out pure entertainment/lifestyle/sports.
DEFAULT_THRESHOLD = 0.55


# ------------------------------------------------------------------
# Topic filter class
# ------------------------------------------------------------------

class TopicFilter:
    """Semantic topic filter using bge-small-zh-v1.5 embeddings."""

    def __init__(
        self,
        *,
        model_name: str = "BAAI/bge-small-zh-v1.5",
        threshold: float = DEFAULT_THRESHOLD,
    ):
        self._model = SentenceTransformer(model_name)
        self._threshold = threshold
        self._topic_embeddings: dict[str, np.ndarray] = {}
        self._build_topic_embeddings()

    def _build_topic_embeddings(self) -> None:
        """Pre-compute averaged embeddings for each topic category."""
        for topic, refs in TOPIC_REFS.items():
            embeddings = self._model.encode(refs, normalize_embeddings=True)
            # Average all reference vectors, then re-normalize
            avg = np.mean(embeddings, axis=0)
            avg = avg / np.linalg.norm(avg)
            self._topic_embeddings[topic] = avg

    def classify(self, *, title: str, summary: str = "") -> dict:
        """Classify an article by its title and summary.

        Args:
            title: Article title (required)
            summary: Article summary/description (optional)

        Returns:
            {
                "keep": bool,
                "score": float,        # max cosine similarity
                "topic": str | None,   # matched topic name
                "all_scores": dict,    # all topic scores
            }
        """
        # Combine title + summary for richer signal
        text = title
        if summary:
            text = f"{title} {summary[:200]}"

        # Encode article text
        article_emb = self._model.encode(
            [text], normalize_embeddings=True
        )[0]

        # Compute cosine similarity against all topics
        scores: dict[str, float] = {}
        for topic, topic_emb in self._topic_embeddings.items():
            sim = float(np.dot(article_emb, topic_emb))
            scores[topic] = round(sim, 4)

        # Find best match
        best_topic = max(scores, key=scores.get)
        best_score = scores[best_topic]

        return {
            "keep": best_score >= self._threshold,
            "score": best_score,
            "topic": best_topic if best_score >= self._threshold else None,
            "all_scores": scores,
        }

    def classify_batch(
        self, articles: list[dict],
    ) -> list[dict]:
        """Classify multiple articles efficiently.

        Args:
            articles: List of dicts with "title" and optional "summary" keys.

        Returns:
            List of classification results (same order as input).
        """
        if not articles:
            return []

        # Build input texts
        texts = []
        for art in articles:
            title = art.get("title", "")
            summary = art.get("summary", "")
            text = f"{title} {summary[:200]}" if summary else title
            texts.append(text)

        # Batch encode
        embeddings = self._model.encode(
            texts, normalize_embeddings=True, batch_size=32
        )

        # Classify each
        results = []
        for emb in embeddings:
            scores: dict[str, float] = {}
            for topic, topic_emb in self._topic_embeddings.items():
                sim = float(np.dot(emb, topic_emb))
                scores[topic] = round(sim, 4)

            best_topic = max(scores, key=scores.get)
            best_score = scores[best_topic]

            results.append({
                "keep": best_score >= self._threshold,
                "score": best_score,
                "topic": best_topic if best_score >= self._threshold else None,
                "all_scores": scores,
            })

        return results

    @property
    def threshold(self) -> float:
        return self._threshold
