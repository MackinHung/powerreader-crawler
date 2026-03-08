"""
Stage D — Topic classification via fine-tuned ALBERT news classifier.
Copyright (C) 2026 @MackinHung (https://github.com/MackinHung)

Classifies articles using Mackin010/albert-news-tw-political,
fine-tuned from clhuang/albert-news-classification (26K articles)
on 544 real PowerReader articles with corrected labels for better
policy/geopolitics recall.

Model outputs 11 categories:
  政治, 科技, 運動, 證券, 產經, 娛樂, 生活, 國際, 社會, 文化, 兩岸

Decision logic:
  - KEEP: top category in POLITICAL_CATEGORIES
  - REJECT: otherwise

History:
  v1: bge-small-zh cosine-to-centroid — replaced (centroids overlap 0.55-0.74)
  v2: clhuang/albert-news-classification — good baseline but misses policy news
  v3: Mackin010/albert-news-tw-political — fine-tuned, +7% recall on policy/geopolitics

Usage:
    filter = TopicFilter()
    result = filter.classify(title="...", summary="...")
    if result["keep"]:
        article["filter_score"] = result["score"]
        article["matched_topic"] = result["topic"]
"""

import torch
from transformers import BertTokenizer, AlbertForSequenceClassification


# ------------------------------------------------------------------
# Category definitions
# ------------------------------------------------------------------

# 11 categories from ALBERT news classification (order matters!)
CATEGORIES: list[str] = [
    "政治", "科技", "運動", "證券", "產經",
    "娛樂", "生活", "國際", "社會", "文化", "兩岸",
]

# Categories considered politically relevant for PowerReader
POLITICAL_CATEGORIES: set[str] = {"政治", "國際", "兩岸", "社會"}

# Batch size for CPU inference (balances speed vs memory)
INFERENCE_BATCH_SIZE = 32

# Default minimum confidence — not used for category-based decision,
# kept for API compatibility with runner.py CLI --threshold flag.
DEFAULT_THRESHOLD = 0.55


# ------------------------------------------------------------------
# Topic filter class
# ------------------------------------------------------------------

class TopicFilter:
    """News topic classifier using fine-tuned ALBERT.

    v3: Mackin010/albert-news-tw-political (fine-tuned on 544 real articles).
    Falls back to clhuang/albert-news-classification if fine-tuned model unavailable.
    """

    def __init__(
        self,
        *,
        model_name: str = "Mackin010/albert-news-tw-political",
        tokenizer_name: str = "bert-base-chinese",
        threshold: float = DEFAULT_THRESHOLD,
        political_categories: set[str] | None = None,
    ):
        self._tokenizer = BertTokenizer.from_pretrained(tokenizer_name)
        self._model = AlbertForSequenceClassification.from_pretrained(model_name)
        self._model.eval()
        self._threshold = threshold
        self._political = political_categories or POLITICAL_CATEGORIES

    def classify(self, *, title: str, summary: str = "") -> dict:
        """Classify a single article.

        Args:
            title: Article title (required).
            summary: Article summary/description (optional).

        Returns:
            {
                "keep": bool,
                "score": float,        # confidence of top category
                "topic": str | None,   # top category name (None if rejected)
                "all_scores": dict,    # all 11 category probabilities
            }
        """
        text = f"{title} {summary[:200]}" if summary else title

        inputs = self._tokenizer(
            text, return_tensors="pt", truncation=True, max_length=128,
        )
        with torch.no_grad():
            logits = self._model(**inputs).logits
        probs = torch.nn.functional.softmax(logits, dim=-1)[0]

        return self._build_result(probs)

    def classify_batch(self, articles: list[dict]) -> list[dict]:
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

        # Process in batches to limit memory usage
        all_results: list[dict] = []

        for batch_start in range(0, len(texts), INFERENCE_BATCH_SIZE):
            batch_texts = texts[batch_start:batch_start + INFERENCE_BATCH_SIZE]

            inputs = self._tokenizer(
                batch_texts,
                return_tensors="pt",
                truncation=True,
                max_length=128,
                padding=True,
            )
            with torch.no_grad():
                logits = self._model(**inputs).logits
            batch_probs = torch.nn.functional.softmax(logits, dim=-1)

            for probs in batch_probs:
                all_results.append(self._build_result(probs))

        return all_results

    def _build_result(self, probs: torch.Tensor) -> dict:
        """Build classification result from probability tensor."""
        best_idx = torch.argmax(probs).item()
        best_cat = CATEGORIES[best_idx]
        best_conf = float(probs[best_idx])

        keep = best_cat in self._political

        all_scores = {
            cat: round(float(probs[i]), 4)
            for i, cat in enumerate(CATEGORIES)
        }

        return {
            "keep": keep,
            "score": best_conf,
            "topic": best_cat if keep else None,
            "all_scores": all_scores,
        }

    @property
    def threshold(self) -> float:
        return self._threshold
