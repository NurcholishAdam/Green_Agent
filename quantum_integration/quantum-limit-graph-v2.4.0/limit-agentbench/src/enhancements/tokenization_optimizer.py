# File: src/enhancements/tokenization_optimizer.py
"""
Tokenization optimizer – language‑aware tokenizer selection, segmentation, and token budgets.
"""

from typing import List, Dict, Any, Optional, Tuple
import re
import logging
from transformers import AutoTokenizer

logger = logging.getLogger(__name__)

class TokenizationOptimizer:
    """
    Optimizes tokenization for sustainability:
    - Selects the most efficient tokenizer per language.
    - Segments input by punctuation to route short segments to rule‑based experts.
    - Enforces per‑expert token budgets.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.tokenizers: Dict[str, AutoTokenizer] = {}
        self.language_map = {
            'en': 'bert-base-uncased',
            'id': 'bert-base-indonesian-1.5G',
            # add more languages
        }
        self._loaded = set()

    def _get_tokenizer(self, language: str) -> AutoTokenizer:
        if language not in self.tokenizers:
            model_name = self.language_map.get(language, 'bert-base-uncased')
            try:
                self.tokenizers[language] = AutoTokenizer.from_pretrained(model_name)
                logger.info(f"Loaded tokenizer for {language}: {model_name}")
            except Exception as e:
                logger.error(f"Failed to load tokenizer for {language}: {e}")
                self.tokenizers[language] = AutoTokenizer.from_pretrained('bert-base-uncased')
        return self.tokenizers[language]

    def detect_language(self, text: str) -> str:
        """Simple language detection (could use langdetect)."""
        # Placeholder – use a library like langdetect
        return 'en'

    def optimize(self, text: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Returns a dict with:
        - 'segments': list of (segment, type)
        - 'total_tokens': int
        - 'tokenizer_used': str
        """
        language = context.get('language') or self.detect_language(text)
        tokenizer = self._get_tokenizer(language)

        # Tokenization
        tokens = tokenizer.encode(text, add_special_tokens=False)
        total_tokens = len(tokens)

        # Segmentation by punctuation
        segments: List[Tuple[str, str]] = []
        parts = re.split(r'(?<=[.!?])\s+', text)
        for part in parts:
            if len(part.split()) < 10:
                segments.append((part, 'short'))
            else:
                segments.append((part, 'long'))

        # Token budget enforcement
        budget = context.get('token_budget', 100)
        if total_tokens > budget:
            summary = self._summarize(text, budget)
            return self.optimize(summary, {**context, 'token_budget': budget})

        return {
            'segments': segments,
            'total_tokens': total_tokens,
            'tokenizer_used': language,
        }

    def _summarize(self, text: str, budget: int) -> str:
        """Simple truncation or call a summarizer expert."""
        # For demo, truncate to budget * 4 characters (rough)
        return text[:budget * 4]

    def get_token_efficiency(self, text: str, language: str = None) -> float:
        """Return tokens per character as a measure of efficiency."""
        lang = language or self.detect_language(text)
        tokenizer = self._get_tokenizer(lang)
        tokens = tokenizer.encode(text, add_special_tokens=False)
        return len(tokens) / len(text) if text else 0.0
