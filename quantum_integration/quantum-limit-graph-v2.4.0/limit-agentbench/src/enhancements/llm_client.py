"""
Lightweight LLM client for generating natural language explanations.
"""

import aiohttp
import json
from typing import Dict, Any, Optional

class LLMClient:
    def __init__(self, endpoint: str = "http://localhost:8000/generate", model: str = "small"):
        self.endpoint = endpoint
        self.model = model

    async def generate_explanation(self, prompt: str) -> str:
        """
        Send prompt to LLM and return generated explanation.
        """
        async with aiohttp.ClientSession() as session:
            payload = {
                "prompt": prompt,
                "model": self.model,
                "max_tokens": 150,
                "temperature": 0.7,
            }
            async with session.post(self.endpoint, json=payload) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    return result.get("text", "")
                else:
                    raise RuntimeError(f"LLM returned {resp.status}")
