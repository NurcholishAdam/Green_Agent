"""
adaptive_precision_controller.py
Hardware-aware precision switching for MOPD training.
"""
from __future__ import annotations
import asyncio
from typing import Any, Dict, Optional

try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False


class AdaptivePrecisionSwitcher:
    ENERGY = {"fp32": 1.0, "tf32": 0.75, "bf16": 0.55, "fp16": 0.5, "int8": 0.3}

    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.current = "fp32"
        self.saved_wh = 0.0

    def _probe(self) -> Dict[str, Any]:
        info = {"cuda": False, "bf16": False, "device": "cpu"}
        if TORCH_AVAILABLE:
            try:
                info["cuda"] = torch.cuda.is_available()
                if info["cuda"]:
                    info["device"] = torch.cuda.get_device_name(0)
                    info["bf16"] = torch.cuda.is_bf16_supported()
            except Exception:
                pass
        return info

    def select_precision(self) -> str:
        hw = self._probe()
        cands = list(getattr(self.config, "precision_levels",
                             ["fp32", "fp16", "bf16", "int8"]))
        if not hw["cuda"]:
            cands = [c for c in cands if c in ("fp32", "int8")]
        if not hw["bf16"]:
            cands = [c for c in cands if c != "bf16"]
        return min(cands, key=lambda c: self.ENERGY.get(c, 1.0))

    async def switch_to(self, target: str, reason: str = "policy") -> bool:
        if target == self.current or target not in self.ENERGY:
            return False
        old = self.current
        self.current = target
        saved = max(0.0, self.ENERGY[old] - self.ENERGY[target])
        self.saved_wh += saved
        await asyncio.to_thread(
            self.storage.save_precision_switch, old, target, reason, saved, 0.0)
        return True

    async def auto_switch(self, recent_acc: float, baseline_acc: float) -> None:
        if baseline_acc <= 0:
            return
        drop = (baseline_acc - recent_acc) / baseline_acc
        thresh = getattr(self.config, "precision_switch_threshold", 0.02)
        if drop > thresh:
            await self.switch_to("fp32", reason=f"acc drop {drop:.3f}")
        elif drop < thresh / 2:
            await self.switch_to(self.select_precision(), reason="headroom")
