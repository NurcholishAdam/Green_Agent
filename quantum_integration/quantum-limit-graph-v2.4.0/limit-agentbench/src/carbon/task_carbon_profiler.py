"""
Task Carbon Profiler
====================

Estimates energy and carbon consumption for ML tasks based on historical telemetry,
model characteristics, and hardware profiles.

Location: src/carbon/task_carbon_profiler.py
"""

from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from datetime import datetime
import numpy as np
import logging
import json
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class CarbonEstimate:
    """Carbon emission estimate for a task"""
    task_id: str
    expected_energy_kwh: float
    expected_carbon_kgco2e: float
    carbon_intensity_gco2kwh: float
    confidence: float  # 0-1
    estimation_method: str
    breakdown: Dict[str, float]


class TaskCarbonProfiler:
    """Profiles and estimates carbon emissions for ML tasks"""
    
    def __init__(self, telemetry_db_path: Optional[Path] = None):
        self.telemetry_db_path = telemetry_db_path or Path("data/telemetry.json")
        self.historical_telemetry: List[Dict[str, Any]] = self._load_telemetry()
        
        # Hardware TDP (Watts)
        self.hardware_tdp = {
            "V100": 300, "A100": 400, "H100": 700, "T4": 70,
            "RTX3090": 350, "CPU": 65, "TPU_v4": 200
        }
        
        # Architecture base energy (kWh per 1B params per 1K samples)
        self.architecture_energy = {
            "transformer": 0.002, "cnn": 0.001, "rnn": 0.0015,
            "hybrid": 0.0018, "dense": 0.0008
        }
    
    def _load_telemetry(self) -> List[Dict[str, Any]]:
        """Load historical telemetry"""
        if not self.telemetry_db_path.exists():
            return []
        try:
            with open(self.telemetry_db_path, 'r') as f:
                return json.load(f)
        except:
            return []
    
    async def estimate_energy(
        self,
        task: Dict[str, Any],
        carbon_intensity: Optional[float] = None
    ) -> CarbonEstimate:
        """Estimate energy and carbon for task"""
        
        # Try historical lookup first
        historical = self._estimate_from_historical(task)
        if historical and historical.confidence > 0.7:
            if carbon_intensity:
                historical.expected_carbon_kgco2e = historical.expected_energy_kwh * carbon_intensity / 1000
                historical.carbon_intensity_gco2kwh = carbon_intensity
            return historical
        
        # Fallback to heuristics
        heuristic = self._estimate_from_heuristics(task)
        if carbon_intensity:
            heuristic.expected_carbon_kgco2e = heuristic.expected_energy_kwh * carbon_intensity / 1000
            heuristic.carbon_intensity_gco2kwh = carbon_intensity
        return heuristic
    
    def _estimate_from_historical(self, task: Dict[str, Any]) -> Optional[CarbonEstimate]:
        """Estimate from historical data"""
        similar = [
            t for t in self.historical_telemetry
            if (t.get("model_name") == task.get("model_name") and
                t.get("hardware") == task.get("hardware"))
        ]
        
        if len(similar) < 3:
            return None
        
        energies = [t["energy_kwh"] for t in similar]
        mean_energy = np.mean(energies)
        std_energy = np.std(energies)
        
        # Adjust for dataset size
        dataset_ratio = task.get("dataset_size", 1) / np.mean([t.get("dataset_size", 1) for t in similar])
        adjusted_energy = mean_energy * dataset_ratio
        
        confidence = min(1.0, len(similar) / 10) * (1 - min(std_energy / mean_energy, 0.5))
        
        return CarbonEstimate(
            task_id=task.get("task_id", "unknown"),
            expected_energy_kwh=adjusted_energy,
            expected_carbon_kgco2e=0.0,
            carbon_intensity_gco2kwh=0.0,
            confidence=confidence,
            estimation_method="historical",
            breakdown={"mean": mean_energy, "std": std_energy, "adjustment": dataset_ratio}
        )
    
    def _estimate_from_heuristics(self, task: Dict[str, Any]) -> CarbonEstimate:
        """Estimate from heuristics"""
        model_name = task.get("model_name", "")
        num_params = task.get("num_parameters", self._guess_params(model_name))
        dataset_size = task.get("dataset_size", 0)
        num_epochs = task.get("num_epochs", 1)
        batch_size = task.get("batch_size", 32)
        hardware = task.get("hardware", "V100")
        
        # Detect architecture
        arch = self._detect_architecture(model_name)
        base_energy = self.architecture_energy.get(arch, 0.002)
        
        # Calculate energy
        iterations = (dataset_size / batch_size) * num_epochs if batch_size > 0 else 0
        time_per_iter = (num_params / 1e9) * 0.1  # seconds
        total_hours = (iterations * time_per_iter) / 3600
        
        hardware_watts = self.hardware_tdp.get(hardware, 300)
        energy_tdp = (hardware_watts * total_hours * 0.7) / 1000
        energy_params = (num_params / 1e9) * base_energy * (dataset_size / 1000)
        
        total_energy = (energy_tdp * 0.7 + energy_params * 0.3) * 2.0  # 2x safety factor
        
        return CarbonEstimate(
            task_id=task.get("task_id", "unknown"),
            expected_energy_kwh=total_energy,
            expected_carbon_kgco2e=0.0,
            carbon_intensity_gco2kwh=0.0,
            confidence=0.5,
            estimation_method="heuristic",
            breakdown={
                "params": num_params,
                "iterations": iterations,
                "hours": total_hours,
                "energy_tdp": energy_tdp,
                "energy_params": energy_params
            }
        )
    
    def _guess_params(self, model_name: str) -> int:
        """Guess parameters from model name"""
        name = model_name.lower()
        if "bert-base" in name: return 110_000_000
        elif "bert-large" in name: return 340_000_000
        elif "gpt2" in name: return 124_000_000
        elif "t5-base" in name: return 220_000_000
        elif "resnet50" in name: return 25_000_000
        else: return 100_000_000
    
    def _detect_architecture(self, model_name: str) -> str:
        """Detect architecture from name"""
        name = model_name.lower()
        if any(x in name for x in ["bert", "gpt", "t5"]): return "transformer"
        elif any(x in name for x in ["resnet", "efficientnet"]): return "cnn"
        elif any(x in name for x in ["lstm", "gru"]): return "rnn"
        else: return "dense"
    
    def add_telemetry_record(self, task: Dict[str, Any], actual_energy: float, actual_carbon: float):
        """Add completed task to telemetry"""
        record = {
            "task_id": task.get("task_id"),
            "timestamp": datetime.now().isoformat(),
            "model_name": task.get("model_name"),
            "hardware": task.get("hardware"),
            "dataset_size": task.get("dataset_size"),
            "energy_kwh": actual_energy,
            "carbon_kgco2e": actual_carbon
        }
        self.historical_telemetry.append(record)
        self._save_telemetry()
    
    def _save_telemetry(self):
        """Save telemetry to disk"""
        try:
            self.telemetry_db_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.telemetry_db_path, 'w') as f:
                json.dump(self.historical_telemetry, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save telemetry: {e}")


if __name__ == "__main__":
    import asyncio
    
    async def main():
        profiler = TaskCarbonProfiler()
        task = {
            "task_id": "bert_test",
            "model_name": "bert-base-uncased",
            "task_type": "fine_tuning",
            "dataset_size": 10_000,
            "num_epochs": 3,
            "batch_size": 32,
            "hardware": "V100"
        }
        
        estimate = await profiler.estimate_energy(task, carbon_intensity=400.0)
        print(f"Energy: {estimate.expected_energy_kwh:.4f} kWh")
        print(f"Carbon: {estimate.expected_carbon_kgco2e:.4f} kgCO2e")
        print(f"Confidence: {estimate.confidence:.2f}")
    
    asyncio.run(main())
