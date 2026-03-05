"""
Workload Interpreter
====================

Intelligently parses AI workloads and converts them into carbon-aware execution DAGs.

This is the ENTRY POINT for the entire Green Agent system.

Location: src/interpretation/workload_interpreter.py
"""

from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass
from enum import Enum
import re
import logging

logger = logging.getLogger(__name__)


class TaskType(Enum):
    """Supported AI task types"""
    FINE_TUNING = "fine_tuning"
    TRAINING = "training"
    INFERENCE = "inference"
    AGENT_SIMULATION = "agent_simulation"
    BENCHMARK_EVALUATION = "benchmark"
    DATA_PROCESSING = "data_processing"
    UNKNOWN = "unknown"


class ModelArchitecture(Enum):
    """Model architecture families"""
    TRANSFORMER = "transformer"
    CNN = "cnn"
    RNN = "rnn"
    HYBRID = "hybrid"
    DENSE = "dense"
    DIFFUSION = "diffusion"
    REINFORCEMENT = "reinforcement"


@dataclass
class DAGStep:
    """Single step in execution DAG"""
    step_id: str
    name: str
    estimated_energy_kwh: float
    estimated_time_hours: float
    dependencies: List[str]
    parallelizable: bool


@dataclass
class WorkloadProfile:
    """Complete workload profile"""
    task_id: str
    task_type: TaskType
    model_architecture: ModelArchitecture
    model_name: str
    model_params: int
    dataset_size: int
    dataset_quality_score: float  # 0-1
    estimated_flops: float
    estimated_memory_gb: float
    estimated_time_hours: float
    estimated_energy_kwh: float
    execution_dag: List[DAGStep]
    optimization_candidates: List[str]
    carbon_optimization_potential: float  # % reduction possible


class WorkloadInterpreter:
    """
    Interprets AI workloads and creates carbon-aware execution plans
    
    Pipeline:
    1. Parse task specification
    2. Analyze model characteristics
    3. Analyze dataset characteristics
    4. Estimate computational complexity
    5. Construct carbon-aware DAG
    6. Identify optimization opportunities
    """
    
    def __init__(self):
        # Model parameter database (approximate)
        self.model_params_db = {
            # Transformers
            "bert-base": 110_000_000,
            "bert-large": 340_000_000,
            "gpt2": 124_000_000,
            "gpt2-medium": 355_000_000,
            "gpt2-large": 774_000_000,
            "gpt2-xl": 1_500_000_000,
            "t5-small": 60_000_000,
            "t5-base": 220_000_000,
            "t5-large": 770_000_000,
            "llama-7b": 7_000_000_000,
            "llama-13b": 13_000_000_000,
            "llama-70b": 70_000_000_000,
            # Vision
            "resnet50": 25_000_000,
            "resnet101": 45_000_000,
            "resnet152": 60_000_000,
            "vit-base": 86_000_000,
            "vit-large": 307_000_000,
            "efficientnet-b0": 5_300_000,
            "efficientnet-b7": 66_000_000,
            # Diffusion
            "stable-diffusion-1.5": 860_000_000,
            "stable-diffusion-2.1": 865_000_000,
        }
        
        # Architecture patterns for detection
        self.architecture_patterns = {
            ModelArchitecture.TRANSFORMER: ["bert", "gpt", "t5", "llama", "bloom", "opt", "transformer"],
            ModelArchitecture.CNN: ["resnet", "vgg", "inception", "efficientnet", "mobilenet", "convnet"],
            ModelArchitecture.RNN: ["lstm", "gru", "rnn"],
            ModelArchitecture.HYBRID: ["vit", "clip", "swin", "beit"],
            ModelArchitecture.DIFFUSION: ["stable-diffusion", "dall-e", "midjourney", "imagen"],
            ModelArchitecture.REINFORCEMENT: ["dqn", "ppo", "a3c", "sac", "td3"]
        }
        
        # FLOPs per parameter per sample (rough heuristics)
        self.flops_per_param = {
            ModelArchitecture.TRANSFORMER: 6.0,  # 2 forward + 4 backward
            ModelArchitecture.CNN: 4.0,
            ModelArchitecture.RNN: 8.0,  # Sequential nature
            ModelArchitecture.HYBRID: 5.0,
            ModelArchitecture.DIFFUSION: 10.0,  # Iterative denoising
            ModelArchitecture.REINFORCEMENT: 3.0,
        }
        
        logger.info("Workload Interpreter initialized")
    
    def interpret(self, task: Dict[str, Any]) -> WorkloadProfile:
        """
        Main interpretation pipeline
        
        Args:
            task: Raw task specification
        
        Returns:
            Complete WorkloadProfile with execution DAG
        """
        
        logger.info(f"Interpreting workload: {task.get('task_id', 'unknown')}")
        
        # Step 1: Parse task type
        task_type = self._parse_task_type(task)
        
        # Step 2: Analyze model
        model_name, model_arch, model_params = self._analyze_model(task)
        
        # Step 3: Analyze dataset
        dataset_size, dataset_quality = self._analyze_dataset(task)
        
        # Step 4: Estimate complexity
        flops, memory_gb, time_hours, energy_kwh = self._estimate_complexity(
            task_type=task_type,
            model_arch=model_arch,
            model_params=model_params,
            dataset_size=dataset_size,
            task=task
        )
        
        # Step 5: Construct execution DAG
        execution_dag = self._construct_dag(
            task_type=task_type,
            model_arch=model_arch,
            energy_kwh=energy_kwh,
            time_hours=time_hours
        )
        
        # Step 6: Identify optimization opportunities
        optimization_candidates, carbon_potential = self._identify_optimizations(
            task_type=task_type,
            model_params=model_params,
            dataset_size=dataset_size,
            dataset_quality=dataset_quality,
            task=task
        )
        
        profile = WorkloadProfile(
            task_id=task.get("task_id", "unknown"),
            task_type=task_type,
            model_architecture=model_arch,
            model_name=model_name,
            model_params=model_params,
            dataset_size=dataset_size,
            dataset_quality_score=dataset_quality,
            estimated_flops=flops,
            estimated_memory_gb=memory_gb,
            estimated_time_hours=time_hours,
            estimated_energy_kwh=energy_kwh,
            execution_dag=execution_dag,
            optimization_candidates=optimization_candidates,
            carbon_optimization_potential=carbon_potential
        )
        
        logger.info(
            f"Workload interpreted: {task_type.value}, {model_params:,} params, "
            f"{energy_kwh:.2f} kWh, {carbon_potential:.0f}% optimization potential"
        )
        
        return profile
    
    def _parse_task_type(self, task: Dict[str, Any]) -> TaskType:
        """Detect task type from specification"""
        
        # Explicit task type
        if "task_type" in task:
            try:
                return TaskType(task["task_type"])
            except ValueError:
                pass
        
        # Infer from keywords
        task_str = str(task).lower()
        
        if any(kw in task_str for kw in ["fine-tune", "fine_tuning", "finetune", "adapt"]):
            return TaskType.FINE_TUNING
        elif any(kw in task_str for kw in ["train", "training", "pretrain"]):
            return TaskType.TRAINING
        elif any(kw in task_str for kw in ["infer", "inference", "predict", "generate"]):
            return TaskType.INFERENCE
        elif any(kw in task_str for kw in ["agent", "simulation", "multi-agent"]):
            return TaskType.AGENT_SIMULATION
        elif any(kw in task_str for kw in ["benchmark", "evaluate", "test"]):
            return TaskType.BENCHMARK_EVALUATION
        elif any(kw in task_str for kw in ["preprocess", "transform", "clean"]):
            return TaskType.DATA_PROCESSING
        else:
            return TaskType.UNKNOWN
    
    def _analyze_model(self, task: Dict[str, Any]) -> Tuple[str, ModelArchitecture, int]:
        """Analyze model characteristics"""
        
        model_name = task.get("model_name", task.get("model", "unknown"))
        
        # Detect architecture
        model_arch = ModelArchitecture.DENSE  # Default
        for arch, patterns in self.architecture_patterns.items():
            if any(pattern in model_name.lower() for pattern in patterns):
                model_arch = arch
                break
        
        # Get parameter count
        if "num_parameters" in task:
            model_params = task["num_parameters"]
        elif "model_params" in task:
            model_params = task["model_params"]
        else:
            # Lookup in database
            model_params = self._lookup_model_params(model_name)
        
        return model_name, model_arch, model_params
    
    def _lookup_model_params(self, model_name: str) -> int:
        """Lookup model parameters in database"""
        
        model_name_lower = model_name.lower()
        
        # Exact match
        if model_name_lower in self.model_params_db:
            return self.model_params_db[model_name_lower]
        
        # Partial match
        for key, params in self.model_params_db.items():
            if key in model_name_lower or model_name_lower in key:
                return params
        
        # Extract from name (e.g., "llama-7b" → 7B)
        match = re.search(r'(\d+)b', model_name_lower)
        if match:
            return int(match.group(1)) * 1_000_000_000
        
        match = re.search(r'(\d+)m', model_name_lower)
        if match:
            return int(match.group(1)) * 1_000_000
        
        # Default: assume medium model
        logger.warning(f"Model {model_name} not found, assuming 100M params")
        return 100_000_000
    
    def _analyze_dataset(self, task: Dict[str, Any]) -> Tuple[int, float]:
        """Analyze dataset characteristics"""
        
        # Dataset size
        if "dataset_size" in task:
            dataset_size = task["dataset_size"]
        elif "num_samples" in task:
            dataset_size = task["num_samples"]
        else:
            # Estimate from file size if available
            if "dataset_size_gb" in task:
                # Rough: 1 sample ~ 1KB for text, 100KB for images
                if "image" in str(task).lower():
                    dataset_size = int(task["dataset_size_gb"] * 1024 * 1024 / 100)
                else:
                    dataset_size = int(task["dataset_size_gb"] * 1024 * 1024)
            else:
                dataset_size = 10_000  # Default
        
        # Dataset quality (heuristic)
        quality_score = task.get("dataset_quality", 0.8)  # Default: good quality
        
        # Adjust based on indicators
        if "synthetic" in str(task).lower():
            quality_score *= 0.9  # Synthetic data slightly lower quality
        if "augmented" in str(task).lower():
            quality_score *= 0.95
        if "cleaned" in str(task).lower():
            quality_score *= 1.1
        if "noisy" in str(task).lower():
            quality_score *= 0.7
        
        quality_score = min(1.0, quality_score)
        
        return dataset_size, quality_score
    
    def _estimate_complexity(
        self,
        task_type: TaskType,
        model_arch: ModelArchitecture,
        model_params: int,
        dataset_size: int,
        task: Dict[str, Any]
    ) -> Tuple[float, float, float, float]:
        """Estimate computational complexity"""
        
        # Get task-specific parameters
        num_epochs = task.get("num_epochs", 3 if task_type == TaskType.FINE_TUNING else 10)
        batch_size = task.get("batch_size", 32)
        
        # Estimate FLOPs
        flops_per_param_sample = self.flops_per_param.get(model_arch, 5.0)
        
        if task_type in [TaskType.TRAINING, TaskType.FINE_TUNING]:
            # Training: forward + backward pass
            total_flops = model_params * flops_per_param_sample * dataset_size * num_epochs
        elif task_type == TaskType.INFERENCE:
            # Inference: forward pass only
            total_flops = model_params * 2.0 * dataset_size  # 2 FLOPs per param (roughly)
        else:
            # Other tasks
            total_flops = model_params * 3.0 * dataset_size
        
        # Estimate memory (GB)
        # Model weights + activations + gradients + optimizer state
        if task_type in [TaskType.TRAINING, TaskType.FINE_TUNING]:
            memory_gb = (model_params * 4 * 4) / (1024**3)  # 4 bytes per param, 4 copies
        else:
            memory_gb = (model_params * 4) / (1024**3)  # Just model weights
        
        # Add batch memory
        memory_gb += batch_size * 0.01  # Rough estimate
        
        # Estimate time (hours)
        # Assume 1 TFLOP/s throughput (conservative)
        throughput_tflops = 1.0
        time_seconds = (total_flops / 1e12) / throughput_tflops
        time_hours = time_seconds / 3600
        
        # Estimate energy (kWh)
        # Assume 300W average power draw (mix of CPU/GPU)
        avg_power_watts = task.get("hardware_power", 300)
        energy_kwh = (avg_power_watts * time_hours) / 1000
        
        return total_flops, memory_gb, time_hours, energy_kwh
    
    def _construct_dag(
        self,
        task_type: TaskType,
        model_arch: ModelArchitecture,
        energy_kwh: float,
        time_hours: float
    ) -> List[DAGStep]:
        """Construct carbon-aware execution DAG"""
        
        dag = []
        
        if task_type in [TaskType.TRAINING, TaskType.FINE_TUNING]:
            # Training DAG
            dag.extend([
                DAGStep(
                    step_id="data_load",
                    name="Data Loading & Preprocessing",
                    estimated_energy_kwh=energy_kwh * 0.05,
                    estimated_time_hours=time_hours * 0.05,
                    dependencies=[],
                    parallelizable=True
                ),
                DAGStep(
                    step_id="model_init",
                    name="Model Initialization",
                    estimated_energy_kwh=energy_kwh * 0.02,
                    estimated_time_hours=time_hours * 0.02,
                    dependencies=["data_load"],
                    parallelizable=False
                ),
                DAGStep(
                    step_id="training",
                    name="Training Loop",
                    estimated_energy_kwh=energy_kwh * 0.85,
                    estimated_time_hours=time_hours * 0.85,
                    dependencies=["model_init"],
                    parallelizable=True
                ),
                DAGStep(
                    step_id="validation",
                    name="Validation",
                    estimated_energy_kwh=energy_kwh * 0.05,
                    estimated_time_hours=time_hours * 0.05,
                    dependencies=["training"],
                    parallelizable=False
                ),
                DAGStep(
                    step_id="checkpoint",
                    name="Model Checkpointing",
                    estimated_energy_kwh=energy_kwh * 0.03,
                    estimated_time_hours=time_hours * 0.03,
                    dependencies=["validation"],
                    parallelizable=False
                )
            ])
        
        elif task_type == TaskType.INFERENCE:
            # Inference DAG (simpler)
            dag.extend([
                DAGStep(
                    step_id="model_load",
                    name="Model Loading",
                    estimated_energy_kwh=energy_kwh * 0.1,
                    estimated_time_hours=time_hours * 0.1,
                    dependencies=[],
                    parallelizable=False
                ),
                DAGStep(
                    step_id="inference",
                    name="Inference Execution",
                    estimated_energy_kwh=energy_kwh * 0.9,
                    estimated_time_hours=time_hours * 0.9,
                    dependencies=["model_load"],
                    parallelizable=True
                )
            ])
        
        else:
            # Generic DAG
            dag.append(
                DAGStep(
                    step_id="execution",
                    name="Task Execution",
                    estimated_energy_kwh=energy_kwh,
                    estimated_time_hours=time_hours,
                    dependencies=[],
                    parallelizable=True
                )
            )
        
        return dag
    
    def _identify_optimizations(
        self,
        task_type: TaskType,
        model_params: int,
        dataset_size: int,
        dataset_quality: float,
        task: Dict[str, Any]
    ) -> Tuple[List[str], float]:
        """Identify potential optimizations"""
        
        candidates = []
        carbon_potential = 0.0
        
        # Parameter-efficient fine-tuning
        if task_type == TaskType.FINE_TUNING:
            if model_params > 1_000_000_000:  # >1B params
                candidates.append("LoRA (r=8)")
                carbon_potential += 85  # 85% reduction
            elif model_params > 100_000_000:  # >100M params
                candidates.append("LoRA (r=16)")
                carbon_potential += 70
            else:
                candidates.append("Adapters")
                carbon_potential += 50
        
        # Quantization
        if model_params > 1_000_000_000:
            candidates.append("INT8 Quantization")
            carbon_potential += 20
        elif model_params > 100_000_000:
            candidates.append("FP16 Mixed Precision")
            carbon_potential += 15
        
        # Synthetic data augmentation
        if dataset_size < 50_000 and dataset_quality < 0.9:
            candidates.append("Synthetic Data Augmentation (20-40%)")
            carbon_potential += 10  # Indirectly saves by improving quality
        
        # Data compression
        if dataset_size > 100_000:
            candidates.append("Active Learning (50% data reduction)")
            carbon_potential += 30
            candidates.append("Data Deduplication")
            carbon_potential += 15
        
        # Distillation
        if task_type == TaskType.TRAINING and model_params > 1_000_000_000:
            candidates.append("Knowledge Distillation (Teacher→Student)")
            carbon_potential += 60
        
        # Model pruning
        if model_params > 100_000_000:
            candidates.append("Structured Pruning (30% sparsity)")
            carbon_potential += 25
        
        # Cap potential at 95%
        carbon_potential = min(95.0, carbon_potential)
        
        return candidates, carbon_potential


if __name__ == "__main__":
    # Example usage
    interpreter = WorkloadInterpreter()
    
    # Example 1: Fine-tuning BERT
    task1 = {
        "task_id": "bert_sentiment",
        "model_name": "bert-base-uncased",
        "task_type": "fine_tuning",
        "dataset_size": 10_000,
        "num_epochs": 3,
        "batch_size": 32
    }
    
    profile1 = interpreter.interpret(task1)
    
    print(f"\n{'='*60}")
    print(f"WORKLOAD PROFILE: {profile1.task_id}")
    print(f"{'='*60}")
    print(f"Task Type: {profile1.task_type.value}")
    print(f"Model: {profile1.model_name} ({profile1.model_params:,} params)")
    print(f"Architecture: {profile1.model_architecture.value}")
    print(f"Dataset: {profile1.dataset_size:,} samples (quality: {profile1.dataset_quality_score:.2f})")
    print(f"Estimated FLOPs: {profile1.estimated_flops:.2e}")
    print(f"Estimated Memory: {profile1.estimated_memory_gb:.1f} GB")
    print(f"Estimated Time: {profile1.estimated_time_hours:.2f} hours")
    print(f"Estimated Energy: {profile1.estimated_energy_kwh:.2f} kWh")
    print(f"\nExecution DAG ({len(profile1.execution_dag)} steps):")
    for step in profile1.execution_dag:
        print(f"  • {step.name}: {step.estimated_energy_kwh:.3f} kWh")
    print(f"\nOptimization Candidates:")
    for candidate in profile1.optimization_candidates:
        print(f"  • {candidate}")
    print(f"Carbon Optimization Potential: {profile1.carbon_optimization_potential:.0f}%")
    
    # Example 2: LLaMA inference
    task2 = {
        "task_id": "llama_inference",
        "model_name": "llama-7b",
        "task_type": "inference",
        "dataset_size": 1_000,
        "batch_size": 1
    }
    
    profile2 = interpreter.interpret(task2)
    
    print(f"\n{'='*60}")
    print(f"WORKLOAD PROFILE: {profile2.task_id}")
    print(f"{'='*60}")
    print(f"Task Type: {profile2.task_type.value}")
    print(f"Model: {profile2.model_name} ({profile2.model_params:,} params)")
    print(f"Estimated Energy: {profile2.estimated_energy_kwh:.2f} kWh")
    print(f"Optimization Potential: {profile2.carbon_optimization_potential:.0f}%")
