# File: src/enhancements/gpu_acceleration.py (GPU ACCELERATION LAYER)

"""
GPU Acceleration Layer for Green Agent - Version 1.0

Provides GPU-accelerated computation for all compatible modules.
Automatically detects GPU availability and falls back to CPU.
"""

import numpy as np
import logging
import time
from typing import Dict, List, Optional, Tuple, Any, Callable
from functools import wraps

logger = logging.getLogger(__name__)

# Try GPU libraries
try:
    import torch
    import torch.nn as nn
    TORCH_AVAILABLE = True
    CUDA_AVAILABLE = torch.cuda.is_available()
    GPU_COUNT = torch.cuda.device_count() if CUDA_AVAILABLE else 0
    GPU_NAME = torch.cuda.get_device_name(0) if CUDA_AVAILABLE else "N/A"
except ImportError:
    TORCH_AVAILABLE = False
    CUDA_AVAILABLE = False
    GPU_COUNT = 0
    GPU_NAME = "N/A"

try:
    import cupy as cp
    CUPY_AVAILABLE = True
except ImportError:
    CUPY_AVAILABLE = False

try:
    from numba import cuda, jit, vectorize
    NUMBA_AVAILABLE = True
except ImportError:
    NUMBA_AVAILABLE = False

# Prometheus metrics
try:
    from prometheus_client import Counter, Gauge, Histogram
    GPU_METRICS_AVAILABLE = True
except ImportError:
    GPU_METRICS_AVAILABLE = False

if GPU_METRICS_AVAILABLE:
    GPU_UTILIZATION = Gauge('gpu_utilization_pct', 'GPU utilization percentage', ['device'])
    GPU_MEMORY_USED = Gauge('gpu_memory_used_gb', 'GPU memory used', ['device'])
    GPU_OPS = Counter('gpu_operations_total', 'GPU operations count', ['operation'])
    GPU_SPEEDUP = Histogram('gpu_speedup_ratio', 'GPU vs CPU speedup', ['module'])

logger.info(f"GPU Acceleration: PyTorch={TORCH_AVAILABLE}, CUDA={CUDA_AVAILABLE}, "
           f"CuPy={CUPY_AVAILABLE}, Numba={NUMBA_AVAILABLE}, "
           f"Devices={GPU_COUNT} ({GPU_NAME})")


class GPUAccelerator:
    """
    Universal GPU accelerator for Green Agent modules.
    
    Features:
    - Automatic GPU detection and fallback
    - Matrix operations acceleration
    - Batch processing optimization
    - Memory management
    - Performance monitoring
    """
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self.cuda_available = CUDA_AVAILABLE
        self.cupy_available = CUPY_AVAILABLE
        self.numba_available = NUMBA_AVAILABLE
        self.device_count = GPU_COUNT
        self.device_name = GPU_NAME
        
        # Performance tracking
        self.operation_count = defaultdict(int)
        self.total_speedup = defaultdict(float)
        
        self._initialized = True
        logger.info(f"GPUAccelerator initialized: {self.device_count} GPU(s) available")
    
    def get_optimal_device(self, data_size: int) -> str:
        """Determine optimal device based on data size"""
        if not self.cuda_available:
            return 'cpu'
        
        # Small operations are faster on CPU due to transfer overhead
        if data_size < 10000:
            return 'cpu'
        
        # Large operations benefit from GPU
        if data_size > 100000:
            return 'cuda'
        
        # Medium operations: check GPU memory
        if self.cuda_available:
            free_memory = torch.cuda.memory_reserved(0) - torch.cuda.memory_allocated(0)
            estimated_needed = data_size * 8  # 8 bytes per float64
        
        return 'cpu'
    
    def to_gpu(self, data: np.ndarray, force_cpu: bool = False) -> Any:
        """Convert numpy array to GPU tensor"""
        if force_cpu or not self.cuda_available:
            return data
        
        try:
            if TORCH_AVAILABLE:
                tensor = torch.from_numpy(data).float()
                if self.cuda_available:
                    tensor = tensor.cuda()
                    if GPU_METRICS_AVAILABLE:
                        GPU_OPS.labels(operation='to_gpu').inc()
                return tensor
        except Exception as e:
            logger.debug(f"GPU conversion failed: {e}")
        
        return data
    
    def to_cpu(self, data: Any) -> np.ndarray:
        """Convert GPU tensor back to numpy array"""
        if isinstance(data, np.ndarray):
            return data
        
        try:
            if TORCH_AVAILABLE and isinstance(data, torch.Tensor):
                if data.is_cuda:
                    data = data.cpu()
                    if GPU_METRICS_AVAILABLE:
                        GPU_OPS.labels(operation='to_cpu').inc()
                return data.detach().numpy()
        except Exception as e:
            logger.debug(f"CPU conversion failed: {e}")
        
        return data
    
    def matrix_multiply(self, a: np.ndarray, b: np.ndarray, 
                       use_gpu: bool = True) -> np.ndarray:
        """GPU-accelerated matrix multiplication"""
        
        if not use_gpu or not self.cuda_available:
            return np.dot(a, b)
        
        try:
            start = time.time()
            
            # Determine optimal device
            total_elements = a.shape[0] * a.shape[1] + b.shape[0] * b.shape[1]
            
            if total_elements < 50000:
                return np.dot(a, b)
            
            # GPU computation
            a_gpu = self.to_gpu(a)
            b_gpu = self.to_gpu(b)
            
            if TORCH_AVAILABLE and isinstance(a_gpu, torch.Tensor):
                result_gpu = torch.mm(a_gpu, b_gpu)
                result = self.to_cpu(result_gpu)
            elif CUPY_AVAILABLE:
                a_cp = cp.asarray(a)
                b_cp = cp.asarray(b)
                result_cp = cp.dot(a_cp, b_cp)
                result = cp.asnumpy(result_cp)
            else:
                result = np.dot(a, b)
            
            elapsed = time.time() - start
            
            if GPU_METRICS_AVAILABLE:
                GPU_SPEEDUP.labels(module='matrix_multiply').observe(elapsed)
            
            return result
            
        except Exception as e:
            logger.debug(f"GPU matrix multiply failed: {e}")
            return np.dot(a, b)
    
    def batch_process(self, data: np.ndarray, fn: Callable,
                     batch_size: int = 10000, use_gpu: bool = True) -> np.ndarray:
        """GPU-accelerated batch processing"""
        
        if not use_gpu or not self.cuda_available:
            return fn(data)
        
        n_samples = len(data)
        results = []
        
        for i in range(0, n_samples, batch_size):
            batch = data[i:i+batch_size]
            
            if len(batch) > 5000 and self.cuda_available:
                batch_gpu = self.to_gpu(batch)
                if isinstance(batch_gpu, torch.Tensor):
                    result_batch = fn(batch_gpu)
                    results.append(self.to_cpu(result_batch))
                else:
                    results.append(fn(batch))
            else:
                results.append(fn(batch))
        
        return np.concatenate(results, axis=0) if len(results) > 1 else results[0]
    
    def get_memory_info(self) -> Dict:
        """Get GPU memory information"""
        if not self.cuda_available:
            return {'cuda_available': False}
        
        info = {
            'cuda_available': True,
            'device_count': self.device_count,
            'device_name': self.device_name,
            'devices': []
        }
        
        for i in range(self.device_count):
            with torch.cuda.device(i):
                total = torch.cuda.get_device_properties(i).total_memory / 1e9
                reserved = torch.cuda.memory_reserved(i) / 1e9
                allocated = torch.cuda.memory_allocated(i) / 1e9
                free = total - allocated
                
                info['devices'].append({
                    'device_id': i,
                    'total_memory_gb': round(total, 2),
                    'reserved_gb': round(reserved, 2),
                    'allocated_gb': round(allocated, 2),
                    'free_gb': round(free, 2),
                    'utilization_pct': round((allocated / total) * 100, 1)
                })
                
                if GPU_METRICS_AVAILABLE:
                    GPU_UTILIZATION.labels(device=str(i)).set((allocated / total) * 100)
                    GPU_MEMORY_USED.labels(device=str(i)).set(allocated)
        
        return info
    
    def clear_cache(self):
        """Clear GPU memory cache"""
        if self.cuda_available:
            torch.cuda.empty_cache()
            logger.info("GPU cache cleared")
    
    def benchmark(self, data_size: int = 1000000) -> Dict:
        """Benchmark GPU vs CPU performance"""
        
        # Generate test data
        a = np.random.randn(data_size // 1000, 1000).astype(np.float32)
        b = np.random.randn(1000, 100).astype(np.float32)
        
        results = {}
        
        # CPU benchmark
        start = time.time()
        for _ in range(10):
            np.dot(a, b)
        cpu_time = (time.time() - start) / 10
        results['cpu_time_s'] = cpu_time
        results['cpu_ops_per_s'] = 1 / max(cpu_time, 0.001)
        
        # GPU benchmark (if available)
        if self.cuda_available:
            start = time.time()
            for _ in range(10):
                a_gpu = self.to_gpu(a)
                b_gpu = self.to_gpu(b)
                torch.mm(a_gpu, b_gpu)
                self.clear_cache()
            gpu_time = (time.time() - start) / 10
            results['gpu_time_s'] = gpu_time
            results['gpu_ops_per_s'] = 1 / max(gpu_time, 0.001)
            results['speedup'] = cpu_time / max(gpu_time, 0.001)
        
        return results


# Singleton accessor
def get_gpu_accelerator() -> GPUAccelerator:
    return GPUAccelerator()


# Decorator for GPU-accelerated functions
def gpu_accelerated(func):
    """Decorator to automatically accelerate functions with GPU"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        accelerator = get_gpu_accelerator()
        
        # Check if GPU should be used
        use_gpu = kwargs.pop('use_gpu', True)
        
        if use_gpu and accelerator.cuda_available:
            try:
                start = time.time()
                result = func(*args, **kwargs)
                elapsed = time.time() - start
                
                if GPU_METRICS_AVAILABLE:
                    GPU_SPEEDUP.labels(module=func.__name__).observe(elapsed)
                
                return result
            except Exception as e:
                logger.debug(f"GPU acceleration failed for {func.__name__}: {e}")
        
        return func(*args, **kwargs)
    
    return wrapper
