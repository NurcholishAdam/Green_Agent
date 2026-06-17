# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/experts/data_expert.py
# Complete enhanced file with causal analysis and natural language explanations

"""
Enhanced Data Expert v5.0.0 - Complete Metabolic Data Processor
With Causal Analysis, Natural Language Explanations, and Quality Reporting
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Union, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import numpy as np
from collections import deque
import hashlib
import json
import zlib
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

# ============================================================================
# Bio-Inspired Import Check
# ============================================================================
try:
    from enhancements.bio_inspired.eco_atp_currency import EcoATPTokenManager, EcoATPConsumer
    from enhancements.bio_inspired.proton_gradient_fields import GradientFieldManager
    from enhancements.bio_inspired.atp_synthase_scheduler import ATPSynthaseScheduler
    from enhancements.bio_inspired.chromatophore_compartments import CompartmentManager, MembranePermeability
    from enhancements.bio_inspired.biomass_storage import BiomassStorage, StorageTier, GuaranteeLevel
    from enhancements.bio_inspired.photosynthetic_harvester import PhotosyntheticHarvester
    BIO_INSPIRED_AVAILABLE = True
except ImportError:
    BIO_INSPIRED_AVAILABLE = False

try:
    from ..expert_registry import ExpertProfile, ExpertDomain, HardwareProfile
except ImportError:
    class ExpertDomain(Enum): DATA = "data_engineering"
    class HardwareProfile(Enum): HYBRID = "hybrid_cpu_gpu"

# ============================================================================
# Enums and Data Classes
# ============================================================================
class DataTier(Enum): HOT = "hot"; WARM = "warm"; COLD = "cold"; FROZEN = "frozen"
class DataQuality(Enum): EXCELLENT = "excellent"; GOOD = "good"; FAIR = "fair"; POOR = "poor"; UNUSABLE = "unusable"
class StreamingMode(Enum): REALTIME = "realtime"; NEAR_REALTIME = "near_realtime"; MICRO_BATCH = "micro_batch"; BATCH = "batch"
class PipelineStatus(Enum): HEALTHY = "healthy"; DEGRADED = "degraded"; RECOVERING = "recovering"; FAILED = "failed"; PAUSED = "paused"

@dataclass
class DataQualityMetrics:
    completeness: float = 0.0; accuracy: float = 0.0; consistency: float = 0.0
    timeliness: float = 0.0; uniqueness: float = 0.0; validity: float = 0.0
    overall_score: float = 0.0; harvester_confidence: float = 0.5
    
    def __post_init__(self):
        weights = {'completeness': 0.25, 'accuracy': 0.25, 'consistency': 0.15,
                   'timeliness': 0.15, 'uniqueness': 0.10, 'validity': 0.10}
        self.overall_score = (self.completeness * weights['completeness'] + self.accuracy * weights['accuracy'] +
                             self.consistency * weights['consistency'] + self.timeliness * weights['timeliness'] +
                             self.uniqueness * weights['uniqueness'] + self.validity * weights['validity'])

@dataclass
class DataLineage:
    lineage_id: str; source: str
    transformations: List[Dict[str, Any]] = field(default_factory=list)
    quality_at_source: Optional[DataQualityMetrics] = None
    carbon_footprint_kg: float = 0.0; helium_consumed: float = 0.0
    created_at: datetime = field(default_factory=datetime.utcnow); checksum: str = ""
    biomass_storage_token: Optional[str] = None; ecoatp_cost: float = 0.0
    
    def add_transformation(self, transform_name: str, params: Dict[str, Any]):
        self.transformations.append({'name': transform_name, 'params': params,
                                     'timestamp': datetime.utcnow().isoformat(), 'checksum_before': self.checksum})

# ============================================================================
# Enhanced Data Expert
# ============================================================================
class DataExpert:
    """Enhanced Data Expert v5.0.0 with Causal Analysis and Explanations"""
    
    def __init__(self, expert_id: str = "data_engineer_v5", max_workers: int = 4,
                 enable_streaming: bool = True, enable_quality: bool = True,
                 enable_lineage: bool = True, enable_bio_integration: bool = True):
        self.expert_id = expert_id; self.version = "5.0.0"
        self.max_workers = max_workers; self.enable_streaming = enable_streaming
        self.enable_quality = enable_quality; self.enable_lineage = enable_lineage
        self.enable_bio_integration = enable_bio_integration and BIO_INSPIRED_AVAILABLE
        
        self.token_manager = None; self.gradient_manager = None; self.scheduler = None
        self.compartment_manager = None; self.biomass_storage = None; self.harvester = None
        
        self.profile = ExpertProfile(expert_id=expert_id, domain=ExpertDomain.DATA, hardware_profile=HardwareProfile.HYBRID,
                                     helium_per_inference=0.015, carbon_per_inference=0.00015, energy_per_inference=0.0015,
                                     avg_latency_ms=20.0, accuracy_score=0.99, reliability_score=0.99, efficiency_score=0.97,
                                     supported_task_types=['data_processing', 'streaming', 'etl', 'data_quality', 'training'])
        
        self.compression_algorithms = {
            'none': {'ratio': 1.0, 'energy_overhead': 0.0, 'latency_impact_ms': 0, 'ecoatp_cost': 0},
            'snappy': {'ratio': 0.45, 'energy_overhead': 0.0003, 'latency_impact_ms': 1, 'ecoatp_cost': 1},
            'lz4': {'ratio': 0.40, 'energy_overhead': 0.0004, 'latency_impact_ms': 2, 'ecoatp_cost': 2},
            'gzip': {'ratio': 0.30, 'energy_overhead': 0.0008, 'latency_impact_ms': 5, 'ecoatp_cost': 3},
            'zstd': {'ratio': 0.22, 'energy_overhead': 0.0015, 'latency_impact_ms': 8, 'ecoatp_cost': 5},
            'brotli': {'ratio': 0.18, 'energy_overhead': 0.0025, 'latency_impact_ms': 15, 'ecoatp_cost': 8},
            'lzma': {'ratio': 0.15, 'energy_overhead': 0.003, 'latency_impact_ms': 25, 'ecoatp_cost': 10}
        }
        
        self.storage_tiers = {
            DataTier.HOT: {'max_latency_ms': 5, 'compression': 'snappy', 'biomass_tier': StorageTier.ATP_CACHE if BIO_INSPIRED_AVAILABLE else None},
            DataTier.WARM: {'max_latency_ms': 50, 'compression': 'lz4', 'biomass_tier': StorageTier.GLYCOGEN_QUEUE if BIO_INSPIRED_AVAILABLE else None},
            DataTier.COLD: {'max_latency_ms': 500, 'compression': 'zstd', 'biomass_tier': StorageTier.STARCH_RESERVE if BIO_INSPIRED_AVAILABLE else None},
            DataTier.FROZEN: {'max_latency_ms': 5000, 'compression': 'lzma', 'biomass_tier': StorageTier.LIPID_DEPOT if BIO_INSPIRED_AVAILABLE else None}
        }
        
        self.active_streams: Dict[str, Any] = {}
        self.lineage_records: Dict[str, DataLineage] = {}
        self.optimization_history: deque = deque(maxlen=10000)
        self.pipeline_status: Dict[str, PipelineStatus] = {}
        self.quality_cache: Dict[str, DataQualityMetrics] = {}
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.total_processed_gb = 0.0; self.total_saved_carbon_kg = 0.0
        self.total_saved_helium = 0.0; self.total_ecoatp_saved = 0.0
        self.biomass_lineage_tokens: Dict[str, str] = {}
        
        logger.info(f"Data Expert v{self.version} initialized")
    
    def inject_bio_core(self, bio_core=None, **kwargs):
        if bio_core:
            self.token_manager = getattr(bio_core, 'token_manager', None)
            self.gradient_manager = getattr(bio_core, 'gradient_manager', None)
            self.scheduler = getattr(bio_core, 'scheduler', None)
            self.compartment_manager = getattr(bio_core, 'compartment_manager', None)
            self.biomass_storage = getattr(bio_core, 'biomass_storage', None)
            self.harvester = getattr(bio_core, 'harvester', None)
        else:
            self.token_manager = kwargs.get('token_manager'); self.gradient_manager = kwargs.get('gradient_manager')
            self.scheduler = kwargs.get('scheduler'); self.compartment_manager = kwargs.get('compartment_manager')
            self.biomass_storage = kwargs.get('biomass_storage'); self.harvester = kwargs.get('harvester')
        if any([self.token_manager, self.gradient_manager, self.compartment_manager]):
            self.enable_bio_integration = True
    
    # ========================================================================
    # Bio-Inspired Data Access
    # ========================================================================
    def _get_token_efficient_compression(self, latency_budget_ms: float) -> str:
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            balance = summary.get('total_balance', 500)
            if balance < 100: return 'zstd' if latency_budget_ms > 10 else 'lz4'
            elif balance < 300: return 'lz4' if latency_budget_ms < 10 else 'gzip'
            else: return 'snappy' if latency_budget_ms < 5 else 'lz4'
        return 'lz4'
    
    def _get_gradient_backpressure(self) -> float:
        if self.gradient_manager:
            carbon = self.gradient_manager.fields.get('carbon')
            if carbon: return carbon.gradient_strength
        return 0.5
    
    def _get_harvester_quality_confidence(self) -> float:
        if self.harvester:
            stats = self.harvester.get_harvesting_stats()
            recent = stats.get('recent_conversions', [])
            if recent: return np.mean([c.get('convertible_energy', 0.5) for c in recent[-10:]])
        return 0.5
    
    def _get_atp_parallelism_level(self) -> int:
        if self.scheduler:
            df = self.scheduler.calculate_gradient_driving_force()
            rs = self.scheduler.calculate_rotation_speed(df)
            rate = self.scheduler.calculate_atp_production_rate(rs)
            return min(8, self.max_workers * 2) if rate > 100 else self.max_workers if rate > 50 else max(1, self.max_workers // 2)
        return self.max_workers
    
    # ========================================================================
    # Primary Optimization
    # ========================================================================
    async def optimize_data_pipeline(self, input_size_mb: float, helium_scarcity: float,
                                    latency_budget_ms: float, data_format: str = 'auto',
                                    streaming_mode: Optional[str] = None,
                                    quality_requirements: Optional[Dict[str, float]] = None,
                                    carbon_budget_kg: Optional[float] = None,
                                    enable_parallel: bool = True, tier_preference: Optional[str] = None,
                                    cross_expert_hints: Optional[Dict[str, Any]] = None,
                                    ecoatp_budget: Optional[float] = None) -> Dict[str, Any]:
        start_time = datetime.utcnow()
        optimization_id = hashlib.md5(f"{input_size_mb}{helium_scarcity}{latency_budget_ms}{start_time}".encode()).hexdigest()[:12]
        
        data_profile = await self._profile_data(input_size_mb, data_format, streaming_mode)
        
        quality_metrics = None
        if self.enable_quality:
            quality_metrics = await self._assess_data_quality(input_size_mb, quality_requirements)
            if self.enable_bio_integration:
                quality_metrics.harvester_confidence = self._get_harvester_quality_confidence()
        
        compression_algo = self._get_token_efficient_compression(latency_budget_ms) if self.enable_bio_integration else 'lz4'
        compression_plan = {'algorithm': compression_algo, 'ratio': self.compression_algorithms[compression_algo]['ratio'],
                           'energy_overhead': self.compression_algorithms[compression_algo]['energy_overhead'],
                           'latency_impact_ms': self.compression_algorithms[compression_algo]['latency_impact_ms'],
                           'ecoatp_cost': self.compression_algorithms[compression_algo].get('ecoatp_cost', 0)}
        
        parallel_workers = self._get_atp_parallelism_level() if enable_parallel and self.enable_bio_integration else (self.max_workers if enable_parallel else 1)
        
        stream_backpressure = 0.5 + self._get_gradient_backpressure() * 0.5 if self.enable_bio_integration and streaming_mode else 0.8
        
        ecoatp_cost = input_size_mb * 0.1 + compression_plan['ecoatp_cost'] if self.enable_bio_integration else 0
        if ecoatp_budget and ecoatp_cost > ecoatp_budget and self.enable_bio_integration:
            compression_algo = 'snappy'
            compression_plan = {'algorithm': compression_algo, 'ratio': self.compression_algorithms[compression_algo]['ratio'],
                               'ecoatp_cost': self.compression_algorithms[compression_algo].get('ecoatp_cost', 0)}
            ecoatp_cost = input_size_mb * 0.1 + compression_plan['ecoatp_cost']
        
        plan = {
            'expert_id': self.expert_id, 'optimization_id': optimization_id, 'version': self.version,
            'compression': compression_plan['algorithm'], 'compression_ratio': compression_plan['ratio'],
            'original_size_mb': input_size_mb, 'compressed_size_mb': input_size_mb * compression_plan['ratio'],
            'estimated_latency_ms': compression_plan['latency_impact_ms'] + (input_size_mb * 0.01),
            'estimated_energy_kwh': input_size_mb * compression_plan['energy_overhead'],
            'estimated_carbon_kg': input_size_mb * compression_plan['energy_overhead'] * 0.4,
            'estimated_ecoatp_cost': ecoatp_cost,
            'strategy': 'bio_optimized' if self.enable_bio_integration else 'standard',
            'parallel_workers': parallel_workers, 'stream_backpressure': stream_backpressure,
            'bio_integration_active': self.enable_bio_integration,
            'gradient_backpressure': self._get_gradient_backpressure() if self.enable_bio_integration else 0.5,
            'harvester_confidence': self._get_harvester_quality_confidence() if self.enable_bio_integration else 0.5,
            'quality_assessment': quality_metrics.__dict__ if quality_metrics else None,
            'recommendations': self._generate_recommendations(data_profile, quality_metrics, ecoatp_cost),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        if self.enable_lineage:
            lineage = DataLineage(lineage_id=f"lineage_{optimization_id}", source=data_profile.get('format', 'unknown'),
                                 quality_at_source=quality_metrics, carbon_footprint_kg=plan['estimated_carbon_kg'],
                                 ecoatp_cost=ecoatp_cost)
            lineage.add_transformation('compression', {'algorithm': compression_algo})
            if self.enable_bio_integration and self.biomass_storage:
                stored, token_id = self.biomass_storage.store_task(
                    task_data={'lineage_id': lineage.lineage_id, 'transformations': lineage.transformations[-5:]},
                    ecoatp_cost=1.0, guarantee=GuaranteeLevel.SILVER, initial_tier=StorageTier.LIPID_DEPOT)
                if stored:
                    lineage.biomass_storage_token = token_id
                    self.biomass_lineage_tokens[lineage.lineage_id] = token_id
            self.lineage_records[lineage.lineage_id] = lineage
            plan['lineage'] = lineage.__dict__
        
        self.optimization_history.append({'timestamp': start_time, 'compression': compression_algo, 'ecoatp_cost': ecoatp_cost, 'plan': plan})
        self.total_processed_gb += input_size_mb / 1000
        self.total_ecoatp_saved += max(0, 10.0 - ecoatp_cost)
        
        return plan
    
    async def _profile_data(self, input_size_mb: float, data_format: str, streaming_mode: Optional[str]) -> Dict[str, Any]:
        profile = {'size_mb': input_size_mb, 'is_streaming': streaming_mode is not None or input_size_mb > 1000,
                   'is_compressible': True, 'estimated_entropy': 0.0, 'recommended_processing': 'batch'}
        if data_format == 'auto':
            data_format = 'json' if input_size_mb < 100 else 'parquet' if input_size_mb > 1000 else 'csv'
        profile['format'] = data_format
        if streaming_mode == 'realtime' or (input_size_mb > 0 and input_size_mb < 10): profile['recommended_processing'] = 'realtime'
        elif input_size_mb < 100: profile['recommended_processing'] = 'near_realtime'
        elif input_size_mb < 1000: profile['recommended_processing'] = 'batch'
        else: profile['recommended_processing'] = 'bulk'
        return profile
    
    async def _assess_data_quality(self, input_size_mb: float, requirements: Optional[Dict[str, float]] = None) -> DataQualityMetrics:
        base_quality = 0.90; size_penalty = min(input_size_mb / 10000, 0.1)
        metrics = DataQualityMetrics(completeness=base_quality - size_penalty * 0.3, accuracy=base_quality - size_penalty * 0.2,
                                     consistency=base_quality - size_penalty * 0.1, timeliness=base_quality - size_penalty * 0.15,
                                     uniqueness=base_quality - size_penalty * 0.25, validity=base_quality - size_penalty * 0.1)
        if self.enable_bio_integration: metrics.harvester_confidence = self._get_harvester_quality_confidence()
        return metrics
    
    def _generate_recommendations(self, data_profile: Dict, quality_metrics: Optional[DataQualityMetrics], ecoatp_cost: float) -> List[str]:
        recs = []
        if quality_metrics and quality_metrics.overall_score < 0.7: recs.append("Data quality below threshold. Consider cleansing.")
        if ecoatp_cost > 5.0: recs.append(f"High Eco-ATP cost ({ecoatp_cost:.1f}). Consider deferring.")
        if data_profile.get('is_streaming'): recs.append("Streaming data detected. Backpressure handling active.")
        return recs if recs else ["Data configuration is optimal."]
    
    # ========================================================================
    # Causal Analysis
    # ========================================================================
    def analyze_causal_factors(self, optimization_result: Dict[str, Any]) -> Dict[str, Any]:
        plan = optimization_result
        compression = plan.get('compression', 'unknown')
        ecoatp_cost = plan.get('estimated_ecoatp_cost', 0)
        gradient_bp = plan.get('gradient_backpressure', 0.5)
        harvester_conf = plan.get('harvester_confidence', 0.5)
        quality = plan.get('quality_assessment', {})
        quality_score = quality.get('overall_score', 0) if quality else 0
        
        causal_chain = []
        
        if ecoatp_cost > 5:
            causal_chain.append({'factor': 'Token Scarcity', 'impact': 'HIGH',
                                'effect': f'Forced {compression} compression to reduce Eco-ATP cost to {ecoatp_cost:.1f}', 'strength': 0.8})
        elif ecoatp_cost > 2:
            causal_chain.append({'factor': 'Token Budget', 'impact': 'MODERATE',
                                'effect': f'Selected {compression} balancing cost ({ecoatp_cost:.1f}) and performance', 'strength': 0.5})
        
        if gradient_bp > 0.7:
            causal_chain.append({'factor': 'High Carbon Gradient', 'impact': 'HIGH',
                                'effect': f'Backpressure {gradient_bp:.2f} forced conservative processing', 'strength': 0.7})
        
        if harvester_conf < 0.4:
            causal_chain.append({'factor': 'Low Harvester Confidence', 'impact': 'MODERATE',
                                'effect': 'Increased quality validation due to uncertain signals', 'strength': 0.5})
        
        if quality_score < 0.7:
            causal_chain.append({'factor': 'Poor Data Quality', 'impact': 'HIGH',
                                'effect': f'Low quality ({quality_score:.2f}) triggered additional validation', 'strength': 0.6})
        
        causal_chain.sort(key=lambda x: (x['impact'] == 'HIGH', x['strength']), reverse=True)
        
        recommendations = []
        for f in causal_chain:
            if f['impact'] == 'HIGH':
                if 'Token' in f['factor']: recommendations.append("Increase token generation or reduce consumption.")
                if 'Carbon' in f['factor']: recommendations.append("Defer processing to lower-carbon windows.")
                if 'Quality' in f['factor']: recommendations.append("Implement data cleansing pipeline.")
        
        return {'decision_type': 'data_pipeline', 'causal_chain': causal_chain,
                'primary_driver': causal_chain[0] if causal_chain else None,
                'recommendations': recommendations, 'timestamp': datetime.utcnow().isoformat()}
    
    # ========================================================================
    # Natural Language Explanations
    # ========================================================================
    def explain_compression_choice(self, compression: str, context: Dict[str, Any]) -> Dict[str, Any]:
        algo = self.compression_algorithms.get(compression, {})
        factors = []
        token_balance = context.get('token_balance', 500)
        if token_balance < 100:
            factors.append({'name': 'Token Scarcity', 'weight': 0.5,
                          'description': f'Low token balance ({token_balance:.0f}) required aggressive compression'})
        elif token_balance > 500:
            factors.append({'name': 'Token Abundance', 'weight': 0.1,
                          'description': f'High token balance ({token_balance:.0f}) allowed quality-focused choice'})
        
        latency_budget = context.get('latency_budget_ms', 100)
        latency_impact = algo.get('latency_impact_ms', 0)
        if latency_impact > latency_budget * 0.5:
            factors.append({'name': 'Latency Constraint', 'weight': 0.3,
                          'description': f'Compression latency ({latency_impact}ms) within budget ({latency_budget}ms)'})
        
        ratio = algo.get('ratio', 0.5)
        factors.append({'name': 'Compression Efficiency', 'weight': 0.2, 'description': f'Achieves {ratio:.0%} ratio'})
        
        primary = max(factors, key=lambda f: f['weight']) if factors else {'name': 'Default'}
        executive = f"Selected {compression} primarily due to {primary['name'].lower()}. Achieves {ratio:.0%} reduction with {algo.get('latency_impact_ms', 0)}ms latency."
        
        alternatives = []
        for name, a in self.compression_algorithms.items():
            if name != compression:
                alternatives.append({'algorithm': name, 'ratio': a['ratio'], 'latency_ms': a['latency_impact_ms'],
                                    'ecoatp_cost': a.get('ecoatp_cost', 0),
                                    'tradeoff': f"{'Better' if a['ratio'] < algo['ratio'] else 'Worse'} compression, "
                                               f"{'faster' if a['latency_impact_ms'] < algo['latency_impact_ms'] else 'slower'}"})
        
        return {'compression': compression, 'executive_summary': executive, 'decision_factors': factors,
                'algorithm_details': {'ratio': algo.get('ratio', 0), 'latency_ms': algo.get('latency_impact_ms', 0),
                                     'ecoatp_cost': algo.get('ecoatp_cost', 0)},
                'alternatives': sorted(alternatives, key=lambda a: a['ecoatp_cost'])[:3]}
    
    def get_data_quality_explanation(self, metrics: DataQualityMetrics) -> Dict[str, Any]:
        score = metrics.overall_score
        if score > 0.9: level, assessment = "EXCELLENT", "Data quality is excellent."
        elif score > 0.7: level, assessment = "GOOD", "Data quality is good. Minor issues present."
        elif score > 0.5: level, assessment = "FAIR", "Data quality is fair. Some issues may affect accuracy."
        elif score > 0.3: level, assessment = "POOR", "Data quality is poor. Significant issues detected."
        else: level, assessment = "UNUSABLE", "Data quality is unusable. Cleansing required."
        
        dimensions = [
            {'name': 'Completeness', 'score': metrics.completeness, 'weight': 0.25,
             'issue': 'Missing values' if metrics.completeness < 0.8 else None},
            {'name': 'Accuracy', 'score': metrics.accuracy, 'weight': 0.25,
             'issue': 'Inaccurate values' if metrics.accuracy < 0.8 else None},
            {'name': 'Consistency', 'score': metrics.consistency, 'weight': 0.15,
             'issue': 'Inconsistent patterns' if metrics.consistency < 0.8 else None},
            {'name': 'Timeliness', 'score': metrics.timeliness, 'weight': 0.15,
             'issue': 'Stale data' if metrics.timeliness < 0.8 else None},
            {'name': 'Uniqueness', 'score': metrics.uniqueness, 'weight': 0.10,
             'issue': 'Duplicates found' if metrics.uniqueness < 0.9 else None},
            {'name': 'Validity', 'score': metrics.validity, 'weight': 0.10,
             'issue': 'Format violations' if metrics.validity < 0.9 else None}
        ]
        
        worst = sorted(dimensions, key=lambda d: d['score'])[:2]
        recommendations = [f"Address {d['name'].lower()}: {d['issue']}." for d in worst if d['issue']]
        
        return {'quality_level': level, 'overall_score': score, 'assessment': assessment,
                'dimension_breakdown': dimensions, 'worst_dimensions': [d['name'] for d in worst],
                'recommendations': recommendations, 'harvester_confidence': metrics.harvester_confidence}
    
    def get_expert_statistics(self) -> Dict[str, Any]:
        recent = list(self.optimization_history)[-100:]
        stats = {
            'expert_id': self.expert_id, 'version': self.version,
            'total_processed_gb': self.total_processed_gb, 'total_ecoatp_saved': self.total_ecoatp_saved,
            'bio_integration_active': self.enable_bio_integration,
            'lineage_records': len(self.lineage_records), 'biomass_lineage_tokens': len(self.biomass_lineage_tokens),
            'recent_optimizations': [{'timestamp': str(h['timestamp']), 'compression': h['compression'],
                                      'ecoatp_cost': h.get('ecoatp_cost', 0)} for h in recent[-10:]]
        }
        if self.enable_bio_integration:
            stats['bio_metrics'] = {'gradient_backpressure': self._get_gradient_backpressure(),
                                    'harvester_confidence': self._get_harvester_quality_confidence(),
                                    'atp_parallelism': self._get_atp_parallelism_level()}
        return stats
    
    def reset_metrics(self):
        self.optimization_history.clear(); self.quality_cache.clear(); self.lineage_records.clear()
        self.active_streams.clear(); self.pipeline_status.clear(); self.biomass_lineage_tokens.clear()
        self.total_processed_gb = 0.0; self.total_ecoatp_saved = 0.0
