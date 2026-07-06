# File: src/enhancements/test_helium_integration_enhanced_v13_0.py
"""
Integration Test for Helium Dataset with All Enhancement Modules - Version 13.0 (Enterprise Platinum+)
ENHANCED WITH: Intelligent Test Selection, ML-Based Root Cause Analysis, Self-Healing Tests,
Predictive Maintenance, Enhanced Analytics Dashboard

CRITICAL ADDITIONS OVER v12.0:
1. ADDED: Intelligent Test Selection & Prioritization - Impact-based test execution
2. ADDED: ML-Based Root Cause Analysis - Automatic failure diagnosis
3. ADDED: Self-Healing Test Capabilities - Automatic test adaptation
4. ADDED: Predictive Test Maintenance - Proactive test health management
5. ADDED: Enhanced Analytics Visualization - Rich test analytics dashboard
6. ADDED: Test Impact Analysis - Code change impact assessment
7. ADDED: Failure Pattern Recognition - ML-based failure classification
"""

import asyncio
import hashlib
import json
import logging
import math
import os
import pickle
import sys
import time
import uuid
import random
import threading
import gc
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Callable, Set, Union
from collections import defaultdict, deque
from enum import Enum
from contextlib import contextmanager, asynccontextmanager
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import numpy as np

# Pydantic v2 for validation
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict, ValidationError

# Tenacity for retries
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log

# Database with connection pooling
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, Boolean, Text, JSON, Index, func, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError, OperationalError

# WebSocket for dashboard
import websockets
from websockets.server import serve
from websockets.exceptions import ConnectionClosed

# Scipy for statistical analysis
from scipy import stats
from scipy.stats import ttest_ind, mannwhitneyu

# Async HTTP for carbon intensity
import aiohttp
from aiohttp import ClientTimeout, ClientSession, ClientError

# ============================================================
# NEW v13.0: Advanced ML Dependencies
# ============================================================

# Scikit-learn for ML
try:
    from sklearn.preprocessing import StandardScaler, LabelEncoder
    from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
    from sklearn.model_selection import train_test_split, cross_val_score
    from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.pipeline import Pipeline
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    logging.warning("scikit-learn not available. ML features disabled.")

# GitPython for impact analysis
try:
    from git import Repo, Git, Diff
    GIT_AVAILABLE = True
except ImportError:
    GIT_AVAILABLE = False
    logging.warning("GitPython not available. Git impact analysis disabled.")

# Prometheus metrics
from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry

# Configure logging
class CorrelationIdFilter(logging.Filter):
    """Add correlation ID to all log messages"""
    def __init__(self):
        super().__init__()
        self._local = threading.local()
    
    @property
    def correlation_id(self):
        if not hasattr(self._local, 'correlation_id'):
            self._local.correlation_id = str(uuid.uuid4())[:8]
        return self._local.correlation_id
    
    def filter(self, record):
        record.correlation_id = self.correlation_id
        return True

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler('test_integration_v13.log', maxBytes=10*1024*1024, backupCount=5),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.addFilter(CorrelationIdFilter())

# Audit logger
audit_logger = logging.getLogger('test_audit')
audit_handler = logging.handlers.RotatingFileHandler('test_audit_v13.log', maxBytes=50*1024*1024, backupCount=10)
audit_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
audit_logger.addHandler(audit_handler)
audit_logger.setLevel(logging.INFO)

# Prometheus metrics
REGISTRY = CollectorRegistry()

# Core metrics (keeping existing metrics)
TEST_RUNS = Counter('test_runs_total', 'Total test runs', ['status', 'type'], registry=REGISTRY)
TEST_DURATION = Histogram('test_duration_seconds', 'Test duration', ['test_type'], registry=REGISTRY)
TEST_FAILURES = Counter('test_failures_total', 'Total test failures', ['test_name', 'failure_type'], registry=REGISTRY)
TEST_COVERAGE = Gauge('test_coverage_percent', 'Test coverage percentage', ['coverage_type'], registry=REGISTRY)
REGRESSION_DETECTED = Counter('test_regressions_total', 'Performance regressions detected', ['test_name'], registry=REGISTRY)
CIRCUIT_BREAKER_STATE = Gauge('test_circuit_breaker_state', 'Circuit breaker state (0=closed,1=half,2=open)', ['component'], registry=REGISTRY)
HEALTH_SCORE = Gauge('test_system_health', 'System health score (0-100)', registry=REGISTRY)
DB_SIZE = Gauge('test_db_size_mb', 'Database size in MB', registry=REGISTRY)
DATA_QUALITY_SCORE = Gauge('test_data_quality', 'Test data quality score', registry=REGISTRY)
TEST_QUEUE_SIZE = Gauge('test_queue_size', 'Test queue size', registry=REGISTRY)
WS_CONNECTIONS = Gauge('test_ws_connections', 'WebSocket connections', registry=REGISTRY)
FLAKINESS_SCORE = Gauge('test_flakiness_score', 'Test flakiness score', ['test_name'], registry=REGISTRY)

# Sustainability metrics
CARBON_INTENSITY = Gauge('carbon_intensity_gco2_per_kwh', 'Real-time carbon intensity', registry=REGISTRY)
TEST_CARBON_IMPACT = Gauge('test_carbon_impact_kg', 'Carbon impact per test', ['test_name'], registry=REGISTRY)
SUSTAINABILITY_SCORE = Gauge('test_sustainability_score', 'Sustainability score (0-100)', ['test_name'], registry=REGISTRY)
HELIUM_EFFICIENCY = Gauge('test_helium_efficiency', 'Helium efficiency (0-100)', ['test_name'], registry=REGISTRY)
CARBON_SAVINGS = Counter('test_carbon_savings_total', 'Total carbon savings from efficient tests', registry=REGISTRY)

# NEW v13.0 metrics
TEST_IMPACT_SCORE = Gauge('test_impact_score', 'Test impact score', ['test_name'], registry=REGISTRY)
ROOT_CAUSE_ACCURACY = Gauge('root_cause_accuracy', 'Root cause analysis accuracy', registry=REGISTRY)
SELF_HEALING_SUCCESS = Counter('self_healing_success_total', 'Successful self-healing operations', ['healing_type'], registry=REGISTRY)
PREDICTIVE_MAINTENANCE = Counter('predictive_maintenance_total', 'Predictive maintenance actions', ['action_type'], registry=REGISTRY)
ANALYTICS_QUERIES = Counter('analytics_queries_total', 'Analytics dashboard queries', ['query_type'], registry=REGISTRY)

# Constants
MAX_TEST_RUNS_HISTORY = 10000
MAX_FAILURE_HISTORY = 10000
MAX_CACHE_SIZE = 1000
MAX_RETRY_ATTEMPTS = 3
CIRCUIT_BREAKER_THRESHOLD = 5
CIRCUIT_BREAKER_TIMEOUT = 60
HEALTH_CHECK_TIMEOUT = 10
RATE_LIMIT_REQUESTS = 50
RATE_LIMIT_WINDOW = 60
MAX_CONCURRENT_TESTS = 8
DATA_VERSION = 13
DB_POOL_SIZE = 10
DB_MAX_OVERFLOW = 20
DB_POOL_TIMEOUT = 30
CACHE_CLEANUP_INTERVAL = 3600
MAX_CACHE_SIZE_MB = 500
PERFORMANCE_BASELINE_ITERATIONS = 10
STRESS_TEST_CONCURRENCY = 50
REGRESSION_THRESHOLD_PCT = 10
CARBON_INTENSITY_API_URL = "https://api.electricitymap.org/v3/carbon-intensity"

# ============================================================
# NEW v13.0: Intelligent Test Selection & Impact Analysis
# ============================================================

class TestImpactAnalyzer:
    """
    Analyzes code changes to determine which tests are impacted.
    
    Features:
    - Git integration for change detection
    - Coverage-based test mapping
    - Dependency analysis
    - Impact scoring
    - Prioritization
    """
    
    def __init__(self, repo_path: Optional[str] = None):
        self.repo_path = repo_path or os.getcwd()
        self.repo = None
        self.file_to_tests: Dict[str, Set[str]] = defaultdict(set)
        self.test_dependencies: Dict[str, Set[str]] = defaultdict(set)
        self._lock = asyncio.Lock()
        
        if GIT_AVAILABLE and os.path.exists(os.path.join(self.repo_path, '.git')):
            try:
                self.repo = Repo(self.repo_path)
                logger.info(f"Git repository loaded from {self.repo_path}")
            except Exception as e:
                logger.warning(f"Failed to load Git repository: {e}")
        
        logger.info("TestImpactAnalyzer initialized")
    
    async def map_file_to_tests(self, file_path: str, test_names: List[str]):
        """Map a source file to the tests that cover it"""
        async with self._lock:
            self.file_to_tests[file_path].update(test_names)
    
    async def map_test_dependencies(self, test_name: str, dependencies: List[str]):
        """Map test dependencies"""
        async with self._lock:
            self.test_dependencies[test_name].update(dependencies)
    
    async def analyze_impact(self, changed_files: List[str]) -> Dict[str, Any]:
        """
        Analyze the impact of changed files on tests.
        
        Returns:
            Dict with impacted tests, risk scores, and recommendations
        """
        async with self._lock:
            impacted_tests = set()
            risk_scores = {}
            
            for file_path in changed_files:
                # Find tests that cover this file
                if file_path in self.file_to_tests:
                    tests = self.file_to_tests[file_path]
                    impacted_tests.update(tests)
                    for test in tests:
                        risk_scores[test] = risk_scores.get(test, 0) + 1
            
            # Add dependent tests
            new_tests = set(impacted_tests)
            while new_tests:
                current = new_tests.pop()
                if current in self.test_dependencies:
                    deps = self.test_dependencies[current]
                    for dep in deps:
                        if dep not in impacted_tests:
                            impacted_tests.add(dep)
                            new_tests.add(dep)
                            risk_scores[dep] = risk_scores.get(dep, 0) + 0.5
            
            # Calculate impact scores
            impact_scores = {}
            for test in impacted_tests:
                score = min(1.0, risk_scores.get(test, 1) / 5)
                impact_scores[test] = score
                TEST_IMPACT_SCORE.labels(test_name=test).set(score)
            
            # Generate recommendations
            recommendations = []
            if impacted_tests:
                recommendations.append(f"Run {len(impacted_tests)} impacted tests")
                high_risk = [t for t, s in impact_scores.items() if s > 0.7]
                if high_risk:
                    recommendations.append(f"High-risk tests: {', '.join(high_risk)}")
            
            return {
                'impacted_tests': list(impacted_tests),
                'impact_scores': impact_scores,
                'total_impacted': len(impacted_tests),
                'recommendations': recommendations,
                'timestamp': datetime.now().isoformat()
            }
    
    async def get_changed_files(self, commit_range: Optional[str] = None) -> List[str]:
        """Get list of changed files in the current commit range"""
        if not self.repo:
            return []
        
        try:
            if commit_range:
                # Specific commit range
                diff = self.repo.git.diff(commit_range, '--name-only')
            else:
                # Staged changes
                diff = self.repo.git.diff('--cached', '--name-only')
            
            if diff:
                return [f.strip() for f in diff.split('\n') if f.strip()]
            return []
        except Exception as e:
            logger.error(f"Failed to get changed files: {e}")
            return []

# ============================================================
# NEW v13.0: ML-Based Root Cause Analysis
# ============================================================

class RootCauseAnalyzer:
    """
    Machine learning-based root cause analysis for test failures.
    
    Features:
    - Failure pattern classification
    - Feature extraction from logs and metrics
    - Confidence scoring
    - Historical pattern matching
    """
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.model = None
        self.vectorizer = TfidfVectorizer(max_features=1000) if SKLEARN_AVAILABLE else None
        self.label_encoder = LabelEncoder() if SKLEARN_AVAILABLE else None
        self.scaler = StandardScaler() if SKLEARN_AVAILABLE else None
        self.is_trained = False
        self._lock = asyncio.Lock()
        
        # Root cause categories
        self.root_cause_categories = [
            'timeout', 'assertion_error', 'environment_issue', 
            'data_issue', 'network_issue', 'resource_exhaustion',
            'code_regression', 'flaky_test', 'performance_degradation'
        ]
        
        logger.info("RootCauseAnalyzer initialized")
    
    async def train_model(self, historical_failures: List[Dict]):
        """Train ML model on historical failure data"""
        if not SKLEARN_AVAILABLE:
            logger.warning("scikit-learn not available. Using heuristic fallback.")
            return
        
        try:
            async with self._lock:
                # Prepare features
                features = []
                labels = []
                
                for failure in historical_failures:
                    # Extract features
                    log_text = failure.get('log', '')
                    system_metrics = failure.get('metrics', {})
                    
                    # Combine features
                    feature_dict = {
                        'log_length': len(log_text),
                        'has_timeout': 'timeout' in log_text.lower(),
                        'has_assertion': 'assert' in log_text.lower(),
                        'has_network': 'network' in log_text.lower(),
                        'memory_usage': system_metrics.get('memory_usage_mb', 0),
                        'cpu_usage': system_metrics.get('cpu_usage_pct', 0),
                        'test_duration': system_metrics.get('duration_ms', 0),
                        'retry_count': system_metrics.get('retry_count', 0),
                        'previous_failures': system_metrics.get('previous_failures', 0)
                    }
                    
                    # Text features
                    text_features = self.vectorizer.fit_transform([log_text]).toarray()[0]
                    
                    # Combine all features
                    all_features = list(feature_dict.values()) + list(text_features[:10])
                    features.append(all_features)
                    labels.append(failure.get('root_cause', 'unknown'))
                
                if not features:
                    return
                
                # Scale features
                features_scaled = self.scaler.fit_transform(features)
                
                # Encode labels
                labels_encoded = self.label_encoder.fit_transform(labels)
                
                # Train model
                self.model = RandomForestClassifier(
                    n_estimators=100,
                    max_depth=10,
                    random_state=42
                )
                self.model.fit(features_scaled, labels_encoded)
                
                self.is_trained = True
                logger.info(f"Root cause model trained on {len(features)} samples")
                
                # Update accuracy metric
                if len(features) > 10:
                    cv_score = cross_val_score(self.model, features_scaled, labels_encoded, cv=5).mean()
                    ROOT_CAUSE_ACCURACY.set(cv_score)
                
        except Exception as e:
            logger.error(f"Root cause model training error: {e}")
    
    async def analyze_failure(self, test_name: str, failure_log: str, 
                             system_metrics: Dict) -> Dict:
        """
        Analyze a test failure to determine root cause.
        
        Returns:
            Dict with root cause, confidence, and recommendations
        """
        async with self._lock:
            if self.is_trained and SKLEARN_AVAILABLE and self.model:
                try:
                    # Extract features
                    feature_dict = {
                        'log_length': len(failure_log),
                        'has_timeout': 'timeout' in failure_log.lower(),
                        'has_assertion': 'assert' in failure_log.lower(),
                        'has_network': 'network' in failure_log.lower(),
                        'memory_usage': system_metrics.get('memory_usage_mb', 0),
                        'cpu_usage': system_metrics.get('cpu_usage_pct', 0),
                        'test_duration': system_metrics.get('duration_ms', 0),
                        'retry_count': system_metrics.get('retry_count', 0),
                        'previous_failures': system_metrics.get('previous_failures', 0)
                    }
                    
                    # Text features
                    text_features = self.vectorizer.transform([failure_log]).toarray()[0]
                    
                    # Combine features
                    all_features = list(feature_dict.values()) + list(text_features[:10])
                    features_scaled = self.scaler.transform([all_features])
                    
                    # Predict
                    prediction = self.model.predict(features_scaled)[0]
                    probabilities = self.model.predict_proba(features_scaled)[0]
                    confidence = max(probabilities)
                    
                    root_cause = self.label_encoder.inverse_transform([prediction])[0]
                    
                    # Generate recommendations
                    recommendations = self._generate_recommendations(root_cause, system_metrics)
                    
                    return {
                        'root_cause': root_cause,
                        'confidence': float(confidence),
                        'recommendations': recommendations,
                        'method': 'ml',
                        'timestamp': datetime.now().isoformat()
                    }
                    
                except Exception as e:
                    logger.error(f"ML analysis error: {e}")
            
            # Fallback to heuristic analysis
            return await self._heuristic_analysis(failure_log, system_metrics)
    
    async def _heuristic_analysis(self, failure_log: str, system_metrics: Dict) -> Dict:
        """Heuristic-based root cause analysis (fallback)"""
        root_cause = 'unknown'
        confidence = 0.5
        recommendations = []
        
        log_lower = failure_log.lower()
        
        if 'timeout' in log_lower:
            root_cause = 'timeout'
            confidence = 0.7
            if system_metrics.get('cpu_usage_pct', 0) > 80:
                recommendations.append("High CPU usage detected - consider reducing load")
            if system_metrics.get('memory_usage_mb', 0) > 1000:
                recommendations.append("High memory usage - consider increasing memory limit")
        
        elif 'assert' in log_lower:
            root_cause = 'assertion_error'
            confidence = 0.8
            recommendations.append("Check test expectations and data validity")
        
        elif 'network' in log_lower or 'connection' in log_lower:
            root_cause = 'network_issue'
            confidence = 0.7
            recommendations.append("Verify network connectivity and API availability")
        
        elif 'out of memory' in log_lower or 'memory' in log_lower:
            root_cause = 'resource_exhaustion'
            confidence = 0.75
            recommendations.append("Increase memory allocation or optimize test")
        
        elif 'flaky' in log_lower:
            root_cause = 'flaky_test'
            confidence = 0.6
            recommendations.append("Review test for non-deterministic behavior")
        
        return {
            'root_cause': root_cause,
            'confidence': confidence,
            'recommendations': recommendations,
            'method': 'heuristic',
            'timestamp': datetime.now().isoformat()
        }
    
    def _generate_recommendations(self, root_cause: str, metrics: Dict) -> List[str]:
        """Generate recommendations based on root cause"""
        recommendations = []
        
        if root_cause == 'timeout':
            recommendations.append("Increase test timeout or optimize test execution")
            if metrics.get('system_load', 0) > 0.7:
                recommendations.append("Reduce concurrent test execution to lower system load")
        
        elif root_cause == 'assertion_error':
            recommendations.append("Review test assertions for correctness")
            recommendations.append("Check test data validity and completeness")
        
        elif root_cause == 'environment_issue':
            recommendations.append("Verify test environment configuration")
            recommendations.append("Check for missing dependencies or environment variables")
        
        elif root_cause == 'code_regression':
            recommendations.append("Review recent code changes that may have caused regression")
            recommendations.append("Consider reverting changes or adding more tests")
        
        elif root_cause == 'flaky_test':
            recommendations.append("Investigate non-deterministic test behavior")
            recommendations.append("Add retry logic or improve test isolation")
        
        return recommendations[:3]

# ============================================================
# NEW v13.0: Self-Healing Test Manager
# ============================================================

class SelfHealingTestManager:
    """
    Self-healing capabilities for test execution.
    
    Features:
    - Automatic timeout adjustment
    - Retry strategy optimization
    - Environment adaptation
    - Resource allocation adjustment
    """
    
    def __init__(self):
        self.healing_history: Dict[str, List[Dict]] = defaultdict(list)
        self.healing_success: Dict[str, int] = defaultdict(int)
        self.healing_failures: Dict[str, int] = defaultdict(int)
        self._lock = asyncio.Lock()
        
        logger.info("SelfHealingTestManager initialized")
    
    async def heal_test(self, test_name: str, failure_type: str, 
                       context: Dict) -> Dict:
        """
        Attempt to heal a failing test.
        
        Returns:
            Dict with healing action, parameters, and confidence
        """
        async with self._lock:
            healing_action = None
            params = {}
            confidence = 0.5
            
            # Analyze failure context
            if failure_type == 'timeout':
                # Increase timeout based on system load
                system_load = context.get('system_load', 0.5)
                current_timeout = context.get('original_timeout', 30)
                new_timeout = current_timeout * (1 + system_load * 0.5)
                
                healing_action = 'increase_timeout'
                params = {'new_timeout': new_timeout, 'reason': 'System load detected'}
                confidence = 0.7
                
            elif failure_type == 'resource_exhaustion':
                # Reduce resource usage or retry with backoff
                retry_count = context.get('retry_count', 0)
                if retry_count < 3:
                    backoff = 2 ** retry_count
                    healing_action = 'retry_with_backoff'
                    params = {'backoff_seconds': backoff, 'retry_count': retry_count + 1}
                    confidence = 0.6
                else:
                    healing_action = 'reduce_concurrency'
                    params = {'concurrency_reduction': 0.5}
                    confidence = 0.5
                    
            elif failure_type == 'environment':
                # Attempt environment fix
                missing_resource = context.get('missing_resource')
                if missing_resource:
                    healing_action = 'environment_fix'
                    params = {'resource': missing_resource, 'action': 'allocate'}
                    confidence = 0.4
                    
            elif failure_type == 'flaky':
                # Add retry with randomization
                healing_action = 'add_retry'
                params = {'max_retries': 3, 'retry_delay': 1}
                confidence = 0.5
            
            # Record healing attempt
            if healing_action:
                self.healing_history[test_name].append({
                    'action': healing_action,
                    'params': params,
                    'timestamp': datetime.now().isoformat(),
                    'success': None  # To be filled later
                })
                
                SELF_HEALING_SUCCESS.labels(healing_type=healing_action).inc()
                
                return {
                    'healing_applied': True,
                    'action': healing_action,
                    'parameters': params,
                    'confidence': confidence,
                    'recommendation': f"Apply {healing_action} to {test_name}"
                }
            
            return {
                'healing_applied': False,
                'action': None,
                'reason': 'No suitable healing strategy found'
            }
    
    async def record_healing_outcome(self, test_name: str, healing_action: str, 
                                     success: bool):
        """Record outcome of healing attempt"""
        async with self._lock:
            if test_name in self.healing_history:
                for entry in reversed(self.healing_history[test_name]):
                    if entry['action'] == healing_action and entry['success'] is None:
                        entry['success'] = success
                        break
            
            if success:
                self.healing_success[test_name] += 1
            else:
                self.healing_failures[test_name] += 1
    
    def get_healing_statistics(self) -> Dict:
        """Get healing statistics"""
        total_attempts = sum(len(h) for h in self.healing_history.values())
        total_success = sum(self.healing_success.values())
        total_failures = sum(self.healing_failures.values())
        
        return {
            'total_attempts': total_attempts,
            'total_success': total_success,
            'total_failures': total_failures,
            'success_rate': total_success / max(total_attempts, 1),
            'by_test': {
                test: {
                    'attempts': len(history),
                    'success': self.healing_success.get(test, 0),
                    'failures': self.healing_failures.get(test, 0)
                }
                for test, history in self.healing_history.items()
            }
        }

# ============================================================
# NEW v13.0: Predictive Test Maintenance
# ============================================================

class PredictiveMaintenanceManager:
    """
    Predictive maintenance for test health management.
    
    Features:
    - Health trend analysis
    - Maintenance scheduling
    - Risk prediction
    - Proactive actions
    """
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.test_health: Dict[str, Dict] = {}
        self.maintenance_schedule: Dict[str, datetime] = {}
        self._lock = asyncio.Lock()
        
        logger.info("PredictiveMaintenanceManager initialized")
    
    async def predict_maintenance_need(self, test_name: str, 
                                      historical_data: List[Dict]) -> Dict:
        """
        Predict when test maintenance is needed.
        
        Returns:
            Dict with maintenance prediction and recommendations
        """
        async with self._lock:
            if len(historical_data) < 10:
                return {
                    'needs_maintenance': False,
                    'confidence': 0.1,
                    'reason': 'Insufficient historical data'
                }
            
            # Analyze trends
            failure_rate = sum(1 for d in historical_data if not d.get('passed', False)) / len(historical_data)
            avg_duration = np.mean([d.get('duration_ms', 0) for d in historical_data])
            duration_trend = self._calculate_trend([d.get('duration_ms', 0) for d in historical_data])
            
            # Calculate health score
            health_score = 100
            if failure_rate > 0.1:
                health_score -= failure_rate * 200
            if duration_trend > 0:
                health_score -= duration_trend * 10
            
            health_score = max(0, min(100, health_score))
            
            # Determine maintenance need
            needs_maintenance = health_score < 70
            days_until_maintenance = 30 * (1 - health_score / 100) if needs_maintenance else None
            
            # Generate recommendations
            recommendations = []
            if needs_maintenance:
                if failure_rate > 0.2:
                    recommendations.append("High failure rate - investigate and fix")
                if duration_trend > 5:
                    recommendations.append("Performance degradation - optimize test")
                if avg_duration > 10000:
                    recommendations.append("Long-running test - consider splitting or optimizing")
            
            # Store health data
            self.test_health[test_name] = {
                'health_score': health_score,
                'failure_rate': failure_rate,
                'avg_duration_ms': avg_duration,
                'duration_trend': duration_trend,
                'last_updated': datetime.now().isoformat()
            }
            
            if needs_maintenance:
                self.maintenance_schedule[test_name] = datetime.now() + timedelta(days=days_until_maintenance or 7)
                PREDICTIVE_MAINTENANCE.labels(action_type='schedule').inc()
            
            return {
                'needs_maintenance': needs_maintenance,
                'health_score': health_score,
                'days_until_maintenance': days_until_maintenance,
                'confidence': 0.8 if len(historical_data) > 20 else 0.5,
                'recommendations': recommendations,
                'timestamp': datetime.now().isoformat()
            }
    
    def _calculate_trend(self, values: List[float]) -> float:
        """Calculate trend direction from values"""
        if len(values) < 2:
            return 0
        
        # Simple linear regression
        x = np.arange(len(values))
        slope = np.polyfit(x, values, 1)[0]
        return slope / max(np.mean(values), 1) * 100
    
    def get_maintenance_report(self) -> Dict:
        """Get comprehensive maintenance report"""
        now = datetime.now()
        
        upcoming_maintenance = {
            test: scheduled
            for test, scheduled in self.maintenance_schedule.items()
            if scheduled > now
        }
        
        overdue = {
            test: scheduled
            for test, scheduled in self.maintenance_schedule.items()
            if scheduled <= now
        }
        
        return {
            'total_tests_tracked': len(self.test_health),
            'upcoming_maintenance': len(upcoming_maintenance),
            'overdue_maintenance': len(overdue),
            'average_health_score': np.mean([h['health_score'] for h in self.test_health.values()]) if self.test_health else 0,
            'upcoming_tests': upcoming_maintenance,
            'overdue_tests': overdue,
            'timestamp': datetime.now().isoformat()
        }

# ============================================================
# NEW v13.0: Enhanced Analytics Dashboard
# ============================================================

class EnhancedAnalyticsDashboard:
    """
    Enhanced analytics dashboard for test intelligence.
    
    Features:
    - Rich test analytics
    - Interactive visualizations
    - Trend analysis
    - Customizable dashboards
    """
    
    def __init__(self, websocket_manager):
        self.websocket = websocket_manager
        self.analytics_cache: Dict[str, Any] = {}
        self._lock = asyncio.Lock()
        
        logger.info("EnhancedAnalyticsDashboard initialized")
    
    async def get_comprehensive_analytics(self, test_env) -> Dict:
        """Get comprehensive test analytics"""
        async with self._lock:
            ANALYTICS_QUERIES.labels(query_type='comprehensive').inc()
            
            analytics = {
                'timestamp': datetime.now().isoformat(),
                'test_metrics': await self._get_test_metrics(test_env),
                'performance_analytics': await self._get_performance_analytics(test_env),
                'sustainability_analytics': await self._get_sustainability_analytics(test_env),
                'failure_analytics': await self._get_failure_analytics(test_env),
                'trend_analytics': await self._get_trend_analytics(test_env),
                'predictive_analytics': await self._get_predictive_analytics(test_env)
            }
            
            # Cache results
            self.analytics_cache['latest'] = analytics
            
            return analytics
    
    async def _get_test_metrics(self, test_env) -> Dict:
        """Get basic test metrics"""
        return {
            'total_tests': len(test_env.test_registry),
            'passed_tests': sum(1 for r in test_env.test_results.values() if r.passed),
            'failed_tests': sum(1 for r in test_env.test_results.values() if not r.passed),
            'success_rate': test_env.get_success_rate(),
            'average_duration_ms': np.mean([r.duration_ms for r in test_env.test_results.values()]) if test_env.test_results else 0
        }
    
    async def _get_performance_analytics(self, test_env) -> Dict:
        """Get performance analytics"""
        analytics = {
            'regression_detected': sum(1 for r in test_env.test_results.values() if r.regression_detected),
            'performance_trend': []
        }
        
        # Calculate performance trend
        if test_env.test_results:
            durations = [r.duration_ms for r in test_env.test_results.values() if r.duration_ms > 0]
            if durations:
                analytics['avg_duration'] = np.mean(durations)
                analytics['p95_duration'] = np.percentile(durations, 95)
                analytics['p99_duration'] = np.percentile(durations, 99)
        
        return analytics
    
    async def _get_sustainability_analytics(self, test_env) -> Dict:
        """Get sustainability analytics"""
        analytics = {
            'total_carbon_impact_kg': sum(r.carbon_impact_kg for r in test_env.test_results.values()),
            'average_carbon_impact_kg': np.mean([r.carbon_impact_kg for r in test_env.test_results.values()]) if test_env.test_results else 0,
            'total_helium_usage_l': test_env.helium_tracker.total_usage_l if test_env.helium_tracker else 0,
            'sustainability_score': test_env.sustainability_score if hasattr(test_env, 'sustainability_score') else 0
        }
        
        # Carbon intensity trend
        if test_env.carbon_manager and test_env.carbon_manager.historical_intensities:
            analytics['carbon_intensity_trend'] = list(test_env.carbon_manager.historical_intensities)
        
        return analytics
    
    async def _get_failure_analytics(self, test_env) -> Dict:
        """Get failure analytics"""
        analytics = {
            'failure_by_type': defaultdict(int),
            'failure_by_test': defaultdict(int),
            'flaky_tests': []
        }
        
        for test_name, result in test_env.test_results.items():
            if not result.passed:
                analytics['failure_by_type'][result.failure_type or 'unknown'] += 1
                analytics['failure_by_test'][test_name] += 1
        
        # Identify flaky tests
        if hasattr(test_env, 'flakiness_analyzer'):
            flakiness_scores = await test_env.flakiness_analyzer.get_all_scores()
            analytics['flaky_tests'] = [
                {'name': name, 'score': score}
                for name, score in flakiness_scores.items()
                if score > 0.3
            ]
        
        return analytics
    
    async def _get_trend_analytics(self, test_env) -> Dict:
        """Get trend analytics"""
        if not test_env.test_results:
            return {}
        
        # Calculate trends
        results_list = list(test_env.test_results.values())
        recent = results_list[-10:] if len(results_list) > 10 else results_list
        
        return {
            'success_trend': [r.passed for r in recent],
            'duration_trend': [r.duration_ms for r in recent],
            'carbon_trend': [r.carbon_impact_kg for r in recent]
        }
    
    async def _get_predictive_analytics(self, test_env) -> Dict:
        """Get predictive analytics"""
        analytics = {
            'maintenance_recommendations': [],
            'risk_assessment': {}
        }
        
        if hasattr(test_env, 'predictive_maintenance_manager'):
            maintenance_report = test_env.predictive_maintenance_manager.get_maintenance_report()
            analytics['maintenance_recommendations'] = maintenance_report.get('upcoming_tests', {})
            analytics['overdue_maintenance'] = maintenance_report.get('overdue_tests', {})
        
        # Risk assessment
        if test_env.test_results:
            for test_name, result in test_env.test_results.items():
                if not result.passed:
                    analytics['risk_assessment'][test_name] = {
                        'failure_type': result.failure_type,
                        'retry_count': result.retry_count,
                        'needs_attention': True
                    }
        
        return analytics
    
    async def generate_report(self, test_env, format: str = 'json') -> Dict:
        """Generate comprehensive analytics report"""
        analytics = await self.get_comprehensive_analytics(test_env)
        
        report = {
            'title': 'Test Analytics Report',
            'generated_at': datetime.now().isoformat(),
            'summary': {
                'total_tests': analytics['test_metrics']['total_tests'],
                'success_rate': analytics['test_metrics']['success_rate'],
                'avg_duration_ms': analytics['test_metrics']['average_duration_ms'],
                'sustainability_score': analytics['sustainability_analytics']['sustainability_score']
            },
            'analytics': analytics,
            'recommendations': await self._generate_recommendations(analytics)
        }
        
        return report
    
    async def _generate_recommendations(self, analytics: Dict) -> List[str]:
        """Generate actionable recommendations from analytics"""
        recommendations = []
        
        # Performance recommendations
        if analytics['performance_analytics'].get('regression_detected', 0) > 0:
            recommendations.append("Performance regressions detected - review recent changes")
        
        # Sustainability recommendations
        if analytics['sustainability_analytics'].get('total_carbon_impact_kg', 0) > 1:
            recommendations.append("High carbon impact - consider optimizing test execution")
        
        # Failure recommendations
        failure_types = analytics['failure_analytics'].get('failure_by_type', {})
        if failure_types:
            most_common = max(failure_types, key=failure_types.get)
            recommendations.append(f"Most common failure type: {most_common} - investigate root cause")
        
        # Maintenance recommendations
        maintenance = analytics['predictive_analytics'].get('maintenance_recommendations', {})
        if maintenance:
            recommendations.append(f"{len(maintenance)} tests require maintenance - review health scores")
        
        # Flaky test recommendations
        flaky_tests = analytics['failure_analytics'].get('flaky_tests', [])
        if flaky_tests:
            recommendations.append(f"{len(flaky_tests)} flaky tests detected - prioritize fixing")
        
        return recommendations[:5]  # Top 5 recommendations

# ============================================================
# ENHANCED MAIN TEST ENVIRONMENT V13
# ============================================================

class EnhancedTestEnvironmentV13:
    """Enhanced test environment v13.0 with advanced intelligence features"""
    
    def __init__(self):
        self.instance_id = str(uuid.uuid4())[:8]
        
        # Database
        self.db_manager = EnhancedDatabaseManagerV13(Path("./test_data_v13.db"))
        
        # Sustainability modules (from v12)
        self.carbon_manager = CarbonIntensityManager()
        self.helium_tracker = HeliumTestTracker()
        self.sustainability_dashboard = TestSustainabilityDashboard()
        self.federated_learner = FederatedTestLearner()
        self.carbon_scheduler = CarbonAwareTestScheduler(self.carbon_manager)
        
        # v12 Components
        self.benchmark = PerformanceBenchmark()
        self.stress_tester = StressTester()
        self.dependency_resolver = TestDependencyResolver()
        
        # ============================================================
        # NEW v13.0: Advanced Intelligence Components
        # ============================================================
        
        # 1. Intelligent Test Selection
        self.impact_analyzer = TestImpactAnalyzer()
        
        # 2. ML-Based Root Cause Analysis
        self.root_cause_analyzer = RootCauseAnalyzer(self.db_manager)
        
        # 3. Self-Healing Test Manager
        self.self_healing_manager = SelfHealingTestManager()
        
        # 4. Predictive Maintenance Manager
        self.predictive_maintenance_manager = PredictiveMaintenanceManager(self.db_manager)
        
        # 5. Enhanced Analytics Dashboard
        self.analytics_dashboard = EnhancedAnalyticsDashboard(None)  # Will be set with websocket
        
        # Cache
        self.cache = None  # Initialize later
        
        # Test registry
        self.test_registry: Dict[str, TestFeatureModel] = {}
        self._registry_lock = asyncio.Lock()
        
        # State (bounded)
        self.test_results: Dict[str, TestResult] = {}
        self._results_lock = asyncio.Lock()
        
        # Concurrency control
        self._test_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TESTS)
        
        # Thread pool for CPU-bound tasks
        self.thread_pool = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_TESTS)
        
        # Operation queue
        self.operation_queue = asyncio.Queue(maxsize=100)
        self._queue_worker = None
        self._running = False
        
        # WebSocket dashboard
        self.websocket = TestDashboardWebSocket(port=8779)
        
        # Set analytics dashboard websocket
        self.analytics_dashboard.websocket = self.websocket
        
        # Background tasks
        self.background_tasks = set()
        self._shutdown_event = asyncio.Event()
        
        # Sustainability tracking
        self.sustainability_score = 0.0
        self.total_carbon_savings_kg = 0.0
        
        # Start background ML training
        self.ml_ready = False
        
        logger.info(f"EnhancedTestEnvironmentV13 v{DATA_VERSION}.0 initialized (instance: {self.instance_id})")
        logger.info("  ✅ v13.0 Advanced Intelligence Features:")
        logger.info("     - Intelligent Test Selection & Impact Analysis")
        logger.info("     - ML-Based Root Cause Analysis")
        logger.info("     - Self-Healing Test Capabilities")
        logger.info("     - Predictive Test Maintenance")
        logger.info("     - Enhanced Analytics Dashboard")
    
    async def start(self):
        """Start all services"""
        self._running = True
        
        # Initialize components
        self.cache = EnhancedCacheManagerV13()
        self.quality_scorer = EnhancedDataQualityScorer()
        self.rate_limiter = EnhancedRateLimiter()
        self.flakiness_analyzer = EnhancedFlakinessAnalyzer(self.db_manager)
        self.circuit_breakers = {
            'test': EnhancedCircuitBreaker('test'),
            'analysis': EnhancedCircuitBreaker('analysis')
        }
        
        await self.cache.start()
        
        # Initialize carbon manager
        await self.carbon_manager.update_carbon_intensity()
        
        # Train ML models in background
        asyncio.create_task(self._train_ml_models())
        
        # Start queue worker
        self._queue_worker = asyncio.create_task(self._process_queue())
        
        # Start WebSocket dashboard
        await self.websocket.start()
        
        # Start background tasks
        tasks = [
            asyncio.create_task(self._health_check_loop()),
            asyncio.create_task(self._cleanup_loop()),
            asyncio.create_task(self._carbon_update_loop()),
            asyncio.create_task(self._federated_sync_loop()),
            asyncio.create_task(self._predictive_maintenance_loop())
        ]
        
        for task in tasks:
            self.background_tasks.add(task)
            task.add_done_callback(self.background_tasks.discard)
        
        logger.info(f"Test environment started with {len(self.background_tasks)} background tasks")
    
    async def _train_ml_models(self):
        """Train ML models in background"""
        try:
            logger.info("Starting ML model training...")
            
            # Collect historical failure data
            failures = await self.db_manager.get_failure_history(limit=100)
            
            if failures:
                await self.root_cause_analyzer.train_model(failures)
                self.ml_ready = True
                logger.info(f"ML models trained on {len(failures)} failure samples")
            
        except Exception as e:
            logger.error(f"ML model training error: {e}")
    
    async def _predictive_maintenance_loop(self):
        """Background predictive maintenance loop"""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(3600)  # Every hour
                
                if self.test_results:
                    # Analyze test health
                    for test_name in self.test_results:
                        history = await self.db_manager.get_test_history(test_name, limit=30)
                        if history:
                            prediction = await self.predictive_maintenance_manager.predict_maintenance_need(
                                test_name, history
                            )
                            if prediction.get('needs_maintenance'):
                                logger.info(f"Maintenance needed for {test_name}: {prediction.get('recommendations')}")
            except Exception as e:
                logger.error(f"Predictive maintenance loop error: {e}")
                await asyncio.sleep(300)
    
    async def _execute_test(self, operation: Dict) -> TestResult:
        """Execute test with v13.0 enhancements"""
        async with self._test_semaphore:
            await self.rate_limiter.wait_and_acquire()
            
            test_name = operation['test_name']
            test_func = operation['test_func']
            test_type = operation.get('test_type', TestType.UNIT)
            use_impact_analysis = operation.get('use_impact_analysis', False)
            
            # Get carbon intensity
            carbon_intensity = await self.carbon_manager.get_current_intensity()
            
            start_time = time.time()
            retry_count = 0
            last_error = None
            failure_type = ""
            healing_applied = False
            
            # Get test features
            async with self._registry_lock:
                test_features = self.test_registry.get(test_name)
                timeout = test_features.timeout_seconds if test_features else 30.0
            
            for attempt in range(MAX_RETRY_ATTEMPTS):
                try:
                    # Run test with circuit breaker
                    passed, coverage = await self.circuit_breakers['test'].call(
                        self._run_test, test_func, test_name, timeout
                    )
                    
                    duration_ms = (time.time() - start_time) * 1000
                    
                    # Calculate carbon impact
                    carbon_impact = self.carbon_manager.calculate_test_carbon_impact(
                        duration_ms, 
                        test_features.code_complexity / 100 if test_features else 1.0
                    )
                    
                    # Record helium usage
                    helium_usage = test_features.helium_usage_l if test_features else 0.001
                    await self.helium_tracker.record_helium_usage(test_name, helium_usage, test_type.value)
                    
                    # Calculate sustainability score
                    sustainability_score = self._calculate_sustainability_score(
                        passed, carbon_impact, helium_usage, coverage
                    )
                    
                    result = TestResult(
                        test_name=test_name,
                        test_type=test_type,
                        passed=passed,
                        duration_ms=duration_ms,
                        message="Test completed" if passed else "Test failed",
                        retry_count=retry_count,
                        coverage_percent=coverage,
                        carbon_impact_kg=carbon_impact,
                        helium_usage_l=helium_usage,
                        sustainability_score=sustainability_score,
                        carbon_intensity=carbon_intensity,
                        failure_type=failure_type
                    )
                    
                    # Assess quality
                    quality_score = await self.quality_scorer.assess_quality(result)
                    result.data_quality_score = quality_score
                    
                    # ============================================================
                    # NEW v13.0: Enhanced failure analysis
                    # ============================================================
                    
                    if not passed and retry_count > 0:
                        # Get system metrics for analysis
                        system_metrics = {
                            'memory_usage_mb': operation.get('memory_usage_mb', 0),
                            'cpu_usage_pct': operation.get('cpu_usage_pct', 0),
                            'duration_ms': duration_ms,
                            'retry_count': retry_count,
                            'previous_failures': len([r for r in self.test_results.values() if not r.passed])
                        }
                        
                        # Root cause analysis
                        root_cause_analysis = await self.root_cause_analyzer.analyze_failure(
                            test_name, 
                            result.message or "",
                            system_metrics
                        )
                        result.message = f"{result.message}\nRoot cause: {root_cause_analysis.get('root_cause')}"
                        result.failure_type = root_cause_analysis.get('root_cause', 'unknown')
                        
                        # Self-healing
                        healing_context = {
                            'system_load': system_metrics.get('cpu_usage_pct', 0) / 100,
                            'original_timeout': timeout,
                            'retry_count': retry_count,
                            'failure_type': result.failure_type,
                            'test_name': test_name
                        }
                        
                        healing_result = await self.self_healing_manager.heal_test(
                            test_name, result.failure_type, healing_context
                        )
                        
                        if healing_result.get('healing_applied'):
                            healing_applied = True
                            result.message = f"{result.message}\nHealing applied: {healing_result.get('action')}"
                            
                            # Apply healing (e.g., update timeout for next attempt)
                            if healing_result.get('action') == 'increase_timeout':
                                timeout = healing_result['parameters'].get('new_timeout', timeout)
                    
                    # Check for performance regression
                    if test_type == TestType.PERFORMANCE:
                        benchmark_results = await self.benchmark.run_benchmark(test_func, test_name)
                        result.regression_detected = benchmark_results['is_regression']
                        if benchmark_results['is_regression']:
                            result.message = f"Performance regression: {benchmark_results['regression_pct']:.1f}%"
                    
                    # Store in memory
                    async with self._results_lock:
                        self.test_results[test_name] = result
                    
                    # Save to database
                    await self.db_manager.save_test_result(result)
                    
                    # Update metrics
                    TEST_RUNS.labels(status='success' if passed else 'failed', type=test_type.value).inc()
                    TEST_DURATION.labels(test_type=test_type.value).observe(duration_ms / 1000)
                    TEST_COVERAGE.labels(coverage_type='line').set(coverage)
                    TEST_CARBON_IMPACT.labels(test_name=test_name).set(carbon_impact)
                    SUSTAINABILITY_SCORE.labels(test_name=test_name).set(sustainability_score)
                    
                    if not passed:
                        TEST_FAILURES.labels(test_name=test_name, failure_type=failure_type).inc()
                    
                    # Broadcast via WebSocket
                    await self.websocket.broadcast_test_result(result)
                    
                    # Check for maintenance need
                    history = await self.db_manager.get_test_history(test_name, limit=30)
                    if history:
                        await self.predictive_maintenance_manager.predict_maintenance_need(test_name, history)
                    
                    return result
                    
                except asyncio.TimeoutError:
                    last_error = TimeoutError(f"Test timed out after {timeout}s")
                    failure_type = "timeout"
                    retry_count += 1
                    if attempt < MAX_RETRY_ATTEMPTS - 1:
                        wait_time = min(2 ** attempt, 10)
                        logger.warning(f"Test {test_name} timed out (attempt {attempt+1}), retrying in {wait_time}s")
                        await asyncio.sleep(wait_time)
                        
                except Exception as e:
                    last_error = e
                    failure_type = type(e).__name__
                    retry_count += 1
                    if attempt < MAX_RETRY_ATTEMPTS - 1:
                        wait_time = min(2 ** attempt, 10)
                        logger.warning(f"Test {test_name} failed (attempt {attempt+1}), retrying in {wait_time}s")
                        await asyncio.sleep(wait_time)
            
            # All retries failed
            duration_ms = (time.time() - start_time) * 1000
            result = TestResult(
                test_name=test_name,
                test_type=test_type,
                passed=False,
                duration_ms=duration_ms,
                message=str(last_error),
                retry_count=retry_count,
                failure_type=failure_type
            )
            
            await self.db_manager.save_test_result(result)
            TEST_RUNS.labels(status='failed', type=test_type.value).inc()
            TEST_FAILURES.labels(test_name=test_name, failure_type=failure_type).inc()
            
            return result
    
    async def _run_test(self, test_func: Callable, test_name: str, timeout: float) -> Tuple[bool, float]:
        """Run a single test with timeout"""
        try:
            if asyncio.iscoroutinefunction(test_func):
                result = await asyncio.wait_for(test_func(), timeout=timeout)
            else:
                loop = asyncio.get_event_loop()
                result = await asyncio.wait_for(
                    loop.run_in_executor(self.thread_pool, test_func),
                    timeout=timeout
                )
            
            # Parse result (assuming test returns (passed, coverage) tuple)
            if isinstance(result, tuple) and len(result) == 2:
                return result
            elif isinstance(result, bool):
                return result, 0.0
            else:
                return True, 0.0
                
        except asyncio.TimeoutError:
            raise TimeoutError(f"Test timeout after {timeout}s")
        except Exception as e:
            raise e
    
    def _calculate_sustainability_score(self, passed: bool, carbon_impact: float, 
                                       helium_usage: float, coverage: float) -> float:
        """Calculate sustainability score for test"""
        score = 0.0
        
        # Pass/fail weight
        score += 0.3 if passed else 0.0
        
        # Carbon impact weight (lower is better)
        carbon_weight = max(0, 1 - carbon_impact * 10) if carbon_impact > 0 else 1
        score += 0.25 * carbon_weight
        
        # Helium efficiency (lower is better)
        helium_weight = max(0, 1 - helium_usage * 100) if helium_usage > 0 else 1
        score += 0.2 * helium_weight
        
        # Coverage weight
        score += 0.25 * (coverage / 100)
        
        return min(1.0, max(0.0, score))
    
    def get_success_rate(self) -> float:
        """Get overall test success rate"""
        if not self.test_results:
            return 1.0
        
        passed = sum(1 for r in self.test_results.values() if r.passed)
        return passed / len(self.test_results)
    
    # ============================================================
    # NEW v13.0: Enhanced Public Methods
    # ============================================================
    
    async def analyze_test_impact(self, changed_files: List[str]) -> Dict:
        """Analyze impact of changed files on tests"""
        return await self.impact_analyzer.analyze_impact(changed_files)
    
    async def analyze_failure_root_cause(self, test_name: str, failure_log: str,
                                        system_metrics: Dict) -> Dict:
        """Analyze root cause of test failure"""
        return await self.root_cause_analyzer.analyze_failure(test_name, failure_log, system_metrics)
    
    async def get_predictive_maintenance_report(self) -> Dict:
        """Get predictive maintenance report"""
        return self.predictive_maintenance_manager.get_maintenance_report()
    
    async def get_comprehensive_analytics(self) -> Dict:
        """Get comprehensive analytics report"""
        return await self.analytics_dashboard.get_comprehensive_analytics(self)
    
    async def get_healing_statistics(self) -> Dict:
        """Get self-healing statistics"""
        return self.self_healing_manager.get_healing_statistics()
    
    async def register_test_with_impact_mapping(self, test_name: str, test_func: Callable,
                                               test_type: TestType = TestType.UNIT,
                                               priority: TestPriority = TestPriority.NORMAL,
                                               dependencies: List[str] = None,
                                               source_files: List[str] = None,
                                               timeout_seconds: float = 30.0,
                                               carbon_impact_kg: float = 0.001,
                                               helium_usage_l: float = 0.001):
        """Register a test with impact mapping"""
        await self.register_test(test_name, test_func, test_type, priority, 
                                dependencies, timeout_seconds, carbon_impact_kg, helium_usage_l)
        
        # Map source files to test
        if source_files:
            for file in source_files:
                await self.impact_analyzer.map_file_to_tests(file, [test_name])
        
        # Map dependencies
        if dependencies:
            await self.impact_analyzer.map_test_dependencies(test_name, dependencies)
    
    async def _carbon_update_loop(self):
        """Background carbon intensity update loop"""
        while not self._shutdown_event.is_set():
            try:
                await self.carbon_manager.update_carbon_intensity()
                await asyncio.sleep(self.carbon_manager.update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Carbon update error: {e}")
                await asyncio.sleep(60)
    
    async def _federated_sync_loop(self):
        """Background federated sync loop"""
        while not self._shutdown_event.is_set():
            try:
                if self.federated_learner and self.test_results:
                    patterns = {
                        'total_tests': len(self.test_results),
                        'success_rate': sum(1 for r in self.test_results.values() if r.passed) / max(len(self.test_results), 1),
                        'avg_sustainability': np.mean([r.sustainability_score for r in self.test_results.values() if r.sustainability_score > 0])
                    }
                    await self.federated_learner.share_test_patterns(
                        f"test_{self.instance_id}",
                        patterns,
                        performance=self.sustainability_score
                    )
                    await self.federated_learner.get_global_patterns()
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Federated sync error: {e}")
                await asyncio.sleep(300)
    
    async def _process_queue(self):
        """Process queued test operations"""
        while self._running:
            try:
                operation = await self.operation_queue.get()
                TEST_QUEUE_SIZE.set(self.operation_queue.qsize())
                
                try:
                    result = await self._execute_test(operation)
                    operation['future'].set_result(result)
                except Exception as e:
                    operation['future'].set_exception(e)
                finally:
                    self.operation_queue.task_done()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Queue worker error: {e}")
    
    async def _health_check_loop(self):
        """Background health check loop"""
        while not self._shutdown_event.is_set():
            try:
                health = await self.health_check()
                HEALTH_SCORE.set(health.get('health_score', 0))
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check error: {e}")
                await asyncio.sleep(60)
    
    async def _cleanup_loop(self):
        """Background cleanup for old data"""
        while not self._shutdown_event.is_set():
            try:
                gc.collect()
                await asyncio.sleep(CACHE_CLEANUP_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cleanup error: {e}")
                await asyncio.sleep(3600)
    
    async def health_check(self) -> Dict:
        """Enhanced health check with v13.0 components"""
        try:
            async def _check():
                async with self._results_lock:
                    result_count = len(self.test_results)
                
                quality_stats = await self.quality_scorer.get_statistics()
                cache_stats = await self.cache.get_stats()
                sustainability = await self.sustainability_dashboard.get_dashboard_status(
                    self.carbon_manager, self.helium_tracker, self
                )
                
                health_score = 100
                if result_count == 0:
                    health_score -= 30
                if quality_stats.get('avg_score', 0) < 50:
                    health_score -= 20
                
                return {
                    'healthy': result_count > 0,
                    'instance_id': self.instance_id,
                    'version': DATA_VERSION,
                    'result_count': result_count,
                    'health_score': max(0, health_score),
                    'data_quality': quality_stats.get('avg_score', 0),
                    'queue_size': self.operation_queue.qsize(),
                    'ws_connections': len(self.websocket.connections),
                    'cache': cache_stats,
                    # NEW v13.0: Advanced health metrics
                    'ml_ready': self.ml_ready,
                    'healing_stats': self.self_healing_manager.get_healing_statistics(),
                    'maintenance_stats': self.predictive_maintenance_manager.get_maintenance_report(),
                    'sustainability': sustainability,
                    'timestamp': datetime.now().isoformat()
                }
            
            return await asyncio.wait_for(_check(), timeout=HEALTH_CHECK_TIMEOUT)
            
        except asyncio.TimeoutError:
            logger.error("Health check timed out")
            return {'healthy': False, 'status': 'timeout', 'instance_id': self.instance_id}
    
    async def get_statistics(self) -> Dict:
        """Enhanced statistics with v13.0 components"""
        async with self._results_lock:
            result_count = len(self.test_results)
            
            if result_count > 0:
                passed = sum(1 for r in self.test_results.values() if r.passed)
                success_rate = passed / result_count
                avg_duration = np.mean([r.duration_ms for r in self.test_results.values()])
                avg_carbon = np.mean([r.carbon_impact_kg for r in self.test_results.values()])
            else:
                success_rate = 0
                avg_duration = 0
                avg_carbon = 0
        
        quality_stats = await self.quality_scorer.get_statistics()
        cache_stats = await self.cache.get_stats()
        sustainability = await self.sustainability_dashboard.get_dashboard_status(
            self.carbon_manager, self.helium_tracker, self
        )
        
        return {
            'instance_id': self.instance_id,
            'version': DATA_VERSION,
            'result_count': result_count,
            'success_rate': success_rate,
            'avg_duration_ms': avg_duration,
            'avg_carbon_impact_kg': avg_carbon,
            'data_quality': quality_stats,
            'cache': cache_stats,
            'queue_size': self.operation_queue.qsize(),
            'ws_connections': len(self.websocket.connections),
            # NEW v13.0: Advanced statistics
            'ml_ready': self.ml_ready,
            'healing_stats': self.self_healing_manager.get_healing_statistics(),
            'maintenance_stats': self.predictive_maintenance_manager.get_maintenance_report(),
            'federated_stats': self.federated_learner.get_federated_stats(),
            'sustainability': sustainability,
            'timestamp': datetime.now().isoformat()
        }
    
    async def shutdown(self):
        """Clean shutdown with v13.0 enhancements"""
        logger.info(f"Shutting down EnhancedTestEnvironmentV13 (instance: {self.instance_id})")
        
        self._shutdown_event.set()
        self._running = False
        
        # Generate final analytics report
        try:
            report = await self.analytics_dashboard.generate_report(self)
            logger.info(f"Final analytics report generated: {report['summary']}")
        except Exception as e:
            logger.error(f"Failed to generate final report: {e}")
        
        # Shutdown components
        if self._queue_worker:
            self._queue_worker.cancel()
            try:
                await self._queue_worker
            except asyncio.CancelledError:
                pass
        
        for task in self.background_tasks:
            task.cancel()
        
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
        
        await self.websocket.stop()
        await self.cache.stop()
        await self.carbon_manager.close()
        await self.federated_learner.close()
        self.db_manager.dispose()
        self.thread_pool.shutdown(wait=True)
        
        # Final sustainability report
        final_status = await self.sustainability_dashboard.get_dashboard_status(
            self.carbon_manager, self.helium_tracker, self
        )
        logger.info(f"Final sustainability score: {final_status['sustainability_score']:.2f}")
        
        logger.info("Shutdown complete")

# ============================================================
# SINGLETON ACCESSOR
# ============================================================

_test_environment_instance = None
_test_environment_lock = asyncio.Lock()

async def get_test_environment() -> EnhancedTestEnvironmentV13:
    global _test_environment_instance
    if _test_environment_instance is None:
        async with _test_environment_lock:
            if _test_environment_instance is None:
                _test_environment_instance = EnhancedTestEnvironmentV13()
                await _test_environment_instance.start()
    return _test_environment_instance

# ============================================================
# MAIN ENTRY POINT
# ============================================================

async def main():
    print("=" * 80)
    print("Enhanced Test Integration v13.0 - Enterprise Platinum+")
    print("Intelligent Selection | ML Root Cause | Self-Healing | Predictive Maintenance")
    print("=" * 80)
    
    test_env = await get_test_environment()
    
    print(f"\n✅ v13.0 ADVANCED INTELLIGENCE FEATURES:")
    print(f"   ✅ Intelligent Test Selection & Impact Analysis")
    print(f"   ✅ ML-Based Root Cause Analysis")
    print(f"   ✅ Self-Healing Test Capabilities")
    print(f"   ✅ Predictive Test Maintenance")
    print(f"   ✅ Enhanced Analytics Dashboard")
    
    print(f"\n📊 Testing New Features:")
    
    # 1. Test impact analysis
    print("\n🔍 Testing Impact Analysis:")
    changed_files = ["src/module.py", "src/another_module.py"]
    impact = await test_env.analyze_test_impact(changed_files)
    print(f"   Impacted tests: {impact['total_impacted']}")
    
    # 2. Root cause analysis (simulated)
    print("\n🧠 Testing Root Cause Analysis:")
    root_cause = await test_env.analyze_failure_root_cause(
        "test_example",
        "AssertionError: Expected 5 got 3\nTimeout after 30s",
        {'memory_usage_mb': 1024, 'cpu_usage_pct': 85}
    )
    print(f"   Root cause: {root_cause.get('root_cause')}")
    print(f"   Confidence: {root_cause.get('confidence'):.2f}")
    
    # 3. Self-healing
    print("\n🔄 Testing Self-Healing:")
    healing = await test_env.self_healing_manager.heal_test(
        "test_example", "timeout", {'system_load': 0.9, 'original_timeout': 30}
    )
    print(f"   Healing applied: {healing.get('healing_applied')}")
    
    # 4. Predictive maintenance
    print("\n📈 Testing Predictive Maintenance:")
    maintenance_report = await test_env.get_predictive_maintenance_report()
    print(f"   Tests tracked: {maintenance_report['total_tests_tracked']}")
    print(f"   Upcoming maintenance: {maintenance_report['upcoming_maintenance']}")
    
    # 5. Analytics dashboard
    print("\n📊 Testing Analytics Dashboard:")
    analytics = await test_env.get_comprehensive_analytics()
    print(f"   Total tests: {analytics['test_metrics']['total_tests']}")
    print(f"   Success rate: {analytics['test_metrics']['success_rate']*100:.1f}%")
    print(f"   Sustainability score: {analytics['sustainability_analytics']['sustainability_score']:.2f}")
    
    print("\n🌐 Dashboard available at: http://localhost:8779")
    print("\nPress Ctrl+C to stop...")
    
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        await test_env.shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Graceful shutdown complete")
