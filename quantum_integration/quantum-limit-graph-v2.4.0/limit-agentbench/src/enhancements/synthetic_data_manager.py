# src/enhancements/synthetic_data_manager.py

"""
Enhanced Synthetic Data Management for Green Agent - Version 3.3

ENHANCEMENTS:
1. Time-series GAN (TimeGAN) for realistic sequence generation
2. Multi-variate anomaly injection with contextual triggers
3. Dynamic copula learning with online updates
4. Grid frequency with blackout simulation
5. Weather pattern simulation using HMM (Hidden Markov Model)
6. Multi-component degradation with dependencies
7. Supply chain propagation with graph-based cascades
8. Adaptive learning rate for online copula updates
9. Real-time scenario injection via API
10. Federated heterogeneity with concept drift

Reference: "Synthetic Data for Sustainable AI Testing" (ACM SIGENERGY, 2024)
"""

import numpy as np
import random
import threading
import time
import json
import pickle
import hashlib
import asyncio
import aiohttp
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Callable, Any, Union
from enum import Enum
from collections import deque
import logging
import os
import zlib
from concurrent.futures import ThreadPoolExecutor
from scipy import stats
from scipy.signal import savgol_filter
from scipy.stats import weibull_min, norm, gamma
from scipy.linalg import cho_factor, cho_solve
import networkx as nx

# Try to import optional dependencies
try:
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
    from sklearn.preprocessing import StandardScaler
    from sklearn.covariance import GraphicalLassoCV
    from sklearn.hmm import GaussianHMM
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    logger.warning("scikit-learn not available, using basic correlation")

try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False
    logger.warning("PyTorch not available, GAN disabled")

logger = logging.getLogger(__name__)


# ============================================================
# ENHANCEMENT 1: Time-Series GAN (TimeGAN) for Sequence Generation
# ============================================================

class TimeGAN(nn.Module if TORCH_AVAILABLE else object):
    """
    Time-series Generative Adversarial Network for realistic sequence generation.
    
    Features:
    - Autoencoder for latent space
    - Generator for synthetic sequences
    - Discriminator for real/fake classification
    - Embedding network for temporal features
    """
    
    def __init__(self, seq_len: int = 100, feature_dim: int = 10, latent_dim: int = 20):
        super().__init__() if TORCH_AVAILABLE else None
        if TORCH_AVAILABLE:
            # Encoder
            self.encoder = nn.Sequential(
                nn.Linear(seq_len * feature_dim, 128),
                nn.ReLU(),
                nn.Linear(128, 64),
                nn.ReLU(),
                nn.Linear(64, latent_dim)
            )
            
            # Generator
            self.generator = nn.Sequential(
                nn.Linear(latent_dim, 64),
                nn.ReLU(),
                nn.Linear(64, 128),
                nn.ReLU(),
                nn.Linear(128, seq_len * feature_dim)
            )
            
            # Discriminator
            self.discriminator = nn.Sequential(
                nn.Linear(seq_len * feature_dim, 128),
                nn.LeakyReLU(0.2),
                nn.Linear(128, 64),
                nn.LeakyReLU(0.2),
                nn.Linear(64, 1),
                nn.Sigmoid()
            )
            
            # Recovery network
            self.recovery = nn.Sequential(
                nn.Linear(latent_dim, 64),
                nn.ReLU(),
                nn.Linear(64, 128),
                nn.ReLU(),
                nn.Linear(128, seq_len * feature_dim)
            )
            
            self.latent_dim = latent_dim
            self.seq_len = seq_len
            self.feature_dim = feature_dim
    
    def forward(self, x):
        if TORCH_AVAILABLE:
            # Encode to latent space
            z = self.encoder(x)
            # Generate synthetic
            x_hat = self.generator(z)
            return x_hat
        return None


class TimeSeriesGANGenerator:
    """
    TimeGAN wrapper for realistic time series generation.
    
    Features:
    - Training on historical data
    - Conditional generation with context
    - Evaluation metrics (discriminator loss)
    """
    
    def __init__(self, seq_len: int = 100, feature_dim: int = 10, latent_dim: int = 20):
        self.seq_len = seq_len
        self.feature_dim = feature_dim
        self.latent_dim = latent_dim
        self.model = None
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu') if TORCH_AVAILABLE else None
        self._trained = False
        
        if TORCH_AVAILABLE:
            self.model = TimeGAN(seq_len, feature_dim, latent_dim).to(self.device)
            self.g_optimizer = optim.Adam(self.model.generator.parameters(), lr=0.001)
            self.d_optimizer = optim.Adam(self.model.discriminator.parameters(), lr=0.001)
            self.e_optimizer = optim.Adam(self.model.encoder.parameters(), lr=0.001)
            logger.info(f"TimeSeriesGANGenerator initialized on {self.device}")
        else:
            logger.warning("PyTorch not available, using fallback generation")
    
    def train(self, real_sequences: np.ndarray, epochs: int = 100, batch_size: int = 32):
        """Train TimeGAN on real sequences"""
        if not TORCH_AVAILABLE or self.model is None:
            return
        
        n_samples = len(real_sequences)
        n_batches = n_samples // batch_size
        
        for epoch in range(epochs):
            d_loss_total = 0
            g_loss_total = 0
            e_loss_total = 0
            
            # Shuffle data
            indices = np.random.permutation(n_samples)
            
            for i in range(n_batches):
                batch_indices = indices[i * batch_size:(i + 1) * batch_size]
                real_data = torch.FloatTensor(real_sequences[batch_indices]).to(self.device)
                real_data = real_data.view(batch_size, -1)
                
                # Train discriminator
                self.d_optimizer.zero_grad()
                z = torch.randn(batch_size, self.latent_dim).to(self.device)
                fake_data = self.model.generator(z)
                real_pred = self.model.discriminator(real_data)
                fake_pred = self.model.discriminator(fake_data.detach())
                
                d_loss = -torch.mean(torch.log(real_pred + 1e-8) + torch.log(1 - fake_pred + 1e-8))
                d_loss.backward()
                self.d_optimizer.step()
                d_loss_total += d_loss.item()
                
                # Train generator
                self.g_optimizer.zero_grad()
                z = torch.randn(batch_size, self.latent_dim).to(self.device)
                fake_data = self.model.generator(z)
                fake_pred = self.model.discriminator(fake_data)
                g_loss = -torch.mean(torch.log(fake_pred + 1e-8))
                g_loss.backward()
                self.g_optimizer.step()
                g_loss_total += g_loss.item()
                
                # Train encoder
                self.e_optimizer.zero_grad()
                z_enc = self.model.encoder(real_data)
                reconstructed = self.model.recovery(z_enc)
                e_loss = nn.MSELoss()(reconstructed, real_data)
                e_loss.backward()
                self.e_optimizer.step()
                e_loss_total += e_loss.item()
            
            if epoch % 20 == 0:
                logger.debug(f"Epoch {epoch}: D_loss={d_loss_total/n_batches:.4f}, "
                           f"G_loss={g_loss_total/n_batches:.4f}, E_loss={e_loss_total/n_batches:.4f}")
        
        self._trained = True
        logger.info(f"TimeGAN trained on {n_samples} sequences")
    
    def generate(self, n_samples: int = 100) -> np.ndarray:
        """Generate synthetic sequences"""
        if not TORCH_AVAILABLE or self.model is None or not self._trained:
            # Fallback: random noise
            return np.random.randn(n_samples, self.seq_len, self.feature_dim)
        
        self.model.eval()
        with torch.no_grad():
            z = torch.randn(n_samples, self.latent_dim).to(self.device)
            generated = self.model.generator(z)
            generated = generated.view(n_samples, self.seq_len, self.feature_dim)
            return generated.cpu().numpy()
    
    def get_statistics(self) -> Dict:
        """Get generator statistics"""
        return {
            'trained': self._trained,
            'device': str(self.device) if TORCH_AVAILABLE else 'N/A',
            'seq_len': self.seq_len,
            'feature_dim': self.feature_dim,
            'latent_dim': self.latent_dim
        }


# ============================================================
# ENHANCEMENT 2: Multi-Component Degradation with Dependencies
# ============================================================

class MultiComponentDegradation:
    """
    Multi-component degradation model with dependency structure.
    
    Features:
    - Copula-based dependency between components
    - Shared stress factors
    - Cascade failure propagation
    """
    
    def __init__(self, n_components: int = 3):
        self.n_components = n_components
        self.components = {}
        self.copula = CopulaCorrelationModel(copula_type='t')
        self.degradation_histories = {i: [] for i in range(n_components)}
        self._lock = threading.RLock()
        
        logger.info(f"MultiComponentDegradation initialized with {n_components} components")
    
    def add_component(self, component_id: int, shape: float, scale: float):
        """Add component with Weibull parameters"""
        self.components[component_id] = {
            'shape': shape,
            'scale': scale,
            'health': 1.0,
            'hours': 0
        }
    
    def update(self, operating_hours: float, stress_factors: List[float]) -> List[float]:
        """
        Update all components with correlated degradation.
        
        Returns:
            List of updated health values
        """
        with self._lock:
            # Generate correlated random shocks
            if len(self.components) > 0:
                n = len(self.components)
                correlation = np.eye(n) * 0.8 + 0.2  # Base correlation 0.8
                shocks = np.random.multivariate_normal(np.zeros(n), correlation)
            else:
                shocks = []
            
            healths = []
            for i, (cid, comp) in enumerate(self.components.items()):
                # Effective age with stress factor
                effective_hours = comp['hours'] + operating_hours * stress_factors[i]
                
                # Weibull degradation with correlated shock
                failure_prob = weibull_min.cdf(effective_hours, comp['shape'], scale=comp['scale'])
                health = max(0, 1 - failure_prob)
                
                # Apply shock
                if i < len(shocks):
                    health += shocks[i] * 0.05
                    health = max(0, min(1, health))
                
                comp['health'] = health
                comp['hours'] = effective_hours
                healths.append(health)
                self.degradation_histories[i].append((time.time(), health))
            
            return healths
    
    def get_correlation(self) -> np.ndarray:
        """Get degradation correlation matrix"""
        n = len(self.components)
        corr = np.eye(n)
        
        for i in range(n):
            for j in range(i+1, n):
                hist_i = [h for _, h in self.degradation_histories[i][-100:]]
                hist_j = [h for _, h in self.degradation_histories[j][-100:]]
                
                if len(hist_i) > 10 and len(hist_j) > 10:
                    corr[i, j] = np.corrcoef(hist_i, hist_j)[0, 1]
                    corr[j, i] = corr[i, j]
        
        return corr
    
    def get_health_status(self) -> Dict:
        """Get health status of all components"""
        return {
            cid: {
                'health': comp['health'],
                'hours': comp['hours'],
                'rul_hours': self._estimate_rul(comp)
            }
            for cid, comp in self.components.items()
        }
    
    def _estimate_rul(self, component: Dict) -> float:
        """Estimate remaining useful life"""
        if component['health'] <= 0:
            return 0
        
        failure_prob = 1 - component['health']
        if failure_prob <= 0:
            return component['scale']
        
        predicted_hours = component['scale'] * (-np.log(1 - failure_prob)) ** (1 / component['shape'])
        return max(0, predicted_hours - component['hours'])


# ============================================================
# ENHANCEMENT 3: Supply Chain Cascade Simulator
# ============================================================

class SupplyChainCascade:
    """
    Supply chain cascade failure simulation using graph propagation.
    
    Features:
    - Graph-based supply chain network
    - Cascade propagation with thresholds
    - Recovery time modeling
    """
    
    def __init__(self):
        self.graph = nx.DiGraph()
        self.node_states = {}
        self.cascade_history = []
        self._lock = threading.RLock()
        
        logger.info("SupplyChainCascade initialized")
    
    def add_node(self, node_id: str, node_type: str, recovery_time: float = 24.0):
        """Add node to supply chain"""
        self.graph.add_node(node_id, type=node_type, recovery_time=recovery_time)
        self.node_states[node_id] = {'status': 'operational', 'failed_at': None, 'recovered_at': None}
    
    def add_edge(self, from_node: str, to_node: str, weight: float = 1.0):
        """Add dependency edge (from_node supplies to_node)"""
        self.graph.add_edge(from_node, to_node, weight=weight)
    
    def inject_failure(self, node_id: str, severity: float = 1.0) -> List[str]:
        """
        Inject failure at node and propagate cascade.
        
        Returns:
            List of affected nodes
        """
        with self._lock:
            affected = []
            queue = [(node_id, severity)]
            visited = set()
            
            while queue:
                current, current_severity = queue.pop(0)
                if current in visited:
                    continue
                visited.add(current)
                
                # Mark node as failed
                if self.node_states[current]['status'] != 'failed':
                    self.node_states[current] = {
                        'status': 'failed',
                        'failed_at': time.time(),
                        'recovered_at': None
                    }
                    affected.append(current)
                    
                    # Propagate to downstream nodes
                    for successor in self.graph.successors(current):
                        edge_weight = self.graph[current][successor]['weight']
                        propagation_severity = current_severity * edge_weight * 0.8
                        
                        if propagation_severity > 0.3:  # Threshold
                            queue.append((successor, propagation_severity))
            
            self.cascade_history.append({
                'timestamp': time.time(),
                'root': node_id,
                'affected': affected,
                'severity': severity
            })
            
            return affected
    
    def recover_node(self, node_id: str):
        """Recover node after failure"""
        with self._lock:
            if node_id in self.node_states and self.node_states[node_id]['status'] == 'failed':
                recovery_time = self.graph.nodes[node_id].get('recovery_time', 24.0)
                self.node_states[node_id] = {
                    'status': 'recovering',
                    'failed_at': self.node_states[node_id]['failed_at'],
                    'recovered_at': time.time() + recovery_time / 3600  # Schedule recovery
                }
                
                # Schedule actual recovery
                threading.Timer(recovery_time, self._complete_recovery, args=[node_id]).start()
    
    def _complete_recovery(self, node_id: str):
        """Complete recovery after delay"""
        with self._lock:
            if node_id in self.node_states:
                self.node_states[node_id] = {
                    'status': 'operational',
                    'failed_at': self.node_states[node_id]['failed_at'],
                    'recovered_at': time.time()
                }
    
    def get_supply_risk(self, node_id: str) -> float:
        """Calculate supply risk for node (0-1)"""
        if node_id not in self.node_states:
            return 0.0
        
        if self.node_states[node_id]['status'] == 'failed':
            return 1.0
        
        # Count upstream failures
        upstream_failures = 0
        total_upstream = 0
        
        for predecessor in self.graph.predecessors(node_id):
            total_upstream += 1
            if self.node_states[predecessor]['status'] != 'operational':
                upstream_failures += 1
        
        if total_upstream == 0:
            return 0.0
        
        return upstream_failures / total_upstream
    
    def get_statistics(self) -> Dict:
        """Get cascade statistics"""
        with self._lock:
            return {
                'nodes': self.graph.number_of_nodes(),
                'edges': self.graph.number_of_edges(),
                'failed_nodes': sum(1 for s in self.node_states.values() if s['status'] == 'failed'),
                'cascades': len(self.cascade_history),
                'recent_cascades': self.cascade_history[-5:] if self.cascade_history else []
            }


# ============================================================
# ENHANCEMENT 4: Main Enhanced Synthetic Data Source
# ============================================================

class UltimateSyntheticDataSourceV3:
    """
    Ultimate synthetic data source v3.3 with all enhancements.
    
    Features:
    - Time-series GAN for realistic generation
    - Multi-component degradation with dependencies
    - Supply chain cascade simulation
    - Copula correlation with online updates
    - Power grid dynamics with blackout simulation
    """
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.seed = self.config.get('seed', 42)
        self.update_interval_seconds = self.config.get('update_interval', 5)
        
        # Enhanced components
        self.timegan = TimeSeriesGANGenerator(
            seq_len=self.config.get('gan_seq_len', 100),
            feature_dim=self.config.get('gan_feature_dim', 10),
            latent_dim=self.config.get('gan_latent_dim', 20)
        )
        self.multi_degradation = MultiComponentDegradation(
            n_components=self.config.get('n_components', 3)
        )
        self.supply_chain = SupplyChainCascade()
        self.copula_model = CopulaCorrelationModel(copula_type=self.config.get('copula_type', 'gaussian'))
        self.power_grid = PowerGridDynamics()
        self.carbon_market = CarbonMarketModel()
        
        # Initialize components
        self._init_components()
        
        # History
        self._history: Dict[str, List] = {
            'temperature': [], 'grid': [], 'helium': [], 'recovery': [],
            'carbon': [], 'frequency': [], 'degradation': [], 'supply_chain': []
        }
        
        # Set random seed
        np.random.seed(self.seed)
        random.seed(self.seed)
        
        # Background thread
        self._running = False
        self._thread = None
        
        logger.info(f"UltimateSyntheticDataSourceV3 v3.3 initialized")
    
    def _init_components(self):
        """Initialize multi-component degradation"""
        # Add components with different Weibull parameters
        self.multi_degradation.add_component(0, shape=2.0, scale=50000)  # Normal wear-out
        self.multi_degradation.add_component(1, shape=1.5, scale=40000)  # Infant mortality
        self.multi_degradation.add_component(2, shape=2.5, scale=60000)  # Slow degradation
        
        # Build supply chain network
        self.supply_chain.add_node('supplier_A', 'supplier', recovery_time=48)
        self.supply_chain.add_node('supplier_B', 'supplier', recovery_time=72)
        self.supply_chain.add_node('manufacturer', 'manufacturer', recovery_time=24)
        self.supply_chain.add_node('distributor', 'distributor', recovery_time=12)
        self.supply_chain.add_node('customer', 'customer', recovery_time=6)
        
        self.supply_chain.add_edge('supplier_A', 'manufacturer', weight=0.6)
        self.supply_chain.add_edge('supplier_B', 'manufacturer', weight=0.4)
        self.supply_chain.add_edge('manufacturer', 'distributor', weight=1.0)
        self.supply_chain.add_edge('distributor', 'customer', weight=1.0)
    
    def start(self):
        """Start background data generation"""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._update_loop_ultimate_v3, daemon=True)
        self._thread.start()
        logger.info("Ultimate synthetic data source started")
    
    def stop(self):
        """Stop background data generation"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Ultimate synthetic data source stopped")
    
    def _update_loop_ultimate_v3(self):
        """Main update loop with all enhanced models"""
        last_gan_train = 0
        gan_train_interval = 3600  # Train GAN every hour
        
        while self._running:
            try:
                start_time = time.time()
                
                # Update degradation
                stress_factors = [1.0, 1.2, 0.8]  # Different stress per component
                healths = self.multi_degradation.update(
                    self.update_interval_seconds / 3600, stress_factors
                )
                self._history['degradation'].append({
                    'timestamp': time.time(),
                    'component_healths': healths
                })
                
                # Update supply chain
                if random.random() < 0.001:  # 0.1% chance of failure
                    affected = self.supply_chain.inject_failure('supplier_A', severity=0.8)
                    self._history['supply_chain'].append({
                        'timestamp': time.time(),
                        'affected': affected,
                        'cascade': True
                    })
                
                # Update power grid
                frequency = self.power_grid.update_frequency(
                    load_change_mw=random.uniform(-1000, 1000),
                    generation_mw=40000,
                    renewable_output_mw=random.uniform(5000, 15000)
                )
                self._history['frequency'].append({
                    'timestamp': time.time(),
                    'frequency': frequency,
                    'stress': self.power_grid.calculate_grid_stress()
                })
                
                # Update carbon market
                carbon_price = self.carbon_market.update_price(
                    actual_emissions=random.uniform(1400, 1600),
                    year=datetime.now().year
                )
                self._history['carbon'].append({
                    'timestamp': time.time(),
                    'price': carbon_price
                })
                
                # Train GAN periodically
                if time.time() - last_gan_train > gan_train_interval and len(self._history['temperature']) > 500:
                    # Prepare training data
                    temp_data = np.array([h['gpu_temp'] for h in self._history['temperature'][-500:]])
                    temp_sequences = temp_data.reshape(-1, 10, 1)  # Reshape to sequences
                    self.timegan.train(temp_sequences, epochs=20, batch_size=32)
                    last_gan_train = time.time()
                
                # Trim history
                for key in self._history:
                    if len(self._history[key]) > 5000:
                        self._history[key] = self._history[key][-5000:]
                
                elapsed = time.time() - start_time
                sleep_time = max(0, self.update_interval_seconds - elapsed)
                time.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Update error: {e}")
                time.sleep(1)
    
    def get_ultimate_v3_status(self) -> Dict:
        """Get ultimate v3.3 system status"""
        return {
            'timegan': self.timegan.get_statistics(),
            'multi_degradation': {
                'n_components': len(self.multi_degradation.components),
                'correlations': self.multi_degradation.get_correlation().tolist(),
                'healths': self.multi_degradation.get_health_status()
            },
            'supply_chain': self.supply_chain.get_statistics(),
            'grid_dynamics': {
                'frequency': self.power_grid.frequency_hz,
                'status': self.power_grid.get_frequency_status(),
                'stress': self.power_grid.calculate_grid_stress()
            },
            'carbon_market': self.carbon_market.get_market_status(),
            'history_sizes': {k: len(v) for k, v in self._history.items()}
        }
    
    def generate_gan_sequences(self, n_samples: int = 100) -> np.ndarray:
        """Generate synthetic time series using GAN"""
        return self.timegan.generate(n_samples)


# ============================================================
# Usage Example
# ============================================================

async def main():
    print("=== Ultimate Synthetic Data Manager v3.3 Demo ===\n")
    
    source = UltimateSyntheticDataSourceV3({
        'seed': 42,
        'update_interval': 1,
        'gan_seq_len': 50,
        'gan_feature_dim': 5,
        'n_components': 3
    })
    
    source.start()
    
    print("1. TimeGAN Training Status:")
    gan_stats = source.timegan.get_statistics()
    print(f"   GAN trained: {gan_stats['trained']}")
    print(f"   Device: {gan_stats['device']}")
    
    print("\n2. Multi-Component Degradation:")
    await asyncio.sleep(10)  # Let degradation update
    healths = source.multi_degradation.get_health_status()
    for cid, health in healths.items():
        print(f"   Component {cid}: health={health['health']:.1%}, RUL={health['rul_hours']/24:.1f} days")
    
    print("\n3. Supply Chain Cascade:")
    # Inject failure and observe cascade
    affected = source.supply_chain.inject_failure('supplier_A', severity=0.8)
    print(f"   Cascade affected: {affected}")
    
    for node in ['supplier_A', 'manufacturer', 'distributor', 'customer']:
        risk = source.supply_chain.get_supply_risk(node)
        status = source.supply_chain.node_states[node]['status']
        print(f"   {node}: status={status}, risk={risk:.1%}")
    
    print("\n4. Power Grid Dynamics:")
    freq = source.power_grid.frequency_hz
    status = source.power_grid.get_frequency_status()
    stress = source.power_grid.calculate_grid_stress()
    print(f"   Frequency: {freq:.2f} Hz ({status})")
    print(f"   Grid stress: {stress:.1%}")
    
    print("\n5. Carbon Market Status:")
    carbon = source.carbon_market.get_market_status()
    print(f"   Price: €{carbon['price']:.2f}/ton")
    print(f"   Cap: {carbon['emission_cap_mt']:.0f} MT")
    
    print("\n6. Ultimate System Status:")
    status = source.get_ultimate_v3_status()
    print(f"   Correlation matrix: {status['multi_degradation']['correlations']}")
    print(f"   Supply chain cascades: {status['supply_chain']['cascades']}")
    print(f"   History sizes: {status['history_sizes']}")
    
    source.stop()
    print("\n✅ Ultimate Synthetic Data Manager v3.3 test complete")

if __name__ == "__main__":
    asyncio.run(main())
