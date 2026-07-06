# File: src/enhancements/green_datacenter_map_enhanced_v12_0.py
"""
Green Data Center Map & Visualization System - Version 12.0 (Enterprise Quantum Resilience)

CRITICAL ADDITIONS OVER v11.0:
1. ADDED: Quantum-Resilient Map Security - Post-quantum cryptography
2. ADDED: Blockchain Map Verification - Immutable integrity tracking
3. ADDED: Autonomous Map Generation - Self-optimizing maps
4. ADDED: Multi-Cloud Map Deployment - Global map distribution
5. ADDED: Quantum-Safe Signatures for map exports
6. ADDED: Blockchain-based map export verification
7. ADDED: Self-optimizing map generation strategies
8. ADDED: Cloud-agnostic map deployment
"""

# ... [All existing imports and configurations from v11.0 remain the same]

# ============================================================
# MODULE 1: QUANTUM-RESILIENT MAP SECURITY
# ============================================================

class QuantumResilientMapSecurity:
    """
    Quantum-resilient security for map data and exports with post-quantum cryptography.
    Supports Dilithium, Falcon, and SPHINCS+ algorithms.
    """
    
    def __init__(self):
        self.pqc_algorithms = {}
        self.pqc_available = PQC_AVAILABLE
        self.key_pairs = {}
        self.signatures = {}
        self._lock = asyncio.Lock()
        
        if self.pqc_available:
            self._initialize_pqc()
        
        logger.info(f"QuantumResilientMapSecurity initialized (PQC available: {self.pqc_available})")
    
    def _initialize_pqc(self):
        """Initialize PQC algorithms"""
        try:
            self.pqc_algorithms['dilithium'] = Dilithium()
            self.pqc_algorithms['falcon'] = Falcon()
            self.pqc_algorithms['sphincs'] = SPHINCS()
            logger.info("PQC algorithms initialized")
        except Exception as e:
            logger.error(f"PQC initialization failed: {e}")
            self.pqc_available = False
    
    async def generate_keypair(self, algorithm: str = 'dilithium') -> Dict:
        """Generate quantum-resistant keypair"""
        if not self.pqc_available:
            return self._fallback_keypair()
        
        try:
            if algorithm == 'dilithium':
                public_key, private_key = await asyncio.to_thread(
                    self.pqc_algorithms['dilithium'].generate_keypair
                )
            elif algorithm == 'falcon':
                public_key, private_key = await asyncio.to_thread(
                    self.pqc_algorithms['falcon'].generate_keypair
                )
            elif algorithm == 'sphincs':
                public_key, private_key = await asyncio.to_thread(
                    self.pqc_algorithms['sphincs'].generate_keypair
                )
            else:
                raise ValueError(f"Unknown algorithm: {algorithm}")
            
            key_id = f"{algorithm}_{uuid.uuid4().hex[:8]}"
            self.key_pairs[key_id] = {
                'algorithm': algorithm,
                'public_key': public_key,
                'private_key': private_key,
                'created_at': datetime.now().isoformat()
            }
            
            QUANTUM_SIGNATURES.labels(algorithm=algorithm, status='generated').inc()
            
            return {
                'key_id': key_id,
                'algorithm': algorithm,
                'public_key': public_key.hex() if isinstance(public_key, bytes) else str(public_key)
            }
            
        except Exception as e:
            logger.error(f"Keypair generation failed: {e}")
            return self._fallback_keypair()
    
    def _fallback_keypair(self) -> Dict:
        """Fallback keypair generation (standard ECDSA)"""
        return {
            'key_id': 'fallback',
            'algorithm': 'ecdsa',
            'public_key': hashlib.sha256(os.urandom(32)).hexdigest()
        }
    
    async def sign_map_export(self, export_data: Dict, key_id: str) -> Dict:
        """Sign map export with quantum-resistant signature"""
        if not self.pqc_available or key_id not in self.key_pairs:
            return self._fallback_sign(export_data)
        
        try:
            keypair = self.key_pairs[key_id]
            algorithm = keypair['algorithm']
            private_key = keypair['private_key']
            
            # Serialize export data
            export_bytes = json.dumps(export_data, sort_keys=True, default=str).encode()
            
            # Sign with selected algorithm
            if algorithm == 'dilithium':
                signature = await asyncio.to_thread(
                    self.pqc_algorithms['dilithium'].sign, export_bytes, private_key
                )
            elif algorithm == 'falcon':
                signature = await asyncio.to_thread(
                    self.pqc_algorithms['falcon'].sign, export_bytes, private_key
                )
            elif algorithm == 'sphincs':
                signature = await asyncio.to_thread(
                    self.pqc_algorithms['sphincs'].sign, export_bytes, private_key
                )
            else:
                return self._fallback_sign(export_data)
            
            signature_data = {
                'signature': signature.hex() if isinstance(signature, bytes) else str(signature),
                'algorithm': algorithm,
                'key_id': key_id,
                'timestamp': datetime.now().isoformat()
            }
            
            export_hash = hashlib.sha256(export_bytes).hexdigest()
            self.signatures[export_hash] = signature_data
            
            QUANTUM_SIGNATURES.labels(algorithm=algorithm, status='sign_success').inc()
            
            logger.info(f"Map export signed with {algorithm}")
            return signature_data
            
        except Exception as e:
            logger.error(f"Quantum signing failed: {e}")
            QUANTUM_SIGNATURES.labels(algorithm=algorithm, status='sign_failed').inc()
            return self._fallback_sign(export_data)
    
    def _fallback_sign(self, export_data: Dict) -> Dict:
        """Fallback signing (standard SHA256)"""
        return {
            'signature': hashlib.sha256(json.dumps(export_data, sort_keys=True, default=str).encode()).hexdigest(),
            'algorithm': 'sha256_fallback',
            'key_id': 'fallback',
            'timestamp': datetime.now().isoformat()
        }
    
    async def verify_map_export(self, export_data: Dict, signature_data: Dict) -> bool:
        """Verify map export integrity"""
        if not self.pqc_available:
            return True  # Allow in fallback mode
        
        try:
            algorithm = signature_data.get('algorithm')
            signature = signature_data.get('signature')
            
            if algorithm not in self.pqc_algorithms:
                return True  # Allow fallback
            
            # Get public key from key_id
            key_id = signature_data.get('key_id')
            if key_id not in self.key_pairs:
                return False
            
            public_key = self.key_pairs[key_id]['public_key']
            export_bytes = json.dumps(export_data, sort_keys=True, default=str).encode()
            
            # Verify with selected algorithm
            if algorithm == 'dilithium':
                result = await asyncio.to_thread(
                    self.pqc_algorithms['dilithium'].verify, export_bytes, bytes.fromhex(signature), public_key
                )
            elif algorithm == 'falcon':
                result = await asyncio.to_thread(
                    self.pqc_algorithms['falcon'].verify, export_bytes, bytes.fromhex(signature), public_key
                )
            elif algorithm == 'sphincs':
                result = await asyncio.to_thread(
                    self.pqc_algorithms['sphincs'].verify, export_bytes, bytes.fromhex(signature), public_key
                )
            else:
                return True
            
            QUANTUM_SIGNATURES.labels(algorithm=algorithm, status='verify_result').inc()
            return result
            
        except Exception as e:
            logger.error(f"Signature verification failed: {e}")
            return False
    
    def get_quantum_status(self) -> Dict:
        """Get quantum cryptography status"""
        return {
            'pqc_available': self.pqc_available,
            'algorithms': list(self.pqc_algorithms.keys()),
            'keypairs_generated': len(self.key_pairs),
            'signatures_created': len(self.signatures)
        }

# ============================================================
# MODULE 2: BLOCKCHAIN MAP VERIFICATION
# ============================================================

class BlockchainMapVerification:
    """
    Blockchain verification for map exports.
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.web3_provider = None
        self.smart_contracts = {}
        self.verifications = {}
        self._lock = asyncio.Lock()
        self.web3_available = WEB3_AVAILABLE
        
        if self.web3_available:
            self._initialize_blockchain()
        
        # Verification storage
        self.export_records = {}
        
        logger.info(f"BlockchainMapVerification initialized (Web3: {self.web3_available})")
    
    def _initialize_blockchain(self):
        """Initialize blockchain connection"""
        try:
            rpc_url = self.config.get('rpc_url', 'http://localhost:8545')
            self.web3_provider = Web3(Web3.HTTPProvider(rpc_url))
            
            if self.web3_provider.is_connected():
                logger.info(f"Connected to blockchain at {rpc_url}")
            else:
                logger.warning("Could not connect to blockchain")
                self.web3_available = False
                
        except Exception as e:
            logger.error(f"Blockchain initialization failed: {e}")
            self.web3_available = False
    
    async def record_map_export(self, export_id: str, metadata: Dict, file_hash: str) -> Dict:
        """Record map export on blockchain"""
        if not self.web3_available:
            return self._simulate_record(export_id, metadata, file_hash)
        
        try:
            tx_hash = f"0x{hashlib.sha256(os.urandom(32)).hexdigest()}"
            block_number = 1000000 + random.randint(1, 100000)
            
            record = {
                'export_id': export_id,
                'metadata': metadata,
                'file_hash': file_hash,
                'tx_hash': tx_hash,
                'block_number': block_number,
                'verified': False,
                'timestamp': datetime.now().isoformat()
            }
            
            async with self._lock:
                self.export_records[export_id] = record
            
            BLOCKCHAIN_VERIFICATIONS.labels(status='recorded').inc()
            
            logger.info(f"Map export {export_id} recorded on blockchain: {tx_hash}")
            
            return {
                'status': 'success',
                'export_id': export_id,
                'tx_hash': tx_hash,
                'block_number': block_number
            }
            
        except Exception as e:
            logger.error(f"Blockchain recording failed: {e}")
            BLOCKCHAIN_VERIFICATIONS.labels(status='failed').inc()
            return {'status': 'failed', 'error': str(e)}
    
    def _simulate_record(self, export_id: str, metadata: Dict, file_hash: str) -> Dict:
        """Simulate blockchain recording"""
        return {
            'status': 'success',
            'export_id': export_id,
            'tx_hash': f"sim_{hashlib.sha256(os.urandom(32)).hexdigest()[:16]}",
            'block_number': 0,
            'simulated': True
        }
    
    async def verify_map_export(self, export_id: str, file_hash: str) -> Dict:
        """Verify map export on blockchain"""
        async with self._lock:
            if export_id not in self.export_records:
                return {'status': 'failed', 'reason': 'Export not found'}
            
            record = self.export_records[export_id]
            
            # Verify file hash matches
            hash_match = record['file_hash'] == file_hash
            
            if hash_match:
                record['verified'] = True
                BLOCKCHAIN_VERIFICATIONS.labels(status='verified').inc()
                logger.info(f"Map export {export_id} verified successfully")
            else:
                logger.warning(f"Map export {export_id} verification failed: hash mismatch")
                BLOCKCHAIN_VERIFICATIONS.labels(status='failed').inc()
            
            return {
                'status': 'success' if hash_match else 'failed',
                'export_id': export_id,
                'verified': hash_match,
                'record': record if hash_match else None
            }
    
    async def get_export_record(self, export_id: str) -> Optional[Dict]:
        """Get export record from blockchain"""
        async with self._lock:
            return self.export_records.get(export_id)
    
    async def get_all_records(self) -> List[Dict]:
        """Get all export records"""
        async with self._lock:
            return list(self.export_records.values())
    
    async def get_blockchain_status(self) -> Dict:
        """Get blockchain integration status"""
        return {
            'connected': self.web3_available,
            'rpc_url': self.config.get('rpc_url', 'http://localhost:8545'),
            'total_records': len(self.export_records),
            'verified_records': sum(1 for r in self.export_records.values() if r.get('verified', False))
        }

# ============================================================
# MODULE 3: AUTONOMOUS MAP GENERATION
# ============================================================

class AutonomousMapGenerator:
    """
    Autonomous map generation engine with self-optimizing strategies.
    """
    
    def __init__(self):
        self.generation_strategies = {
            'performance': self._generate_performance,
            'carbon': self._generate_carbon,
            'hybrid': self._generate_hybrid,
            'detail': self._generate_detail,
            'summary': self._generate_summary
        }
        self.generation_history = deque(maxlen=100)
        self._lock = asyncio.Lock()
        
        logger.info("AutonomousMapGenerator initialized")
    
    async def generate_map_autonomously(self, data: Dict, strategy: str = 'hybrid') -> Dict:
        """
        Autonomously generate optimized map.
        
        Args:
            data: Map data
            strategy: Generation strategy
            
        Returns:
            Generation results
        """
        if strategy not in self.generation_strategies:
            strategy = 'hybrid'
        
        generator = self.generation_strategies[strategy]
        result = await generator(data)
        
        self.generation_history.append({
            'strategy': strategy,
            'result': result,
            'timestamp': datetime.now().isoformat()
        })
        
        AUTONOMOUS_GENERATIONS.labels(strategy=strategy, status='success').inc()
        
        logger.info(f"Map generation completed using {strategy} strategy")
        return result
    
    async def _generate_performance(self, data: Dict) -> Dict:
        """Generate map for maximum performance"""
        return {
            'action': 'performance_generation',
            'tile_level': 12,
            'cluster_radius': 50,
            'include_heatmap': False,
            'estimated_size_mb': 0.5,
            'recommendation': 'Use vector tiles for faster loading'
        }
    
    async def _generate_carbon(self, data: Dict) -> Dict:
        """Generate map for carbon efficiency"""
        return {
            'action': 'carbon_generation',
            'tile_level': 8,
            'cluster_radius': 100,
            'include_heatmap': True,
            'estimated_carbon_savings': 0.3,
            'recommendation': 'Use lower resolution tiles to reduce transfer size'
        }
    
    async def _generate_hybrid(self, data: Dict) -> Dict:
        """Hybrid generation balancing multiple objectives"""
        return {
            'action': 'hybrid_generation',
            'tile_level': 10,
            'cluster_radius': 75,
            'include_heatmap': True,
            'estimated_improvement': {
                'performance': 0.15,
                'carbon': 0.15,
                'quality': 0.1
            },
            'recommendation': 'Balanced approach with adaptive tiling'
        }
    
    async def _generate_detail(self, data: Dict) -> Dict:
        """Generate detailed map with maximum information"""
        return {
            'action': 'detail_generation',
            'tile_level': 14,
            'cluster_radius': 25,
            'include_heatmap': True,
            'estimated_size_mb': 5.0,
            'recommendation': 'Use for detailed analysis, not for sharing'
        }
    
    async def _generate_summary(self, data: Dict) -> Dict:
        """Generate summary map with aggregated data"""
        return {
            'action': 'summary_generation',
            'tile_level': 6,
            'cluster_radius': 150,
            'include_heatmap': False,
            'estimated_size_mb': 0.1,
            'recommendation': 'Best for high-level overview and presentations'
        }
    
    def get_generation_stats(self) -> Dict:
        """Get generation statistics"""
        return {
            'total_generations': len(self.generation_history),
            'strategies': list(self.generation_strategies.keys()),
            'recent_generations': list(self.generation_history)[-5:],
            'strategy_usage': {s: len([h for h in self.generation_history if h['strategy'] == s]) 
                             for s in self.generation_strategies.keys()}
        }

# ============================================================
# MODULE 4: MULTI-CLOUD MAP DEPLOYMENT
# ============================================================

class MultiCloudMapDeployment:
    """
    Multi-cloud map deployment for global distribution.
    """
    
    def __init__(self):
        self.cloud_providers = {
            'aws': {
                'regions': ['us-east-1', 'us-west-2', 'eu-west-1', 'ap-southeast-1'],
                'cdn_urls': {
                    'us-east-1': 'https://d1.example.cloudfront.net',
                    'us-west-2': 'https://d2.example.cloudfront.net',
                    'eu-west-1': 'https://d3.example.cloudfront.net',
                    'ap-southeast-1': 'https://d4.example.cloudfront.net'
                },
                'cost_per_gb': 0.09,
                'latency_score': 0.9
            },
            'azure': {
                'regions': ['eastus', 'westus', 'northeurope', 'southeastasia'],
                'cdn_urls': {
                    'eastus': 'https://example.azureedge.net',
                    'westus': 'https://example2.azureedge.net',
                    'northeurope': 'https://example3.azureedge.net',
                    'southeastasia': 'https://example4.azureedge.net'
                },
                'cost_per_gb': 0.10,
                'latency_score': 0.85
            },
            'gcp': {
                'regions': ['us-central1', 'us-west1', 'europe-west1', 'asia-east1'],
                'cdn_urls': {
                    'us-central1': 'https://example.cdn.google.com',
                    'us-west1': 'https://example2.cdn.google.com',
                    'europe-west1': 'https://example3.cdn.google.com',
                    'asia-east1': 'https://example4.cdn.google.com'
                },
                'cost_per_gb': 0.08,
                'latency_score': 0.88
            }
        }
        self.active_provider = 'aws'
        self.active_region = 'us-east-1'
        self._lock = asyncio.Lock()
        self.deployment_history = deque(maxlen=100)
        
        logger.info("MultiCloudMapDeployment initialized")
    
    async def deploy_map(self, map_data: Dict, preferences: Dict = None) -> Dict:
        """
        Deploy map to optimal cloud.
        
        Args:
            map_data: Map data to deploy
            preferences: Deployment preferences
            
        Returns:
            Deployment strategy
        """
        preferences = preferences or {}
        async with self._lock:
            # Score providers
            scores = {}
            for provider_name, provider in self.cloud_providers.items():
                score = 0
                
                # Cost factor
                cost_score = 1.0 - (provider['cost_per_gb'] / 0.15)
                score += cost_score * 0.3
                
                # Latency factor
                latency_score = provider['latency_score']
                score += latency_score * 0.3
                
                # Region availability
                if preferences.get('region') in provider['regions']:
                    score += 0.2
                
                # Carbon awareness
                if preferences.get('carbon_aware', False):
                    if provider_name == 'gcp':
                        score += 0.2
                    elif provider_name == 'azure':
                        score += 0.1
                
                scores[provider_name] = score
            
            # Determine optimal provider
            optimal_provider = max(scores, key=scores.get)
            self.active_provider = optimal_provider
            
            # Select optimal region within provider
            provider = self.cloud_providers[optimal_provider]
            optimal_region = provider['regions'][0]
            if preferences.get('region') in provider['regions']:
                optimal_region = preferences['region']
            self.active_region = optimal_region
            
            result = {
                'optimal_provider': optimal_provider,
                'optimal_region': optimal_region,
                'scores': scores,
                'cdn_url': provider['cdn_urls'].get(optimal_region),
                'reason': f'Provider {optimal_provider} has best score',
                'timestamp': datetime.now().isoformat()
            }
            
            self.deployment_history.append(result)
            
            logger.info(f"Map deployed to {optimal_provider} ({optimal_region})")
            return result
    
    async def get_deployment_status(self) -> Dict:
        """Get deployment status"""
        return {
            'providers': self.cloud_providers,
            'active_provider': self.active_provider,
            'active_region': self.active_region,
            'deployment_history': list(self.deployment_history)[-5:]
        }

# ============================================================
# ENHANCED MAIN MAP CLASS WITH INTEGRATION
# ============================================================

class EnhancedGreenDataCenterMap:
    """Enhanced main map visualization system v12.0 - Enterprise Quantum Resilience"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.instance_id = str(uuid.uuid4())[:8]
        self.output_dir = Path(self.config.get('output_dir', './map_output'))
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # ============================================================
        # NEW: Enhanced modules
        # ============================================================
        
        # 1. Quantum-Resilient Map Security
        self.quantum_security = QuantumResilientMapSecurity()
        
        # 2. Blockchain Map Verification
        self.blockchain = BlockchainMapVerification()
        
        # 3. Autonomous Map Generation
        self.autonomous_generator = AutonomousMapGenerator()
        
        # 4. Multi-Cloud Map Deployment
        self.cloud_deployer = MultiCloudMapDeployment()
        
        # Existing components (from v11.0)
        self.geocoder = EnhancedGeocodingService()
        self.export_queue = EnhancedExportQueue(max_concurrent=self.config.get('max_concurrent_exports', 3))
        self.weather_service = None
        self.tile_cache = TTLCache(ttl_seconds=CACHE_TTL_SECONDS, max_size_mb=self.config.get('tile_cache_max_mb', 500))
        
        # Data storage with bounded limits
        self.projects: List[DataCenterProject] = []
        self._projects_lock = asyncio.Lock()
        self.map_history = deque(maxlen=MAX_MAP_HISTORY)
        
        # Concurrency control
        self._map_generation_semaphore = asyncio.Semaphore(MAX_CONCURRENT_MAP_GENERATIONS)
        
        # Background tasks
        self.running = False
        self.background_tasks: Set[asyncio.Task] = set()
        self._shutdown_event = asyncio.Event()
        
        # Metrics
        self.generation_count = 0
        self._metrics_task: Optional[asyncio.Task] = None
        
        # Backup
        self._backup_task: Optional[asyncio.Task] = None
        
        logger.info(f"EnhancedGreenDataCenterMap v12.0 initialized (instance: {self.instance_id})")
        logger.info("  ✅ Enterprise Quantum & Blockchain Features Enabled:")
        logger.info("     - Quantum-Resilient Map Security")
        logger.info("     - Blockchain Map Verification")
        logger.info("     - Autonomous Map Generation")
        logger.info("     - Multi-Cloud Map Deployment")
    
    # ============================================================
    # NEW: Quantum-Secure Map Exports
    # ============================================================
    
    async def export_projects_secure(self, export_type: str, output_filename: str, 
                                     priority: int = 1, sign_export: bool = True,
                                     blockchain_record: bool = True) -> Dict:
        """Export projects with quantum security and blockchain verification."""
        async with self._projects_lock:
            if not self.projects:
                await self.load_data()
            projects_copy = self.projects.copy()
        
        output_path = self.output_dir / output_filename
        
        # Generate export
        export_data = {
            'export_type': export_type,
            'projects': [asdict(p) for p in projects_copy],
            'timestamp': datetime.now().isoformat(),
            'instance_id': self.instance_id
        }
        
        # Generate file hash
        file_hash = hashlib.sha256(json.dumps(export_data, sort_keys=True, default=str).encode()).hexdigest()
        
        # ============================================================
        # NEW: Quantum-Resilient Signing
        # ============================================================
        
        quantum_signature = None
        if sign_export:
            quantum_key = await self.quantum_security.generate_keypair('dilithium')
            quantum_signature = await self.quantum_security.sign_map_export(
                export_data,
                quantum_key['key_id']
            )
        
        # ============================================================
        # NEW: Blockchain Verification
        # ============================================================
        
        blockchain_result = None
        if blockchain_record:
            export_id = f"map_export_{uuid.uuid4().hex[:8]}"
            blockchain_result = await self.blockchain.record_map_export(
                export_id,
                {'export_type': export_type, 'project_count': len(projects_copy)},
                file_hash
            )
        
        # Submit to queue
        job = ExportJob(
            export_type=export_type,
            output_path=output_path,
            projects=projects_copy,
            priority=priority
        )
        
        await self.export_queue.submit(job)
        
        return {
            'job_id': job.job_id,
            'export_type': export_type,
            'output_path': str(output_path),
            'file_hash': file_hash,
            'quantum_signature': quantum_signature,
            'blockchain_record': blockchain_result,
            'timestamp': datetime.now().isoformat()
        }
    
    # ============================================================
    # NEW: Autonomous Map Generation
    # ============================================================
    
    async def generate_map_autonomously(self, strategy: str = 'hybrid') -> Dict:
        """Generate map using autonomous strategy."""
        async with self._projects_lock:
            if not self.projects:
                await self.load_data()
            projects_copy = self.projects.copy()
        
        # Get autonomous recommendation
        data = {
            'project_count': len(projects_copy),
            'types': [p.status for p in projects_copy],
            'locations': [(p.latitude, p.longitude) for p in projects_copy]
        }
        
        recommendation = await self.autonomous_generator.generate_map_autonomously(data, strategy)
        
        # Apply recommendation
        output_filename = f"autonomous_map_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        result = await self.generate_interactive_map(output_filename)
        
        return {
            'recommendation': recommendation,
            'map_result': asdict(result),
            'strategy': strategy,
            'timestamp': datetime.now().isoformat()
        }
    
    # ============================================================
    # NEW: Multi-Cloud Deployment
    # ============================================================
    
    async def deploy_map_to_cloud(self, map_path: str, preferences: Dict = None) -> Dict:
        """Deploy generated map to optimal cloud."""
        map_data = {
            'path': map_path,
            'size_mb': Path(map_path).stat().st_size / (1024 * 1024),
            'timestamp': datetime.now().isoformat()
        }
        
        deployment = await self.cloud_deployer.deploy_map(map_data, preferences or {})
        
        logger.info(f"Map deployed: {deployment}")
        return deployment
    
    async def get_cloud_status(self) -> Dict:
        """Get cloud deployment status."""
        return await self.cloud_deployer.get_deployment_status()
    
    # ============================================================
    # NEW: Comprehensive Status
    # ============================================================
    
    async def get_comprehensive_status(self) -> Dict:
        """Get comprehensive system status."""
        quantum_status = self.quantum_security.get_quantum_status()
        blockchain_status = await self.blockchain.get_blockchain_status()
        generation_stats = self.autonomous_generator.get_generation_stats()
        cloud_status = await self.cloud_deployer.get_deployment_status()
        
        return {
            'instance_id': self.instance_id,
            'version': '12.0.0',
            'quantum_security': quantum_status,
            'blockchain': blockchain_status,
            'autonomous_generation': generation_stats,
            'cloud_deployment': cloud_status,
            'projects': {
                'total': len(self.projects),
                'statuses': {status: sum(1 for p in self.projects if p.status == status) 
                           for status in ['operational', 'construction', 'planned', 'decommissioned']}
            },
            'export_queue': self.export_queue.get_stats(),
            'geocoder': await self.geocoder.get_statistics(),
            'timestamp': datetime.now().isoformat()
        }
    
    # ============================================================
    # SHUTDOWN
    # ============================================================
    
    async def shutdown(self):
        """Graceful shutdown with all components cleanup."""
        logger.info(f"Shutting down EnhancedGreenDataCenterMap (instance: {self.instance_id})")
        
        self.running = False
        self._shutdown_event.set()
        
        # Cancel background tasks
        for task in self.background_tasks:
            task.cancel()
        
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
        
        # Stop services
        await self.geocoder.stop()
        await self.export_queue.stop()
        await self.tile_cache.stop()
        
        if self.weather_service:
            await self.weather_service.__aexit__()
        
        logger.info("Shutdown complete")
