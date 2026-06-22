# File: carbon_nas_enhanced_v12_bio.py
"""
Carbon-Aware Neural Architecture Search - Version 12.0 (Enterprise Platinum)
Enhanced with bio-inspired features from v5.0.0
"""

# ... [All existing imports from v12.0 remain]

# ============================================================
# ADDED: BIO-INSPIRED COMPONENTS FROM V5.0.0
# ============================================================

class BioInspiredOptimizer:
    """Bio-inspired optimization components integrated into v12.0"""
    
    def __init__(self, token_manager=None, gradient_manager=None, knowledge_transfer=None):
        self.token_manager = token_manager
        self.gradient_manager = gradient_manager
        self.knowledge_transfer = knowledge_transfer
        
        # Token economy state
        self.token_balance = 0
        self.token_efficiency_score = 0.5
        
        # Gradient fields
        self.gradient_pressure = 0.5
        
        # Knowledge bank
        self.knowledge_bank = {}
        
        # Continuous learning state
        self.continuous_cycle_count = 0
        
        logger.info("BioInspiredOptimizer initialized")
    
    def get_gradient_pressure(self) -> float:
        """Get current gradient pressure from gradient manager"""
        if self.gradient_manager:
            # Simulate gradient pressure based on carbon metrics
            return min(1.0, self.gradient_pressure * (1.0 + 0.1 * self.continuous_cycle_count))
        return 0.5
    
    def get_token_efficiency(self, token_cost: float) -> float:
        """Calculate token efficiency score"""
        if token_cost <= 0:
            return 1.0
        efficiency = 1.0 / (1.0 + token_cost * 0.01)
        return max(0.0, min(1.0, efficiency))
    
    async def allocate_budget(self, num_evals: int) -> Tuple[bool, float]:
        """Allocate token budget for evaluations"""
        if not self.token_manager:
            return True, 5.0  # Default cost per evaluation
        
        ecoatp_per = 5.0
        balance = self.token_balance
        
        # Dynamic pricing based on balance
        if balance < 100:
            ecoatp_per *= 3.0  # Expensive when low
        elif balance > 500:
            ecoatp_per *= 0.5  # Cheap when abundant
        
        total = ecoatp_per * num_evals
        
        if balance < total:
            # Can only afford partial
            affordable = int(balance / ecoatp_per)
            return affordable > 0, ecoatp_per
        
        return True, ecoatp_per
    
    async def capture_knowledge(self, architecture_config: Dict, performance: Dict) -> str:
        """Capture knowledge from successful architectures"""
        if not self.knowledge_transfer:
            return None
        
        package_id = f"know_{hashlib.md5(str(architecture_config).encode()).hexdigest()[:12]}"
        
        self.knowledge_bank[package_id] = {
            'config': architecture_config,
            'performance': performance,
            'survival_score': performance.get('composite_score', 0.5),
            'timestamp': datetime.now()
        }
        
        # Prune knowledge bank if too large
        if len(self.knowledge_bank) > 1000:
            # Keep only top 500 by survival score
            sorted_packages = sorted(
                self.knowledge_bank.items(),
                key=lambda x: x[1]['survival_score'],
                reverse=True
            )[:500]
            self.knowledge_bank = dict(sorted_packages)
        
        logger.info(f"Captured knowledge package {package_id}")
        return package_id
    
    def inject_knowledge(self, population: List, injection_rate: float = 0.2) -> List:
        """Inject knowledge into population"""
        if not self.knowledge_bank or injection_rate <= 0:
            return population
        
        # Get best knowledge packages
        best_packages = sorted(
            self.knowledge_bank.items(),
            key=lambda x: x[1]['survival_score'],
            reverse=True
        )[:5]
        
        if not best_packages:
            return population
        
        # Inject into population
        replace_count = int(len(population) * injection_rate)
        replace_count = min(replace_count, len(population) // 2)
        
        for i in range(replace_count):
            if i < len(best_packages) and i < len(population):
                # Replace worst-performing with variant of best knowledge
                pkg = best_packages[i % len(best_packages)][1]
                variant = self._create_variant(pkg['config'])
                population[-(i+1)] = variant  # Replace from end
        
        logger.info(f"Injected knowledge into {replace_count} population members")
        return population
    
    def _create_variant(self, base_config: Dict) -> Dict:
        """Create variant of known good configuration"""
        import copy
        import random
        
        variant = copy.deepcopy(base_config)
        
        # Small mutations
        if 'num_layers' in variant:
            variant['num_layers'] += random.choice([-2, -1, 0, 1, 2])
            variant['num_layers'] = max(1, variant['num_layers'])
        
        if 'hidden_dim' in variant:
            variant['hidden_dim'] += random.choice([-32, -16, 0, 16, 32])
            variant['hidden_dim'] = max(32, variant['hidden_dim'])
        
        return variant

# ============================================================
# ENHANCED MAIN NAS CLASS WITH BIO-INSPIRED FEATURES
# ============================================================

class EnhancedCarbonAwareNAS:
    """Enhanced carbon-aware NAS v12.0 with bio-inspired features"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.instance_id = str(uuid.uuid4())[:8]
        
        # Bio-inspired optimizer (NEW)
        self.bio_optimizer = BioInspiredOptimizer()
        
        # Database (existing)
        self.db_manager = EnhancedDatabaseManager(Path("./carbon_nas_data.db"))
        
        # Components (existing)
        self.cache = EnhancedCacheManager()
        self.quality_scorer = EnhancedDataQualityScorer()
        self.rate_limiter = EnhancedRateLimiter()
        self.trainer = EnhancedModelTrainer(device=torch.device('cuda' if torch.cuda.is_available() else 'cpu'))
        self.circuit_breakers = {
            'evaluation': EnhancedCircuitBreaker('evaluation'),
            'worker': EnhancedCircuitBreaker('worker')
        }
        
        # State (existing)
        self.architecture_history = deque(maxlen=MAX_ARCH_HISTORY)
        self.cycle_results = deque(maxlen=MAX_CYCLE_RESULTS)
        self._history_lock = asyncio.Lock()
        
        # Pareto frontier (existing)
        self.pareto_frontier: List[Dict] = []
        self.best_accuracy = 0.0
        self.total_carbon_kg = 0.0
        
        # Thread pool (existing)
        self.thread_pool = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_EVALUATIONS)
        self.operation_queue = asyncio.Queue(maxsize=100)
        self._queue_worker = None
        self._running = False
        
        # Device
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        
        # Background tasks (existing)
        self.background_tasks = set()
        self._shutdown_event = asyncio.Event()
        
        # Continuous learning (NEW)
        self.continuous_learning_enabled = self.config.get('continuous_learning', True)
        self.knowledge_injection_rate = self.config.get('knowledge_injection_rate', 0.2)
        self.continuous_generations = self.config.get('continuous_generations', 5)
        self.continuous_interval_seconds = self.config.get('continuous_interval_seconds', 3600)
        
        logger.info(f"EnhancedCarbonAwareNAS v{DATA_VERSION}.0 with bio-inspired features initialized (instance: {self.instance_id})")
    
    # ADDED: Continuous learning loop from v5.0.0
    async def _continuous_learning_loop(self):
        """Background loop for continuous learning and knowledge transfer"""
        while not self._shutdown_event.is_set():
            try:
                if self.continuous_learning_enabled:
                    # Wait for idle period
                    await asyncio.sleep(self.continuous_interval_seconds)
                    
                    # Check if system is idle (queue empty)
                    if self.operation_queue.qsize() == 0:
                        logger.info("Starting continuous learning cycle")
                        
                        # Get current gradient pressure
                        pressure = self.bio_optimizer.get_gradient_pressure()
                        
                        # Generate lightweight architectures
                        n_architectures = min(10, self.continuous_generations * 2)
                        architectures = self.generate_architectures(n_architectures)
                        
                        # Apply knowledge injection
                        if self.bio_optimizer.knowledge_bank:
                            architectures = self.bio_optimizer.inject_knowledge(
                                architectures, 
                                self.knowledge_injection_rate
                            )
                        
                        # Evaluate architectures (if data available)
                        if hasattr(self, 'last_train_loader') and hasattr(self, 'last_val_loader'):
                            try:
                                cycle_result = await self.run_cycle(
                                    architectures,
                                    self.last_train_loader,
                                    self.last_val_loader
                                )
                                
                                # Capture knowledge from best architecture
                                if cycle_result.best_accuracy > 0.8:
                                    best_arch = next(
                                        (a for a in architectures if a['accuracy'] == cycle_result.best_accuracy),
                                        None
                                    )
                                    if best_arch:
                                        await self.bio_optimizer.capture_knowledge(
                                            best_arch,
                                            {'accuracy': cycle_result.best_accuracy, 
                                             'composite_score': cycle_result.best_accuracy / 100}
                                        )
                                
                                self.bio_optimizer.continuous_cycle_count += 1
                                logger.info(f"Continuous learning cycle {self.bio_optimizer.continuous_cycle_count} completed")
                            except Exception as e:
                                logger.error(f"Continuous learning evaluation failed: {e}")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Continuous learning error: {e}")
                await asyncio.sleep(60)  # Back off on error
    
    # ADDED: Start background tasks
    async def start(self):
        """Start all background services including continuous learning"""
        # Start original queue worker
        self._running = True
        self._queue_worker = asyncio.create_task(self._process_queue())
        
        # Start background tasks
        tasks = [
            asyncio.create_task(self._health_check_loop()),
            asyncio.create_task(self._cleanup_loop()),
            asyncio.create_task(self._continuous_learning_loop())  # NEW
        ]
        
        for task in tasks:
            self.background_tasks.add(task)
            task.add_done_callback(self.background_tasks.discard)
        
        logger.info(f"NAS system started with {len(self.background_tasks)} background tasks (including continuous learning)")
    
    # MODIFIED: Enhanced evaluation with token economy
    async def _execute_evaluation(self, operation: Dict) -> ArchitectureResult:
        """Execute architecture evaluation with bio-inspired token economy"""
        await self.rate_limiter.wait_and_acquire()
        
        arch_spec = operation['architecture']
        
        # Validate architecture
        try:
            validated = ArchitectureSpec(**arch_spec)
        except ValidationError as e:
            raise ValueError(f"Invalid architecture: {e}")
        
        # Allocate budget using bio-inspired token economy
        can_afford, token_cost = await self.bio_optimizer.allocate_budget(1)
        if not can_afford:
            raise Exception("Insufficient token budget for evaluation")
        
        # Assess data quality
        quality_score = await self.quality_scorer.assess_quality(operation['train_loader'])
        
        # Evaluate with circuit breaker
        result = await self.circuit_breakers['evaluation'].call(
            self._evaluate_architecture, validated, operation['train_loader'],
            operation['val_loader'], quality_score
        )
        
        # Update token economy
        self.bio_optimizer.token_balance -= token_cost
        self.bio_optimizer.token_efficiency_score = self.bio_optimizer.get_token_efficiency(token_cost)
        
        # Store in memory (bounded)
        async with self._history_lock:
            self.architecture_history.append(result)
            self._update_pareto_frontier(result)
            
            if result.accuracy > self.best_accuracy:
                self.best_accuracy = result.accuracy
                BEST_ACCURACY.set(self.best_accuracy)
        
        # Save to database
        await self.db_manager.save_architecture(result)
        
        # Update carbon metrics
        self.total_carbon_kg += result.carbon_kg
        CARBON_EMITTED.set(self.total_carbon_kg)
        
        EVALUATION_QUEUE_SIZE.set(self.operation_queue.qsize())
        
        logger.info(f"Architecture {validated.arch_id}: accuracy={result.accuracy:.2f}%, "
                   f"carbon={result.carbon_kg:.4f}kg, token_cost={token_cost:.2f}")
        return result
    
    # MODIFIED: Enhanced run_cycle with knowledge capture
    async def run_cycle(self, architectures: List[Dict], train_loader: DataLoader,
                        val_loader: DataLoader) -> CycleResult:
        """Run one NAS cycle with knowledge capture"""
        start_time = time.time()
        cycle_id = len(self.cycle_results) + 1
        
        # Store loaders for continuous learning
        self.last_train_loader = train_loader
        self.last_val_loader = val_loader
        
        results = []
        for arch in architectures:
            future = asyncio.Future()
            
            await self.operation_queue.put({
                'type': 'evaluation',
                'architecture': arch,
                'train_loader': train_loader,
                'val_loader': val_loader,
                'future': future
            })
            EVALUATION_QUEUE_SIZE.set(self.operation_queue.qsize())
            
            result = await future
            results.append(result)
        
        # Find best in cycle
        best = max(results, key=lambda x: x.accuracy)
        
        # Capture knowledge from best architecture
        if best.accuracy > 0.8:  # Only capture high-quality architectures
            await self.bio_optimizer.capture_knowledge(
                {'arch_id': best.arch_id, 'layers': best.arch_id},  # Simplified
                {'accuracy': best.accuracy, 'composite_score': best.accuracy / 100}
            )
        
        cycle_result = CycleResult(
            cycle_id=cycle_id,
            architectures_evaluated=len(results),
            best_accuracy=best.accuracy,
            pareto_size=len(self.pareto_frontier),
            carbon_kg=sum(r.carbon_kg for r in results),
            duration_ms=(time.time() - start_time) * 1000
        )
        
        # Store in memory
        async with self._history_lock:
            self.cycle_results.append(cycle_result)
        
        # Save to database
        await self.db_manager.save_cycle(cycle_result)
        
        NAS_CYCLES.labels(status='success').inc()
        
        logger.info(f"Cycle {cycle_id} completed: best={best.accuracy:.2f}%, "
                   f"carbon={cycle_result.carbon_kg:.2f}kg, "
                   f"knowledge_packages={len(self.bio_optimizer.knowledge_bank)}")
        return cycle_result
    
    # ADDED: Enhanced statistics with bio-inspired metrics
    async def get_statistics(self) -> Dict:
        """Get comprehensive statistics including bio-inspired metrics"""
        async with self._history_lock:
            arch_count = len(self.architecture_history)
            cycle_count = len(self.cycle_results)
        
        quality_stats = await self.quality_scorer.get_statistics()
        
        return {
            'instance_id': self.instance_id,
            'version': DATA_VERSION,
            'architecture_count': arch_count,
            'cycle_count': cycle_count,
            'best_accuracy': self.best_accuracy,
            'total_carbon_kg': self.total_carbon_kg,
            'pareto_size': len(self.pareto_frontier),
            'data_quality': quality_stats,
            'queue_size': self.operation_queue.qsize(),
            'cache_hit_rate': self.cache.get_hit_rate() * 100,
            'circuit_breakers': {name: cb.get_metrics() for name, cb in self.circuit_breakers.items()},
            # NEW: Bio-inspired metrics
            'bio_metrics': {
                'token_balance': self.bio_optimizer.token_balance,
                'token_efficiency': self.bio_optimizer.token_efficiency_score,
                'gradient_pressure': self.bio_optimizer.get_gradient_pressure(),
                'knowledge_packages': len(self.bio_optimizer.knowledge_bank),
                'continuous_cycles': self.bio_optimizer.continuous_cycle_count
            },
            'timestamp': datetime.now().isoformat()
        }
    
    # ADDED: Export state including bio-inspired data
    async def export_state(self) -> Dict:
        """Export current state including bio-inspired knowledge"""
        async with self._history_lock:
            return {
                'instance_id': self.instance_id,
                'version': DATA_VERSION,
                'architecture_history': [a.to_dict() for a in self.architecture_history],
                'cycle_results': [c.to_dict() for c in self.cycle_results],
                'best_accuracy': self.best_accuracy,
                'total_carbon_kg': self.total_carbon_kg,
                'pareto_frontier': self.pareto_frontier,
                # NEW: Include bio-inspired state
                'bio_knowledge': self.bio_optimizer.knowledge_bank,
                'bio_token_balance': self.bio_optimizer.token_balance,
                'bio_continuous_cycles': self.bio_optimizer.continuous_cycle_count,
                'exported_at': datetime.now().isoformat()
            }
    
    # ... [All other existing methods remain the same]
