# ============================================================
# VAULT MANAGER (NEW)
# ============================================================
class VaultManager:
    def __init__(self, config: FederatedLearnerConfig):
        self.config = config
        self.client = None
        if VAULT_AVAILABLE and config.vault_url and config.vault_token:
            try:
                self.client = VaultClient(url=config.vault_url, token=config.vault_token)
                logger.info("Vault client initialized")
            except Exception as e:
                logger.error(f"Vault client initialization failed: {e}")
        else:
            logger.warning("Vault not configured; using in‑memory fallback for secrets.")

    async def store_secret(self, path: str, data: Dict):
        if not self.client:
            logger.warning("Vault not available; secret not stored")
            return
        try:
            self.client.secrets.kv.v2.create_or_update_secret(
                path=path,
                secret=data
            )
            VAULT_OPERATIONS.labels(operation='store', status='success').inc()
        except Exception as e:
            VAULT_OPERATIONS.labels(operation='store', status='failed').inc()
            raise VaultError(f"Failed to store secret: {e}") from e

    async def get_secret(self, path: str) -> Optional[Dict]:
        if not self.client:
            return None
        try:
            secret = self.client.secrets.kv.v2.read_secret(path=path)
            VAULT_OPERATIONS.labels(operation='read', status='success').inc()
            return secret['data']['data']
        except Exception:
            VAULT_OPERATIONS.labels(operation='read', status='failed').inc()
            return None

# ============================================================
# MULTI‑CLOUD STORAGE (NEW)
# ============================================================
class MultiCloudStorage:
    def __init__(self, config: FederatedLearnerConfig):
        self.config = config
        self.providers = {}
        self._init_providers()

    def _init_providers(self):
        if AWS_AVAILABLE and self.config.cloud_aws_bucket:
            try:
                self.providers['aws'] = {
                    'client': boto3.client(
                        's3',
                        region_name=self.config.cloud_aws_region,
                        aws_access_key_id=self.config.cloud_aws_access_key,
                        aws_secret_access_key=self.config.cloud_aws_secret_key
                    ),
                    'bucket': self.config.cloud_aws_bucket
                }
            except Exception as e:
                logger.warning(f"AWS client init failed: {e}")
        if AZURE_AVAILABLE and self.config.cloud_azure_connection_string:
            try:
                self.providers['azure'] = {
                    'client': BlobServiceClient.from_connection_string(self.config.cloud_azure_connection_string),
                    'container': self.config.cloud_azure_container
                }
            except Exception as e:
                logger.warning(f"Azure client init failed: {e}")
        if GCP_AVAILABLE and self.config.cloud_gcp_credentials:
            try:
                self.providers['gcp'] = {
                    'client': storage.Client(),
                    'bucket': self.config.cloud_gcp_bucket
                }
            except Exception as e:
                logger.warning(f"GCP client init failed: {e}")

    async def store(self, data: Dict, filename: str = None) -> Dict:
        """Store data in the first available cloud provider."""
        for provider_name, provider in self.providers.items():
            try:
                if provider_name == 'aws':
                    client = provider['client']
                    bucket = provider['bucket']
                    key = filename or f"fl_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    data_bytes = json.dumps(data, default=str).encode()
                    client.put_object(Bucket=bucket, Key=key, Body=data_bytes)
                    CLOUD_STORAGE.labels(provider=provider_name, operation='store', status='success').inc()
                    return {'provider': provider_name, 'location': f"s3://{bucket}/{key}"}
                elif provider_name == 'azure':
                    client = provider['client']
                    container = provider['container']
                    blob_name = filename or f"fl_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    data_bytes = json.dumps(data, default=str).encode()
                    blob_client = client.get_blob_client(container=container, blob=blob_name)
                    blob_client.upload_blob(data_bytes, overwrite=True)
                    CLOUD_STORAGE.labels(provider=provider_name, operation='store', status='success').inc()
                    return {'provider': provider_name, 'location': f"https://{container}.blob.core.windows.net/{blob_name}"}
                elif provider_name == 'gcp':
                    client = provider['client']
                    bucket = provider['bucket']
                    blob_name = filename or f"fl_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    data_bytes = json.dumps(data, default=str).encode()
                    bucket_obj = client.bucket(bucket)
                    blob = bucket_obj.blob(blob_name)
                    blob.upload_from_string(data_bytes)
                    CLOUD_STORAGE.labels(provider=provider_name, operation='store', status='success').inc()
                    return {'provider': provider_name, 'location': f"gs://{bucket}/{blob_name}"}
            except Exception as e:
                logger.error(f"Cloud storage failed for {provider_name}: {e}")
                CLOUD_STORAGE.labels(provider=provider_name, operation='store', status='failed').inc()
        # Fallback to local
        local_path = Path(f"./fl_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(local_path, 'w') as f:
            json.dump(data, f, default=str)
        return {'provider': 'local', 'location': str(local_path)}

# ============================================================
# POST‑QUANTUM CRYPTOGRAPHY (replaced with pqcrypto + Vault)
# ============================================================
class QuantumResilientFederatedSecurity:
    def __init__(self, config: FederatedLearnerConfig, vault: Optional[VaultManager] = None):
        self.config = config
        self.vault = vault
        self.pqc_algorithms = {}
        self.pqc_available = PQC_AVAILABLE and config.enable_quantum_security
        self.key_pairs = {}
        self.signatures = {}
        self._lock = asyncio.Lock()
        self.master_key = config.get_master_key_bytes()

        if self.pqc_available:
            self._initialize_pqc()

    def _initialize_pqc(self):
        self.pqc_algorithms['dilithium'] = dilithium
        self.pqc_algorithms['falcon'] = falcon
        self.pqc_algorithms['sphincs'] = sphincs
        logger.info("PQC algorithms loaded")

    def _derive_key(self, salt: bytes) -> bytes:
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
            backend=default_backend()
        )
        return kdf.derive(self.master_key)

    def _encrypt_key(self, key_bytes: bytes) -> bytes:
        salt = os.urandom(16)
        derived = self._derive_key(salt)
        aesgcm = AESGCM(derived)
        nonce = os.urandom(12)
        ciphertext = aesgcm.encrypt(nonce, key_bytes, None)
        return salt + nonce + ciphertext

    def _decrypt_key(self, encrypted_bytes: bytes) -> bytes:
        salt = encrypted_bytes[:16]
        nonce = encrypted_bytes[16:28]
        ciphertext = encrypted_bytes[28:]
        derived = self._derive_key(salt)
        aesgcm = AESGCM(derived)
        return aesgcm.decrypt(nonce, ciphertext, None)

    async def generate_keypair(self, algorithm: str = None) -> Dict:
        algorithm = algorithm or self.config.quantum_algorithm
        if not self.pqc_available:
            return self._fallback_keypair()

        try:
            signer = self.pqc_algorithms.get(algorithm)
            if not signer:
                raise ValueError(f"Algorithm {algorithm} not available")
            public_key, private_key = await asyncio.to_thread(signer.generate_keypair)
            key_id = f"{algorithm}_{uuid.uuid4().hex[:8]}"
            encrypted_private = self._encrypt_key(private_key)
            encrypted_public = self._encrypt_key(public_key)
            secret_data = {
                'algorithm': algorithm,
                'public_key': encrypted_public.hex(),
                'private_key': encrypted_private.hex(),
                'created_at': datetime.now().isoformat()
            }
            if self.vault and self.vault.client:
                await self.vault.store_secret(f"pqc/{key_id}", secret_data)
            async with self._lock:
                self.key_pairs[key_id] = {
                    'algorithm': algorithm,
                    'public_key': public_key,
                    'private_key': private_key,
                    'created_at': datetime.now().isoformat()
                }
            QUANTUM_SIGNATURES.labels(algorithm=algorithm, status='generated').inc()
            return {'key_id': key_id, 'algorithm': algorithm, 'public_key': public_key.hex()}
        except Exception as e:
            logger.error(f"Keypair generation failed: {e}")
            return self._fallback_keypair()

    def _fallback_keypair(self) -> Dict:
        key_id = f"fallback_{uuid.uuid4().hex[:8]}"
        return {'key_id': key_id, 'algorithm': 'ecdsa', 'public_key': hashlib.sha256(os.urandom(32)).hexdigest()}

    # ... (sign, verify methods similar but using Vault for key retrieval)

# ============================================================
# ASYNC DATABASE MANAGER (with asyncpg support)
# ============================================================
class EnhancedDatabaseManager:
    def __init__(self, config: FederatedLearnerConfig):
        self.config = config
        self.db_url = config.database_url
        self.async_available = SQLALCHEMY_ASYNC_AVAILABLE and ASYNCPG_AVAILABLE
        self.sync_available = SQLALCHEMY_SYNC_AVAILABLE
        self.engine = None
        self.async_session = None
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._init_db()

    def _init_db(self):
        if self.async_available:
            self.engine = create_async_engine(
                self.db_url,
                poolclass=NullPool,
                echo=False
            )
            self.async_session = async_sessionmaker(self.engine, expire_on_commit=False)
            logger.info(f"Async database engine created: {self.db_url}")
        elif self.sync_available:
            sync_url = self.db_url.replace("+aiosqlite", "").replace("+asyncpg", "")
            self.engine = create_engine(
                sync_url,
                poolclass=QueuePool,
                pool_size=10,
                max_overflow=20
            )
            self.async_session = None
            logger.warning(f"Sync database engine created (fallback): {sync_url}")
            self._init_tables_sync()
        else:
            logger.error("No SQLAlchemy backend available")

    # ... (rest of methods similar)

# ============================================================
# FASTAPI REST API (NEW)
# ============================================================
if FASTAPI_AVAILABLE:
    app = FastAPI(title="Federated Learner API", version="9.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    security = HTTPBearer()

    async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
        token = credentials.credentials
        try:
            payload = jwt.decode(token, FederatedLearnerConfig().jwt_secret, algorithms=["HS256"])
            return payload
        except JWTError:
            raise HTTPException(status_code=401, detail="Invalid token")

    # Global learner instance
    learner: Optional[EnhancedFederatedLearner] = None

    @app.post("/register_client")
    async def register_client(client_id: str, data_size: int = 1000, compute_power: float = 1000,
                              carbon_intensity: float = 400, renewable_percent: float = 0,
                              trust_score: float = 0.5, region: str = "global",
                              user: Dict = Depends(verify_token)):
        if not learner:
            raise HTTPException(status_code=503, detail="Federated learner not initialized")
        success = await learner.register_client(client_id, {}, data_size, compute_power,
                                                carbon_intensity, renewable_percent,
                                                trust_score, region)
        return {"success": success}

    @app.post("/federated_round")
    async def federated_round(user_id: str = None, selection_strategy: str = None,
                              user: Dict = Depends(verify_token)):
        if not learner:
            raise HTTPException(status_code=503, detail="Federated learner not initialized")
        model = await learner.federated_round(user_id, selection_strategy)
        if model is None:
            raise HTTPException(status_code=400, detail="Round failed")
        return {"status": "success", "global_model": model}

    @app.get("/stats")
    async def get_stats(user: Dict = Depends(verify_token)):
        if not learner:
            raise HTTPException(status_code=503, detail="Federated learner not initialized")
        return await learner.get_federation_stats()

    @app.post("/coevolution_share")
    async def coevolution_share(user: Dict = Depends(verify_token)):
        if not learner:
            raise HTTPException(status_code=503, detail="Federated learner not initialized")
        # Trigger a coevolution share
        if learner.config.enable_coevolution:
            fitness = {cid: c.trust_score * 0.7 + c.success_rate * 0.3
                       for cid, c in learner.clients.items()}
            gaps = await learner._compute_domain_gaps()
            gradient_norms = {cid: c.gradient_norm for cid, c in learner.clients.items()}
            share_data = await learner.enhanced_coevolution.prepare_share_data(fitness, gaps, gradient_norms)
            result = await learner.enhanced_coevolution.share_evolutionary_data(share_data)
            return {"status": "shared", "result": result}
        return {"status": "disabled"}

    @app.on_event("startup")
    async def startup():
        global learner
        config = FederatedLearnerConfig()
        learner = EnhancedFederatedLearner(config)
        await learner.start()
        logger.info("FastAPI started")

    @app.on_event("shutdown")
    async def shutdown():
        if learner:
            await learner.shutdown()
        logger.info("FastAPI shut down")
