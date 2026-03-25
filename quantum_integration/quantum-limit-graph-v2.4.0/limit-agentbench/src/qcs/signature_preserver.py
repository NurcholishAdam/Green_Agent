# src/qcs/signature_preserver.py

from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import json
import numpy as np

@dataclass
class ThoughtSignature:
    """Meta-cognitive thought signature"""
    signature_id: str
    thought_hash: str
    quantum_amplitude_hash: str
    classical_context_hash: str
    created_at: datetime
    carbon_ledger_entry: Optional[str] = None
    blockchain_anchor: Optional[str] = None
    verified: bool = False

@dataclass
class SignatureChain:
    """Chain of signatures for audit trail"""
    chain_id: str
    signatures: List[ThoughtSignature]
    merkle_root: str
    created_at: datetime
    verified: bool = False

class SignaturePreserver:
    """
    Cryptographic signature preservation across quantum-classical boundary
    
    Responsibilities:
    - Hash quantum state amplitudes to thoughtSignature
    - Ensure audit trail across quantum-classical boundary
    - Support SLSA Level 3 provenance requirements
    - Anchor signatures to blockchain for immutability
    """
    
    def __init__(
        self,
        carbon_ledger_endpoint: str,
        blockchain_endpoint: Optional[str] = None,
        hash_algorithm: str = "sha256"
    ):
        self.carbon_ledger_endpoint = carbon_ledger_endpoint
        self.blockchain_endpoint = blockchain_endpoint
        self.hash_algorithm = hash_algorithm
        
        self._signature_cache: Dict[str, ThoughtSignature] = {}
        _signature_chains: Dict[str, SignatureChain] = {}
        
    async def create_signature(
        self,
        thought_data: Dict[str, Any],
        quantum_amplitudes: np.ndarray,
        classical_context: Dict[str, Any]
    ) -> ThoughtSignature:
        """
        Create thought signature from quantum-classical state
        
        Args:
            thought_data: Meta-cognitive thought data from Layer 3
            quantum_amplitudes: Quantum state vector amplitudes
            classical_context: Classical execution context
            
        Returns:
            ThoughtSignature with cryptographic hashes
        """
        signature_id = self._generate_signature_id()
        
        # Hash thought data
        thought_hash = self._hash_data(thought_data)
        
        # Hash quantum amplitudes (convert to serializable format)
        amplitude_hash = self._hash_amplitudes(quantum_amplitudes)
        
        # Hash classical context
        context_hash = self._hash_data(classical_context)
        
        # Create signature
        signature = ThoughtSignature(
            signature_id=signature_id,
            thought_hash=thought_hash,
            quantum_amplitude_hash=amplitude_hash,
            classical_context_hash=context_hash,
            created_at=datetime.now()
        )
        
        # Record to carbon ledger
        ledger_entry = await self._record_to_carbon_ledger(signature)
        signature.carbon_ledger_entry = ledger_entry
        
        # Optional: Anchor to blockchain
        if self.blockchain_endpoint:
            blockchain_tx = await self._anchor_to_blockchain(signature)
            signature.blockchain_anchor = blockchain_tx
            
        # Cache signature
        self._signature_cache[signature_id] = signature
        
        return signature
        
    async def verify_signature(
        self,
        signature_id: str,
        thought_data: Dict[str, Any],
        quantum_amplitudes: np.ndarray,
        classical_context: Dict[str, Any]
    ) -> bool:
        """
        Verify signature integrity
        
        Args:
            signature_id: Signature to verify
            thought_data: Current thought data
            quantum_amplitudes: Current quantum amplitudes
            classical_context: Current classical context
            
        Returns:
            True if signature is valid and intact
        """
        if signature_id not in self._signature_cache:
            # Try to fetch from carbon ledger
            signature = await self._fetch_from_carbon_ledger(signature_id)
            if signature is None:
                return False
        else:
            signature = self._signature_cache[signature_id]
            
        # Verify hashes
        thought_hash = self._hash_data(thought_data)
        amplitude_hash = self._hash_amplitudes(quantum_amplitudes)
        context_hash = self._hash_data(classical_context)
        
        valid = (
            thought_hash == signature.thought_hash and
            amplitude_hash == signature.quantum_amplitude_hash and
            context_hash == signature.classical_context_hash
        )
        
        if valid:
            signature.verified = True
            
        return valid
        
    async def create_signature_chain(
        self,
        signature_ids: List[str]
    ) -> SignatureChain:
        """
        Create Merkle tree chain of signatures
        
        Args:
            signature_ids: List of signature IDs to chain
            
        Returns:
            SignatureChain with Merkle root
        """
        chain_id = self._generate_chain_id()
        
        # Get signatures
        signatures = []
        for sig_id in signature_ids:
            if sig_id in self._signature_cache:
                signatures.append(self._signature_cache[sig_id])
                
        # Calculate Merkle root
        merkle_root = self._calculate_merkle_root(signatures)
        
        chain = SignatureChain(
            chain_id=chain_id,
            signatures=signatures,
            merkle_root=merkle_root,
            created_at=datetime.now(),
            verified=False
        )
        
        self._signature_chains[chain_id] = chain
        return chain
        
    def _hash_data(self, data: Dict[str, Any]) -> str:
        """Hash data dictionary"""
        serialized = json.dumps(data, sort_keys=True)
        if self.hash_algorithm == "sha256":
            return hashlib.sha256(serialized.encode()).hexdigest()
        elif self.hash_algorithm == "sha512":
            return hashlib.sha512(serialized.encode()).hexdigest()
        else:
            return hashlib.sha256(serialized.encode()).hexdigest()
            
    def _hash_amplitudes(self, amplitudes: np.ndarray) -> str:
        """Hash quantum amplitudes (complex numbers)"""
        # Convert complex amplitudes to serializable format
        serialized = json.dumps({
            'real': amplitudes.real.tolist(),
            'imag': amplitudes.imag.tolist()
        }, sort_keys=True)
        
        if self.hash_algorithm == "sha256":
            return hashlib.sha256(serialized.encode()).hexdigest()
        else:
            return hashlib.sha256(serialized.encode()).hexdigest()
            
    def _calculate_merkle_root(self, signatures: List[ThoughtSignature]) -> str:
        """Calculate Merkle root from signatures"""
        if not signatures:
            return self._hash_data({})
            
        # Get leaf hashes
        hashes = [sig.signature_id for sig in signatures]
        
        # Build Merkle tree
        while len(hashes) > 1:
            if len(hashes) % 2 == 1:
                hashes.append(hashes[-1])  # Duplicate last if odd
                
            new_level = []
            for i in range(0, len(hashes), 2):
                combined = hashes[i] + hashes[i+1]
                new_hash = hashlib.sha256(combined.encode()).hexdigest()
                new_level.append(new_hash)
                
            hashes = new_level
            
        return hashes[0] if hashes else ""
        
    async def _record_to_carbon_ledger(self, signature: ThoughtSignature) -> str:
        """Record signature to carbon ledger"""
        # Implementation: HTTP POST to carbon ledger API
        ledger_entry_id = f"ledger:{signature.signature_id}"
        return ledger_entry_id
        
    async def _fetch_from_carbon_ledger(self, signature_id: str) -> Optional[ThoughtSignature]:
        """Fetch signature from carbon ledger"""
        # Implementation: HTTP GET from carbon ledger API
        return None  # Placeholder
        
    async def _anchor_to_blockchain(self, signature: ThoughtSignature) -> str:
        """Anchor signature to blockchain"""
        # Implementation: Submit transaction to blockchain
        tx_hash = f"0x{signature.signature_id}"
        return tx_hash
        
    def _generate_signature_id(self) -> str:
        """Generate unique signature ID"""
        return hashlib.sha256(
            f"ts:{datetime.now().isoformat()}:{np.random.random()}".encode()
        ).hexdigest()[:16]
        
    def _generate_chain_id(self) -> str:
        """Generate unique chain ID"""
        return hashlib.sha256(
            f"sc:{datetime.now().isoformat()}".encode()
        ).hexdigest()[:16]
        
    async def get_signature(self, signature_id: str) -> Optional[ThoughtSignature]:
        """Get signature by ID"""
        return self._signature_cache.get(signature_id)
        
    async def get_chain(self, chain_id: str) -> Optional[SignatureChain]:
        """Get signature chain by ID"""
        return self._signature_chains.get(chain_id)
