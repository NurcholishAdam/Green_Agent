# src/continuum/state_synchronizer.py

from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
import asyncio
from datetime import datetime
import hashlib
import json

@dataclass
class StateSnapshot:
    """Snapshot of edge state for synchronization"""
    snapshot_id: str
    device_id: str
    timestamp: datetime
    task_states: Dict[str, Dict]
    model_versions: Dict[str, str]
    config_hash: str
    checksum: str

class StateSynchronizer:
    """
    Distributed state manager for edge-cloud consistency
    
    Responsibilities:
    - Maintain consistency across edge-cloud boundary
    - Sync model weights, task state, results
    - Handle network partitions gracefully
    - Support offline-first edge operation
    """
    
    def __init__(
        self,
        device_id: str,
        cloud_storage_endpoint: str,
        sync_interval_seconds: int = 30,
        max_offline_duration_seconds: int = 1800  # 30 minutes
    ):
        self.device_id = device_id
        self.cloud_endpoint = cloud_storage_endpoint
        self.sync_interval = sync_interval_seconds
        self.max_offline_duration = max_offline_duration_seconds
        
        self._local_state: Dict[str, Any] = {}
        self._remote_state: Dict[str, Any] = {}
        self._pending_sync: List[Dict] = []
        self._offline_since: Optional[datetime] = None
        self._running = False
        
    async def start(self):
        """Start state synchronization loop"""
        self._running = True
        asyncio.create_task(self._sync_loop())
        
    async def stop(self):
        """Stop synchronization loop"""
        self._running = False
        
    def update_local_state(self, key: str, value: Any):
        """Update local state (triggers sync on next interval)"""
        self._local_state[key] = {
            'value': value,
            'updated_at': datetime.now(),
            'synced': False
        }
        
    async def get_state(self, key: str, prefer_local: bool = True) -> Optional[Any]:
        """Get state value (local or remote)"""
        if prefer_local and key in self._local_state:
            return self._local_state[key]['value']
        elif key in self._remote_state:
            return self._remote_state[key]
        return None
        
    async def _sync_loop(self):
        """Background loop for state synchronization"""
        while self._running:
            try:
                if self._is_online():
                    await self._sync_to_cloud()
                    await self._sync_from_cloud()
                    self._offline_since = None
                else:
                    # Queue for later sync
                    await self._queue_pending_sync()
                    
                await asyncio.sleep(self.sync_interval)
                
            except Exception as e:
                logger.warning(f"State sync error: {e}")
                if self._offline_since is None:
                    self._offline_since = datetime.now()
                await asyncio.sleep(60)
                
    def _is_online(self) -> bool:
        """Check if network connection is available"""
        # Implementation: Ping cloud endpoint or check network interface
        return True  # Placeholder
        
    async def _sync_to_cloud(self):
        """Sync local state changes to cloud"""
        # Find unsynced local state
        unsynced = {
            k: v for k, v in self._local_state.items()
            if not v.get('synced', False)
        }
        
        if not unsynced:
            return
            
        # Create snapshot
        snapshot = StateSnapshot(
            snapshot_id=self._generate_snapshot_id(),
            device_id=self.device_id,
            timestamp=datetime.now(),
            task_states=unsynced,
            model_versions=self._get_model_versions(),
            config_hash=self._calculate_config_hash(),
            checksum=''  # Calculated below
        )
        
        # Calculate checksum
        snapshot.checksum = self._calculate_snapshot_checksum(snapshot)
        
        # Upload to cloud storage
        await self._upload_snapshot(snapshot)
        
        # Mark as synced
        for key in unsynced.keys():
            self._local_state[key]['synced'] = True
            
    async def _sync_from_cloud(self):
        """Sync remote state changes from cloud"""
        # Download latest snapshot from cloud
        remote_snapshot = await self._download_latest_snapshot()
        
        if remote_snapshot is None:
            return
            
        # Merge remote state with local (conflict resolution)
        for key, value in remote_snapshot.task_states.items():
            if key not in self._local_state:
                # New key from remote
                self._local_state[key] = value
                self._local_state[key]['synced'] = True
            else:
                # Conflict: use last-write-wins
                local_updated = self._local_state[key].get('updated_at')
                remote_updated = value.get('updated_at')
                
                if remote_updated and (local_updated is None or remote_updated > local_updated):
                    self._local_state[key] = value
                    self._local_state[key]['synced'] = True
                    
        # Update remote state cache
        self._remote_state = remote_snapshot.task_states
        
    async def _queue_pending_sync(self):
        """Queue state changes for later sync (offline mode)"""
        unsynced = {
            k: v for k, v in self._local_state.items()
            if not v.get('synced', False)
        }
        
        for key, value in unsynced.items():
            self._pending_sync.append({
                'key': key,
                'value': value,
                'queued_at': datetime.now()
            })
            
        # Check if offline too long
        if self._offline_since:
            offline_duration = (datetime.now() - self._offline_since).total_seconds()
            if offline_duration > self.max_offline_duration:
                logger.warning(f"Offline for {offline_duration}s, may lose data")
                
    def _generate_snapshot_id(self) -> str:
        """Generate unique snapshot ID"""
        return hashlib.sha256(
            f"{self.device_id}:{datetime.now().isoformat()}".encode()
        ).hexdigest()[:16]
        
    def _calculate_snapshot_checksum(self, snapshot: StateSnapshot) -> str:
        """Calculate checksum for snapshot integrity"""
        data = json.dumps({
            'snapshot_id': snapshot.snapshot_id,
            'device_id': snapshot.device_id,
            'timestamp': snapshot.timestamp.isoformat(),
            'task_states': snapshot.task_states,
        }, sort_keys=True)
        return hashlib.sha256(data.encode()).hexdigest()
        
    def _get_model_versions(self) -> Dict[str, str]:
        """Get current model versions"""
        # Implementation: Query model registry
        return {}
        
    def _calculate_config_hash(self) -> str:
        """Calculate hash of current configuration"""
        # Implementation: Hash config files
        return hashlib.sha256(b"config").hexdigest()[:16]
        
    async def _upload_snapshot(self, snapshot: StateSnapshot):
        """Upload snapshot to cloud storage"""
        # Implementation: HTTP PUT to cloud storage endpoint
        pass
        
    async def _download_latest_snapshot(self) -> Optional[StateSnapshot]:
        """Download latest snapshot from cloud"""
        # Implementation: HTTP GET from cloud storage endpoint
        return None  # Placeholder
        
    async def get_sync_status(self) -> Dict:
        """Get current synchronization status"""
        return {
            'device_id': self.device_id,
            'online': self._is_online(),
            'offline_since': self._offline_since.isoformat() if self._offline_since else None,
            'local_state_count': len(self._local_state),
            'remote_state_count': len(self._remote_state),
            'pending_sync_count': len(self._pending_sync),
            'last_sync': self._get_last_sync_time()
        }
        
    def _get_last_sync_time(self) -> Optional[str]:
        """Get timestamp of last successful sync"""
        synced_items = [
            v.get('updated_at') for v in self._local_state.values()
            if v.get('synced', False) and v.get('updated_at')
        ]
        if synced_items:
            return max(synced_items).isoformat()
        return None
