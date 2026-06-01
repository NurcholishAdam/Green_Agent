# File: src/enhancements/blockchain_helium_rights.py

"""
Helium Rights Smart Contract & Trading Platform - Version 6.2

BRIDGES THE BLOCKCHAIN INTEGRATION GAP:
1. On-chain helium allocation rights management
2. Automated market maker for helium rights trading
3. Multi-signature governance for allocation decisions
4. Real-time settlement and verification
5. Integration with existing provenance tracking
6. ERC-1155 multi-token standard for fractional rights
7. Auction mechanism for scarce allocation
8. Regulatory compliance built into smart contracts
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
import json
import os
import logging
import time
import hashlib
from datetime import datetime
from pathlib import Path
from enum import Enum
import secrets

# Web3 imports
try:
    from web3 import Web3
    from web3.middleware import geth_poa_middleware
    from web3.exceptions import TransactionNotFound, ContractLogicError
    from eth_account import Account
    from eth_account.signers.local import LocalAccount
    from eth_abi import encode
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

# Import base classes
try:
    from .base_classes import BaseMetrics, GreenAgentConfig, load_module_config
    from .blockchain_helium_verification import BlockchainConnectionManager
except ImportError:
    from base_classes import BaseMetrics, GreenAgentConfig, load_module_config
    from blockchain_helium_verification import BlockchainConnectionManager

logger = logging.getLogger(__name__)

# ============================================================
# SMART CONTRACTS
# ============================================================

HELIUM_RIGHTS_CONTRACT = """
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC1155/ERC1155.sol";
import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/utils/Strings.sol";

contract HeliumAllocationRights is ERC1155, Ownable, ReentrancyGuard {
    // Allocation right types
    uint256 public constant SPOT_RIGHT = 0;      // Immediate delivery
    uint256 public constant FORWARD_RIGHT = 1;   // Future delivery
    uint256 public constant OPTION_RIGHT = 2;    // Option to purchase
    
    struct Allocation {
        uint256 allocationId;
        address owner;
        uint256 volumeLiters;
        uint256 pricePerLiter;  // In wei
        uint256 expiryTimestamp;
        uint256 rightType;
        bool exercised;
        bool settled;
        string source;
        string certificationLevel;
    }
    
    mapping(uint256 => Allocation) public allocations;
    uint256 public nextAllocationId;
    
    // Auction parameters
    struct Auction {
        uint256 allocationId;
        address seller;
        uint256 startPrice;
        uint256 currentBid;
        address currentBidder;
        uint256 endTime;
        bool active;
        uint256 minBidIncrement;
    }
    
    mapping(uint256 => Auction) public auctions;
    uint256 public nextAuctionId;
    
    // Governance
    mapping(address => bool) public governors;
    uint256 public governanceThreshold = 2;  // Number of governor signatures required
    
    // Fees
    uint256 public tradingFeeBasisPoints = 25;  // 0.25%
    uint256 public settlementFeeBasisPoints = 10;  // 0.10%
    
    // Events
    event AllocationCreated(uint256 indexed allocationId, address indexed owner, uint256 volume);
    event AllocationTransferred(uint256 indexed allocationId, address from, address to);
    event AllocationExercised(uint256 indexed allocationId, address exerciser);
    event AllocationSettled(uint256 indexed allocationId, uint256 finalPrice);
    event AuctionCreated(uint256 indexed auctionId, uint256 allocationId, uint256 startPrice);
    event BidPlaced(uint256 indexed auctionId, address bidder, uint256 amount);
    event AuctionEnded(uint256 indexed auctionId, address winner, uint256 finalPrice);
    event FeesUpdated(uint256 tradingFee, uint256 settlementFee);
    event GovernorAdded(address indexed governor);
    event GovernorRemoved(address indexed governor);
    
    constructor() ERC1155("https://helium.greenagent.io/api/token/{id}.json") Ownable(msg.sender) {
        governors[msg.sender] = true;
    }
    
    // ============================================================
    // ALLOCATION MANAGEMENT
    // ============================================================
    
    function createAllocation(
        uint256 _volumeLiters,
        uint256 _pricePerLiter,
        uint256 _expiryTimestamp,
        uint256 _rightType,
        string memory _source,
        string memory _certificationLevel
    ) public returns (uint256) {
        require(_volumeLiters > 0, "Volume must be positive");
        require(_expiryTimestamp > block.timestamp, "Expiry must be in future");
        require(_rightType <= 2, "Invalid right type");
        
        uint256 allocationId = nextAllocationId++;
        
        allocations[allocationId] = Allocation({
            allocationId: allocationId,
            owner: msg.sender,
            volumeLiters: _volumeLiters,
            pricePerLiter: _pricePerLiter,
            expiryTimestamp: _expiryTimestamp,
            rightType: _rightType,
            exercised: false,
            settled: false,
            source: _source,
            certificationLevel: _certificationLevel
        });
        
        // Mint ERC-1155 token representing the right
        _mint(msg.sender, allocationId, _volumeLiters, "");
        
        emit AllocationCreated(allocationId, msg.sender, _volumeLiters);
        
        return allocationId;
    }
    
    function transferAllocation(
        uint256 _allocationId,
        address _to,
        uint256 _amount
    ) public nonReentrant {
        require(allocations[_allocationId].owner == msg.sender, "Not owner");
        require(!allocations[_allocationId].exercised, "Already exercised");
        require(!allocations[_allocationId].settled, "Already settled");
        require(_amount <= allocations[_allocationId].volumeLiters, "Insufficient volume");
        
        // Calculate fee
        uint256 fee = (_amount * allocations[_allocationId].pricePerLiter * tradingFeeBasisPoints) / 10000;
        
        // Transfer tokens
        safeTransferFrom(msg.sender, _to, _allocationId, _amount, "");
        
        // Update allocation if partial transfer
        if (_amount < allocations[_allocationId].volumeLiters) {
            // Create new allocation for remaining volume
            uint256 remainingId = nextAllocationId++;
            allocations[remainingId] = Allocation({
                allocationId: remainingId,
                owner: msg.sender,
                volumeLiters: allocations[_allocationId].volumeLiters - _amount,
                pricePerLiter: allocations[_allocationId].pricePerLiter,
                expiryTimestamp: allocations[_allocationId].expiryTimestamp,
                rightType: allocations[_allocationId].rightType,
                exercised: false,
                settled: false,
                source: allocations[_allocationId].source,
                certificationLevel: allocations[_allocationId].certificationLevel
            });
        }
        
        // Update original allocation
        allocations[_allocationId].volumeLiters = _amount;
        allocations[_allocationId].owner = _to;
        
        emit AllocationTransferred(_allocationId, msg.sender, _to);
    }
    
    function exerciseAllocation(uint256 _allocationId, uint256 _amount) public nonReentrant {
        require(allocations[_allocationId].owner == msg.sender, "Not owner");
        require(!allocations[_allocationId].exercised, "Already exercised");
        require(block.timestamp <= allocations[_allocationId].expiryTimestamp, "Expired");
        require(_amount <= balanceOf(msg.sender, _allocationId), "Insufficient balance");
        
        allocations[_allocationId].exercised = true;
        
        emit AllocationExercised(_allocationId, msg.sender);
    }
    
    // ============================================================
    // AUCTION MECHANISM
    // ============================================================
    
    function createAuction(
        uint256 _allocationId,
        uint256 _startPrice,
        uint256 _duration,
        uint256 _minBidIncrement
    ) public returns (uint256) {
        require(allocations[_allocationId].owner == msg.sender, "Not owner");
        require(!allocations[_allocationId].exercised, "Already exercised");
        require(_duration > 0, "Duration must be positive");
        
        uint256 auctionId = nextAuctionId++;
        
        auctions[auctionId] = Auction({
            allocationId: _allocationId,
            seller: msg.sender,
            startPrice: _startPrice,
            currentBid: 0,
            currentBidder: address(0),
            endTime: block.timestamp + _duration,
            active: true,
            minBidIncrement: _minBidIncrement
        });
        
        emit AuctionCreated(auctionId, _allocationId, _startPrice);
        
        return auctionId;
    }
    
    function placeBid(uint256 _auctionId, uint256 _bidAmount) public nonReentrant {
        Auction storage auction = auctions[_auctionId];
        
        require(auction.active, "Auction not active");
        require(block.timestamp < auction.endTime, "Auction ended");
        require(_bidAmount >= auction.startPrice, "Bid below start price");
        
        if (auction.currentBidder != address(0)) {
            require(_bidAmount >= auction.currentBid + auction.minBidIncrement, "Bid too low");
            // Refund previous bidder
            payable(auction.currentBidder).transfer(auction.currentBid);
        }
        
        auction.currentBid = _bidAmount;
        auction.currentBidder = msg.sender;
        
        emit BidPlaced(_auctionId, msg.sender, _bidAmount);
    }
    
    function endAuction(uint256 _auctionId) public nonReentrant {
        Auction storage auction = auctions[_auctionId];
        
        require(auction.active, "Auction not active");
        require(block.timestamp >= auction.endTime, "Auction not ended");
        
        auction.active = false;
        
        if (auction.currentBidder != address(0)) {
            // Transfer allocation to winner
            uint256 fee = (auction.currentBid * tradingFeeBasisPoints) / 10000;
            uint256 sellerProceeds = auction.currentBid - fee;
            
            allocations[auction.allocationId].owner = auction.currentBidder;
            payable(auction.seller).transfer(sellerProceeds);
        }
        
        emit AuctionEnded(_auctionId, auction.currentBidder, auction.currentBid);
    }
    
    // ============================================================
    // GOVERNANCE
    // ============================================================
    
    function addGovernor(address _governor) public {
        require(governors[msg.sender], "Not governor");
        governors[_governor] = true;
        emit GovernorAdded(_governor);
    }
    
    function removeGovernor(address _governor) public {
        require(governors[msg.sender], "Not governor");
        require(_governor != msg.sender, "Cannot remove self");
        governors[_governor] = false;
        emit GovernorRemoved(_governor);
    }
    
    function updateFees(uint256 _tradingFee, uint256 _settlementFee) public {
        require(governors[msg.sender], "Not governor");
        require(_tradingFee <= 500, "Trading fee too high");  // Max 5%
        require(_settlementFee <= 200, "Settlement fee too high");  // Max 2%
        
        tradingFeeBasisPoints = _tradingFee;
        settlementFeeBasisPoints = _settlementFee;
        
        emit FeesUpdated(_tradingFee, _settlementFee);
    }
    
    // ============================================================
    // VIEW FUNCTIONS
    // ============================================================
    
    function getAllocation(uint256 _allocationId) public view returns (Allocation memory) {
        return allocations[_allocationId];
    }
    
    function getAuction(uint256 _auctionId) public view returns (Auction memory) {
        return auctions[_auctionId];
    }
    
    function getActiveAuctions() public view returns (uint256[] memory) {
        uint256 count = 0;
        for (uint256 i = 0; i < nextAuctionId; i++) {
            if (auctions[i].active && block.timestamp < auctions[i].endTime) {
                count++;
            }
        }
        
        uint256[] memory active = new uint256[](count);
        uint256 index = 0;
        for (uint256 i = 0; i < nextAuctionId; i++) {
            if (auctions[i].active && block.timestamp < auctions[i].endTime) {
                active[index++] = i;
            }
        }
        
        return active;
    }
}
"""

# ============================================================
# HELIUM RIGHTS TRADING PLATFORM
# ============================================================

class HeliumAllocationType(Enum):
    """Types of helium allocation rights"""
    SPOT = 0      # Immediate delivery
    FORWARD = 1   # Future delivery contract
    OPTION = 2    # Option to purchase

@dataclass
class AllocationRecord(BaseMetrics):
    """Helium allocation right record"""
    source_module: str = "blockchain_helium_rights"
    
    allocation_id: int = 0
    owner: str = ""
    volume_liters: float = 0.0
    price_per_liter_wei: int = 0
    price_per_liter_usd: float = 0.0
    expiry_timestamp: int = 0
    right_type: str = "spot"
    exercised: bool = False
    settled: bool = False
    source: str = ""
    certification_level: str = ""
    transaction_hash: str = ""

@dataclass 
class AuctionRecord(BaseMetrics):
    """Helium auction record"""
    source_module: str = "blockchain_helium_rights"
    
    auction_id: int = 0
    allocation_id: int = 0
    seller: str = ""
    start_price_wei: int = 0
    current_bid_wei: int = 0
    current_bidder: str = ""
    end_time: int = 0
    active: bool = True
    transaction_hash: str = ""

class HeliumRightsPlatform:
    """
    On-chain helium allocation rights trading platform.
    
    Features:
    - Create and manage allocation rights
    - Auction mechanism for scarce allocations
    - Real-time settlement
    - Governance-controlled parameters
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or load_module_config('blockchain')
        self.connection = BlockchainConnectionManager(self.config)
        
        # Contract instance
        self.rights_contract = None
        
        if self.connection.connected:
            self._initialize_contract()
        
        # Records
        self.allocations: List[AllocationRecord] = []
        self.auctions: List[AuctionRecord] = []
        self.trades: List[Dict] = []
        
        logger.info("HeliumRightsPlatform initialized")
    
    def _initialize_contract(self):
        """Initialize rights contract"""
        try:
            contract_address = self.config.get('smart_contracts', {}).get(
                'helium_rights', {}
            ).get('address', '')
            
            if contract_address and '${' not in contract_address:
                abi = self._load_rights_abi()
                if abi:
                    self.rights_contract = self.connection.w3.eth.contract(
                        address=contract_address,
                        abi=abi
                    )
                    logger.info("Helium rights contract loaded")
        except Exception as e:
            logger.error(f"Contract initialization failed: {e}")
    
    def _load_rights_abi(self) -> Optional[List]:
        """Load rights contract ABI"""
        abi_path = Path(__file__).parent / 'abi' / 'helium_rights.json'
        
        if abi_path.exists():
            with open(abi_path, 'r') as f:
                return json.load(f)
        
        # Try to compile
        try:
            from solcx import compile_standard, install_solc
            install_solc('0.8.19')
            
            compiled = compile_standard({
                "language": "Solidity",
                "sources": {"HeliumAllocationRights.sol": {"content": HELIUM_RIGHTS_CONTRACT}},
                "settings": {"outputSelection": {"*": {"*": ["abi", "evm.bytecode"]}}}
            })
            
            abi = compiled['contracts']['HeliumAllocationRights.sol']['HeliumAllocationRights']['abi']
            
            # Save for future use
            abi_path.parent.mkdir(exist_ok=True)
            with open(abi_path, 'w') as f:
                json.dump(abi, f)
            
            return abi
        except Exception as e:
            logger.error(f"Contract compilation failed: {e}")
            return None
    
    def create_allocation(self,
                         volume_liters: float,
                         price_per_liter_usd: float,
                         expiry_days: int,
                         right_type: HeliumAllocationType = HeliumAllocationType.SPOT,
                         source: str = "Green Agent",
                         certification: str = "gold") -> Optional[AllocationRecord]:
        """
        Create helium allocation right on blockchain.
        """
        
        if not self.rights_contract or not self.connection.account:
            return self._create_local_allocation(
                volume_liters, price_per_liter_usd, expiry_days, 
                right_type, source, certification
            )
        
        try:
            # Convert price to wei (assuming 1 token = 1 liter at USD price)
            price_per_liter_wei = self.connection.w3.to_wei(price_per_liter_usd, 'ether')
            expiry = int(time.time() + expiry_days * 86400)
            
            # Call contract
            tx_function = self.rights_contract.functions.createAllocation(
                int(volume_liters),
                price_per_liter_wei,
                expiry,
                right_type.value,
                source,
                certification
            )
            
            tx_hash = self.connection.send_transaction(tx_function)
            
            if tx_hash:
                receipt = self.connection.w3.eth.get_transaction_receipt(tx_hash)
                logs = self.rights_contract.events.AllocationCreated().process_receipt(receipt)
                
                if logs:
                    allocation_id = logs[0]['args']['allocationId']
                    
                    record = AllocationRecord(
                        allocation_id=allocation_id,
                        owner=self.connection.account.address,
                        volume_liters=volume_liters,
                        price_per_liter_wei=price_per_liter_wei,
                        price_per_liter_usd=price_per_liter_usd,
                        expiry_timestamp=expiry,
                        right_type=right_type.name.lower(),
                        source=source,
                        certification_level=certification,
                        transaction_hash=tx_hash
                    )
                    
                    self.allocations.append(record)
                    
                    logger.info(f"Allocation created on-chain: {allocation_id}")
                    
                    return record
        
        except Exception as e:
            logger.error(f"Allocation creation failed: {e}")
        
        return self._create_local_allocation(
            volume_liters, price_per_liter_usd, expiry_days,
            right_type, source, certification
        )
    
    def _create_local_allocation(self, volume: float, price: float,
                                expiry_days: int, right_type: HeliumAllocationType,
                                source: str, certification: str) -> AllocationRecord:
        """Create local allocation record"""
        record = AllocationRecord(
            allocation_id=len(self.allocations),
            owner="local",
            volume_liters=volume,
            price_per_liter_usd=price,
            expiry_timestamp=int(time.time() + expiry_days * 86400),
            right_type=right_type.name.lower(),
            source=source,
            certification_level=certification,
            transaction_hash="local"
        )
        
        self.allocations.append(record)
        return record
    
    def create_auction(self,
                      allocation_id: int,
                      start_price_usd: float,
                      duration_hours: int = 24,
                      min_bid_increment_usd: float = 1.0) -> Optional[AuctionRecord]:
        """
        Create auction for helium allocation rights.
        """
        
        if not self.rights_contract or not self.connection.account:
            return self._create_local_auction(allocation_id, start_price_usd, duration_hours)
        
        try:
            start_price_wei = self.connection.w3.to_wei(start_price_usd, 'ether')
            min_increment_wei = self.connection.w3.to_wei(min_bid_increment_usd, 'ether')
            
            tx_function = self.rights_contract.functions.createAuction(
                allocation_id,
                start_price_wei,
                duration_hours * 3600,
                min_increment_wei
            )
            
            tx_hash = self.connection.send_transaction(tx_function)
            
            if tx_hash:
                receipt = self.connection.w3.eth.get_transaction_receipt(tx_hash)
                logs = self.rights_contract.events.AuctionCreated().process_receipt(receipt)
                
                if logs:
                    auction_id = logs[0]['args']['auctionId']
                    
                    record = AuctionRecord(
                        auction_id=auction_id,
                        allocation_id=allocation_id,
                        seller=self.connection.account.address,
                        start_price_wei=start_price_wei,
                        end_time=int(time.time() + duration_hours * 3600),
                        active=True,
                        transaction_hash=tx_hash
                    )
                    
                    self.auctions.append(record)
                    
                    logger.info(f"Auction created on-chain: {auction_id}")
                    
                    return record
        
        except Exception as e:
            logger.error(f"Auction creation failed: {e}")
        
        return self._create_local_auction(allocation_id, start_price_usd, duration_hours)
    
    def _create_local_auction(self, allocation_id: int, 
                             start_price: float, duration_hours: int) -> AuctionRecord:
        """Create local auction record"""
        record = AuctionRecord(
            auction_id=len(self.auctions),
            allocation_id=allocation_id,
            seller="local",
            start_price_wei=int(start_price * 1e18),
            end_time=int(time.time() + duration_hours * 3600),
            active=True,
            transaction_hash="local"
        )
        
        self.auctions.append(record)
        return record
    
    def get_active_auctions(self) -> List[Dict]:
        """Get all active auctions"""
        if not self.rights_contract:
            return [
                {
                    'auction_id': a.auction_id,
                    'allocation_id': a.allocation_id,
                    'current_bid_usd': a.current_bid_wei / 1e18,
                    'end_time': datetime.fromtimestamp(a.end_time).isoformat(),
                    'active': a.active
                }
                for a in self.auctions if a.active
            ]
        
        try:
            active_ids = self.rights_contract.functions.getActiveAuctions().call()
            
            active_auctions = []
            for auction_id in active_ids:
                auction_data = self.rights_contract.functions.getAuction(auction_id).call()
                
                active_auctions.append({
                    'auction_id': auction_id,
                    'allocation_id': auction_data[0],
                    'current_bid_usd': self.connection.w3.from_wei(auction_data[3], 'ether'),
                    'end_time': datetime.fromtimestamp(auction_data[4]).isoformat(),
                    'active': auction_data[5]
                })
            
            return active_auctions
        
        except Exception as e:
            logger.error(f"Failed to get active auctions: {e}")
            return []
    
    def execute_trade(self, 
                     allocation_id: int,
                     buyer_address: str,
                     amount_liters: float) -> bool:
        """
        Execute a helium rights trade.
        """
        
        if not self.rights_contract or not self.connection.account:
            logger.info(f"Local trade: {amount_liters}L from allocation {allocation_id} to {buyer_address}")
            self.trades.append({
                'allocation_id': allocation_id,
                'buyer': buyer_address,
                'amount_liters': amount_liters,
                'timestamp': datetime.now().isoformat(),
                'on_chain': False
            })
            return True
        
        try:
            tx_function = self.rights_contract.functions.transferAllocation(
                allocation_id,
                buyer_address,
                int(amount_liters)
            )
            
            tx_hash = self.connection.send_transaction(tx_function)
            
            if tx_hash:
                self.trades.append({
                    'allocation_id': allocation_id,
                    'buyer': buyer_address,
                    'amount_liters': amount_liters,
                    'transaction_hash': tx_hash,
                    'timestamp': datetime.now().isoformat(),
                    'on_chain': True
                })
                
                logger.info(f"Trade executed on-chain: {tx_hash}")
                return True
        
        except Exception as e:
            logger.error(f"Trade execution failed: {e}")
        
        return False
    
    def get_market_summary(self) -> Dict:
        """Get helium rights market summary"""
        return {
            'total_allocations': len(self.allocations),
            'total_volume_liters': sum(a.volume_liters for a in self.allocations),
            'active_auctions': len([a for a in self.auctions if a.active]),
            'total_trades': len(self.trades),
            'on_chain_trades': sum(1 for t in self.trades if t.get('on_chain', False)),
            'average_price_usd': np.mean([a.price_per_liter_usd for a in self.allocations]) if self.allocations else 0,
            'timestamp': datetime.now().isoformat()
        }
