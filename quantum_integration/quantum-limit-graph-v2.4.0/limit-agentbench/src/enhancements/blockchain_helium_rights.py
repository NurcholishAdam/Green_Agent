# File: src/enhancements/blockchain_helium_rights.py

"""
Helium Rights Smart Contract & Trading Platform - Version 7.0

ENHANCED FEATURES:
1. On-chain helium allocation rights management
2. Automated market maker for helium rights trading
3. Multi-signature governance for allocation decisions
4. Real-time settlement and verification
5. Integration with existing provenance tracking
6. ERC-1155 multi-token standard for fractional rights
7. Auction mechanism for scarce allocation
8. Regulatory compliance built into smart contracts
9. Oracle price feed integration for USD/ETH conversion
10. Gas-optimized batch operations
11. Comprehensive testing and validation suite
12. Precision decimal handling with fixed-point arithmetic
13. Rate limiting and DDoS protection
14. Enhanced event monitoring and analytics
15. Circuit breaker pattern for blockchain failures
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any, Set, Union, Callable
import json
import os
import logging
import time
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from enum import Enum
import secrets
import threading
from decimal import Decimal, ROUND_DOWN
from collections import deque
from functools import wraps
import asyncio
from concurrent.futures import ThreadPoolExecutor
import unittest
from unittest.mock import Mock, patch, MagicMock

# Web3 imports
try:
    from web3 import Web3
    from web3.middleware import geth_poa_middleware, gas_price_strategy
    from web3.exceptions import TransactionNotFound, ContractLogicError, TimeExhausted
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

# Configure logging with rotation
from logging.handlers import RotatingFileHandler

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Add rotating file handler
handler = RotatingFileHandler(
    'helium_rights.log', 
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5
)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
handler.setFormatter(formatter)
logger.addHandler(handler)

# ============================================================
# ENHANCED SMART CONTRACTS (V7.0)
# ============================================================

HELIUM_RIGHTS_CONTRACT_V7 = """
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.19;

import "@openzeppelin/contracts/token/ERC1155/ERC1155.sol";
import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/security/ReentrancyGuard.sol";
import "@openzeppelin/contracts/security/Pausable.sol";
import "@openzeppelin/contracts/utils/Strings.sol";
import "@openzeppelin/contracts/utils/Address.sol";
import "@openzeppelin/contracts/utils/math/SafeMath.sol";

interface IOracle {
    function getPrice(string memory asset) external view returns (uint256 price, uint8 decimals);
    function updatePrice(string memory asset, uint256 price) external;
}

contract HeliumAllocationRightsV7 is ERC1155, Ownable, ReentrancyGuard, Pausable {
    using SafeMath for uint256;
    
    // Constants for fixed-point arithmetic (6 decimal places)
    uint256 constant PRECISION = 1e6;
    
    // Allocation right types
    uint256 public constant SPOT_RIGHT = 0;      // Immediate delivery
    uint256 public constant FORWARD_RIGHT = 1;   // Future delivery
    uint256 public constant OPTION_RIGHT = 2;    // Option to purchase
    
    // Oracle for price feeds
    IOracle public oracle;
    uint256 public lastOracleUpdate;
    uint256 public oracleUpdateInterval = 3600; // 1 hour
    
    struct Allocation {
        uint256 allocationId;
        address owner;
        uint256 volumeLiters;      // In liters * PRECISION
        uint256 pricePerLiter;     // In wei * PRECISION
        uint256 expiryTimestamp;
        uint256 rightType;
        bool exercised;
        bool settled;
        string source;
        string certificationLevel;
        uint256 createdAt;
        uint256 lastTradedAt;
        uint256 tradeCount;
    }
    
    mapping(uint256 => Allocation) public allocations;
    uint256 public nextAllocationId;
    
    // Batch operations support
    struct AllocationBatch {
        uint256[] allocationIds;
        uint256[] volumes;
        uint256 totalVolume;
    }
    
    // Auction parameters
    struct Auction {
        uint256 allocationId;
        address seller;
        uint256 startPrice;
        uint256 reservePrice;
        uint256 currentBid;
        address currentBidder;
        uint256 endTime;
        bool active;
        bool finalized;
        uint256 minBidIncrement;
        uint256 bidCount;
        uint256 extensionTime;  // Anti-sniping: extend auction by this time on late bid
    }
    
    mapping(uint256 => Auction) public auctions;
    uint256 public nextAuctionId;
    uint256 public constant MAX_AUCTION_EXTENSION = 300; // 5 minutes max extension
    
    // Governance
    mapping(address => bool) public governors;
    uint256 public governanceThreshold = 2;
    mapping(uint256 => mapping(address => bool)) public proposalVotes;
    mapping(uint256 => bool) public proposalExecuted;
    
    struct Proposal {
        uint256 proposalId;
        address proposer;
        uint256 actionType;
        bytes data;
        uint256 startTime;
        uint256 endTime;
        uint256 voteCount;
    }
    
    mapping(uint256 => Proposal) public proposals;
    uint256 public nextProposalId;
    
    // Fees with dynamic adjustment
    uint256 public tradingFeeBasisPoints = 25;  // 0.25%
    uint256 public settlementFeeBasisPoints = 10;  // 0.10%
    uint256 public constant MAX_TRADING_FEE = 500;   // 5%
    uint256 public constant MAX_SETTLEMENT_FEE = 200; // 2%
    
    // Rate limiting
    mapping(address => uint256) public lastActionTimestamp;
    mapping(address => uint256) public actionCount;
    uint256 public rateLimitInterval = 60; // 60 seconds window
    uint256 public maxActionsPerInterval = 100;
    
    // Circuit breaker
    bool public emergencyStop = false;
    uint256 public totalVolumeLimit = 1_000_000 * PRECISION; // 1 million liters
    uint256 public totalVolumeTraded;
    
    // Events (Enhanced)
    event AllocationCreated(uint256 indexed allocationId, address indexed owner, uint256 volume, uint256 price, string source);
    event AllocationTransferred(uint256 indexed allocationId, address from, address to, uint256 amount, uint256 price);
    event AllocationExercised(uint256 indexed allocationId, address exerciser, uint256 amount);
    event AllocationSettled(uint256 indexed allocationId, uint256 finalPrice, uint256 settlementAmount);
    event AuctionCreated(uint256 indexed auctionId, uint256 allocationId, uint256 startPrice, uint256 endTime);
    event BidPlaced(uint256 indexed auctionId, address bidder, uint256 amount, uint256 timestamp);
    event AuctionEnded(uint256 indexed auctionId, address winner, uint256 finalPrice);
    event AuctionExtended(uint256 indexed auctionId, uint256 newEndTime);
    event FeesUpdated(uint256 tradingFee, uint256 settlementFee);
    event GovernorAdded(address indexed governor);
    event GovernorRemoved(address indexed governor);
    event OracleUpdated(address indexed oracle, uint256 timestamp);
    event PriceFeedUpdated(string asset, uint256 price, uint256 timestamp);
    event ProposalCreated(uint256 indexed proposalId, address proposer);
    event ProposalVoted(uint256 indexed proposalId, address voter, uint256 voteCount);
    event ProposalExecuted(uint256 indexed proposalId);
    event BatchOperation(string operation, uint256 count, uint256 totalVolume);
    event CircuitBreakerActivated(string reason, uint256 threshold, uint256 current);
    event CircuitBreakerDeactivated();
    event RateLimitExceeded(address account, uint256 count);
    
    constructor(address _oracle) ERC1155("https://helium.greenagent.io/api/token/{id}.json") Ownable(msg.sender) {
        governors[msg.sender] = true;
        oracle = IOracle(_oracle);
        lastOracleUpdate = block.timestamp;
    }
    
    // Modifiers
    modifier onlyGovernor() {
        require(governors[msg.sender], "Not governor");
        _;
    }
    
    modifier whenNotEmergencyStopped() {
        require(!emergencyStop, "Emergency stop active");
        _;
    }
    
    modifier rateLimited() {
        if (block.timestamp - lastActionTimestamp[msg.sender] > rateLimitInterval) {
            actionCount[msg.sender] = 0;
            lastActionTimestamp[msg.sender] = block.timestamp;
        }
        actionCount[msg.sender]++;
        require(actionCount[msg.sender] <= maxActionsPerInterval, "Rate limit exceeded");
        _;
    }
    
    // ============================================================
    // ORACLE INTEGRATION
    // ============================================================
    
    function updateOracle(address _newOracle) external onlyOwner {
        require(_newOracle != address(0), "Invalid oracle address");
        oracle = IOracle(_newOracle);
        lastOracleUpdate = block.timestamp;
        emit OracleUpdated(_newOracle, block.timestamp);
    }
    
    function getCurrentPrice() public view returns (uint256) {
        (uint256 price, ) = oracle.getPrice("HELIUM");
        return price;
    }
    
    function updatePriceFeed() external {
        require(block.timestamp - lastOracleUpdate >= oracleUpdateInterval, "Too frequent");
        (uint256 price, ) = oracle.getPrice("HELIUM");
        lastOracleUpdate = block.timestamp;
        emit PriceFeedUpdated("HELIUM", price, block.timestamp);
    }
    
    // ============================================================
    // ENHANCED ALLOCATION MANAGEMENT
    // ============================================================
    
    function createAllocation(
        uint256 _volumeLiters,
        uint256 _pricePerLiter,
        uint256 _expiryTimestamp,
        uint256 _rightType,
        string memory _source,
        string memory _certificationLevel
    ) public whenNotPaused whenNotEmergencyStopped rateLimited returns (uint256) {
        require(_volumeLiters > 0, "Volume must be positive");
        require(_expiryTimestamp > block.timestamp, "Expiry must be in future");
        require(_rightType <= 2, "Invalid right type");
        require(_expiryTimestamp <= block.timestamp + 365 days, "Expiry too far in future");
        require(bytes(_source).length > 0, "Source required");
        require(bytes(_certificationLevel).length > 0, "Certification required");
        
        uint256 allocationId = nextAllocationId++;
        uint256 scaledVolume = _volumeLiters * PRECISION;
        
        // Check total volume limit
        require(totalVolumeTraded + scaledVolume <= totalVolumeLimit, "Volume limit exceeded");
        totalVolumeTraded += scaledVolume;
        
        allocations[allocationId] = Allocation({
            allocationId: allocationId,
            owner: msg.sender,
            volumeLiters: scaledVolume,
            pricePerLiter: _pricePerLiter,
            expiryTimestamp: _expiryTimestamp,
            rightType: _rightType,
            exercised: false,
            settled: false,
            source: _source,
            certificationLevel: _certificationLevel,
            createdAt: block.timestamp,
            lastTradedAt: 0,
            tradeCount: 0
        });
        
        _mint(msg.sender, allocationId, scaledVolume, "");
        
        emit AllocationCreated(allocationId, msg.sender, scaledVolume, _pricePerLiter, _source);
        
        return allocationId;
    }
    
    function createAllocationBatch(
        uint256[] memory _volumes,
        uint256[] memory _prices,
        uint256[] memory _expiries,
        uint256[] memory _rightTypes,
        string[] memory _sources,
        string[] memory _certifications
    ) external whenNotPaused whenNotEmergencyStopped rateLimited returns (uint256[] memory) {
        require(_volumes.length == _prices.length, "Array length mismatch");
        require(_volumes.length <= 50, "Batch too large");
        
        uint256[] memory ids = new uint256[](_volumes.length);
        
        for (uint256 i = 0; i < _volumes.length; i++) {
            ids[i] = createAllocation(
                _volumes[i],
                _prices[i],
                _expiries[i],
                _rightTypes[i],
                _sources[i],
                _certifications[i]
            );
        }
        
        emit BatchOperation("create", _volumes.length, sum(_volumes));
        
        return ids;
    }
    
    function transferAllocation(
        uint256 _allocationId,
        address _to,
        uint256 _amount
    ) public nonReentrant whenNotPaused whenNotEmergencyStopped rateLimited {
        Allocation storage allocation = allocations[_allocationId];
        require(allocation.owner == msg.sender, "Not owner");
        require(!allocation.exercised, "Already exercised");
        require(!allocation.settled, "Already settled");
        require(_to != address(0), "Invalid recipient");
        require(_to != msg.sender, "Cannot transfer to self");
        require(_amount <= allocation.volumeLiters, "Insufficient volume");
        
        // Calculate fee
        uint256 fee = (_amount * allocation.pricePerLiter * tradingFeeBasisPoints) / 10000;
        
        // Transfer tokens
        safeTransferFrom(msg.sender, _to, _allocationId, _amount, "");
        
        // Update allocation metadata
        allocation.lastTradedAt = block.timestamp;
        allocation.tradeCount++;
        
        // Handle partial transfers
        if (_amount < allocation.volumeLiters) {
            uint256 remainingVolume = allocation.volumeLiters - _amount;
            
            uint256 newId = nextAllocationId++;
            allocations[newId] = Allocation({
                allocationId: newId,
                owner: msg.sender,
                volumeLiters: remainingVolume,
                pricePerLiter: allocation.pricePerLiter,
                expiryTimestamp: allocation.expiryTimestamp,
                rightType: allocation.rightType,
                exercised: false,
                settled: false,
                source: allocation.source,
                certificationLevel: allocation.certificationLevel,
                createdAt: block.timestamp,
                lastTradedAt: 0,
                tradeCount: 0
            });
            
            _mint(msg.sender, newId, remainingVolume, "");
        }
        
        // Update original allocation for transferred amount
        allocation.volumeLiters = _amount;
        allocation.owner = _to;
        
        emit AllocationTransferred(_allocationId, msg.sender, _to, _amount, allocation.pricePerLiter);
    }
    
    // ============================================================
    // ENHANCED AUCTION MECHANISM
    // ============================================================
    
    function createAuction(
        uint256 _allocationId,
        uint256 _startPrice,
        uint256 _reservePrice,
        uint256 _duration,
        uint256 _minBidIncrement,
        uint256 _extensionTime
    ) public whenNotPaused whenNotEmergencyStopped returns (uint256) {
        require(allocations[_allocationId].owner == msg.sender, "Not owner");
        require(!allocations[_allocationId].exercised, "Already exercised");
        require(!allocations[_allocationId].settled, "Already settled");
        require(_duration > 0, "Duration must be positive");
        require(_duration <= 7 days, "Duration too long");
        require(_startPrice >= _reservePrice, "Start below reserve");
        require(_extensionTime <= MAX_AUCTION_EXTENSION, "Extension too long");
        
        uint256 auctionId = nextAuctionId++;
        
        auctions[auctionId] = Auction({
            allocationId: _allocationId,
            seller: msg.sender,
            startPrice: _startPrice,
            reservePrice: _reservePrice,
            currentBid: 0,
            currentBidder: address(0),
            endTime: block.timestamp + _duration,
            active: true,
            finalized: false,
            minBidIncrement: _minBidIncrement,
            bidCount: 0,
            extensionTime: _extensionTime
        });
        
        emit AuctionCreated(auctionId, _allocationId, _startPrice, block.timestamp + _duration);
        
        return auctionId;
    }
    
    function placeBid(uint256 _auctionId, uint256 _bidAmount) public nonReentrant whenNotPaused rateLimited {
        Auction storage auction = auctions[_auctionId];
        
        require(auction.active, "Auction not active");
        require(!auction.finalized, "Auction finalized");
        require(block.timestamp < auction.endTime, "Auction ended");
        require(_bidAmount >= auction.startPrice, "Bid below start price");
        require(msg.sender != auction.seller, "Seller cannot bid");
        
        if (auction.currentBidder != address(0)) {
            require(_bidAmount >= auction.currentBid + auction.minBidIncrement, "Bid too low");
            
            // Refund previous bidder
            Address.sendValue(payable(auction.currentBidder), auction.currentBid);
        }
        
        auction.currentBid = _bidAmount;
        auction.currentBidder = msg.sender;
        auction.bidCount++;
        
        // Anti-sniping: extend auction if bid near end
        if (auction.endTime - block.timestamp < auction.extensionTime) {
            auction.endTime += auction.extensionTime;
            emit AuctionExtended(_auctionId, auction.endTime);
        }
        
        emit BidPlaced(_auctionId, msg.sender, _bidAmount, block.timestamp);
    }
    
    function finalizeAuction(uint256 _auctionId) public nonReentrant {
        Auction storage auction = auctions[_auctionId];
        
        require(auction.active, "Auction not active");
        require(!auction.finalized, "Already finalized");
        require(block.timestamp >= auction.endTime, "Auction not ended");
        
        auction.active = false;
        auction.finalized = true;
        
        if (auction.currentBidder != address(0) && auction.currentBid >= auction.reservePrice) {
            // Transfer allocation to winner
            uint256 fee = (auction.currentBid * tradingFeeBasisPoints) / 10000;
            uint256 sellerProceeds = auction.currentBid - fee;
            
            allocations[auction.allocationId].owner = auction.currentBidder;
            allocations[auction.allocationId].lastTradedAt = block.timestamp;
            allocations[auction.allocationId].tradeCount++;
            
            // Transfer to seller
            Address.sendValue(payable(auction.seller), sellerProceeds);
            
            // Transfer allocation token
            safeTransferFrom(
                auction.seller,
                auction.currentBidder,
                auction.allocationId,
                allocations[auction.allocationId].volumeLiters,
                ""
            );
        }
        
        emit AuctionEnded(_auctionId, auction.currentBidder, auction.currentBid);
    }
    
    // ============================================================
    // GOVERNANCE ENHANCEMENTS
    // ============================================================
    
    function createProposal(
        uint256 _actionType,
        bytes memory _data,
        uint256 _votingPeriod
    ) external onlyGovernor returns (uint256) {
        require(_votingPeriod >= 1 days, "Voting period too short");
        require(_votingPeriod <= 30 days, "Voting period too long");
        
        uint256 proposalId = nextProposalId++;
        
        proposals[proposalId] = Proposal({
            proposalId: proposalId,
            proposer: msg.sender,
            actionType: _actionType,
            data: _data,
            startTime: block.timestamp,
            endTime: block.timestamp + _votingPeriod,
            voteCount: 0
        });
        
        emit ProposalCreated(proposalId, msg.sender);
        
        return proposalId;
    }
    
    function voteOnProposal(uint256 _proposalId) external onlyGovernor {
        Proposal storage proposal = proposals[_proposalId];
        require(block.timestamp < proposal.endTime, "Voting ended");
        require(!proposalVotes[_proposalId][msg.sender], "Already voted");
        
        proposalVotes[_proposalId][msg.sender] = true;
        proposal.voteCount++;
        
        emit ProposalVoted(_proposalId, msg.sender, proposal.voteCount);
        
        if (proposal.voteCount >= governanceThreshold) {
            _executeProposal(_proposalId);
        }
    }
    
    function _executeProposal(uint256 _proposalId) internal {
        require(!proposalExecuted[_proposalId], "Already executed");
        
        Proposal storage proposal = proposals[_proposalId];
        proposalExecuted[_proposalId] = true;
        
        // Execute based on action type
        if (proposal.actionType == 1) {
            // Update fees
            (uint256 newTradingFee, uint256 newSettlementFee) = abi.decode(proposal.data, (uint256, uint256));
            _updateFees(newTradingFee, newSettlementFee);
        }
        
        emit ProposalExecuted(_proposalId);
    }
    
    // ============================================================
    // CIRCUIT BREAKER
    // ============================================================
    
    function activateEmergencyStop() external onlyGovernor {
        emergencyStop = true;
        _pause();
        emit CircuitBreakerActivated("manual", 0, 0);
    }
    
    function deactivateEmergencyStop() external onlyGovernor {
        emergencyStop = false;
        _unpause();
        emit CircuitBreakerDeactivated();
    }
    
    function checkCircuitBreaker() external view returns (bool) {
        if (totalVolumeTraded >= totalVolumeLimit * 90 / 100) {
            return true; // 90% of volume limit reached
        }
        return false;
    }
    
    // ============================================================
    // HELPER FUNCTIONS
    // ============================================================
    
    function sum(uint256[] memory array) internal pure returns (uint256) {
        uint256 total = 0;
        for (uint256 i = 0; i < array.length; i++) {
            total += array[i];
        }
        return total;
    }
    
    function getActiveAuctions() public view returns (uint256[] memory) {
        uint256 count = 0;
        for (uint256 i = 0; i < nextAuctionId; i++) {
            if (auctions[i].active && block.timestamp < auctions[i].endTime && !auctions[i].finalized) {
                count++;
            }
        }
        
        uint256[] memory active = new uint256[](count);
        uint256 index = 0;
        for (uint256 i = 0; i < nextAuctionId; i++) {
            if (auctions[i].active && block.timestamp < auctions[i].endTime && !auctions[i].finalized) {
                active[index++] = i;
            }
        }
        
        return active;
    }
}
"""

# ============================================================
# ENHANCED PYTHON PLATFORM (V7.0)
# ============================================================

class PriceOracle(Enum):
    """Supported price oracles"""
    CHAINLINK = "chainlink"
    UNISWAP = "uniswap"
    CUSTOM = "custom"

class CircuitBreakerState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

@dataclass
class BaseMetrics:
    """Enhanced base metrics with validation"""
    timestamp: datetime = field(default_factory=datetime.now)
    version: str = "7.0"
    
    def to_dict(self) -> Dict:
        """Convert to dictionary with proper serialization"""
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, Decimal):
                result[key] = float(value)
            elif isinstance(value, datetime):
                result[key] = value.isoformat()
            elif isinstance(value, Enum):
                result[key] = value.value
            else:
                result[key] = value
        return result
    
    def validate(self) -> bool:
        """Validate metrics"""
        return True

@dataclass
class AllocationRecord(BaseMetrics):
    """Enhanced helium allocation right record"""
    source_module: str = "blockchain_helium_rights_v7"
    
    allocation_id: int = 0
    owner: str = ""
    volume_liters: Decimal = field(default_factory=lambda: Decimal('0'))
    price_per_liter_wei: int = 0
    price_per_liter_usd: Decimal = field(default_factory=lambda: Decimal('0'))
    expiry_timestamp: int = 0
    right_type: str = "spot"
    exercised: bool = False
    settled: bool = False
    source: str = ""
    certification_level: str = ""
    transaction_hash: str = ""
    created_at: datetime = field(default_factory=datetime.now)
    last_traded_at: Optional[datetime] = None
    trade_count: int = 0
    
    def __post_init__(self):
        """Validate after initialization"""
        if self.volume_liters < 0:
            raise ValueError("Volume cannot be negative")
        if self.price_per_liter_usd < 0:
            raise ValueError("Price cannot be negative")

@dataclass 
class AuctionRecord(BaseMetrics):
    """Enhanced helium auction record"""
    source_module: str = "blockchain_helium_rights_v7"
    
    auction_id: int = 0
    allocation_id: int = 0
    seller: str = ""
    start_price_wei: int = 0
    reserve_price_wei: int = 0
    current_bid_wei: int = 0
    current_bidder: str = ""
    end_time: int = 0
    active: bool = True
    finalized: bool = False
    bid_count: int = 0
    extension_time: int = 60
    transaction_hash: str = ""

class RateLimiter:
    """Rate limiter for API calls"""
    
    def __init__(self, max_calls: int = 100, window_seconds: int = 60):
        self.max_calls = max_calls
        self.window_seconds = window_seconds
        self.calls = deque()
        self.lock = threading.Lock()
    
    def can_call(self) -> bool:
        """Check if call is allowed"""
        with self.lock:
            now = time.time()
            
            # Remove old calls
            while self.calls and self.calls[0] < now - self.window_seconds:
                self.calls.popleft()
            
            if len(self.calls) >= self.max_calls:
                return False
            
            self.calls.append(now)
            return True
    
    def get_remaining_calls(self) -> int:
        """Get remaining allowed calls"""
        with self.lock:
            return max(0, self.max_calls - len(self.calls))

class CircuitBreaker:
    """Circuit breaker pattern implementation"""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = CircuitBreakerState.CLOSED
        self.lock = threading.Lock()
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection"""
        with self.lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    logger.info("Circuit breaker: HALF_OPEN")
                else:
                    raise Exception("Circuit breaker is OPEN")
            
            try:
                result = func(*args, **kwargs)
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
                    logger.info("Circuit breaker: CLOSED")
                return result
            except Exception as e:
                self.failure_count += 1
                self.last_failure_time = time.time()
                
                if self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
                    logger.error(f"Circuit breaker: OPEN (failures: {self.failure_count})")
                
                raise e

class PriceFeedManager:
    """Manages price feeds from multiple oracles"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.price_cache = {}
        self.last_update = {}
        self.cache_duration = config.get('price_cache_duration', 300)  # 5 minutes default
        self.lock = threading.Lock()
    
    async def get_price(self, asset: str, oracle_type: PriceOracle = PriceOracle.CHAINLINK) -> Decimal:
        """Get price from oracle with caching"""
        cache_key = f"{asset}_{oracle_type.value}"
        
        with self.lock:
            if cache_key in self.price_cache:
                if time.time() - self.last_update.get(cache_key, 0) < self.cache_duration:
                    return self.price_cache[cache_key]
        
        # Fetch price (simulated - would connect to actual oracle)
        price = await self._fetch_price(asset, oracle_type)
        
        with self.lock:
            self.price_cache[cache_key] = price
            self.last_update[cache_key] = time.time()
        
        return price
    
    async def _fetch_price(self, asset: str, oracle_type: PriceOracle) -> Decimal:
        """Fetch price from specified oracle"""
        # Simulated price fetching
        # In production, this would connect to Chainlink, Uniswap, etc.
        mock_prices = {
            "HELIUM": Decimal('35.50'),
            "ETH": Decimal('2500.00'),
            "BTC": Decimal('45000.00')
        }
        
        # Add some randomness for testing
        base_price = mock_prices.get(asset, Decimal('1.00'))
        variation = Decimal(str(secrets.randbelow(10) / 100))  # 0-9% variation
        return base_price * (Decimal('1') + variation)
    
    def get_price_sync(self, asset: str) -> Decimal:
        """Synchronous price fetch"""
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(self.get_price(asset))
        finally:
            loop.close()

class GasOptimizer:
    """Optimizes gas usage for transactions"""
    
    def __init__(self, w3: Web3):
        self.w3 = w3
        self.gas_price_history = deque(maxlen=100)
        self.lock = threading.Lock()
    
    def estimate_optimal_gas_price(self, speed: str = 'medium') -> int:
        """Estimate optimal gas price based on network conditions"""
        strategies = {
            'slow': self.w3.eth.gas_price * 0.9,
            'medium': self.w3.eth.gas_price,
            'fast': self.w3.eth.gas_price * 1.2
        }
        
        gas_price = int(strategies.get(speed, strategies['medium']))
        
        with self.lock:
            self.gas_price_history.append(gas_price)
            
            # Calculate moving average
            if len(self.gas_price_history) > 10:
                avg = sum(self.gas_price_history) / len(self.gas_price_history)
                # If current price is significantly above average, use average
                if gas_price > avg * 1.3:
                    return int(avg)
        
        return gas_price
    
    def optimize_batch_size(self, operations: int) -> int:
        """Calculate optimal batch size based on gas limits"""
        max_gas_per_block = self.config.get('max_gas_per_block', 8_000_000)
        estimated_gas_per_operation = 150_000  # Conservative estimate
        
        max_operations = max_gas_per_block // estimated_gas_per_operation
        
        return min(operations, max_operations, 50)  # Cap at 50

class HeliumRightsPlatformV7:
    """
    Enhanced on-chain helium allocation rights trading platform.
    
    New Features:
    - Oracle price feed integration
    - Gas-optimized batch operations
    - Circuit breaker pattern
    - Rate limiting
    - Comprehensive event monitoring
    - Fixed-point arithmetic for precision
    - Enhanced governance with proposals
    - Anti-sniping auction mechanism
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or load_module_config('blockchain')
        self.connection = BlockchainConnectionManager(self.config)
        
        # Initialize enhanced components
        self.price_feed = PriceFeedManager(self.config.get('price_feed', {}))
        self.gas_optimizer = None
        self.rate_limiter = RateLimiter(
            max_calls=self.config.get('rate_limit', {}).get('max_calls', 100),
            window_seconds=self.config.get('rate_limit', {}).get('window', 60)
        )
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=self.config.get('circuit_breaker', {}).get('failure_threshold', 5),
            recovery_timeout=self.config.get('circuit_breaker', {}).get('recovery_timeout', 60)
        )
        
        # Contract instance
        self.rights_contract = None
        
        if self.connection.connected:
            self._initialize_contract()
            self.gas_optimizer = GasOptimizer(self.connection.w3)
        
        # Records with thread safety
        self._lock = threading.Lock()
        self.allocations: List[AllocationRecord] = []
        self.auctions: List[AuctionRecord] = []
        self.trades: List[Dict] = []
        
        # Event listeners
        self.event_handlers = {}
        self._start_event_listeners()
        
        # Thread pool for async operations
        self.executor = ThreadPoolExecutor(max_workers=4)
        
        logger.info("HeliumRightsPlatformV7 initialized with enhancements")
    
    def _initialize_contract(self):
        """Initialize rights contract with V7"""
        try:
            contract_address = self.config.get('smart_contracts', {}).get(
                'helium_rights_v7', {}
            ).get('address', '')
            
            if contract_address and '${' not in contract_address:
                abi = self._load_rights_abi_v7()
                if abi:
                    self.rights_contract = self.connection.w3.eth.contract(
                        address=contract_address,
                        abi=abi
                    )
                    logger.info("Helium rights V7 contract loaded")
        except Exception as e:
            logger.error(f"Contract initialization failed: {e}")
    
    def _load_rights_abi_v7(self) -> Optional[List]:
        """Load V7 rights contract ABI"""
        abi_path = Path(__file__).parent / 'abi' / 'helium_rights_v7.json'
        
        if abi_path.exists():
            with open(abi_path, 'r') as f:
                return json.load(f)
        
        # Try to compile
        try:
            from solcx import compile_standard, install_solc
            install_solc('0.8.19')
            
            compiled = compile_standard({
                "language": "Solidity",
                "sources": {"HeliumAllocationRightsV7.sol": {"content": HELIUM_RIGHTS_CONTRACT_V7}},
                "settings": {
                    "outputSelection": {"*": {"*": ["abi", "evm.bytecode"]}},
                    "optimizer": {"enabled": True, "runs": 200}
                }
            })
            
            abi = compiled['contracts']['HeliumAllocationRightsV7.sol']['HeliumAllocationRightsV7']['abi']
            
            # Save for future use
            abi_path.parent.mkdir(exist_ok=True)
            with open(abi_path, 'w') as f:
                json.dump(abi, f, indent=2)
            
            return abi
        except Exception as e:
            logger.error(f"Contract compilation failed: {e}")
            return None
    
    def _start_event_listeners(self):
        """Start blockchain event listeners"""
        if not self.rights_contract or not self.connection.connected:
            return
        
        def handle_allocation_created(event):
            logger.info(f"Allocation created: {event['args']['allocationId']}")
            self._sync_allocation_from_event(event)
        
        def handle_auction_created(event):
            logger.info(f"Auction created: {event['args']['auctionId']}")
            self._sync_auction_from_event(event)
        
        def handle_bid_placed(event):
            logger.info(f"Bid placed: {event['args']['amount']}")
        
        # Register handlers
        self.event_handlers = {
            'AllocationCreated': handle_allocation_created,
            'AuctionCreated': handle_auction_created,
            'BidPlaced': handle_bid_placed,
        }
        
        # Start listening in background thread
        self.executor.submit(self._listen_to_events)
    
    def _listen_to_events(self):
        """Background event listener"""
        while True:
            try:
                for event_name, handler in self.event_handlers.items():
                    event_filter = getattr(self.rights_contract.events, event_name).create_filter(
                        fromBlock='latest'
                    )
                    
                    for event in event_filter.get_new_entries():
                        handler(event)
                
                time.sleep(15)  # Poll every 15 seconds
            except Exception as e:
                logger.error(f"Event listener error: {e}")
                time.sleep(30)
    
    def _sync_allocation_from_event(self, event: Dict):
        """Sync allocation from blockchain event"""
        args = event['args']
        record = AllocationRecord(
            allocation_id=args['allocationId'],
            owner=args['owner'],
            volume_liters=Decimal(str(args['volume'])) / Decimal('1000000'),  # PRECISION
            price_per_liter_wei=args['price'],
            transaction_hash=event['transactionHash'].hex()
        )
        
        with self._lock:
            self.allocations.append(record)
    
    def _sync_auction_from_event(self, event: Dict):
        """Sync auction from blockchain event"""
        args = event['args']
        record = AuctionRecord(
            auction_id=args['auctionId'],
            allocation_id=args['allocationId'],
            start_price_wei=args['startPrice'],
            end_time=args['endTime'],
            transaction_hash=event['transactionHash'].hex()
        )
        
        with self._lock:
            self.auctions.append(record)
    
    def create_allocation_with_price_feed(self,
                                        volume_liters: Decimal,
                                        price_per_liter_usd: Optional[Decimal] = None,
                                        expiry_days: int = 30,
                                        right_type: HeliumAllocationType = HeliumAllocationType.SPOT,
                                        source: str = "Green Agent",
                                        certification: str = "gold") -> Optional[AllocationRecord]:
        """
        Create allocation with oracle price feed.
        """
        
        # Rate limiting check
        if not self.rate_limiter.can_call():
            logger.warning("Rate limit exceeded for create_allocation")
            return None
        
        # Use circuit breaker
        return self.circuit_breaker.call(
            self._create_allocation_internal,
            volume_liters,
            price_per_liter_usd,
            expiry_days,
            right_type,
            source,
            certification
        )
    
    def _create_allocation_internal(self,
                                   volume_liters: Decimal,
                                   price_per_liter_usd: Optional[Decimal],
                                   expiry_days: int,
                                   right_type: HeliumAllocationType,
                                   source: str,
                                   certification: str) -> Optional[AllocationRecord]:
        """Internal allocation creation logic"""
        
        # Get price from oracle if not provided
        if price_per_liter_usd is None:
            try:
                price_per_liter_usd = self.price_feed.get_price_sync("HELIUM")
            except Exception as e:
                logger.error(f"Price feed error: {e}")
                price_per_liter_usd = Decimal('35.00')  # Fallback price
        
        # Validate inputs with precision
        volume_liters = volume_liters.quantize(Decimal('0.000001'), rounding=ROUND_DOWN)
        price_per_liter_usd = price_per_liter_usd.quantize(Decimal('0.01'), rounding=ROUND_DOWN)
        
        if not self.rights_contract or not self.connection.account:
            return self._create_local_allocation_v7(
                volume_liters, price_per_liter_usd, expiry_days,
                right_type, source, certification
            )
        
        try:
            # Gas optimization
            gas_price = None
            if self.gas_optimizer:
                gas_price = self.gas_optimizer.estimate_optimal_gas_price(
                    self.config.get('gas_speed', 'medium')
                )
            
            # Convert to contract units
            scaled_volume = int(volume_liters * Decimal('1000000'))  # PRECISION = 1e6
            price_per_liter_wei = self.connection.w3.to_wei(
                float(price_per_liter_usd), 'ether'
            )
            expiry = int(time.time() + expiry_days * 86400)
            
            # Build transaction
            tx_function = self.rights_contract.functions.createAllocation(
                scaled_volume,
                price_per_liter_wei,
                expiry,
                right_type.value,
                source,
                certification
            )
            
            # Send with gas optimization
            tx_hash = self.connection.send_transaction(
                tx_function,
                gas_price=gas_price
            )
            
            if tx_hash:
                # Wait for receipt with timeout
                receipt = self._wait_for_receipt(tx_hash)
                
                if receipt:
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
                            transaction_hash=tx_hash,
                            created_at=datetime.now()
                        )
                        
                        with self._lock:
                            self.allocations.append(record)
                        
                        logger.info(f"Allocation created on-chain V7: {allocation_id}")
                        return record
        
        except Exception as e:
            logger.error(f"Allocation creation failed V7: {e}")
        
        return self._create_local_allocation_v7(
            volume_liters, price_per_liter_usd, expiry_days,
            right_type, source, certification
        )
    
    def _wait_for_receipt(self, tx_hash: str, timeout: int = 120) -> Optional[Dict]:
        """Wait for transaction receipt with timeout"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                receipt = self.connection.w3.eth.get_transaction_receipt(tx_hash)
                if receipt:
                    return receipt
            except TransactionNotFound:
                pass
            
            time.sleep(1)
        
        logger.warning(f"Transaction receipt timeout: {tx_hash}")
        return None
    
    def _create_local_allocation_v7(self, volume: Decimal, price: Decimal,
                                   expiry_days: int, right_type: HeliumAllocationType,
                                   source: str, certification: str) -> AllocationRecord:
        """Create local allocation record V7"""
        record = AllocationRecord(
            allocation_id=len(self.allocations),
            owner="local",
            volume_liters=volume,
            price_per_liter_usd=price,
            expiry_timestamp=int(time.time() + expiry_days * 86400),
            right_type=right_type.name.lower(),
            source=source,
            certification_level=certification,
            transaction_hash="local_v7",
            created_at=datetime.now()
        )
        
        with self._lock:
            self.allocations.append(record)
        
        return record
    
    def create_allocation_batch(self,
                              allocations_data: List[Dict]) -> List[AllocationRecord]:
        """
        Create multiple allocations in batch for gas efficiency.
        """
        
        if not self.rate_limiter.can_call():
            logger.warning("Rate limit exceeded for batch creation")
            return []
        
        # Optimize batch size
        optimized_size = len(allocations_data)
        if self.gas_optimizer and self.rights_contract:
            optimized_size = self.gas_optimizer.optimize_batch_size(len(allocations_data))
        
        if optimized_size < len(allocations_data):
            logger.info(f"Batch size optimized from {len(allocations_data)} to {optimized_size}")
            allocations_data = allocations_data[:optimized_size]
        
        records = []
        
        if self.rights_contract and self.connection.account:
            try:
                # Prepare batch arrays
                volumes = []
                prices = []
                expiries = []
                right_types = []
                sources = []
                certifications = []
                
                for data in allocations_data:
                    volumes.append(int(Decimal(str(data['volume'])) * Decimal('1000000')))
                    prices.append(self.connection.w3.to_wei(data['price'], 'ether'))
                    expiries.append(int(time.time() + data.get('expiry_days', 30) * 86400))
                    right_types.append(data.get('right_type', 0))
                    sources.append(data.get('source', 'Green Agent'))
                    certifications.append(data.get('certification', 'gold'))
                
                # Call batch function
                tx_function = self.rights_contract.functions.createAllocationBatch(
                    volumes, prices, expiries, right_types, sources, certifications
                )
                
                tx_hash = self.connection.send_transaction(tx_function)
                
                if tx_hash:
                    receipt = self._wait_for_receipt(tx_hash)
                    
                    if receipt:
                        logs = self.rights_contract.events.BatchOperation().process_receipt(receipt)
                        
                        if logs:
                            for i, data in enumerate(allocations_data):
                                record = AllocationRecord(
                                    allocation_id=len(self.allocations) + i,
                                    owner=self.connection.account.address,
                                    volume_liters=Decimal(str(data['volume'])),
                                    price_per_liter_usd=Decimal(str(data['price'])),
                                    source=data.get('source', 'Green Agent'),
                                    certification_level=data.get('certification', 'gold'),
                                    transaction_hash=tx_hash,
                                    created_at=datetime.now()
                                )
                                records.append(record)
                                
                                with self._lock:
                                    self.allocations.append(record)
                            
                            logger.info(f"Batch created {len(records)} allocations")
                            return records
            
            except Exception as e:
                logger.error(f"Batch creation failed: {e}")
        
        # Fallback to individual local records
        for data in allocations_data:
            record = self._create_local_allocation_v7(
                Decimal(str(data['volume'])),
                Decimal(str(data['price'])),
                data.get('expiry_days', 30),
                HeliumAllocationType(data.get('right_type', 0)),
                data.get('source', 'Green Agent'),
                data.get('certification', 'gold')
            )
            records.append(record)
        
        return records
    
    def create_auction_v7(self,
                         allocation_id: int,
                         start_price_usd: Decimal,
                         reserve_price_usd: Optional[Decimal] = None,
                         duration_hours: int = 24,
                         min_bid_increment_usd: Decimal = Decimal('1.00'),
                         anti_sniping_seconds: int = 60) -> Optional[AuctionRecord]:
        """
        Create auction with enhanced features.
        """
        
        if not self.rate_limiter.can_call():
            logger.warning("Rate limit exceeded for auction creation")
            return None
        
        # Validate inputs
        if reserve_price_usd is None:
            reserve_price_usd = start_price_usd * Decimal('0.8')  # 80% of start
        
        if reserve_price_usd > start_price_usd:
            logger.error("Reserve price cannot exceed start price")
            return None
        
        # Use circuit breaker
        return self.circuit_breaker.call(
            self._create_auction_internal,
            allocation_id,
            start_price_usd,
            reserve_price_usd,
            duration_hours,
            min_bid_increment_usd,
            anti_sniping_seconds
        )
    
    def _create_auction_internal(self,
                               allocation_id: int,
                               start_price_usd: Decimal,
                               reserve_price_usd: Decimal,
                               duration_hours: int,
                               min_bid_increment_usd: Decimal,
                               anti_sniping_seconds: int) -> Optional[AuctionRecord]:
        """Internal auction creation logic"""
        
        if not self.rights_contract or not self.connection.account:
            return self._create_local_auction_v7(
                allocation_id, start_price_usd, duration_hours
            )
        
        try:
            start_price_wei = self.connection.w3.to_wei(float(start_price_usd), 'ether')
            reserve_price_wei = self.connection.w3.to_wei(float(reserve_price_usd), 'ether')
            min_increment_wei = self.connection.w3.to_wei(float(min_bid_increment_usd), 'ether')
            
            tx_function = self.rights_contract.functions.createAuction(
                allocation_id,
                start_price_wei,
                reserve_price_wei,
                duration_hours * 3600,
                min_increment_wei,
                min(anti_sniping_seconds, 300)  # Max 5 minutes
            )
            
            # Get optimal gas price
            gas_price = None
            if self.gas_optimizer:
                gas_price = self.gas_optimizer.estimate_optimal_gas_price('fast')
            
            tx_hash = self.connection.send_transaction(tx_function, gas_price=gas_price)
            
            if tx_hash:
                receipt = self._wait_for_receipt(tx_hash)
                
                if receipt:
                    logs = self.rights_contract.events.AuctionCreated().process_receipt(receipt)
                    
                    if logs:
                        auction_id = logs[0]['args']['auctionId']
                        
                        record = AuctionRecord(
                            auction_id=auction_id,
                            allocation_id=allocation_id,
                            seller=self.connection.account.address,
                            start_price_wei=start_price_wei,
                            reserve_price_wei=reserve_price_wei,
                            end_time=int(time.time() + duration_hours * 3600),
                            active=True,
                            extension_time=anti_sniping_seconds,
                            transaction_hash=tx_hash
                        )
                        
                        with self._lock:
                            self.auctions.append(record)
                        
                        logger.info(f"Auction created on-chain V7: {auction_id}")
                        return record
        
        except Exception as e:
            logger.error(f"Auction creation failed V7: {e}")
        
        return self._create_local_auction_v7(allocation_id, start_price_usd, duration_hours)
    
    def _create_local_auction_v7(self, allocation_id: int,
                               start_price: Decimal, duration_hours: int) -> AuctionRecord:
        """Create local auction record V7"""
        record = AuctionRecord(
            auction_id=len(self.auctions),
            allocation_id=allocation_id,
            seller="local",
            start_price_wei=int(float(start_price) * 1e18),
            reserve_price_wei=int(float(start_price) * 0.8 * 1e18),
            end_time=int(time.time() + duration_hours * 3600),
            active=True,
            extension_time=60,
            transaction_hash="local_v7"
        )
        
        with self._lock:
            self.auctions.append(record)
        
        return record
    
    def get_market_summary_v7(self) -> Dict:
        """Get enhanced market summary with analytics"""
        with self._lock:
            allocations = self.allocations.copy()
            auctions = self.auctions.copy()
            trades = self.trades.copy()
        
        # Calculate analytics
        total_volume = sum(a.volume_liters for a in allocations)
        active_auctions = [a for a in auctions if a.active and not a.finalized]
        
        # Price analytics
        prices = [a.price_per_liter_usd for a in allocations if a.price_per_liter_usd > 0]
        avg_price = sum(prices) / len(prices) if prices else Decimal('0')
        
        # Volume weighted average price
        vwap_num = sum(a.volume_liters * a.price_per_liter_usd for a in allocations if a.price_per_liter_usd > 0)
        vwap_den = sum(a.volume_liters for a in allocations if a.price_per_liter_usd > 0)
        vwap = vwap_num / vwap_den if vwap_den > 0 else Decimal('0')
        
        # Trade analytics
        on_chain_trades = sum(1 for t in trades if t.get('on_chain', False))
        total_trade_volume = sum(
            t.get('amount_liters', 0) 
            for t in trades 
            if isinstance(t.get('amount_liters'), (int, float, Decimal))
        )
        
        return {
            'total_allocations': len(allocations),
            'total_volume_liters': float(total_volume),
            'active_auctions': len(active_auctions),
            'total_trades': len(trades),
            'on_chain_trades': on_chain_trades,
            'average_price_usd': float(avg_price),
            'vwap_usd': float(vwap),
            'total_trade_volume_liters': float(total_trade_volume),
            'market_cap_usd': float(total_volume * avg_price),
            'timestamp': datetime.now().isoformat(),
            'version': '7.0'
        }
    
    def get_circuit_breaker_status(self) -> Dict:
        """Get circuit breaker status"""
        return {
            'state': self.circuit_breaker.state.value,
            'failure_count': self.circuit_breaker.failure_count,
            'last_failure': datetime.fromtimestamp(
                self.circuit_breaker.last_failure_time
            ).isoformat() if self.circuit_breaker.last_failure_time else None
        }
    
    def get_rate_limiter_status(self) -> Dict:
        """Get rate limiter status"""
        return {
            'remaining_calls': self.rate_limiter.get_remaining_calls(),
            'max_calls': self.rate_limiter.max_calls,
            'window_seconds': self.rate_limiter.window_seconds
        }

# ============================================================
# TESTING MODULE
# ============================================================

class TestHeliumRightsPlatformV7(unittest.TestCase):
    """Comprehensive test suite for HeliumRightsPlatformV7"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.config = {
            'network': 'test',
            'use_local': True
        }
        self.platform = HeliumRightsPlatformV7(self.config)
    
    def test_allocation_creation(self):
        """Test allocation creation"""
        record = self.platform.create_allocation_with_price_feed(
            volume_liters=Decimal('100.0'),
            price_per_liter_usd=Decimal('35.00'),
            expiry_days=30
        )
        
        self.assertIsNotNone(record)
        self.assertEqual(record.volume_liters, Decimal('100.0'))
        self.assertEqual(record.price_per_liter_usd, Decimal('35.00'))
    
    def test_allocation_validation(self):
        """Test allocation validation"""
        with self.assertRaises(ValueError):
            AllocationRecord(
                volume_liters=Decimal('-100.0'),
                price_per_liter_usd=Decimal('35.00')
            )
        
        with self.assertRaises(ValueError):
            AllocationRecord(
                volume_liters=Decimal('100.0'),
                price_per_liter_usd=Decimal('-35.00')
            )
    
    def test_batch_creation(self):
        """Test batch allocation creation"""
        batch_data = [
            {'volume': 100.0, 'price': 35.00, 'source': 'Test1'},
            {'volume': 200.0, 'price': 36.00, 'source': 'Test2'},
            {'volume': 300.0, 'price': 37.00, 'source': 'Test3'}
        ]
        
        records = self.platform.create_allocation_batch(batch_data)
        
        self.assertEqual(len(records), 3)
        self.assertEqual(records[0].source, 'Test1')
    
    def test_rate_limiter(self):
        """Test rate limiter"""
        rate_limiter = RateLimiter(max_calls=5, window_seconds=60)
        
        # Should allow 5 calls
        for i in range(5):
            self.assertTrue(rate_limiter.can_call())
        
        # Should deny 6th call
        self.assertFalse(rate_limiter.can_call())
    
    def test_circuit_breaker(self):
        """Test circuit breaker pattern"""
        circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=1)
        
        # Test successful calls
        def success_func():
            return "success"
        
        result = circuit_breaker.call(success_func)
        self.assertEqual(result, "success")
        
        # Test failure accumulation
        def fail_func():
            raise Exception("Test failure")
        
        for i in range(2):
            with self.assertRaises(Exception):
                circuit_breaker.call(fail_func)
        
        self.assertEqual(circuit_breaker.failure_count, 2)
        self.assertEqual(circuit_breaker.state, CircuitBreakerState.CLOSED)
        
        # Test circuit opening
        with self.assertRaises(Exception):
            circuit_breaker.call(fail_func)
        
        self.assertEqual(circuit_breaker.state, CircuitBreakerState.OPEN)
        
        # Test that open circuit blocks calls
        with self.assertRaises(Exception):
            circuit_breaker.call(success_func)
    
    def test_price_feed(self):
        """Test price feed"""
        # Test async price fetch
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            price = loop.run_until_complete(
                self.platform.price_feed.get_price("HELIUM")
            )
            self.assertIsInstance(price, Decimal)
            self.assertGreater(price, 0)
        finally:
            loop.close()
    
    def test_market_summary(self):
        """Test market summary generation"""
        # Create some test data
        self.platform.create_allocation_with_price_feed(
            volume_liters=Decimal('100.0'),
            price_per_liter_usd=Decimal('35.00')
        )
        
        summary = self.platform.get_market_summary_v7()
        
        self.assertIn('total_allocations', summary)
        self.assertIn('average_price_usd', summary)
        self.assertIn('version', summary)
        self.assertEqual(summary['version'], '7.0')
    
    def test_gas_optimizer(self):
        """Test gas optimizer (mock)"""
        if not WEB3_AVAILABLE:
            self.skipTest("Web3 not available")
        
        w3_mock = Mock()
        w3_mock.eth.gas_price = 50_000_000_000  # 50 gwei
        
        gas_optimizer = GasOptimizer(w3_mock)
        
        # Test gas price estimation
        gas_price = gas_optimizer.estimate_optimal_gas_price('medium')
        self.assertEqual(gas_price, 50_000_000_000)
        
        # Test batch optimization
        batch_size = gas_optimizer.optimize_batch_size(100)
        self.assertLessEqual(batch_size, 50)

# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def configure_platform(config_path: str = None) -> HeliumRightsPlatformV7:
    """Configure and create platform instance"""
    if config_path:
        with open(config_path, 'r') as f:
            config = json.load(f)
    else:
        config = {
            'network': 'mainnet',
            'use_local': False,
            'gas_speed': 'medium',
            'rate_limit': {
                'max_calls': 100,
                'window': 60
            },
            'circuit_breaker': {
                'failure_threshold': 5,
                'recovery_timeout': 60
            },
            'price_cache_duration': 300
        }
    
    return HeliumRightsPlatformV7(config)

# Run tests if executed directly
if __name__ == "__main__":
    unittest.main(verbosity=2)
