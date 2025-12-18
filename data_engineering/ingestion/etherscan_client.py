"""
Etherscan API client for blockchain data ingestion.

This module provides a robust client for interacting with the Etherscan API,
including rate limiting, error handling, and pagination support.
"""

import time
import requests
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from .config import CONFIG
from .utils import setup_logger, retry_with_backoff, normalize_address


class EtherscanClient:
    """
    Client for interacting with the Etherscan API.
    
    Features:
    - Rate limiting to respect API limits
    - Automatic retry with exponential backoff
    - Pagination handling for large datasets
    - Response validation
    """
    
    def __init__(self, api_key: str = None):
        """
        Initialize Etherscan client.
        
        Args:
            api_key: Etherscan API key (defaults to config)
        """
        self.api_key = api_key or CONFIG.etherscan.api_key
        self.base_url = CONFIG.etherscan.base_url
        self.rate_limit = CONFIG.etherscan.rate_limit
        self.timeout = CONFIG.etherscan.timeout
        self.logger = setup_logger(__name__)
        
        self._last_request_time = 0
        self._request_interval = 1.0 / self.rate_limit
    
    def _rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._request_interval:
            time.sleep(self._request_interval - elapsed)
        self._last_request_time = time.time()
    
    @retry_with_backoff(max_retries=3, base_delay=2.0)
    def _make_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Make a rate-limited request to Etherscan API.
        
        Args:
            params: Request parameters
            
        Returns:
            Dict: API response data
            
        Raises:
            Exception: If API returns error or request fails
        """
        self._rate_limit()
        
        params["apikey"] = self.api_key
        
        response = requests.get(
            self.base_url,
            params=params,
            timeout=self.timeout
        )
        response.raise_for_status()
        
        data = response.json()
        
        # Check for API errors
        if data.get("status") == "0":
            message = data.get("message", "Unknown error")
            result = data.get("result", "")
            
            # "No transactions found" is not an error
            if "No transactions found" in str(result):
                return {"status": "1", "result": []}
            
            raise Exception(f"Etherscan API error: {message} - {result}")
        
        return data
    
    def get_transactions_by_address(
        self,
        address: str,
        start_block: int = 0,
        end_block: int = 99999999,
        page: int = 1,
        offset: int = 10000,
        sort: str = "asc"
    ) -> List[Dict]:
        """
        Get normal transactions for an address.
        
        Args:
            address: Ethereum address
            start_block: Start block number
            end_block: End block number
            page: Page number for pagination
            offset: Number of results per page (max 10000)
            sort: Sort order ('asc' or 'desc')
            
        Returns:
            List[Dict]: List of transaction records
        """
        params = {
            "module": "account",
            "action": "txlist",
            "address": normalize_address(address),
            "startblock": start_block,
            "endblock": end_block,
            "page": page,
            "offset": offset,
            "sort": sort
        }
        
        response = self._make_request(params)
        return response.get("result", [])
    
    def get_transactions_by_block_range(
        self,
        start_block: int,
        end_block: int,
        address: str = None
    ) -> List[Dict]:
        """
        Get transactions within a block range.
        
        Note: Etherscan API doesn't support block range queries directly,
        so this uses internal transactions endpoint with block filter.
        
        Args:
            start_block: Start block number
            end_block: End block number
            address: Optional address filter
            
        Returns:
            List[Dict]: List of transaction records
        """
        # For block range queries, we need to use a different approach
        # Using the 'txlistinternal' for internal transactions
        # or specific address queries
        
        if address:
            return self.get_transactions_by_address(
                address=address,
                start_block=start_block,
                end_block=end_block
            )
        
        self.logger.warning(
            "Block range query without address not directly supported. "
            "Consider using Web3 provider for block-level queries."
        )
        return []
    
    def get_internal_transactions(
        self,
        address: str,
        start_block: int = 0,
        end_block: int = 99999999,
        page: int = 1,
        offset: int = 10000
    ) -> List[Dict]:
        """
        Get internal transactions for an address.
        
        Args:
            address: Ethereum address
            start_block: Start block number
            end_block: End block number
            page: Page number
            offset: Results per page
            
        Returns:
            List[Dict]: List of internal transaction records
        """
        params = {
            "module": "account",
            "action": "txlistinternal",
            "address": normalize_address(address),
            "startblock": start_block,
            "endblock": end_block,
            "page": page,
            "offset": offset,
            "sort": "asc"
        }
        
        response = self._make_request(params)
        return response.get("result", [])
    
    def get_token_transfers(
        self,
        address: str,
        contract_address: str = None,
        start_block: int = 0,
        end_block: int = 99999999,
        page: int = 1,
        offset: int = 10000
    ) -> List[Dict]:
        """
        Get ERC-20 token transfer events.
        
        Args:
            address: Ethereum address
            contract_address: Optional token contract address filter
            start_block: Start block number
            end_block: End block number
            page: Page number
            offset: Results per page
            
        Returns:
            List[Dict]: List of token transfer records
        """
        params = {
            "module": "account",
            "action": "tokentx",
            "address": normalize_address(address),
            "startblock": start_block,
            "endblock": end_block,
            "page": page,
            "offset": offset,
            "sort": "asc"
        }
        
        if contract_address:
            params["contractaddress"] = normalize_address(contract_address)
        
        response = self._make_request(params)
        return response.get("result", [])
    
    def get_block_by_number(self, block_number: int) -> Dict:
        """
        Get block information by block number.
        
        Args:
            block_number: Block number
            
        Returns:
            Dict: Block information
        """
        params = {
            "module": "block",
            "action": "getblockreward",
            "blockno": block_number
        }
        
        response = self._make_request(params)
        return response.get("result", {})
    
    def get_latest_block_number(self) -> int:
        """
        Get the latest block number.
        
        Returns:
            int: Latest block number
        """
        params = {
            "module": "proxy",
            "action": "eth_blockNumber"
        }
        
        response = self._make_request(params)
        result = response.get("result", "0x0")
        return int(result, 16)
    
    def get_address_balance(self, address: str) -> int:
        """
        Get ETH balance for an address in Wei.
        
        Args:
            address: Ethereum address
            
        Returns:
            int: Balance in Wei
        """
        params = {
            "module": "account",
            "action": "balance",
            "address": normalize_address(address),
            "tag": "latest"
        }
        
        response = self._make_request(params)
        return int(response.get("result", "0"))
    
    def get_multi_address_balance(self, addresses: List[str]) -> List[Dict]:
        """
        Get ETH balance for multiple addresses.
        
        Args:
            addresses: List of Ethereum addresses (max 20)
            
        Returns:
            List[Dict]: List of address balances
        """
        if len(addresses) > 20:
            self.logger.warning("Max 20 addresses per request. Truncating list.")
            addresses = addresses[:20]
        
        params = {
            "module": "account",
            "action": "balancemulti",
            "address": ",".join(normalize_address(a) for a in addresses),
            "tag": "latest"
        }
        
        response = self._make_request(params)
        return response.get("result", [])

