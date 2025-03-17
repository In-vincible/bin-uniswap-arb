import asyncio
import logging
import time  # Add this import for sleep functionality
from typing import List, Dict, Optional, Tuple
from web3 import Web3, AsyncWeb3  # Add AsyncWeb3 import

from config import Config

class TokenMonitor:
    """
    Asynchronously monitors blockchain network and token-related metrics.
    
    This class provides real-time monitoring of:
    - Network status (up/down)
    - Gas prices
    - Block production
    - Token-specific transaction metrics
    - Gas estimates for different operations (cached)
    
    The monitor caches all data for efficient access by external arbitrage calculations.
    
    Parameters:
        token_addresses (List[str]): List of token contract addresses to monitor.
                                   Native ETH (using a placeholder address) will be automatically added if not included.
        infura_url (str): URL for Ethereum node connection.
        v3_pool_addresses (List[str], optional): List of Uniswap V3 pool contract addresses to monitor.
        refresh_interval (int): How often to refresh data, in seconds (default: 60).
        blocks_to_analyze (int): Number of recent blocks to analyze for token metrics (default: 5).
    """
    def __init__(
        self, 
        token_addresses: List[str], 
        infura_url: str,
        v3_pool_addresses: List[str] = None,
        refresh_interval: int = 60,
        blocks_to_analyze: int = 5
    ):
        # Define ETH placeholder address used for identifying native ETH
        self.ETH_ADDRESS = "0x3e6b04c2f793d77d6414075aae1d44ef474b483e"
        
        # Convert all addresses to checksum format
        token_addrs = [Web3.to_checksum_address(addr) for addr in token_addresses]
        
        # Check if ETH is already in the token addresses list (case-insensitive check)
        if not any(addr.lower() == self.ETH_ADDRESS.lower() for addr in token_addrs):
            # If ETH is not included, add it automatically
            token_addrs.append(Web3.to_checksum_address(self.ETH_ADDRESS))
            logging.info("Native ETH monitoring automatically added")
            
        self.token_addresses = token_addrs
        self.refresh_interval = refresh_interval
        # Update to use both sync and async Web3 instances
        self.web3 = Web3(Web3.HTTPProvider(infura_url))
        self.async_web3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(infura_url))
        self.blocks_to_analyze = blocks_to_analyze
        
        # Store V3 pool addresses (with defaults if none provided)
        default_v3_pools = [
            "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",  # USDC-ETH 0.05%
            "0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8",  # USDC-ETH 0.3%
            "0x7BeA39867e4169DBe237d55C8242a8f2fcDcc387",  # USDC-ETH 1%
        ]
        if v3_pool_addresses:
            self.v3_pool_addresses = [Web3.to_checksum_address(addr) for addr in v3_pool_addresses]
        else:
            self.v3_pool_addresses = default_v3_pools
            
        logging.info(f"Monitoring {len(self.v3_pool_addresses)} V3 pools for gas estimation")
        
        # Data cache
        self.token_data: Dict[str, Dict] = {}
        self.network_status: bool = False
        self.current_gas_price: int = 0
        self.network_congestion: Dict[str, any] = {
            'avg_block_time': 0,
            'pending_tx_count': 0,
            'is_congested': False,
            'last_updated': 0
        }
        
        # Cache timestamps
        self.last_network_check: float = 0
        self.last_gas_price_check: float = 0
        self.last_token_check: Dict[str, float] = {}
        
        # Gas estimate caches
        self.cached_token_transfer_gas: Dict[str, Dict] = {}
        self.cached_v3_swap_gas: Dict[str, Dict] = {}
        self.cached_eth_wrap_gas: Dict = {}
        self.cached_eth_unwrap_gas: Dict = {}
        self.cached_token_wrap_gas: Dict[str, Dict] = {}
        self.cached_token_unwrap_gas: Dict[str, Dict] = {}
        
        # Cache refresh intervals (in seconds)
        self.gas_cache_refresh_interval = 300  # 5 minutes
        
        # Monitor task
        self._refresh_task = None
        self._gas_cache_refresh_task = None
        
        # Standard dummy address for gas estimations
        self.dummy_sender = "0x1111111111111111111111111111111111111111"
        self.dummy_recipient = "0x2222222222222222222222222222222222222222"
        
        logging.info("TokenMonitor initialized with %d tokens", len(self.token_addresses))

    async def start_monitoring(self):
        """
        Start the background monitoring loop.
        
        Returns:
            asyncio.Task: The background task that can be awaited or cancelled.
        """
        if self._refresh_task and not self._refresh_task.done():
            logging.warning("Monitoring already started")
            return self._refresh_task
            
        self._refresh_task = asyncio.create_task(self._refresh_loop())
        self._gas_cache_refresh_task = asyncio.create_task(self._refresh_gas_cache_loop())
        
        # Populate gas cache immediately
        if self.network_status:
            await self._refresh_gas_estimates()
        
        # Perform initial data refresh immediately
        await self.refresh_data()
                    
        logging.info("Token monitoring started with initial data")
        return self._refresh_task

    async def _refresh_loop(self):
        """
        Background loop that refreshes data every refresh_interval seconds.
        """
        while True:
            try:
                await self.refresh_data()
                # Store timestamp of last update
                current_time = time.time()
                self.last_network_check = current_time
                self.last_gas_price_check = current_time
                self.network_congestion['last_updated'] = current_time
                
                await asyncio.sleep(self.refresh_interval)
            except Exception as e:
                logging.error(f"Error in refresh loop: {e}")
                await asyncio.sleep(self.refresh_interval)  # Continue the loop despite errors
    
    async def _refresh_gas_cache_loop(self):
        """
        Background loop that refreshes gas estimate caches periodically.
        """
        while True:
            try:
                if self.network_status:
                    await self._refresh_gas_estimates()
                    logging.info("Gas estimate caches refreshed")
                
                await asyncio.sleep(self.gas_cache_refresh_interval)
            except Exception as e:
                logging.error(f"Error in gas cache refresh loop: {e}")
                await asyncio.sleep(self.gas_cache_refresh_interval)  # Continue despite errors

    async def _refresh_gas_estimates(self, high_priority_tokens: List[str] = None):
        """
        Refresh all gas estimate caches with current values.
        
        Args:
            high_priority_tokens (List[str], optional): List of token addresses to refresh with priority
        """
        try:
            # Refresh ETH wrap/unwrap gas estimates
            logging.debug("Refreshing ETH wrap/unwrap gas estimates")
            refresh_tasks = []
            
            # Add wrap/unwrap tasks
            refresh_tasks.append(asyncio.create_task(self._refresh_eth_wrap_unwrap_estimates()))
            
            # Refresh high priority tokens first if specified
            if high_priority_tokens:
                for token in high_priority_tokens:
                    token = Web3.to_checksum_address(token)
                    if token in self.token_addresses:
                        refresh_tasks.append(asyncio.create_task(self._refresh_token_gas_estimate(token)))
            
            # Refresh remaining token transfer gas estimates
            tokens_to_refresh = [t for t in self.token_addresses if not high_priority_tokens or t not in high_priority_tokens]
            for token in tokens_to_refresh:
                refresh_tasks.append(asyncio.create_task(self._refresh_token_gas_estimate(token)))
            
            # Refresh V3 swap gas estimates for configured pools in parallel
            for pool in self.v3_pool_addresses:
                refresh_tasks.append(asyncio.create_task(self._refresh_pool_gas_estimate(pool)))
            
            # Wait for all refresh tasks to complete
            await asyncio.gather(*refresh_tasks)
            logging.info(f"Successfully refreshed all gas estimates ({len(refresh_tasks)} items)")
            
        except Exception as e:
            logging.error(f"Error refreshing gas estimates: {e}")
            import traceback
            logging.error(f"Traceback: {traceback.format_exc()}")

    async def _refresh_token_gas_estimate(self, token_address: str):
        """Helper method to refresh a single token's gas estimate"""
        token_address = Web3.to_checksum_address(token_address)
        try:
            self.cached_token_transfer_gas[token_address] = await self._estimate_token_transfer_gas(token_address)
            logging.debug(f"Updated gas estimate for token {token_address}")
        except Exception as e:
            logging.error(f"Failed to update gas estimate for token {token_address}: {e}")

    async def _refresh_pool_gas_estimate(self, pool_address: str):
        """Helper method to refresh a single pool's gas estimate"""
        pool_address = Web3.to_checksum_address(pool_address)
        try:
            self.cached_v3_swap_gas[pool_address] = await self._estimate_v3_swap_gas(pool_address)
            logging.debug(f"Updated gas estimate for pool {pool_address}")
        except Exception as e:
            logging.error(f"Failed to update gas estimate for pool {pool_address}: {e}")

    async def _refresh_eth_wrap_unwrap_estimates(self):
        """Helper method to refresh ETH wrap/unwrap gas estimates"""
        try:
            self.cached_eth_wrap_gas = await self._estimate_eth_wrap_gas()
            self.cached_eth_unwrap_gas = await self._estimate_eth_unwrap_gas()
            logging.debug("Updated ETH wrap/unwrap gas estimates")
        except Exception as e:
            logging.error(f"Failed to update ETH wrap/unwrap gas estimates: {e}")

    async def refresh_data(self):
        """
        Refreshes all monitored data:
        - Network status
        - Gas price
        - Network congestion metrics
        - Token transaction metrics
        
        All data is cached for efficient access.
        """
        logging.debug("Starting data refresh")
        
        # Check network status
        self.network_status = await self._check_network_status()

        if not self.network_status:
            logging.warning("Network appears to be down, skipping other data collection")
            return

        # Update current gas price
        self.current_gas_price = await self._get_current_gas_price()
        
        # Update network congestion metrics
        await self._update_network_congestion()

        # Update token-specific data
        for token in self.token_addresses:
            try:
                token_metrics = await self._analyze_token(token)
                self.token_data[token] = token_metrics
                self.last_token_check[token] = time.time()
            except Exception as e:
                logging.error(f"Error analyzing token {token}: {e}")
                # Keep previous data if available, otherwise set to empty
                if token not in self.token_data:
                    self.token_data[token] = {
                        'name': 'Unknown',
                        'symbol': 'Unknown',
                        'average_gas': 0,
                        'transaction_count': 0,
                        'transfer_success_rate': 0,
                        'last_updated': int(time.time())
                    }

        logging.info("Data refresh complete")

    async def _check_network_status(self, timeout: int = 10) -> bool:
        """
        Checks if the connected blockchain network is responsive.
        
        Args:
            timeout (int): Maximum time to wait for network response in seconds
            
        Returns:
            bool: True if network is up and responding, False otherwise
        """
        try:
            # Try to get the latest block using async web3
            block_number = await self.async_web3.eth.block_number
            logging.info(f"Latest block number: {block_number}")
            return True
        except Exception as e:
            logging.error(f"Network check failed: {e}")
            return False

    async def _get_current_gas_price(self) -> int:
        """
        Gets the current gas price from the network.
        
        Returns:
            int: Current gas price in wei, or 0 if there was an error
        """
        try:
            gas_price = await self.async_web3.eth.gas_price
            logging.info(f"Current gas price: {gas_price} wei")
            return gas_price
        except Exception as e:
            logging.error(f"Error fetching gas price: {e}")
            return 0

    async def _update_network_congestion(self):
        """
        Updates network congestion metrics including:
        - Average block time
        - Pending transaction count
        - Congestion assessment
        """
        try:
            # Get recent blocks to calculate average block time
            blocks = await self._fetch_recent_blocks(self.blocks_to_analyze)
            
            # Calculate average block time
            if len(blocks) >= 2:
                # Blocks are in descending order (newest first)
                total_time = 0
                for i in range(len(blocks) - 1):
                    time_diff = blocks[i]['timestamp'] - blocks[i+1]['timestamp']
                    total_time += time_diff
                
                avg_block_time = total_time / (len(blocks) - 1)
            else:
                avg_block_time = 0
            
            # Get pending transaction count (mempool size)
            try:
                pending_tx_count = await self.async_web3.eth.get_block_transaction_count('pending')
            except Exception as e:
                # If we can't get the pending tx count, use the last known value or default to 0
                pending_tx_count = self.network_congestion.get('pending_tx_count', 0)
                logging.warning(f"Using previous pending tx count value: {pending_tx_count}. Error: {e}")
            
            # Update the congestion data
            self.network_congestion = {
                'avg_block_time': avg_block_time,
                'pending_tx_count': pending_tx_count,
                # Network is considered congested if average block time is over 15 seconds
                # or there are more than 2000 pending transactions
                'is_congested': avg_block_time > 15 or pending_tx_count > 2000
            }
            
            logging.info(
                f"Network congestion metrics: avg_block_time={avg_block_time:.2f}s, "
                f"pending_tx={pending_tx_count}, congested={self.network_congestion['is_congested']}"
            )
            
        except Exception as e:
            logging.error(f"Error updating network congestion: {e}")
            # Keep existing values or set defaults
            if not self.network_congestion:
                self.network_congestion = {
                    'avg_block_time': 0,
                    'pending_tx_count': 0,
                    'is_congested': False
                }

    async def _fetch_recent_blocks(self, num_blocks: int = 10, max_retries: int = 3, initial_delay: float = 1.0) -> List[dict]:
        """
        Fetches recent blocks from the blockchain with retry logic.
        
        Args:
            num_blocks (int): Number of recent blocks to fetch
            max_retries (int): Maximum number of retry attempts per block
            initial_delay (float): Initial delay between retries in seconds (will increase exponentially)
            
        Returns:
            List[dict]: List of block data dictionaries
        """
        latest = await self.async_web3.eth.block_number
        blocks = []
        
        for i in range(latest, latest - num_blocks, -1):
            retries = 0
            delay = initial_delay
            success = False
            
            while not success and retries < max_retries:
                try:
                    block = await self.async_web3.eth.get_block(i)
                    blocks.append(block)
                    success = True
                except Exception as e:
                    retries += 1
                    if "Too Many Requests" in str(e) or "429" in str(e):
                        logging.warning(f"Rate limited while fetching block {i}, retrying after {delay}s (attempt {retries}/{max_retries})")
                        await asyncio.sleep(delay)
                        # Exponential backoff
                        delay *= 2
                    else:
                        logging.error(f"Error fetching block {i}: {e}")
                        if retries < max_retries:
                            await asyncio.sleep(delay)
                            delay *= 1.5
                        else:
                            break
            
            if not success:
                logging.error(f"Failed to fetch block {i} after {max_retries} attempts")
        
        return blocks

    async def _analyze_token(self, token_address: str) -> Dict:
        """
        Analyzes recent transactions for a specific token.
        
        Args:
            token_address (str): The token contract address to analyze
            
        Returns:
            Dict: Token metrics including gas usage and transaction count
        """
        token_address = Web3.to_checksum_address(token_address)
        logging.info(f"Starting analysis for token: {token_address}")
        
        # Get token details if available
        try:
            token_name = await self._get_token_name(token_address)
            token_symbol = await self._get_token_symbol(token_address)
            logging.info(f"Got token details: {token_name} ({token_symbol})")
        except Exception as e:
            logging.error(f"Error getting token details: {str(e)}")
            token_name = "Unknown"
            token_symbol = "Unknown"
        
        # Analyze recent transactions
        try:
            logging.info(f"Analyzing transactions for token: {token_address}")
            avg_gas, tx_count, success_rate = await self._analyze_token_transactions(token_address, self.blocks_to_analyze)
            logging.info(f"Transaction analysis complete: {tx_count} transactions found")
        except Exception as e:
            logging.error(f"Error analyzing token transactions: {str(e)}")
            import traceback
            logging.error(f"Traceback: {traceback.format_exc()}")
            avg_gas, tx_count, success_rate = 0, 0, 0
        
        try:
            latest_block = await self.async_web3.eth.get_block('latest')
            last_updated = latest_block['timestamp']
        except Exception as e:
            logging.error(f"Error getting latest block: {str(e)}")
            last_updated = int(time.time())  # Use current time as fallback
        
        token_data = {
            'name': token_name,
            'symbol': token_symbol,
            'average_gas': avg_gas,
            'transaction_count': tx_count,
            'transfer_success_rate': success_rate,
            'last_updated': last_updated
        }
        logging.info(f"Token analysis complete for {token_address}: {token_data}")
        return token_data

    async def _analyze_token_transactions(self, token_address: str, num_blocks: int = 10) -> Tuple[float, int, float]:
        """
        Analyzes token transactions in recent blocks.
        
        Args:
            token_address (str): The token contract address
            num_blocks (int): Number of recent blocks to analyze
            
        Returns:
            Tuple[float, int, float]: (average gas price, transaction count, success rate)
        """
        token_address = Web3.to_checksum_address(token_address)
        logging.info(f"Fetching recent blocks for token transaction analysis: {token_address}")
        
        # Get recent blocks with full transaction data
        try:
            blocks = await self._fetch_recent_blocks_with_txs(num_blocks)
            logging.info(f"Retrieved {len(blocks)} blocks with transactions")
        except Exception as e:
            logging.error(f"Error fetching blocks with transactions: {str(e)}")
            import traceback
            logging.error(f"Traceback: {traceback.format_exc()}")
            return 0, 0, 0
        
        total_gas = 0
        tx_count = 0
        successful_txs = 0
        
        # Standard ERC20 Transfer event signature
        transfer_signature = self.web3.keccak(text="Transfer(address,address,uint256)").hex()
        
        for block_idx, block in enumerate(blocks):
            logging.debug(f"Analyzing block {block_idx+1}/{len(blocks)}")
            if 'transactions' not in block or not block.get('transactions'):
                logging.warning(f"Block {block_idx+1}/{len(blocks)} has no transactions")
                continue
                
            for tx in block.get('transactions', []):
                # Check if transaction is to the token contract
                if tx.get('to') and Web3.to_checksum_address(tx.get('to', '0x0')) == token_address:
                    tx_count += 1
                    total_gas += tx.get('gasPrice', 0)
                    
                    # Check if transaction succeeded
                    try:
                        receipt = await self.async_web3.eth.get_transaction_receipt(tx.get('hash'))
                        if receipt.get('status') == 1:
                            successful_txs += 1
                    except Exception as e:
                        logging.debug(f"Error getting receipt for tx {tx.get('hash')}: {str(e)}")
                        pass  # Skip if can't get receipt
        
        logging.info(f"Token analysis complete: {tx_count} transactions found, {successful_txs} successful")
        avg_gas = total_gas / tx_count if tx_count > 0 else 0
        success_rate = successful_txs / tx_count if tx_count > 0 else 0
        
        return avg_gas, tx_count, success_rate

    async def _fetch_recent_blocks_with_txs(self, num_blocks: int = 10, max_retries: int = 3, initial_delay: float = 1.0) -> List[dict]:
        """
        Fetches recent blocks with full transaction data with retry logic.
        
        Args:
            num_blocks (int): Number of recent blocks to fetch
            max_retries (int): Maximum number of retry attempts per block
            initial_delay (float): Initial delay between retries in seconds (will increase exponentially)
            
        Returns:
            List[dict]: List of blocks with full transaction data
        """
        try:
            latest = await self.async_web3.eth.block_number
            logging.info(f"Latest block number for transaction analysis: {latest}")
            
            # For performance reasons, limit the number of blocks if too high
            actual_num_blocks = min(num_blocks, 5)  # Limit to 5 blocks to reduce load
            if actual_num_blocks < num_blocks:
                logging.info(f"Limiting block analysis to {actual_num_blocks} blocks to reduce load")
            
            blocks = []
            
            for i in range(latest, latest - actual_num_blocks, -1):
                retries = 0
                delay = initial_delay
                success = False
                
                while not success and retries < max_retries:
                    try:
                        logging.debug(f"Fetching block {i} with full transactions")
                        block = await self.async_web3.eth.get_block(i, full_transactions=True)
                        if block and 'transactions' in block:
                            blocks.append(block)
                            success = True
                        else:
                            logging.warning(f"Block {i} has no transactions field")
                            success = True  # Count as success but with empty block
                    except Exception as e:
                        retries += 1
                        if "Too Many Requests" in str(e) or "429" in str(e):
                            logging.warning(f"Rate limited while fetching block with txs {i}, retrying after {delay}s (attempt {retries}/{max_retries})")
                            await asyncio.sleep(delay)
                            # Exponential backoff
                            delay *= 2
                        else:
                            logging.error(f"Error fetching block with txs {i}: {e}")
                            if retries < max_retries:
                                await asyncio.sleep(delay)
                                delay *= 1.5
                            else:
                                break
                
                if not success:
                    logging.error(f"Failed to fetch block with txs {i} after {max_retries} attempts")
            
            logging.info(f"Successfully fetched {len(blocks)} blocks with transactions")
            return blocks
            
        except Exception as e:
            logging.error(f"Error in _fetch_recent_blocks_with_txs: {str(e)}")
            import traceback
            logging.error(f"Traceback: {traceback.format_exc()}")
            return []  # Return empty list as fallback

    async def _get_token_name(self, token_address: str) -> str:
        """
        Gets the name of an ERC20 token.
        
        Args:
            token_address (str): Token contract address
            
        Returns:
            str: Token name or 'Unknown' if not available
        """
        # Special case for ETH
        if token_address.lower() == self.ETH_ADDRESS.lower():
            return "Ethereum"
        
        try:
            # Basic ERC20 ABI with name function
            abi = [{"constant":True,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"type":"function"}]
            contract = self.async_web3.eth.contract(address=token_address, abi=abi)
            try:
                name = await contract.functions.name().call()
                return name
            except Exception as e:
                logging.warning(f"Error calling name() on token {token_address}: {str(e)}")
                # Try fallback for tokens that don't implement the standard correctly
                return "Unknown"
        except Exception as e:
            logging.warning(f"Error creating contract for token {token_address}: {str(e)}")
            return "Unknown"

    async def _get_token_symbol(self, token_address: str) -> str:
        """
        Gets the symbol of an ERC20 token.
        
        Args:
            token_address (str): Token contract address
            
        Returns:
            str: Token symbol or 'Unknown' if not available
        """
        # Special case for ETH
        if token_address.lower() == self.ETH_ADDRESS.lower():
            return "ETH"
        
        try:
            # Basic ERC20 ABI with symbol function
            abi = [{"constant":True,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"type":"function"}]
            contract = self.async_web3.eth.contract(address=token_address, abi=abi)
            try:
                symbol = await contract.functions.symbol().call()
                return symbol
            except Exception as e:
                logging.warning(f"Error calling symbol() on token {token_address}: {str(e)}")
                # Try fallback for tokens that don't implement the standard correctly
                return "Unknown"
        except Exception as e:
            logging.warning(f"Error creating contract for token {token_address}: {str(e)}")
            return "Unknown"

    def get_token_data(self, token_address: Optional[str] = None) -> Dict:
        """
        Returns the latest token-specific data stored in memory.
        
        Args:
            token_address (Optional[str]): Specific token address to retrieve,
                                        returns all tokens if None
        
        Returns:
            Dict: Dictionary mapping token addresses to their metrics, or
                 metrics for a specific token if token_address is provided
        """
        if token_address:
            token_address = Web3.to_checksum_address(token_address)
            return self.token_data.get(token_address, {})
        return self.token_data

    def get_network_status(self) -> Dict:
        """
        Returns cached information about network status.
        
        Returns:
            Dict: Network status information including:
                - up (bool): Whether network is up
                - last_check (float): Timestamp of last status check 
        """
        return {
            'up': self.network_status,
            'last_check': self.last_network_check
        }

    def get_gas_data(self) -> Dict:
        """
        Returns cached gas and network congestion information.
        
        Returns:
            Dict: Gas and congestion data including:
                - gas_price (int): Current gas price in wei
                - last_updated (float): Timestamp of last update
                - congestion_level (str): 'high' or 'normal'
                - avg_block_time (float): Average time between blocks
                - pending_tx_count (int): Number of pending transactions
        """
        return {
            'gas_price': self.current_gas_price,
            'last_updated': self.last_gas_price_check,
            'congestion_level': 'high' if self.is_network_congested() else 'normal',
            'avg_block_time': self.network_congestion.get('avg_block_time', 0),
            'pending_tx_count': self.network_congestion.get('pending_tx_count', 0)
        }
    
    def get_network_congestion(self) -> Dict:
        """
        Returns network congestion metrics.
        
        Returns:
            Dict: Network congestion data including average block time,
                 pending transaction count and congestion assessment
        """
        return self.network_congestion
    
    def is_network_congested(self) -> bool:
        """
        Returns whether the network is currently congested.
        
        Returns:
            bool: True if network is congested, False otherwise
        """
        return self.network_congestion.get('is_congested', False)
    
    async def get_estimated_transaction_time(self, gas_price: Optional[int] = None) -> Dict:
        """
        Estimates the transaction time based on current network conditions.
        
        Args:
            gas_price (Optional[int]): Gas price to use for estimation in wei.
                                      If None, uses current network gas price.
        
        Returns:
            Dict: Transaction time estimation data
        """
        return self.estimate_transfer_time(gas_price)
        
    def estimate_transfer_time(self, gas_price: Optional[int] = None) -> Dict:
        """
        Estimates the transfer time based on current network conditions.
        
        Args:
            gas_price (Optional[int]): Gas price to use for estimation in wei.
                                      If None, uses current network gas price.
        
        Returns:
            Dict: Transfer time estimation data
        """
        if not self.network_status:
            return {
                'status': 'error',
                'message': 'Network is down',
                'estimate_seconds': None
            }
        
        gas_price_to_use = gas_price if gas_price is not None else self.current_gas_price
        current_price = self.current_gas_price
        
        # Base time is the average block time
        base_time = self.network_congestion.get('avg_block_time', 15)  # Default to 15s if not available
        
        # Calculate priority factor (how much higher than current gas price)
        priority_factor = 1.0
        if current_price > 0:
            priority_factor = gas_price_to_use / current_price
        
        # Adjust time based on congestion and gas price
        if self.network_congestion.get('is_congested', False):
            # If congested, estimate depends heavily on gas price relative to current
            if priority_factor < 1.1:
                # Less than 10% above current price
                estimated_time = base_time * 3  # Could take multiple blocks
            elif priority_factor < 1.5:
                # Between 10-50% above current price
                estimated_time = base_time * 2  # Likely next block or the one after
            else:
                # More than 50% above current price
                estimated_time = base_time  # Likely next block
        else:
            # Not congested
            estimated_time = base_time  # Likely next block
            
        return {
            'status': 'ok',
            'is_congested': self.network_congestion.get('is_congested', False),
            'current_gas_price': current_price,
            'estimate_seconds': estimated_time,
            'estimated_blocks': max(1, round(estimated_time / base_time))
        }

    async def estimate_token_transfer_gas(self, token_address: str, amount: float = None, recipient: str = None) -> Dict:
        """
        Returns gas estimation for a token transfer from cache.
        
        Args:
            token_address (str): The ERC20 token contract address
            amount (float, optional): Not used, kept for backward compatibility
            recipient (str, optional): Not used, kept for backward compatibility
            
        Returns:
            Dict: Gas estimation data from cache
        """
        # Simply return the cached value
        return self.get_token_transfer_gas(token_address)

    async def estimate_pool_swap_gas(self, pool_address: str, amount_in: float = None, 
                              token_in: str = None, token_out: str = None) -> Dict:
        """
        Returns gas estimation for a Uniswap V3 swap from cache.
        
        Args:
            pool_address (str): The pool contract address
            amount_in (float, optional): Not used, kept for backward compatibility
            token_in (str, optional): Not used, kept for backward compatibility
            token_out (str, optional): Not used, kept for backward compatibility
            
        Returns:
            Dict: Gas estimation data from cache
        """
        # Simply return the cached value
        return self.get_v3_swap_gas(pool_address)

    async def estimate_eth_wrap_unwrap_gas(self, is_wrap: bool = True, amount: float = None) -> Dict:
        """
        Returns gas estimation for wrapping ETH to WETH or unwrapping WETH to ETH from cache.
        
        Args:
            is_wrap (bool): True for wrapping ETH to WETH, False for unwrapping WETH to ETH
            amount (float, optional): Not used, kept for backward compatibility
            
        Returns:
            Dict: Gas estimation data from cache
        """
        # Return the appropriate cached value based on operation
        if is_wrap:
            return self.get_eth_wrap_gas()
        else:
            return self.get_eth_unwrap_gas()

    async def _estimate_token_transfer_gas(self, token_address: str) -> Dict:
        """
        Internal method to estimate gas for token transfers.
        Used to populate the cache.
        """
        token_address = Web3.to_checksum_address(token_address)
        
        # Special handling for ETH (which is not an ERC20 token)
        if token_address.lower() == self.ETH_ADDRESS.lower():
            return {
                'status': 'ok',
                'gas_limit': 21000,  # Standard ETH transfer gas
                'gas_price': self.current_gas_price,
                'cost_wei': 21000 * self.current_gas_price,
                'token_symbol': 'ETH',
                'token_name': 'Ethereum',
                'last_updated': time.time()
            }
        
        try:
            # For ERC20 transfers, we need the transfer function selector and encoded parameters
            # Get token details
            token_symbol = await self._get_token_symbol(token_address)
            token_name = await self._get_token_name(token_address)
            
            # Use standard parameters for estimation
            recipient = self.dummy_recipient  # Use non-zero address to avoid common errors
            amount = 1000000  # Use a standard amount in raw units
            
            # Encode the transfer function call manually
            # transfer(address,uint256) selector is 0xa9059cbb
            function_selector = "0xa9059cbb"
            
            # Encode the address parameter - 32 bytes padded (remove '0x' and pad)
            encoded_address = recipient[2:].lower().zfill(64)
            
            # Encode the uint256 parameter (amount) - 32 bytes padded
            encoded_amount = hex(amount)[2:].zfill(64)
            
            # Combine into complete data payload
            data = function_selector + encoded_address + encoded_amount
            
            # Create a transaction object for gas estimation
            tx_dict = {
                'from': self.dummy_sender,
                'to': token_address,
                'value': 0,
                'data': data
            }
            
            # Estimate gas
            try:
                gas_limit = await self.async_web3.eth.estimate_gas(tx_dict)
            except Exception as e:
                logging.warning(f"Estimation failed for token {token_address}: {str(e)}")
                gas_limit = 70000  # Fallback to standard ERC20 transfer gas
            
            gas_price = self.current_gas_price
            cost_wei = gas_limit * gas_price
            
            return {
                'status': 'ok',
                'gas_limit': gas_limit,
                'gas_price': gas_price,
                'cost_wei': cost_wei,
                'token_symbol': token_symbol,
                'token_name': token_name,
                'last_updated': time.time()
            }
            
        except Exception as e:
            logging.error(f"Error estimating token transfer gas: {str(e)}")
            
            return {
                'status': 'error',
                'message': str(e),
                'gas_limit': 70000,  # Fallback estimate for ERC20 transfers
                'gas_price': self.current_gas_price,
                'cost_wei': 70000 * self.current_gas_price,
                'last_updated': time.time()
            }

    async def _estimate_v3_swap_gas(self, pool_address: str) -> Dict:
        """
        Internal method to estimate gas for Uniswap V3 swap operations.
        Used to populate the cache.
        """
        pool_address = Web3.to_checksum_address(pool_address)
        
        try:
            # Uniswap V3 swap estimated gas:
            # - 0.05% fee tier: ~180,000-200,000 gas
            # - 0.3% fee tier: ~165,000-190,000 gas 
            # - 1% fee tier: ~165,000-190,000 gas
            
            # Get fee tier from pool address (rough mapping based on known pools)
            estimated_gas_limit = 180000  # Default V3 estimate
            fee_tier = '0.3%'  # Default fee tier
            
            # Known pool mappings
            if pool_address == '0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640':
                fee_tier = '0.05%'
                estimated_gas_limit = 195000
            elif pool_address == '0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8':
                fee_tier = '0.3%'
                estimated_gas_limit = 180000
            elif pool_address == '0x7BeA39867e4169DBe237d55C8242a8f2fcDcc387':
                fee_tier = '1%'
                estimated_gas_limit = 170000
            
            gas_price = self.current_gas_price
            cost_wei = estimated_gas_limit * gas_price
            
            return {
                'status': 'ok',
                'gas_limit': estimated_gas_limit,
                'gas_price': gas_price,
                'cost_wei': cost_wei,
                'pool_address': pool_address,
                'fee_tier': fee_tier,
                'protocol': 'Uniswap V3',
                'last_updated': time.time()
            }
            
        except Exception as e:
            logging.error(f"Error estimating V3 swap gas: {str(e)}")
            
            return {
                'status': 'error',
                'message': str(e),
                'gas_limit': 180000,  # Fallback estimate for V3 swaps
                'gas_price': self.current_gas_price,
                'cost_wei': 180000 * self.current_gas_price,
                'protocol': 'Uniswap V3',
                'last_updated': time.time()
            }

    async def _estimate_eth_wrap_gas(self) -> Dict:
        """
        Internal method to estimate gas for wrapping ETH to WETH.
        Used to populate the cache.
        """
        try:
            # WETH contract on Ethereum mainnet
            weth_address = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
            
            # For ETH wrap, we don't need encoded data since it's just a deposit function
            wei_amount = Web3.to_wei(1, 'ether')  # Standard amount - 1 ETH
            
            # Create transaction dict
            tx_dict = {
                'from': self.dummy_sender,
                'to': weth_address,
                'value': wei_amount,
                'data': "0xd0e30db0"  # deposit() function selector
            }
            
            # Try to get a more accurate estimate, fall back to average if it fails
            try:
                gas_limit = await self.async_web3.eth.estimate_gas(tx_dict)
            except Exception as e:
                logging.warning(f"Falling back to average gas limit for ETH wrap: {str(e)}")
                gas_limit = 50000  # Standard ETH wrap gas
            
            gas_price = self.current_gas_price
            cost_wei = gas_limit * gas_price
            
            return {
                'status': 'ok',
                'operation': 'wrap',
                'gas_limit': gas_limit,
                'gas_price': gas_price,
                'cost_wei': cost_wei,
                'last_updated': time.time()
            }
            
        except Exception as e:
            logging.error(f"Error estimating ETH wrap gas: {str(e)}")
            
            return {
                'status': 'error',
                'message': str(e),
                'operation': 'wrap',
                'gas_limit': 50000,  # Fallback estimate
                'gas_price': self.current_gas_price,
                'cost_wei': 50000 * self.current_gas_price,
                'last_updated': time.time()
            }

    async def _estimate_eth_unwrap_gas(self) -> Dict:
        """
        Internal method to estimate gas for unwrapping WETH to ETH.
        Used to populate the cache.
        """
        try:
            # WETH contract on Ethereum mainnet
            weth_address = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
            
            # For ETH unwrap, we need the withdraw function selector and encoded amount
            wei_amount = Web3.to_wei(1, 'ether')  # Standard amount - 1 WETH
            
            # Encode the withdraw function call manually
            # withdraw(uint256) selector is 0x2e1a7d4d
            function_selector = "0x2e1a7d4d"
            # Encode the uint256 parameter (amount in wei) - 32 bytes padded
            encoded_amount = hex(wei_amount)[2:].zfill(64)
            data = function_selector + encoded_amount
            
            # Create transaction dict
            tx_dict = {
                'from': self.dummy_sender,
                'to': weth_address,
                'value': 0,
                'data': data
            }
            
            # Try to get a more accurate estimate, fall back to average if it fails
            try:
                gas_limit = await self.async_web3.eth.estimate_gas(tx_dict)
            except Exception as e:
                logging.warning(f"Falling back to average gas limit for ETH unwrap: {str(e)}")
                gas_limit = 35000  # Standard ETH unwrap gas
            
            gas_price = self.current_gas_price
            cost_wei = gas_limit * gas_price
            
            return {
                'status': 'ok',
                'operation': 'unwrap',
                'gas_limit': gas_limit,
                'gas_price': gas_price,
                'cost_wei': cost_wei,
                'last_updated': time.time()
            }
            
        except Exception as e:
            logging.error(f"Error estimating ETH unwrap gas: {str(e)}")
            
            return {
                'status': 'error',
                'message': str(e),
                'operation': 'unwrap',
                'gas_limit': 35000,  # Fallback estimate
                'gas_price': self.current_gas_price,
                'cost_wei': 35000 * self.current_gas_price,
                'last_updated': time.time()
            }

    # New Public API methods that return cached data

    def get_token_transfer_gas(self, token_address: str) -> Dict:
        """
        Returns cached gas estimation for a token transfer.
        
        Args:
            token_address (str): The ERC20 token contract address
            
        Returns:
            Dict: Cached gas estimation data
        """
        token_address = Web3.to_checksum_address(token_address)
        
        # Return cached value if available
        if token_address in self.cached_token_transfer_gas:
            return self.cached_token_transfer_gas[token_address]
        
        # Return a default estimate if not cached yet
        return {
            'status': 'pending',
            'message': 'Gas estimate not yet cached for this token',
            'gas_limit': 70000,  # Default ERC20 transfer estimate
            'gas_price': self.current_gas_price,
            'cost_wei': 70000 * self.current_gas_price
        }

    def get_v3_swap_gas(self, pool_address: str) -> Dict:
        """
        Returns cached gas estimation for a Uniswap V3 swap.
        
        Args:
            pool_address (str): The Uniswap V3 pool contract address
            
        Returns:
            Dict: Cached gas estimation data
        """
        pool_address = Web3.to_checksum_address(pool_address)
        
        # Return cached value if available
        if pool_address in self.cached_v3_swap_gas:
            return self.cached_v3_swap_gas[pool_address]
        
        # Return a default estimate if not cached yet
        return {
            'status': 'pending',
            'message': 'Gas estimate not yet cached for this pool',
            'gas_limit': 180000,  # Default V3 swap estimate
            'gas_price': self.current_gas_price,
            'cost_wei': 180000 * self.current_gas_price,
            'protocol': 'Uniswap V3'
        }

    def get_eth_wrap_gas(self) -> Dict:
        """
        Returns cached gas estimation for wrapping ETH to WETH.
        
        Returns:
            Dict: Cached gas estimation data
        """
        # Return cached value if available
        if self.cached_eth_wrap_gas:
            return self.cached_eth_wrap_gas
        
        # Return a default estimate if not cached yet
        return {
            'status': 'pending',
            'message': 'Gas estimate not yet cached',
            'gas_limit': 50000,  # Default ETH wrap estimate
            'gas_price': self.current_gas_price,
            'cost_wei': 50000 * self.current_gas_price,
            'operation': 'wrap'
        }

    def get_eth_unwrap_gas(self) -> Dict:
        """
        Returns cached gas estimation for unwrapping WETH to ETH.
        
        Returns:
            Dict: Cached gas estimation data
        """
        # Return cached value if available
        if self.cached_eth_unwrap_gas:
            return self.cached_eth_unwrap_gas
        
        # Return a default estimate if not cached yet
        return {
            'status': 'pending',
            'message': 'Gas estimate not yet cached',
            'gas_limit': 35000,  # Default ETH unwrap estimate
            'gas_price': self.current_gas_price,
            'cost_wei': 35000 * self.current_gas_price,
            'operation': 'unwrap'
        }

    def check_network_conditions(self) -> Dict:
        """
        Provides a comprehensive check of current network conditions for transaction viability.
        
        Returns:
            Dict: Network condition data including:
                - status (str): 'ok', 'warning', or 'critical'
                - is_congested (bool): Whether network is congested
                - gas_price (int): Current gas price in wei
                - avg_block_time (float): Average time between blocks
                - pending_tx_count (int): Number of pending transactions
                - message (str): Human-readable summary of network conditions
        """
        if not self.network_status:
            return {
                'status': 'critical',
                'is_congested': True,
                'gas_price': 0,
                'avg_block_time': 0,
                'pending_tx_count': 0,
                'message': 'Network is down or not responding'
            }
            
        gas_data = self.get_gas_data()
        congestion = self.get_network_congestion()
        
        # Determine status based on various metrics
        is_congested = self.is_network_congested()
        pending_tx_count = congestion.get('pending_tx_count', 0)
        avg_block_time = congestion.get('avg_block_time', 0)
        
        status = 'ok'
        message = 'Network conditions are good for transactions'
        
        if is_congested:
            status = 'warning'
            message = 'Network is congested, transactions may be delayed'
            
        if is_congested and pending_tx_count > 5000:
            status = 'critical'
            message = 'Network is heavily congested, consider delaying non-critical transactions'
            
        return {
            'status': status,
            'is_congested': is_congested,
            'gas_price': gas_data['gas_price'],
            'avg_block_time': avg_block_time,
            'pending_tx_count': pending_tx_count,
            'message': message,
            'last_updated': self.last_network_check
        }


# Example of using the gas estimation functions
async def example_gas_estimations():
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(levelname)s - %(message)s')
    
    config = Config()
    
    # Token addresses (excluding ETH to demonstrate automatic ETH monitoring)
    weth_address = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"  # WETH
    usdc_address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"  # USDC
    
    # Uniswap V3 Pool addresses
    v3_pool_addresses = [
        "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",  # USDC-ETH 0.05%
        "0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8",  # USDC-ETH 0.3%
    ]
    
    # Create monitor instance with specific V3 pools to monitor
    # Note: ETH will be added automatically
    monitor = TokenMonitor(
        token_addresses=[weth_address, usdc_address], 
        infura_url=config.infura_url,
        v3_pool_addresses=v3_pool_addresses,
        refresh_interval=60,
        blocks_to_analyze=3
    )
    
    # Start monitoring - initial data refresh happens here
    monitor_task = await monitor.start_monitoring()
    
    try:
        logging.info("Initial data refresh completed")
        
        # Check if network is suitable for transactions
        network_check = monitor.check_network_conditions()
        logging.info(f"Network conditions: {network_check['status']}")
        logging.info(f"Message: {network_check['message']}")
        
        start_time = time.time()
        if network_check['status'] != 'critical':
            # 1. Get cached gas for token transfer (USDC)
            token_transfer = monitor.get_token_transfer_gas(usdc_address)
            logging.info(f"USDC transfer gas estimate (cached): {token_transfer}")

            # 2. Get cached gas for token transfer (ETH) - which was automatically added
            eth_address = monitor.ETH_ADDRESS  # Use the class constant
            eth_transfer = monitor.get_token_transfer_gas(eth_address)
            logging.info(f"ETH transfer gas estimate (cached): {eth_transfer}")
            
            # 3. Get cached gas for V3 pool swap (0.3% fee tier)
            v3_pool_swap = monitor.get_v3_swap_gas(v3_pool_addresses[1])
            logging.info(f"Uniswap V3 swap gas estimate (cached): {v3_pool_swap}")
            
            # 4. Get cached gas for ETH wrapping
            eth_wrap = monitor.get_eth_wrap_gas()
            logging.info(f"ETH wrap gas estimate (cached): {eth_wrap}")
            
            # 5. Get cached gas for WETH unwrapping
            weth_unwrap = monitor.get_eth_unwrap_gas()
            logging.info(f"WETH unwrap gas estimate (cached): {weth_unwrap}")
            
            # 6. Estimate transfer time with current gas price
            transfer_time = monitor.estimate_transfer_time()
            logging.info(f"Estimated transfer time: {transfer_time}")
            
            # 7. Use backward-compatible interface
            v3_swap_compat = await monitor.estimate_pool_swap_gas(v3_pool_addresses[0])
            logging.info(f"V3 swap gas (backward compatible): {v3_swap_compat}")
        
        logging.info(f"Total time taken: {(time.time() - start_time) * 1_000_000:.2f} microseconds")
    except Exception as e:
        logging.error(f"Error in example: {e}")
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")
    finally:
        # Clean up
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass

# To run the example from command line
if __name__ == "__main__":
    asyncio.run(example_gas_estimations())

