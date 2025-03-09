import asyncio
import logging
import time  # Add this import for sleep functionality
from typing import List, Dict, Optional, Tuple
from web3 import Web3

from config import Config

class Transaction:
    """
    Encapsulates all data required for a blockchain transaction, specifically for gas estimation.
    
    This class provides a structured way to represent transaction parameters needed by Web3.eth.estimate_gas
    and other transaction-related methods. It helps standardize transaction data across different methods
    and makes it easier to estimate gas costs accurately.
    
    Attributes:
        from_address (str): The address sending the transaction
        to_address (str): The recipient address of the transaction
        value (int): The amount of Ether to send in wei (0 for contract interactions)
        data (str, optional): The encoded data payload for contract interactions
        gas (int, optional): Gas limit for the transaction
        gas_price (int, optional): Gas price in wei (for legacy transactions)
        max_fee_per_gas (int, optional): Maximum fee per gas in wei (for EIP-1559 transactions)
        max_priority_fee_per_gas (int, optional): Maximum priority fee per gas in wei (for EIP-1559 transactions)
        nonce (int, optional): Transaction nonce
    """
    
    def __init__(
        self,
        from_address: str,
        to_address: str,
        value: int = 0,
        data: str = None,
        gas: int = None,
        gas_price: int = None,
        max_fee_per_gas: int = None,
        max_priority_fee_per_gas: int = None,
        nonce: int = None
    ):
        """
        Initialize a new Transaction object with the provided parameters.
        
        Args:
            from_address (str): The address sending the transaction
            to_address (str): The recipient address of the transaction
            value (int, optional): The amount of Ether to send in wei (0 for contract interactions)
            data (str, optional): The encoded data payload for contract interactions
            gas (int, optional): Gas limit for the transaction
            gas_price (int, optional): Gas price in wei (for legacy transactions)
            max_fee_per_gas (int, optional): Maximum fee per gas in wei (for EIP-1559 transactions)
            max_priority_fee_per_gas (int, optional): Maximum priority fee per gas in wei (for EIP-1559 transactions)
            nonce (int, optional): Transaction nonce
        """
        self.from_address = from_address
        self.to_address = to_address
        self.value = value
        self.data = data
        self.gas = gas
        self.gas_price = gas_price
        self.max_fee_per_gas = max_fee_per_gas
        self.max_priority_fee_per_gas = max_priority_fee_per_gas
        self.nonce = nonce
    
    def to_dict(self) -> Dict[str, any]:
        """
        Convert the transaction object to a dictionary format suitable for Web3.py methods.
        
        Returns:
            Dict[str, any]: Transaction parameters as a dictionary
        """
        tx_dict = {
            'from': self.from_address,
            'to': self.to_address,
            'value': self.value
        }
        
        # Add optional parameters only if they are set
        if self.data is not None:
            tx_dict['data'] = self.data
        if self.gas is not None:
            tx_dict['gas'] = self.gas
        if self.gas_price is not None:
            tx_dict['gasPrice'] = self.gas_price
        if self.max_fee_per_gas is not None:
            tx_dict['maxFeePerGas'] = self.max_fee_per_gas
        if self.max_priority_fee_per_gas is not None:
            tx_dict['maxPriorityFeePerGas'] = self.max_priority_fee_per_gas
        if self.nonce is not None:
            tx_dict['nonce'] = self.nonce
            
        return tx_dict
    
    def estimate_gas(self, web3: Web3) -> int:
        """
        Estimate the gas required for this transaction.
        
        Args:
            web3 (Web3): Web3 instance to use for estimation
            
        Returns:
            int: Estimated gas amount
            
        Raises:
            Exception: If gas estimation fails
        """
        try:
            return web3.eth.estimate_gas(self.to_dict())
        except Exception as e:
            logging.error(f"Gas estimation failed: {str(e)}")
            raise

    def __str__(self) -> str:
        """
        Return a string representation of the transaction.
        
        Returns:
            str: Human-readable transaction details
        """
        return f"Transaction(from={self.from_address}, to={self.to_address}, value={self.value})"

class TokenMonitor:
    """
    Asynchronously monitors blockchain network and token-related metrics.
    
    This class provides real-time monitoring of:
    - Network status (up/down)
    - Gas prices
    - Block production
    - Token-specific transaction metrics
    
    The monitor refreshes data at regular intervals and provides methods to
    retrieve the latest information about blockchain conditions for token transfers.
    
    Parameters:
        token_addresses (List[str]): List of token contract addresses to monitor.
        web3 (Web3): An initialized Web3 instance connected to an Ethereum node.
        refresh_interval (int): How often to refresh data, in seconds (default: 60).
        blocks_to_analyze (int): Number of recent blocks to analyze for token metrics (default: 10).
    """
    def __init__(
        self, 
        token_addresses: List[str], 
        web3: Web3, 
        refresh_interval: int = 60,
        blocks_to_analyze: int = 10
    ):
        self.token_addresses = [Web3.toChecksumAddress(addr) for addr in token_addresses]
        self.refresh_interval = refresh_interval
        self.web3 = web3
        self.blocks_to_analyze = blocks_to_analyze

        # Data storage
        self.token_data: Dict[str, Dict] = {}
        self.network_status: bool = False
        self.current_gas_price: int = 0
        self.network_congestion: Dict[str, any] = {
            'avg_block_time': 0,
            'pending_tx_count': 0,
            'is_congested': False
        }
        
        # Monitor task
        self._refresh_task = None
        
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
        logging.info("Token monitoring started")
        return self._refresh_task

    async def _refresh_loop(self):
        """
        Background loop that refreshes data every refresh_interval seconds.
        """
        while True:
            try:
                await self.refresh_data()
                await asyncio.sleep(self.refresh_interval)
            except Exception as e:
                logging.error(f"Error in refresh loop: {e}")
                await asyncio.sleep(self.refresh_interval)  # Continue the loop despite errors

    async def refresh_data(self):
        """
        Refreshes all monitored data:
        - Network status
        - Gas price
        - Network congestion metrics
        - Token transaction metrics
        
        All blocking web3 calls are wrapped with asyncio.to_thread to avoid blocking the event loop.
        """
        logging.debug("Starting data refresh")
        
        # Check network status
        self.network_status = await asyncio.to_thread(self._check_network_status)

        if not self.network_status:
            logging.warning("Network appears to be down, skipping other data collection")
            return

        # Update current gas price
        self.current_gas_price = await asyncio.to_thread(self._get_current_gas_price)
        
        # Update network congestion metrics
        await self._update_network_congestion()

        # Update token-specific data
        for token in self.token_addresses:
            try:
                token_metrics = await self._analyze_token(token)
                self.token_data[token] = token_metrics
            except Exception as e:
                logging.error(f"Error analyzing token {token}: {e}")
                # Keep previous data if available, otherwise set to empty
                if token not in self.token_data:
                    self.token_data[token] = {
                        'average_gas': 0,
                        'transaction_count': 0,
                        'transfer_success_rate': 0
                    }

        logging.info("Data refresh complete")

    def _check_network_status(self, timeout: int = 10) -> bool:
        """
        Checks if the connected blockchain network is responsive.
        
        Args:
            timeout (int): Maximum time to wait for network response in seconds
            
        Returns:
            bool: True if network is up and responding, False otherwise
        """
        try:
            # Try to get the latest block
            block_number = self.web3.eth.block_number
            logging.info(f"Latest block number: {block_number}")
            return True
        except Exception as e:
            logging.error(f"Network check failed: {e}")
            return False

    def _get_current_gas_price(self) -> int:
        """
        Gets the current gas price from the network.
        
        Returns:
            int: Current gas price in wei, or 0 if there was an error
        """
        try:
            gas_price = self.web3.eth.gas_price
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
            blocks = await asyncio.to_thread(self._fetch_recent_blocks, self.blocks_to_analyze)
            
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
            
            # Get pending transaction count (mempool size) with retry
            try:
                pending_tx_count = await self._make_web3_call_with_retry(
                    lambda: self.web3.eth.get_block_transaction_count('pending')
                )
            except Exception:
                # If we can't get the pending tx count, use the last known value or default to 0
                pending_tx_count = self.network_congestion.get('pending_tx_count', 0)
                logging.warning(f"Using previous pending tx count value: {pending_tx_count}")
            
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

    def _fetch_recent_blocks(self, num_blocks: int = 10, max_retries: int = 3, initial_delay: float = 1.0) -> List[dict]:
        """
        Fetches recent blocks from the blockchain with retry logic.
        
        Args:
            num_blocks (int): Number of recent blocks to fetch
            max_retries (int): Maximum number of retry attempts per block
            initial_delay (float): Initial delay between retries in seconds (will increase exponentially)
            
        Returns:
            List[dict]: List of block data dictionaries
        """
        latest = self.web3.eth.block_number
        blocks = []
        
        for i in range(latest, latest - num_blocks, -1):
            retries = 0
            delay = initial_delay
            success = False
            
            while not success and retries < max_retries:
                try:
                    block = self.web3.eth.get_block(i)
                    blocks.append(block)
                    success = True
                except Exception as e:
                    retries += 1
                    if "Too Many Requests" in str(e) or "429" in str(e):
                        logging.warning(f"Rate limited while fetching block {i}, retrying after {delay}s (attempt {retries}/{max_retries})")
                        time.sleep(delay)
                        # Exponential backoff
                        delay *= 2
                    else:
                        logging.error(f"Error fetching block {i}: {e}")
                        if retries < max_retries:
                            time.sleep(delay)
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
        token_address = Web3.toChecksumAddress(token_address)
        
        # Get token details if available
        try:
            token_name = await self._get_token_name(token_address)
            token_symbol = await self._get_token_symbol(token_address)
        except Exception as e:
            logging.error(f"Error getting token details: {e}")
            token_name = "Unknown"
            token_symbol = "Unknown"
        
        # Analyze recent transactions with retry
        try:
            avg_gas, tx_count, success_rate = await self._make_web3_call_with_retry(
                self._analyze_token_transactions, token_address, self.blocks_to_analyze
            )
        except Exception as e:
            logging.error(f"Error analyzing token transactions: {e}")
            avg_gas, tx_count, success_rate = 0, 0, 0
        
        try:
            latest_block = await self._make_web3_call_with_retry(
                lambda: self.web3.eth.get_block('latest')
            )
            last_updated = latest_block['timestamp']
        except Exception:
            last_updated = int(time.time())  # Use current time as fallback
        
        return {
            'name': token_name,
            'symbol': token_symbol,
            'average_gas': avg_gas,
            'transaction_count': tx_count,
            'transfer_success_rate': success_rate,
            'last_updated': last_updated
        }

    async def _get_token_name(self, token_address: str) -> str:
        """
        Gets the name of an ERC20 token.
        
        Args:
            token_address (str): Token contract address
            
        Returns:
            str: Token name or 'Unknown' if not available
        """
        try:
            # Basic ERC20 ABI with name function
            abi = [{"constant":True,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"type":"function"}]
            contract = self.web3.eth.contract(address=token_address, abi=abi)
            name = await asyncio.to_thread(contract.functions.name().call)
            return name
        except Exception:
            return "Unknown"

    async def _get_token_symbol(self, token_address: str) -> str:
        """
        Gets the symbol of an ERC20 token.
        
        Args:
            token_address (str): Token contract address
            
        Returns:
            str: Token symbol or 'Unknown' if not available
        """
        try:
            # Basic ERC20 ABI with symbol function
            abi = [{"constant":True,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"type":"function"}]
            contract = self.web3.eth.contract(address=token_address, abi=abi)
            symbol = await asyncio.to_thread(contract.functions.symbol().call)
            return symbol
        except Exception:
            return "Unknown"

    def _analyze_token_transactions(self, token_address: str, num_blocks: int = 10) -> Tuple[float, int, float]:
        """
        Analyzes token transactions in recent blocks.
        
        Args:
            token_address (str): The token contract address
            num_blocks (int): Number of recent blocks to analyze
            
        Returns:
            Tuple[float, int, float]: (average gas price, transaction count, success rate)
        """
        token_address = Web3.toChecksumAddress(token_address)
        
        # Get recent blocks with full transaction data
        blocks = self._fetch_recent_blocks_with_txs(num_blocks)
        
        total_gas = 0
        tx_count = 0
        successful_txs = 0
        
        # Standard ERC20 Transfer event signature
        transfer_signature = self.web3.keccak(text="Transfer(address,address,uint256)").hex()
        
        for block in blocks:
            for tx in block.get('transactions', []):
                # Check if transaction is to the token contract
                if tx.get('to') and Web3.toChecksumAddress(tx.get('to')) == token_address:
                    tx_count += 1
                    total_gas += tx.get('gasPrice', 0)
                    
                    # Check if transaction succeeded
                    try:
                        receipt = self.web3.eth.get_transaction_receipt(tx.get('hash'))
                        if receipt.get('status') == 1:
                            successful_txs += 1
                    except Exception:
                        pass  # Skip if can't get receipt
        
        avg_gas = total_gas / tx_count if tx_count > 0 else 0
        success_rate = successful_txs / tx_count if tx_count > 0 else 0
        
        return avg_gas, tx_count, success_rate

    def _fetch_recent_blocks_with_txs(self, num_blocks: int = 10, max_retries: int = 3, initial_delay: float = 1.0) -> List[dict]:
        """
        Fetches recent blocks with full transaction data with retry logic.
        
        Args:
            num_blocks (int): Number of recent blocks to fetch
            max_retries (int): Maximum number of retry attempts per block
            initial_delay (float): Initial delay between retries in seconds (will increase exponentially)
            
        Returns:
            List[dict]: List of blocks with full transaction data
        """
        latest = self.web3.eth.block_number
        blocks = []
        
        for i in range(latest, latest - num_blocks, -1):
            retries = 0
            delay = initial_delay
            success = False
            
            while not success and retries < max_retries:
                try:
                    block = self.web3.eth.get_block(i, full_transactions=True)
                    blocks.append(block)
                    success = True
                except Exception as e:
                    retries += 1
                    if "Too Many Requests" in str(e) or "429" in str(e):
                        logging.warning(f"Rate limited while fetching block with txs {i}, retrying after {delay}s (attempt {retries}/{max_retries})")
                        time.sleep(delay)
                        # Exponential backoff
                        delay *= 2
                    else:
                        logging.error(f"Error fetching block with txs {i}: {e}")
                        if retries < max_retries:
                            time.sleep(delay)
                            delay *= 1.5
                        else:
                            break
            
            if not success:
                logging.error(f"Failed to fetch block with txs {i} after {max_retries} attempts")
        
        return blocks

    def get_token_data(self) -> Dict[str, Dict]:
        """
        Returns the latest token-specific data stored in memory.
        
        Returns:
            Dict[str, Dict]: Dictionary mapping token addresses to their metrics
        """
        return self.token_data

    def get_network_status(self) -> bool:
        """
        Returns whether the blockchain network is currently responsive.
        
        Returns:
            bool: True if network is up, False otherwise
        """
        return self.network_status

    def get_current_gas_price(self) -> int:
        """
        Returns the current gas price in wei.
        
        Returns:
            int: Current gas price in wei
        """
        return self.current_gas_price
    
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
    
    def validate_token_transfer(self, token_address: str, amount: float, 
                               gas_price: Optional[int] = None,
                               max_allowed_wait_time: int = 120) -> Dict[str, any]:
        """
        Validates if a token transfer of the specified amount is likely to succeed
        based on current network conditions and token metrics.
        
        This function considers:
        - Current network status
        - Network congestion
        - Token-specific transaction success rate
        - Estimated wait time based on gas price
        
        Args:
            token_address (str): The token address to validate
            amount (float): The amount to transfer
            gas_price (Optional[int]): Gas price to use (in wei), defaults to current network price
            max_allowed_wait_time (int): Maximum acceptable wait time in seconds, defaults to 120s
        
        Returns:
            Dict[str, any]: Validation result with the following structure:
                {
                    'valid': bool,              # Whether the transfer is likely to succeed
                    'reason': Optional[str],    # Reason if validation failed
                    'details': Dict             # Additional validation details
                }
        """
        result = {
            'valid': False,
            'reason': None,
            'details': {}
        }
        
        # Convert address to checksum format
        try:
            token_address = Web3.toChecksumAddress(token_address)
        except ValueError:
            result['reason'] = "Invalid token address format"
            return result
        
        # Check if the token is being monitored
        if token_address not in self.token_data:
            result['reason'] = "Token not monitored by this instance"
            return result
        
        # Check if network is up
        if not self.network_status:
            result['reason'] = "Network is currently down"
            return result
        
        # Check token's success rate
        token_data = self.token_data.get(token_address, {})
        success_rate = token_data.get('transfer_success_rate', 0)
        result['details']['success_rate'] = success_rate
        
        # If success rate is very low, transfers may be problematic
        if success_rate < 0.8 and token_data.get('transaction_count', 0) > 5:
            result['reason'] = f"Token has low transfer success rate ({success_rate:.1%})"
            return result
        
        # Calculate estimated transfer time based on gas price
        transfer_estimate = self.estimate_transfer_time(gas_price)
        result['details']['estimated_wait'] = transfer_estimate
        
        # Check if the estimated wait time exceeds the maximum allowed
        if transfer_estimate.get('status') == 'ok':
            estimated_time = transfer_estimate.get('estimate_seconds', 0)
            if estimated_time > max_allowed_wait_time:
                result['reason'] = f"Estimated wait time ({estimated_time}s) exceeds maximum ({max_allowed_wait_time}s)"
                return result
        else:
            result['reason'] = transfer_estimate.get('message', "Unable to estimate transfer time")
            return result
        
        # All checks passed
        result['valid'] = True
        result['details']['is_congested'] = self.is_network_congested()
        result['details']['current_gas'] = self.get_current_gas_price()
        
        logging.info(f"Token transfer validation passed for {token_data.get('symbol', token_address)}, "
                    f"amount: {amount}, estimated wait: {transfer_estimate.get('estimate_seconds')}s")
        return result

    def validate_token_trade(self, token_address: str, amount: float,
                            slippage_threshold: float = 0.03,
                            max_congestion_level: int = 3000,
                            gas_price: Optional[int] = None) -> Dict[str, any]:
        """
        Validates if a token trade of the specified amount is reasonable
        based on current market conditions, token metrics, and network status.
        
        This function considers:
        - Network status and congestion
        - Token transaction patterns
        - Relative size of the trade compared to typical transaction volumes
        - Gas price and expected processing time
        
        Args:
            token_address (str): The token address to validate
            amount (float): The trade amount
            slippage_threshold (float): Maximum acceptable slippage (0.03 = 3%)
            max_congestion_level (int): Maximum acceptable pending transaction count
            gas_price (Optional[int]): Gas price to use (in wei), defaults to current network price
        
        Returns:
            Dict[str, any]: Validation result with the following structure:
                {
                    'valid': bool,              # Whether the trade is reasonable
                    'reason': Optional[str],    # Reason if validation failed
                    'details': Dict             # Additional validation details
                }
        """
        # First perform all the basic transfer validations
        transfer_validation = self.validate_token_transfer(
            token_address=token_address,
            amount=amount,
            gas_price=gas_price
        )
        
        # Initialize result with transfer validation results
        result = transfer_validation.copy()
        
        # If basic transfer validation failed, no need to check further
        if not transfer_validation['valid']:
            result['reason'] = f"Transfer validation failed: {transfer_validation['reason']}"
            return result
        
        # Additional trade-specific validations
        token_address = Web3.toChecksumAddress(token_address)
        token_data = self.token_data.get(token_address, {})
        
        # Check network congestion level for trading
        pending_tx_count = self.network_congestion.get('pending_tx_count', 0)
        if pending_tx_count > max_congestion_level:
            result['valid'] = False
            result['reason'] = f"Network congestion too high for trading ({pending_tx_count} pending transactions)"
            return result
        
        # Check if the trade size is reasonable
        # This is a simplified example - in a real system, you'd compare against liquidity data
        # Since we removed pool data, we'll use a heuristic based on token transaction counts
        tx_count = token_data.get('transaction_count', 0)
        if tx_count < 5:
            result['valid'] = False
            result['reason'] = "Insufficient transaction history to validate trade"
            return result
        
        # All checks passed
        result['valid'] = True
        result['details']['pending_tx_count'] = pending_tx_count
        result['details']['slippage_threshold'] = slippage_threshold
        result['details']['network_conditions'] = 'optimal' if not self.is_network_congested() else 'congested'
        
        logging.info(f"Token trade validation passed for {token_data.get('symbol', token_address)}, "
                    f"amount: {amount}, congestion level: {pending_tx_count}")
        return result

    def calculate_arbitrage_gas_price(self, expected_profit: float, 
                                     gas_limit: int = 250000,
                                     profit_margin: float = 0.2) -> Dict[str, any]:
        """
        Calculates the optimal gas price for an arbitrage transaction based on expected profit.
        
        This function helps determine a gas price that:
        1. Is competitive enough to get the transaction mined quickly
        2. Leaves enough profit margin after gas costs
        3. Adapts to current network conditions
        
        Args:
            expected_profit (float): Expected profit from the arbitrage (in wei)
            gas_limit (int): Estimated gas limit for the transaction
            profit_margin (float): Minimum profit margin to maintain after gas costs (0.2 = 20%)
            
        Returns:
            Dict[str, any]: Gas price information with the following structure:
                {
                    'gas_price': int,              # Recommended gas price in wei
                    'gas_cost': int,               # Total gas cost in wei
                    'profit_after_gas': float,     # Expected profit after gas costs
                    'profitable': bool,            # Whether the transaction remains profitable
                    'recommendation': str          # Human-readable recommendation
                }
        """
        if not self.network_status:
            return {
                'gas_price': 0,
                'gas_cost': 0,
                'profit_after_gas': 0,
                'profitable': False,
                'recommendation': 'Network is down - arbitrage not possible'
            }
        
        # Get current gas price
        current_gas = self.current_gas_price
        
        # Calculate maximum viable gas price that still leaves desired profit margin
        usable_profit = expected_profit * (1 - profit_margin)
        max_viable_gas_price = usable_profit / gas_limit if gas_limit > 0 else 0
        
        # Based on network congestion, calculate required gas price premium
        if self.is_network_congested():
            # In congested network, we need higher premium to get included quickly
            premium_factor = 1.5  # 50% above base price
            congestion_level = 'high'
        else:
            # In normal conditions, a smaller premium is sufficient
            premium_factor = 1.2  # 20% above base price
            congestion_level = 'normal'
        
        # Calculate recommended gas price (with premium)
        recommended_gas_price = int(current_gas * premium_factor)
        
        # Use the lower of max viable price and recommended price to ensure profitability
        optimal_gas_price = min(int(max_viable_gas_price), recommended_gas_price) if max_viable_gas_price > 0 else recommended_gas_price
        
        # Calculate total gas cost and remaining profit
        gas_cost = optimal_gas_price * gas_limit
        profit_after_gas = expected_profit - gas_cost
        
        # Determine if the transaction is still profitable
        profitable = profit_after_gas > 0
        
        # Format a recommendation based on the analysis
        if not profitable:
            recommendation = "Transaction not profitable after gas costs"
        elif optimal_gas_price < recommended_gas_price:
            recommendation = "Transaction viable but gas price limited to maintain profitability"
        else:
            recommendation = "Transaction viable with optimal gas price"
        
        # Add block time estimate with this gas price
        transfer_estimate = self.estimate_transfer_time(optimal_gas_price)
        estimated_time = transfer_estimate.get('estimate_seconds', 0) if transfer_estimate.get('status') == 'ok' else 0
        estimated_blocks = transfer_estimate.get('estimated_blocks', 0) if transfer_estimate.get('status') == 'ok' else 0
        
        result = {
            'gas_price': optimal_gas_price,
            'gas_cost': gas_cost,
            'profit_after_gas': profit_after_gas,
            'profitable': profitable,
            'network_congestion': congestion_level,
            'estimated_time': estimated_time,
            'estimated_blocks': estimated_blocks,
            'recommendation': recommendation
        }
        
        logging.info(f"Arbitrage gas calculation: profit={expected_profit} wei, " 
                    f"gas_price={optimal_gas_price} wei, profitable={profitable}")
        return result

    def validate_arbitrage_opportunity(self, token_address: str, amount: float, 
                                      expected_profit: float, gas_limit: int = 250000,
                                      max_wait_time: int = 30, transaction: Transaction = None) -> Dict[str, any]:
        """
        Validates an arbitrage opportunity against current blockchain conditions
        and calculates optimal gas price to execute the arbitrage.
        
        Args:
            token_address (str): Address of the token being traded
            amount (float): Amount of the token being traded
            expected_profit (float): Expected profit from the arbitrage (in wei)
            gas_limit (int): Estimated gas limit for the transaction (used if transaction is None)
            max_wait_time (int): Maximum acceptable wait time in seconds
            transaction (Transaction, optional): Transaction object with all parameters for gas estimation
            
        Returns:
            Dict[str, any]: Validation result with information about:
                - Whether the arbitrage is viable
                - Optimal gas price
                - Expected profits after gas costs
                - Network conditions
        """
        # First check if token trades are viable at all
        trade_validation = self.validate_token_trade(
            token_address=token_address,
            amount=amount,
            slippage_threshold=0.005,  # Tighter slippage for arbitrage (0.5%)
            max_congestion_level=5000  # Allow higher congestion for arbitrage
        )
        
        # If basic trade validation failed, arbitrage is not viable
        if not trade_validation['valid']:
            return {
                'valid': False,
                'reason': f"Trade validation failed: {trade_validation['reason']}",
                'details': trade_validation['details']
            }
        
        # If a transaction object is provided, use it to estimate gas
        actual_gas_limit = gas_limit
        if transaction is not None:
            try:
                # Use the transaction object to get a more accurate gas estimate
                actual_gas_limit = transaction.estimate_gas(self.web3)
                logging.info(f"Estimated gas for arbitrage: {actual_gas_limit} (from transaction object)")
            except Exception as e:
                logging.warning(f"Failed to estimate gas from transaction object: {str(e)}. Using provided gas_limit.")
        
        # Calculate optimal gas price for profitable arbitrage
        gas_calc = self.calculate_arbitrage_gas_price(
            expected_profit=expected_profit,
            gas_limit=actual_gas_limit
        )
        
        # Check if the arbitrage is profitable after gas costs
        if not gas_calc['profitable']:
            return {
                'valid': False,
                'reason': "Not profitable after gas costs",
                'details': {
                    'gas_price': gas_calc['gas_price'],
                    'gas_cost': gas_calc['gas_cost'],
                    'profit_after_gas': gas_calc['profit_after_gas'],
                    'expected_profit': expected_profit,
                    'gas_limit': actual_gas_limit
                }
            }
        
        # Check if the estimated time exceeds the maximum allowed wait time
        if gas_calc['estimated_time'] > max_wait_time:
            return {
                'valid': False,
                'reason': f"Estimated wait time ({gas_calc['estimated_time']}s) exceeds maximum ({max_wait_time}s)",
                'details': {
                    'gas_price': gas_calc['gas_price'],
                    'estimated_time': gas_calc['estimated_time'],
                    'max_wait_time': max_wait_time
                }
            }
        
        # All checks passed - arbitrage is viable
        return {
            'valid': True,
            'gas_price': gas_calc['gas_price'],
            'gas_cost': gas_calc['gas_cost'],
            'profit_after_gas': gas_calc['profit_after_gas'],
            'estimated_time': gas_calc['estimated_time'],
            'network_congestion': gas_calc['network_congestion'],
            'recommendation': gas_calc['recommendation'],
            'gas_limit': actual_gas_limit
        }

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

    # Add a general helper method for web3 calls with retry
    async def _make_web3_call_with_retry(self, func, *args, max_retries: int = 3, initial_delay: float = 1.0):
        """
        Helper method to make web3 calls with retry logic.
        
        Args:
            func: The function to call
            *args: Arguments to pass to the function
            max_retries (int): Maximum number of retry attempts
            initial_delay (float): Initial delay between retries in seconds
            
        Returns:
            The result of the function call
        
        Raises:
            Exception: If all retry attempts fail
        """
        retries = 0
        delay = initial_delay
        last_error = None
        
        while retries < max_retries:
            try:
                return await asyncio.to_thread(func, *args)
            except Exception as e:
                retries += 1
                last_error = e
                
                if "Too Many Requests" in str(e) or "429" in str(e):
                    logging.warning(f"Rate limited in web3 call, retrying after {delay}s (attempt {retries}/{max_retries})")
                    await asyncio.sleep(delay)
                    # Exponential backoff
                    delay *= 2
                else:
                    logging.error(f"Error in web3 call: {e}")
                    if retries < max_retries:
                        await asyncio.sleep(delay)
                        delay *= 1.5
                    else:
                        break
        
        logging.error(f"Failed web3 call after {max_retries} attempts. Last error: {last_error}")
        raise last_error

# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(levelname)s - %(message)s')
    
    config = Config()
    
    # Initialize Web3 using an Infura URL (replace with your actual project ID)
    web3 = Web3(Web3.HTTPProvider(config.infura_url))
    
    # Example token addresses (Ethereum mainnet)
    tokens = [
        "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",  # WETH
        "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
        "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",  # WBTC
        "0x6B175474E89094C44Da98b954EedeAC495271d0F"   # DAI
    ]
    
    # Create the monitor instance
    monitor = TokenMonitor(token_addresses=tokens, web3=web3, refresh_interval=60)
    
    async def main():
        # Start the monitoring task
        monitor_task = await monitor.start_monitoring()
        
        try:
            # Wait for initial data refresh
            logging.info("Waiting for initial data refresh...")
            await asyncio.sleep(5)  # Give time for first refresh
            
            # Main loop
            while True:
                # Wait for token data to be populated
                if not monitor.token_data:
                    logging.info("Waiting for token data to be populated...")
                    await asyncio.sleep(5)
                    continue
                
                # Display current network status
                logging.info("Network Status: %s", "UP" if monitor.network_status else "DOWN")
                
                if monitor.network_status:
                    current_gas = monitor.current_gas_price
                    logging.info("Current Gas Price: %d wei", current_gas)
                    
                    congestion = monitor.network_congestion
                    logging.info("Network Congestion: %s (Avg Block Time: %.2fs, Pending TXs: %d)", 
                                "HIGH" if congestion.get('is_congested', False) else "NORMAL",
                                congestion.get('avg_block_time', 0),
                                congestion.get('pending_tx_count', 0))
                    
                    logging.info("Estimated Transfer Times:")
                    logging.info("  - At current gas price: %s", 
                               monitor.estimate_transfer_time(current_gas))
                    logging.info("  - At 20%% higher: %s", 
                               monitor.estimate_transfer_time(int(current_gas * 1.2)))
                    logging.info("  - At 50%% higher: %s", 
                               monitor.estimate_transfer_time(int(current_gas * 1.5)))
                    
                    logging.info("Token Data:")
                    for addr, data in monitor.get_token_data().items():
                        logging.info("  - %s (%s): %s txs, %s avg gas, %s%% success rate", 
                                   data.get('symbol', 'Unknown'), 
                                   addr[:10] + '...',
                                   data.get('transaction_count', 0),
                                   data.get('average_gas', 0),
                                   data.get('transfer_success_rate', 0) * 100)
                    
                    # Example arbitrage validation
                    # Make sure we use a token that's in our monitored list and has data
                    token_to_use = None
                    for addr in tokens:
                        checksum_addr = Web3.toChecksumAddress(addr)
                        if checksum_addr in monitor.token_data:
                            token_to_use = checksum_addr
                            break
                    
                    if token_to_use:
                        s_time = time.time()
                        logging.info(f"Validating arbitrage for token: {token_to_use}")
                        
                        # Create a Transaction object for more accurate gas estimation
                        tx = Transaction(
                            from_address=web3.eth.accounts[0] if web3.eth.accounts else "0x0000000000000000000000000000000000000000",
                            to_address=token_to_use,
                            value=0,  # No ETH being sent
                            data="0x"  # Empty data for this example
                        )
                        
                        arb_validation = monitor.validate_arbitrage_opportunity(
                            token_address=token_to_use,
                            amount=1000.0,  
                            expected_profit=5e16,  # 0.05 ETH profit in wei
                            gas_limit=250000,
                            max_wait_time=30,  # Must execute within 30 seconds
                            transaction=tx  # Pass the transaction object
                        )
                        logging.info(f"Arbitrage validation took {time.time() - s_time} seconds")
                        
                        if arb_validation['valid']:
                            logging.info(f"Arbitrage opportunity valid! Use gas price: {arb_validation['gas_price']} wei")
                            logging.info(f"Expected profit after gas: {arb_validation['profit_after_gas']} wei")
                            logging.info(f"Estimated wait time: {arb_validation['estimated_time']} seconds")
                        else:
                            logging.info(f"Arbitrage not viable: {arb_validation['reason']}")
                    else:
                        logging.warning("No monitored tokens have data yet, skipping arbitrage validation")
                
                # Wait before next update
                await asyncio.sleep(30)
                
        except asyncio.CancelledError:
            # Handle clean cancellation
            monitor_task.cancel()
            await monitor_task
        except Exception as e:
            logging.error(f"Error in main loop: {e}")
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass
    
    # Start the async main loop
    asyncio.run(main())

