import math
from typing import Any, Dict
from web3 import Web3, AsyncWeb3
from decimal import Decimal, getcontext
from web3.providers.persistent import WebSocketProvider
from web3.utils.subscriptions import LogsSubscription
import logging
import asyncio
import traceback

from config import Config
from .base_connector import BaseExchange

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("uniswap_pool_monitor")
TICK_BASE = Decimal(1.0001)

class Uniswap(BaseExchange):
    """
    A class for monitoring and interacting with Uniswap V3 pools.
    
    This class provides methods to fetch metadata about a specific Uniswap pool
    and its associated tokens. It establishes a connection to the Ethereum blockchain
    and interacts with smart contracts to retrieve the information.
    
    The metadata is loaded synchronously during initialization.
    """
    
    # Uniswap V3 Pool ABI (Minimal)
    POOL_ABI = [
        # Metadata
        {"constant": True, "inputs": [], "name": "token0", "outputs": [{"internalType": "address", "name": "", "type": "address"}], "type": "function"},
        {"constant": True, "inputs": [], "name": "token1", "outputs": [{"internalType": "address", "name": "", "type": "address"}], "type": "function"},
        {"constant": True, "inputs": [], "name": "fee", "outputs": [{"internalType": "uint24", "name": "", "type": "uint24"}], "type": "function"},
        {"inputs":[],"name":"tickSpacing","outputs":[{"internalType":"int24","name":"","type":"int24"}],"stateMutability":"view","type":"function"},
        # Events
        {"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"owner","type":"address"},{"indexed":True,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":True,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":False,"internalType":"uint128","name":"amount","type":"uint128"},{"indexed":False,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Burn","type":"event"},
        {"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"owner","type":"address"},{"indexed":True,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":True,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":False,"internalType":"uint128","name":"amount","type":"uint128"},{"indexed":False,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Mint","type":"event"},
        {"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"sender","type":"address"},{"indexed":True,"internalType":"address","name":"recipient","type":"address"},{"indexed":False,"internalType":"int256","name":"amount0","type":"int256"},{"indexed":False,"internalType":"int256","name":"amount1","type":"int256"},{"indexed":False,"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"indexed":False,"internalType":"uint128","name":"liquidity","type":"uint128"},{"indexed":False,"internalType":"int24","name":"tick","type":"int24"}],"name":"Swap","type":"event"},
        {"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"owner","type":"address"},{"indexed":False,"internalType":"address","name":"recipient","type":"address"},{"indexed":True,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":True,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":False,"internalType":"uint128","name":"amount0","type":"uint128"},{"indexed":False,"internalType":"uint128","name":"amount1","type":"uint128"}],"name":"Collect","type":"event"}
    ]

    # ERC20 Token ABI (Minimal)
    ERC20_ABI = [
        {"constant": True, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}], "payable": False, "stateMutability": "view", "type": "function"},
        {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "payable": False, "stateMutability": "view", "type": "function"},
        {"constant": True, "inputs": [{"name": "_owner", "type": "address"}], "name": "balanceOf", "outputs": [{"name": "balance", "type": "uint256"}], "payable": False, "stateMutability": "view", "type": "function"},
        {"constant": False, "inputs": [{"name": "_to", "type": "address"}, {"name": "_value", "type": "uint256"}], "name": "transfer", "outputs": [{"name": "success", "type": "bool"}], "payable": False, "stateMutability": "nonpayable", "type": "function"},
    ]
    
    def __init__(self, infura_url, infura_ws_url, pool_address, private_key, balance_update_interval=15):
        """
        Initialize the PoolMonitor with Ethereum connection and pool contract.
        
        Args:
            infura_url (str): HTTP URL endpoint for Infura or other Ethereum provider
            infura_ws_url (str): WebSocket URL endpoint for Infura or other Ethereum provider
            pool_address (str): Ethereum address of the Uniswap V3 pool to monitor
        
        Note:
            Metadata is loaded synchronously during initialization.
        """
        self.w3 = Web3(Web3.HTTPProvider(infura_url))
        self.async_w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(infura_url))
        self.ws_url = infura_ws_url
        self.w3_ws = None # async web3
        self.pool_address = Web3.to_checksum_address(pool_address)
        
        self.pool_contract = self.w3.eth.contract(address=self.pool_address, abi=self.POOL_ABI)
        self.wallet_private_key = private_key
        self.account = self.w3.eth.account.from_key(private_key)
        self.metadata = self.load_metadata()
        self.liquidity = None
        self.sqrt_price = None
        self.tick = None
        self.balances = {}
        self.balance_update_interval = balance_update_interval
    
    def load_metadata(self):
        """
        Fetch and log metadata about the Uniswap pool and its tokens.
        
        This method retrieves information about:
        - The addresses of the two tokens in the pool
        - The fee tier of the pool
        - The tick spacing
        - The symbols and decimal places of both tokens
        
        Returns:
            dict: A dictionary containing all the pool and token metadata
        """
        # Pool metadata
        fee_tier = self.pool_contract.functions.fee().call()
        tick_spacing = self.pool_contract.functions.tickSpacing().call()
        token0_address = self.pool_contract.functions.token0().call()
        token1_address = self.pool_contract.functions.token1().call()
        self.burn_topic = self.pool_contract.events.Burn.build_filter().topics[0]
        self.mint_topic = self.pool_contract.events.Mint.build_filter().topics[0]
        self.swap_topic = self.pool_contract.events.Swap.build_filter().topics[0]

        logger.info(f'Token0: {token0_address}')
        logger.info(f'Token1: {token1_address}')
        logger.info(f'Fee Tier: {fee_tier}')
        logger.info(f'Tick Spacing: {tick_spacing}')

        # Token metadata
        token0_contract = self.w3.eth.contract(address=token0_address, abi=self.ERC20_ABI)
        token1_contract = self.w3.eth.contract(address=token1_address, abi=self.ERC20_ABI)
        
        token0_symbol = token0_contract.functions.symbol().call()
        token0_decimals = token0_contract.functions.decimals().call()
        token1_symbol = token1_contract.functions.symbol().call()
        token1_decimals = token1_contract.functions.decimals().call()
        
        logger.info(f'Token0 Symbol: {token0_symbol}')
        logger.info(f'Token0 Decimals: {token0_decimals}')
        logger.info(f'Token1 Symbol: {token1_symbol}')
        logger.info(f'Token1 Decimals: {token1_decimals}')
        
        metadata = {
            "fee_tier": fee_tier,
            "tick_spacing": tick_spacing,
            "token0": {
                "address": token0_address,
                "symbol": token0_symbol,
                "decimals": token0_decimals
            },
            "token1": {
                "address": token1_address,
                "symbol": token1_symbol,
                "decimals": token1_decimals
            }
        }
        return metadata
    
    async def get_balance(self, symbol, live=False):
        """
        Get the balance of a token.
        """
        if live:
            return await self.update_balance_cache()
        else:
            return self.balances[symbol.lower()]
    
    async def update_balance_cache(self):
        """
        Update the balance cache.
        """
        for symbol_info in [self.metadata['token0'], self.metadata['token1']]:
            contract = self.async_w3.eth.contract(address=symbol_info['address'], abi=self.ERC20_ABI)
            symbol = symbol_info['symbol']
            decimals = symbol_info['decimals']
            balance = await contract.functions.balanceOf(self.account.address).call()
            symbol_balance = balance / (10 ** decimals)
            self.balances[symbol.lower()] = symbol_balance
        return self.balances
    
    async def _balance_update_loop(self):
        """
        Background loop to periodically update the balance cache.
        
        Args:
            address (str): Ethereum address to check balances for
        """
        while True:
            try:
                # Update the balance cache
                balance_data = await self.update_balance_cache()
                
                # Log the balances
                logger.info(f"Current balances for {self.account.address}:")
                for symbol, balance in balance_data.items():
                    logger.info(f"{symbol}: {balance}")
                
                # Sleep for the balance update interval
                await asyncio.sleep(self.balance_update_interval)
            except Exception as e:
                logger.error(f"Error in background balance update: {e}")
                logger.error(traceback.format_exc())
                await asyncio.sleep(1)

    async def _initialize_ws_connection(self):
        """
        Initialize the WebSocket connection.
        """
        if self.w3_ws is None:
            self.w3_ws = await AsyncWeb3(WebSocketProvider(self.ws_url))
        
    def get_relevant_pool_events(self):
        """
        Get the topic hashes for relevant Uniswap pool events.
        
        Returns:
            dict: A dictionary mapping event names to their keccak hashes with 0x prefix
        """
        topic_hashes = {
            "Swap":  self.swap_topic,
            "Mint": self.mint_topic,
            "Burn": self.burn_topic,
        }
        return topic_hashes
    
    def tick_to_sqrt_price_x96(self, tick):
        """
        Converts a tick to sqrtPriceX96.
        
        Args:
            tick (int): The tick value
            
        Returns:
            int: The sqrtPriceX96 value
        """
        return int(math.sqrt(TICK_BASE**tick) * (2**96))
    
    def get_tick_boundaries(self, target_tick=None):
        """
        Gets tick boundaries.
        """
        if target_tick is None:
            target_tick = self.tick

        tick_spacing = self.metadata['tick_spacing']
        lower_tick = (target_tick // tick_spacing) * tick_spacing
        upper_tick = lower_tick + tick_spacing

        return lower_tick, upper_tick
            
    def get_sqrt_price_x96_boundaries(self, target_tick=None):
        """
        Gets sqrtPriceX96 values for tick boundaries.
        
        Args:
            target_tick (int): The target tick
            
        Returns:
            tuple: (sqrt_price_lower, sqrt_price_upper) - The sqrtPriceX96 values for the boundaries
        """
        try:
            # Calculate the lower and upper tick boundaries
            lower_tick, upper_tick = self.get_tick_boundaries(target_tick)
            
            # Calculate sqrtPriceX96 values for the tick boundaries
            sqrt_price_lower = self.tick_to_sqrt_price_x96(lower_tick)
            sqrt_price_upper = self.tick_to_sqrt_price_x96(upper_tick)
            
            return sqrt_price_lower, sqrt_price_upper
        except Exception as e:
            logger.error(f"Error getting sqrtPriceX96 boundaries: {e}")
            logger.error(traceback.format_exc())
            return None, None
    
    # Sell token1, Buy token0
    # When token0 is taken out of pool => tick increases
    def compute_upward_swap_within_tick_boundaries(self):
        """
        Computes the maximum token0 in (and corresponding token1 out) that can be swapped 
        without crossing into the next tick boundary.
        
        For upward price movement (selling token0, buying token1), this calculates how much
        token0 can be added before reaching the next tick boundary.
        
        Args:
            tick (int): The current tick
            
        Returns:
            tuple: (token0_amount, token1_amount) - For upward swaps: (token0_in_max, token1_out_max)
        """
        if self.liquidity is None or self.sqrt_price is None:
            logger.warning("Liquidity or sqrt_price not available, cannot compute upward swap.")
            return Decimal(0), Decimal(0)
        
        # Get sqrtPriceX96 boundaries
        _, sqrt_price_upper = self.get_sqrt_price_x96_boundaries(self.tick)
        
        # Convert sqrtPrice values to Decimal and scale them down from X96 format
        # Divide by 2^96 to get the correct scale for price calculations
        scale_factor = Decimal(2) ** 96
        sqrt_price_current = Decimal(self.sqrt_price) / scale_factor
        sqrt_price_next = Decimal(sqrt_price_upper) / scale_factor
        
        # Convert liquidity to Decimal for precision
        # Liquidity in Uniswap v3 is expressed in L units
        liquidity = Decimal(self.liquidity)
        
        # Calculate maximum amounts using the Uniswap V3 formula
        # Δx = L * (√P₂ - √P₁) / (√P₁ * √P₂)
        token0_amount = liquidity * (sqrt_price_next - sqrt_price_current) / (sqrt_price_current * sqrt_price_next)
        
        # Δy = L * (√P₂ - √P₁)
        token1_amount = liquidity * (sqrt_price_next - sqrt_price_current)
        
        # Apply token decimal adjustments
        # These conversions depend on the specific token's decimal places
        token0_amount = token0_amount / Decimal(10**self.metadata['token0']['decimals'])
        token1_amount = token1_amount / Decimal(10**self.metadata['token1']['decimals'])
        
        return token0_amount, token1_amount
    
    # Buy token1, Sell token0
    # When token0 is added to pool => tick decreases
    def compute_downward_swap_within_tick_boundaries(self):
        """
        Computes the maximum token1 in (and corresponding token0 out) that can be swapped 
        without crossing into the previous tick boundary.
        
        For downward price movement (selling token1, buying token0), this calculates how much
        token1 can be added before reaching the previous tick boundary.
        
        Args:
            tick (int): The current tick
            
        Returns:
            tuple: (token0_amount, token1_amount) - For downward swaps: (token0_out_max, token1_in_max)
        """
        if self.liquidity is None or self.sqrt_price is None:
            logger.warning("Liquidity or sqrt_price not available, cannot compute downward swap.")
            return Decimal(0), Decimal(0)
            
        # Get sqrtPriceX96 boundaries
        sqrt_price_lower, _ = self.get_sqrt_price_x96_boundaries(self.tick)
        
        # Convert sqrtPrice values to Decimal and scale them down from X96 format
        # Divide by 2^96 to get the correct scale for price calculations
        scale_factor = Decimal(2) ** 96
        sqrt_price_current = Decimal(self.sqrt_price) / scale_factor
        sqrt_price_prev = Decimal(sqrt_price_lower) / scale_factor
        
        # Convert liquidity to Decimal for precision
        liquidity = Decimal(self.liquidity)
        
        # Calculate maximum amounts using the Uniswap V3 formula
        # For downward price movement (√P₁ > √P₀):
        # Δy = L * (√P₁ - √P₀)
        # This should always be positive since we're moving from current price to lower price
        token1_in_max = liquidity * (sqrt_price_current - sqrt_price_prev)
        
        # Δx = L * (√P₁ - √P₀) / (√P₀ * √P₁)
        # This should always be positive since we're receiving token0
        token0_out_max = liquidity * (sqrt_price_current - sqrt_price_prev) / (sqrt_price_current * sqrt_price_prev)
        
        # Apply token decimal adjustments
        token0_out_max = token0_out_max / Decimal(10**self.metadata['token0']['decimals'])
        token1_in_max = token1_in_max / Decimal(10**self.metadata['token1']['decimals'])
        
        # Ensure we don't return negative values due to calculation precision errors
        token0_out_max = max(Decimal(0), token0_out_max)
        token1_in_max = max(Decimal(0), token1_in_max)
        
        # Return in token0, token1 order for consistency
        return token0_out_max, token1_in_max
    
    def compute_price_from_sqrt_price_x96(self, sqrt_price_x96=None):
        """
        Calculates the price from sqrtPriceX96.

        for token0 and token1
        """
        if sqrt_price_x96 is None:
            sqrt_price_x96 = self.sqrt_price

        # Convert sqrtPriceX96 to Decimal and scale it down from X96 format
        scale_factor = Decimal(2) ** 96
        sqrt_price_x96 = Decimal(sqrt_price_x96) / scale_factor
        
        # Calculate price of token0 in terms of token1
        price_token0_in_token1 = sqrt_price_x96 ** 2
        
        # Calculate price of token1 in terms of token0 (inverse)
        price_token1_in_token0 = 1 / price_token0_in_token1 if price_token0_in_token1 != 0 else 0

        # Adjust for decimal differences
        decimal_adjustment = Decimal(10**self.metadata['token1']['decimals']) / Decimal(10**self.metadata['token0']['decimals'])
        price_token0_in_token1 = price_token0_in_token1 / decimal_adjustment
        price_token1_in_token0 = price_token1_in_token0 * decimal_adjustment
        
        return price_token0_in_token1, price_token1_in_token0
    
    def calculate_reserves_from_liquidity(self):
        """
        Calculate the reserves of token0 and token1 using the Uniswap V3 whitepaper formulas.
        
        Using formulas: 
            x = L/√Pa - L/√Pb
            y = L(√Pb - √Pa)
        
        Where:
            - x is the reserves of token0
            - y is the reserves of token1
            - L is the liquidity
            - Pa is the lower bound price (sqrtRatioAX96)
            - Pb is the upper bound price (sqrtRatioBX96)
        
        Args:
            liquidity (int): The current liquidity in the pool
            sqrt_price_x96 (int): The current square root price in X96 format
            current_tick (int): The current tick
            
        Returns:
            tuple: (reserve0, reserve1, reserve0_hr, reserve1_hr) - Reserves in both raw and human-readable formats
        """
        try:
            # Convert inputs to safer floating-point numbers
            liquidity = float(self.liquidity)
            sqrt_price_x96 = float(self.sqrt_price)
            
            # Calculate current sqrt price (P)
            sqrt_price = sqrt_price_x96 / (2**96)
            
            # Get the tick boundaries and their sqrtPriceX96 values
            sqrt_price_a_x96, sqrt_price_b_x96 = self.get_sqrt_price_x96_boundaries()
            
            # Convert to floating point for calculations
            sqrt_price_a = sqrt_price_a_x96 / (2**96)
            sqrt_price_b = sqrt_price_b_x96 / (2**96)
            
            # Calculate reserves using the whitepaper formulas
            # For token0 (x): L/√Pa - L/√Pb
            if sqrt_price <= sqrt_price_b and sqrt_price >= sqrt_price_a:
                # Current price is within range
                reserve0 = liquidity * (1/sqrt_price - 1/sqrt_price_b)
                reserve1 = liquidity * (sqrt_price - sqrt_price_a)
            elif sqrt_price < sqrt_price_a:
                # Current price is below range
                reserve0 = liquidity * (1/sqrt_price_a - 1/sqrt_price_b)
                reserve1 = 0
            else:  # sqrt_price > sqrt_price_b
                # Current price is above range
                reserve0 = 0
                reserve1 = liquidity * (sqrt_price_b - sqrt_price_a)
            
            # Convert to human-readable format with proper decimals
            reserve0 = reserve0 / (10 ** self.metadata['token0']['decimals'])
            reserve1 = reserve1 / (10 ** self.metadata['token1']['decimals'])

            return reserve0, reserve1
        
        except Exception as e:
            logger.error(f"Error calculating reserves: {e}")
            logger.error(traceback.format_exc())
            return None, None

    
    def process_mint(self, event):
        """
        Process a mint event.
        """
        mint_event = self.pool_contract.events.Mint().process_log(event)
        tick_lower = mint_event.args.tickLower
        tick_upper = mint_event.args.tickUpper
        if tick_lower <= self.tick and tick_upper >= self.tick:
            self.liquidity += mint_event.args.amount
            self.log_current_state()
        else:
            logger.info(f"Mint event is not within the tick boundaries: {tick_lower} <= {self.tick} <= {tick_upper}")
    
    def process_burn(self, event):
        """
        Process a burn event.
        """
        burn_event = self.pool_contract.events.Burn().process_log(event)
        tick_lower = burn_event.args.tickLower
        tick_upper = burn_event.args.tickUpper
        if tick_lower <= self.tick and tick_upper >= self.tick:
            self.liquidity -= burn_event.args.amount
            self.log_current_state()
        else:
            logger.info(f"Burn event is not within the tick boundaries: {tick_lower} <= {self.tick} <= {tick_upper}")
        
    def process_swaps(self, event):
        """
        Process a swap event.
        """
        swap_event = self.pool_contract.events.Swap().process_log(event)
        self.liquidity = swap_event.args.liquidity
        self.sqrt_price = swap_event.args.sqrtPriceX96
        self.tick = swap_event.args.tick
        
        sqrt_price_lower, sqrt_price_upper = self.get_sqrt_price_x96_boundaries(self.tick)
        lower_tick, upper_tick = self.get_tick_boundaries(self.tick)
        logger.info(f"Tick: {self.tick}, Sqrt price: {self.sqrt_price}")
        logger.info(f"Lower tick: {lower_tick}, Upper tick: {upper_tick}")
        logger.info(f"Sqrt price lower: {sqrt_price_lower}, Sqrt price upper: {sqrt_price_upper}")
        assert sqrt_price_lower <= self.sqrt_price <= sqrt_price_upper, "sqrtPriceX96 is not within the tick boundaries"
        self.log_current_state()

    def log_current_state(self):
        # Compute max theoretical size of the position within the tick boundaries
        
        # For upward price movement (selling token1, buying token0)
        token0_out_max, token1_in_max = self.compute_upward_swap_within_tick_boundaries()

        # For downward price movement (selling token1, buying token0)
        token0_in_max, token1_out_max = self.compute_downward_swap_within_tick_boundaries()

        # Compute the price from sqrtPriceX96 - this is the current market price
        token0_price, token1_price = self.compute_price_from_sqrt_price_x96()

        # Compute the reserves
        reserve0, reserve1 = self.calculate_reserves_from_liquidity()

        # Calculate effective swap prices
        # For buying WETH (token1): How much USDC (token0) per WETH
        upward_effective_price = token0_in_max / token1_out_max if token1_out_max > 0 else 0
        
        # For selling WETH (token1): How much USDC (token0) per WETH
        downward_effective_price = token0_out_max / token1_in_max if token1_in_max > 0 else 0
        
        # The standard price quote is USDC per WETH (token1_price)
        current_market_price = token1_price
        
        # Calculate price impact compared to current price
        # For buying WETH: price impact should be positive if effective price is higher than market (worse)
        upward_price_impact = ((upward_effective_price / current_market_price) - 1) * 10_000 if upward_effective_price > 0 else 0
        
        # For selling WETH: price impact should be positive if effective price is lower than market (worse)
        downward_price_impact = (1 - (downward_effective_price / current_market_price)) * 10_000 if downward_effective_price > 0 else 0
        
        # Log with more intuitive naming and include price impacts
        logger.info(f"Current market price: 1 {self.metadata['token1']['symbol']} = {current_market_price:.4f} {self.metadata['token0']['symbol']}")
        
        logger.info(f"Upward swap (Buy {self.metadata['token1']['symbol']}): "
                   f"{self.metadata['token0']['symbol']} in max: {token0_in_max:.6f}, "
                   f"{self.metadata['token1']['symbol']} out max: {token1_out_max:.6f}, "
                   f"Effective price: {upward_effective_price:.4f} "
                   f"(Price impact: {upward_price_impact:.2f} bps)")
                   
        logger.info(f"Downward swap (Sell {self.metadata['token1']['symbol']}): "
                   f"{self.metadata['token1']['symbol']} in max: {token1_in_max:.6f}, "
                   f"{self.metadata['token0']['symbol']} out max: {token0_out_max:.6f}, "
                   f"Effective price: {downward_effective_price:.4f} "
                   f"(Price impact: {downward_price_impact:.2f} bps)")
                   
        logger.info(f"Reserves - {self.metadata['token0']['symbol']}: {reserve0:.2f}, "
                   f"{self.metadata['token1']['symbol']}: {reserve1:.2f}")

    async def process_event(self, event):
        """
        Process a pool event.
        
        Args:
            event: The event data received from the blockchain
        """
        try:
            event = event.result

            # Get the topic hash from the event
            topic_hash = f"0x{event['topics'][0].hex()}" if 'topics' in event and event['topics'] else 'Unknown'
            
            # Find which event type this is
            event_type = next((name for name, hash in self.get_relevant_pool_events().items() 
                              if hash.lower() == topic_hash.lower()), "Unknown")
            
            logger.info(f"Processing {event_type}")
            
            # Add more detailed event processing here based on event_type
            if event_type == "Swap":
                self.process_swaps(event)
            elif event_type == "Mint":
                self.process_mint(event)
            elif event_type == "Burn":
                self.process_burn(event)
            else:
                logger.error(f"Unknown event: {event}")
            
        except Exception as e:
            logger.error(f"Error processing event: {e}")
            logger.error(traceback.format_exc())
    
    async def start_listening_chain_events(self):
        """
        Start listening for relevant pool events.
        """
        try:
            await self._initialize_ws_connection()
            logger.info(f'Subscribing to {", ".join(self.get_relevant_pool_events().keys())} topics on {self.pool_address}')
            
            # Create the subscription
            pool_subscription = LogsSubscription(
                label='pool_subscription',
                address=self.pool_address,  
                topics=[list(self.get_relevant_pool_events().values())],
                handler=self.process_event
            )
            
            subscription_id = await self.w3_ws.subscription_manager.subscribe([pool_subscription])
            logger.info(f'Subscription ID: {subscription_id}')
            
            # Handle subscriptions (this is a long-running task)
            await self.w3_ws.subscription_manager.handle_subscriptions()
            
        except Exception as e:
            logger.error(f"Error in event listener: {e}")
            logger.error(traceback.format_exc())

    async def start_event_listener(self):
        """
        Start the event listener.
        """
        asyncio.create_task(self._balance_update_loop())
        asyncio.create_task(self.start_listening_chain_events())
    
    async def get_deposit_address(self, instrument: str) -> str:
        """
        Get the deposit address for a specific instrument.
        """
        return self.account.address
    
    async def confirm_trade(self, trade: Dict[str, Any]) -> float:
        """
        Confirm that a trade was executed and return the confirmed size.
        """
        return trade['size']
    
    async def get_current_price(self, asset: str) -> float:
        """
        Get the current price of an asset.
        """
        token0_price, token1_price = self.compute_price_from_sqrt_price_x96()
        if asset == self.metadata['token0']['symbol']:
            return token0_price
        elif asset == self.metadata['token1']['symbol']:
            return token1_price
        else:
            raise ValueError(f"Unsupported asset: {asset}")
    
    def get_asset_address(self, asset: str) -> str:
        """
        Get the address of an asset.
        """
        if asset == self.metadata['token0']['symbol']:
            return self.metadata['token0']['address']
        elif asset == self.metadata['token1']['symbol']:
            return self.metadata['token1']['address']
        elif asset == 'ETH':
            return self.async_w3.to_checksum_address("0x3e6b04c2f793d77d6414075aae1d44ef474b483e")
        else:
            raise ValueError(f"Unsupported asset: {asset}")
    
    def get_asset_decimals(self, asset: str) -> int:
        """
        Get the decimals of an asset.
        """
        if asset == self.metadata['token0']['symbol']:
            return self.metadata['token0']['decimals']
        elif asset == self.metadata['token1']['symbol']:
            return self.metadata['token1']['decimals']
        elif asset == 'ETH':
            return 18
        else:
            raise ValueError(f"Unsupported asset: {asset}")
    


    async def withdraw(self, asset: str, address: str, amount: float) -> Dict[str, Any]:
        """
        Withdraw assets from the account to a specified address.
        
        WHY: This method enables withdrawing both ETH and ERC20 tokens from the account,
        handling the different logic required for each case while maintaining consistent error handling.
        
        Args:
            asset (str): Asset symbol to withdraw (e.g. "ETH", "USDC")
            address (str): Destination address to withdraw to
            amount (float): Amount to withdraw
            
        Returns:
            Dict containing:
                status (str): 'success' or 'error'
                tx_hash (str): Transaction hash if successful
                error (str): Error message if failed
                amount (float): Amount withdrawn
        """
        try:
            # Validate address
            to_address = self.async_w3.to_checksum_address(address)
            token_address = self.get_asset_address(asset)
            contract = self.async_w3.eth.contract(
                address=token_address, 
                abi=self.ERC20_ABI
            )
            token_decimals = self.get_asset_decimals(asset)
            amount_wei = int(amount * (10 ** token_decimals))
            
            # Check balance
            if asset == 'ETH':
                balance = await self.async_w3.eth.get_balance(self.account.address)
            else:
                balance = await contract.functions.balanceOf(self.account.address).call()
            if balance < amount_wei:
                raise ValueError(f"Insufficient {asset} balance {balance / (10 ** token_decimals)}")

            # Build transaction
            nonce = await self.async_w3.eth.get_transaction_count(self.account.address)
            tx = await contract.functions.transfer(to_address, amount_wei).build_transaction({
                'nonce': nonce,
                'gas': await contract.functions.transfer(to_address, amount_wei).estimate_gas(),
                'gasPrice': await self.async_w3.eth.gas_price,  # or web3.toWei('20', 'gwei')
                'chainId': await self.async_w3.eth.chain_id,
            })

            signed_tx = self.async_w3.eth.account.sign_transaction(tx, self.wallet_private_key)
            while True:
                tx_hash = await self.async_w3.eth.send_raw_transaction(signed_tx.raw_transaction)
                tx_hash = self.async_w3.to_hex(tx_hash)
                if tx_hash:
                    break
                else:
                    logger.info("Transaction failed, retrying...")
                    await asyncio.sleep(1)

            return {
                'status': 'success',
                'tx_hash': tx_hash,
                'amount': amount
            }
        except Exception as e:
            logger.error(f"Withdrawal failed: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                'status': 'error',
                'error': str(e),
                'amount': 0
            }
        
    async def confirm_withdrawal(self, withdrawal_info: Dict[str, Any]) -> float:
        """
        Confirm that a withdrawal was completed and return the confirmed size.
        """
        if withdrawal_info['status'] == 'error':
            return 0
        else:
            tx_receipt = await self.async_w3.eth.wait_for_transaction_receipt(withdrawal_info['tx_hash'])
            logger.info(f"Transaction receipt: {tx_receipt}")
            if tx_receipt and 'status' in tx_receipt:
                return withdrawal_info['amount']
            else:
                return 0
    
    async def confirm_deposit(self, asset: str, amount: float, tolerance: float = 1e-6) -> float:
        """
        Confirm that a deposit was completed and return the confirmed size.
        """
        # Get contract instance for the token
        address = self.get_asset_address(asset)
        contract = self.async_w3.eth.contract(
            address=address,
            abi=self.ERC20_ABI
        )
        
        # Get recent transfer events to this address
        latest_block = await self.async_w3.eth.block_number
        from_block = latest_block - 1000  # Look back ~1000 blocks
        
        # Get Transfer events where recipient is our address
        transfer_filter = contract.events.Transfer.create_filter(
            fromBlock=from_block,
            toBlock='latest',
            argument_filters={'to': self.account.address}
        )
        
        # Check transfer events for matching amount
        events = await transfer_filter.get_all_entries()
        for event in events:
            transfer_amount = event.args.value / (10 ** self.get_asset_decimals(asset))
            if abs(transfer_amount - amount) <= tolerance:
                return transfer_amount
    
    async def execute_trade(self, trade_direction, trade_size):
        """
        Execute a trade.
        """
        logger.info(f"Executing {trade_direction} trade of {trade_size}")
        return trade_size
    

async def main():
    """
    Main async function to run the pool monitor.
    """
    try:
        config = Config()
        # Example usage
        infura_url = config.infura_url
        infura_ws_url = config.infura_ws_url
        pool_address = config.instrument_config[0]['pool_address']  
        private_key = config.wallet_private_key
        
        # Create monitor - metadata is loaded synchronously in __init__
        uniswap = Uniswap(infura_url, infura_ws_url, pool_address, private_key)
        logger.info(f"Pool contains {uniswap.metadata['token0']['symbol']} and {uniswap.metadata['token1']['symbol']}")
        
        # Start the event listener
        await uniswap.start_event_listener()

        # Test withdraw
        deposit_address = "0x3e6b04c2f793d77d6414075aae1d44ef474b483e"
        withdrawal_info = await uniswap.withdraw(asset="ETH", address=deposit_address, amount=0.002)
        logger.info(f"Withdrawal info: {withdrawal_info}")
        # withdrawal_info = {'status': 'success', 'tx_hash': '0xbe06419a19b6b13c14f7aa1a950bc78b00b3321378f5bd0b5c4d1dc2c7148ded', 'amount': 0.002}
        confirmed_amount = await uniswap.confirm_withdrawal(withdrawal_info)
        logger.info(f"Confirmed amount: {confirmed_amount}")


        while True:
            await asyncio.sleep(10)
    except Exception as e:
        logger.error(f"Error in main: {e}")
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())