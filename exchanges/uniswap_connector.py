import json
import math
import time
from typing import Any, Dict
from web3 import Web3, AsyncWeb3
from web3.providers.persistent import WebSocketProvider
from web3.utils.subscriptions import LogsSubscription
import logging
import asyncio
import traceback

from config import Config
from exchanges.base_connector import BaseExchange
from token_monitoring import TokenMonitor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger("uniswap_pool_monitor")
TICK_BASE = float(1.0001)

class Token:
    def __init__(self, address: str, symbol: str, decimals: int):
        self.address = address
        self.symbol = symbol
        self.decimals = decimals

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
    
    def __init__(self, infura_url, infura_ws_url, pool_address, private_key, base_asset: str, quote_asset: str, balance_update_interval=15):
        """
        Initialize the PoolMonitor with Ethereum connection and pool contract.
        
        Args:
            infura_url (str): HTTP URL endpoint for Infura or other Ethereum provider
            infura_ws_url (str): WebSocket URL endpoint for Infura or other Ethereum provider
            pool_address (str): Ethereum address of the Uniswap V3 pool to monitor
        
        Note:
            Metadata is loaded synchronously during initialization.
        """
        self.infura_url = infura_url
        self.async_w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(infura_url))
        self.ws_url = infura_ws_url
        self.w3_ws = None # async web3
        self.pool_address = Web3.to_checksum_address(pool_address)
        self.wallet_private_key = private_key
        self.account = self.async_w3.eth.account.from_key(self.wallet_private_key)
        
        self.pool_contract = self.async_w3.eth.contract(address=self.pool_address, abi=self.POOL_ABI)
        self.liquidity = None
        self.sqrt_price = None
        self.tick = None
        self.balances = {}
        self.balance_update_interval = balance_update_interval
        self.base_asset = base_asset
        self.quote_asset = quote_asset
    
    async def init(self):
        """
        Initialize the Uniswap connector.
        """
        self.metadata = await self.load_metadata()
        self.token_monitor = TokenMonitor(
            token_addresses=[self.metadata['token0'].address, self.metadata['token1'].address],
            infura_url=self.infura_url,
            v3_pool_addresses=[self.pool_address]
        )
        await self.start_event_listener()
    
    async def load_metadata(self):
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
        fee_tier = await self.pool_contract.functions.fee().call()
        tick_spacing = await self.pool_contract.functions.tickSpacing().call()
        token0_address = await self.pool_contract.functions.token0().call()
        token1_address = await self.pool_contract.functions.token1().call()
        self.burn_topic = self.pool_contract.events.Burn.build_filter().topics[0]
        self.mint_topic = self.pool_contract.events.Mint.build_filter().topics[0]
        self.swap_topic = self.pool_contract.events.Swap.build_filter().topics[0]

        logger.info(f'Token0: {token0_address}')
        logger.info(f'Token1: {token1_address}')
        logger.info(f'Fee Tier: {fee_tier}')
        logger.info(f'Tick Spacing: {tick_spacing}')

        # Token metadata
        token0_contract = self.async_w3.eth.contract(address=token0_address, abi=self.ERC20_ABI)
        token1_contract = self.async_w3.eth.contract(address=token1_address, abi=self.ERC20_ABI)
        
        token0_symbol = await token0_contract.functions.symbol().call()
        token0_decimals = await token0_contract.functions.decimals().call()
        token1_symbol = await token1_contract.functions.symbol().call()
        token1_decimals = await token1_contract.functions.decimals().call()
        
        logger.info(f'Token0 Symbol: {token0_symbol}')
        logger.info(f'Token0 Decimals: {token0_decimals}')
        logger.info(f'Token1 Symbol: {token1_symbol}')
        logger.info(f'Token1 Decimals: {token1_decimals}')
        
        metadata = {
            "fee_tier": fee_tier,
            "tick_spacing": tick_spacing,
            "token0": Token(token0_address, token0_symbol, token0_decimals),
            "token1": Token(token1_address, token1_symbol, token1_decimals)
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
            contract = self.async_w3.eth.contract(address=symbol_info.address, abi=self.ERC20_ABI)
            symbol = symbol_info.symbol
            decimals = symbol_info.decimals
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
            return float(0), float(0)
        
        # Get sqrtPriceX96 boundaries
        _, sqrt_price_upper = self.get_sqrt_price_x96_boundaries(self.tick)
        
        # Convert sqrtPrice values to Decimal and scale them down from X96 format
        # Divide by 2^96 to get the correct scale for price calculations
        scale_factor = float(2) ** 96
        sqrt_price_current = float(self.sqrt_price) / scale_factor
        sqrt_price_next = float(sqrt_price_upper) / scale_factor
        
        # Convert liquidity to Decimal for precision
        # Liquidity in Uniswap v3 is expressed in L units
        liquidity = float(self.liquidity)
        
        # Calculate maximum amounts using the Uniswap V3 formula
        # Δx = L * (√P₂ - √P₁) / (√P₁ * √P₂)
        token0_amount = liquidity * (sqrt_price_next - sqrt_price_current) / (sqrt_price_current * sqrt_price_next)
        
        # Δy = L * (√P₂ - √P₁)
        token1_amount = liquidity * (sqrt_price_next - sqrt_price_current)
        
        # Apply token decimal adjustments
        # These conversions depend on the specific token's decimal places
        token0_amount = token0_amount / float(10**self.metadata['token0'].decimals)
        token1_amount = token1_amount / float(10**self.metadata['token1'].decimals)
        
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
            return float(0), float(0)
            
        # Get sqrtPriceX96 boundaries
        sqrt_price_lower, _ = self.get_sqrt_price_x96_boundaries(self.tick)
        
        # Convert sqrtPrice values to Decimal and scale them down from X96 format
        # Divide by 2^96 to get the correct scale for price calculations
        scale_factor = float(2) ** 96
        sqrt_price_current = float(self.sqrt_price) / scale_factor
        sqrt_price_prev = float(sqrt_price_lower) / scale_factor
        
        # Convert liquidity to Decimal for precision
        liquidity = float(self.liquidity)
        
        # Calculate maximum amounts using the Uniswap V3 formula
        # For downward price movement (√P₁ > √P₀):
        # Δy = L * (√P₁ - √P₀)
        # This should always be positive since we're moving from current price to lower price
        token1_in_max = liquidity * (sqrt_price_current - sqrt_price_prev)
        
        # Δx = L * (√P₁ - √P₀) / (√P₀ * √P₁)
        # This should always be positive since we're receiving token0
        token0_out_max = liquidity * (sqrt_price_current - sqrt_price_prev) / (sqrt_price_current * sqrt_price_prev)
        
        # Apply token decimal adjustments
        token0_out_max = token0_out_max / float(10**self.metadata['token0'].decimals)
        token1_in_max = token1_in_max / float(10**self.metadata['token1'].decimals)
        
        # Ensure we don't return negative values due to calculation precision errors
        token0_out_max = max(float(0), token0_out_max)
        token1_in_max = max(float(0), token1_in_max)
        
        # Return in token0, token1 order for consistency
        return token0_out_max, token1_in_max
    
    def compute_price_from_sqrt_price_x96(self, sqrt_price_x96=None):
        """
        Calculates the price from sqrtPriceX96.

        for token0 and token1
        """
        if sqrt_price_x96 is None:
            sqrt_price_x96 = self.sqrt_price

        if sqrt_price_x96 is None:
            return None, None
        # Convert sqrtPriceX96 to Decimal and scale it down from X96 format
        scale_factor = float(2) ** 96
        sqrt_price_x96 = float(sqrt_price_x96) / scale_factor
        
        # Calculate price of token0 in terms of token1
        price_token0_in_token1 = sqrt_price_x96 ** 2
        
        # Calculate price of token1 in terms of token0 (inverse)
        price_token1_in_token0 = 1 / price_token0_in_token1 if price_token0_in_token1 != 0 else 0

        # Adjust for decimal differences
        decimal_adjustment = float(10**self.metadata['token1'].decimals) / float(10**self.metadata['token0'].decimals)
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
            reserve0 = reserve0 / (10 ** self.metadata['token0'].decimals)
            reserve1 = reserve1 / (10 ** self.metadata['token1'].decimals)

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
        logger.info(f"Current market price: 1 {self.metadata['token1'].symbol} = {current_market_price:.4f} {self.metadata['token0'].symbol}")
        
        logger.info(f"Upward swap (Buy {self.metadata['token1'].symbol}): "
                   f"{self.metadata['token0'].symbol} in max: {token0_in_max:.6f}, "
                   f"{self.metadata['token1'].symbol} out max: {token1_out_max:.6f}, "
                   f"Effective price: {upward_effective_price:.4f} "
                   f"(Price impact: {upward_price_impact:.2f} bps)")
                   
        logger.info(f"Downward swap (Sell {self.metadata['token1'].symbol}): "
                   f"{self.metadata['token1'].symbol} in max: {token1_in_max:.6f}, "
                   f"{self.metadata['token0'].symbol} out max: {token0_out_max:.6f}, "
                   f"Effective price: {downward_effective_price:.4f} "
                   f"(Price impact: {downward_price_impact:.2f} bps)")
                   
        logger.info(f"Reserves - {self.metadata['token0'].symbol}: {reserve0:.2f}, "
                   f"{self.metadata['token1'].symbol}: {reserve1:.2f}")

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
        await self.token_monitor.start_monitoring()
    
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
        if asset == self.metadata['token0'].symbol:
            return token0_price
        elif asset == self.metadata['token1'].symbol:
            return token1_price
        else:
            raise ValueError(f"Unsupported asset: {asset}")
    
    def get_asset_address(self, asset: str) -> str:
        """
        Get the address of an asset.
        """
        if asset == self.metadata['token0'].symbol:
            return self.metadata['token0'].address
        elif asset == self.metadata['token1'].symbol:
            return self.metadata['token1'].address
        elif asset == 'ETH':
            return self.async_w3.to_checksum_address(self.token_monitor.ETH_ADDRESS)
        else:
            raise ValueError(f"Unsupported asset: {asset}")
    
    def get_asset_decimals(self, asset: str) -> int:
        """
        Get the decimals of an asset.
        """
        if asset == self.metadata['token0'].symbol:
            return self.metadata['token0'].decimals
        elif asset == self.metadata['token1'].symbol:
            return self.metadata['token1'].decimals
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
            token_decimals = self.get_asset_decimals(asset)
            amount_wei = int(amount * (10 ** token_decimals))
            
            # Handle ETH and token transfers differently
            if asset == 'ETH':
                # Check ETH balance
                balance = await self.async_w3.eth.get_balance(self.account.address)
                if balance < amount_wei:
                    raise ValueError(f"Insufficient ETH balance: {balance / 10**18} ETH")
                
                # For ETH transfers, we directly create a transaction with value
                nonce = await self.async_w3.eth.get_transaction_count(self.account.address)
                gas_price = await self.async_w3.eth.gas_price
                
                # Create the transaction
                tx = {
                    'nonce': nonce,
                    'to': to_address,
                    'value': amount_wei,  # This is the ETH amount in wei
                    'gas': 21000,  # Standard gas limit for ETH transfers
                    'gasPrice': gas_price,
                    'chainId': await self.async_w3.eth.chain_id,
                }
                
                logger.info(f"ETH transfer: amount_wei={amount_wei}, tx={tx}")
            else:
                # For tokens, get the token contract
                token_address = self.get_asset_address(asset)
                contract = self.async_w3.eth.contract(
                    address=token_address, 
                    abi=self.ERC20_ABI
                )
                
                # Check token balance
                balance = await contract.functions.balanceOf(self.account.address).call()
                if balance < amount_wei:
                    raise ValueError(f"Insufficient {asset} balance {balance / (10 ** token_decimals)}")

                # Build token transfer transaction
                nonce = await self.async_w3.eth.get_transaction_count(self.account.address)
                tx = await contract.functions.transfer(to_address, amount_wei).build_transaction({
                    'nonce': nonce,
                    'gas': await contract.functions.transfer(to_address, amount_wei).estimate_gas(),
                    'gasPrice': await self.async_w3.eth.gas_price,
                    'chainId': await self.async_w3.eth.chain_id,
                })
                
                logger.info(f"Token transfer: amount_wei={amount_wei}, tx={tx}")

            # Sign and send the transaction (same for both ETH and tokens)
            signed_tx = self.async_w3.eth.account.sign_transaction(tx, self.wallet_private_key)
            tx_hash = await self.async_w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            tx_hash = self.async_w3.to_hex(tx_hash)

            return {
                'status': 'success',
                'tx_hash': tx_hash,
                'amount': amount,
                'asset': asset
            }
        except Exception as e:
            logger.error(f"Withdrawal failed: {str(e)}")
            logger.error(traceback.format_exc())
            return {
                'status': 'error',
                'error': str(e),
                'amount': 0,
                'asset': asset
            }
        
    async def confirm_withdrawal(self, withdrawal_info: Dict[str, Any]) -> float:
        """
        Confirm that a withdrawal was completed and return the confirmed size.
        
        WHY: Verifies both transaction success and amount to ensure the withdrawal
        completed as expected before reporting back to the caller.
        """
        if withdrawal_info['status'] == 'error':
            return 0
        
        tx_receipt = await self.async_w3.eth.wait_for_transaction_receipt(withdrawal_info['tx_hash'])
        logger.info(f"Transaction receipt: {tx_receipt}")
        
        if tx_receipt and tx_receipt.get('status') == 1:
            # Verify the transaction amount from the transaction data
            tx = await self.async_w3.eth.get_transaction(withdrawal_info['tx_hash'])
            logger.info(f"Transaction: {tx}")
            decimals = self.get_asset_decimals(withdrawal_info['asset'])
            confirmed_amount = tx['value'] / 10**decimals
            return confirmed_amount
        
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
    
    async def swap_instrument(self, amount: float, token_address: str, slippage: float = 0.05, deadline: int = 60) -> str:
        """
        Execute a swap transaction using Uniswap V3 SwapRouter.
        """
        try:
            # Define Uniswap V3 Router address
            UNISWAP_V3_ROUTER = "0xE592427A0AEce92De3Edee1F18E0157C05861564"  # Mainnet SwapRouter
            
            # Verify token is in pool
            token0 = self.metadata['token0'].address
            token1 = self.metadata['token1'].address
            token_address = self.async_w3.to_checksum_address(token_address)
            
            logger.info(f"Swap parameters: amount={amount}, token_address={token_address}")
            logger.info(f"Pool tokens: token0={token0}, token1={token1}")
            
            if token_address not in [token0, token1]:
                raise ValueError(f"Token address {token_address} must be one of the pool's tokens ({token0} or {token1})")
            
            # Determine which token is being swapped in/out
            is_token0 = (token_address == token0)
            token_in = token0 if is_token0 else token1
            token_out = token1 if is_token0 else token0
            
            logger.info(f"Token in: {token_in}, Token out: {token_out}, Is token0: {is_token0}")
            
            # Get token contract and decimals
            token_contract = self.async_w3.eth.contract(address=token_address, abi=self.erc20_abi)
            decimals = self.get_asset_decimals(token_in)
            logger.info(f"Token decimals: {decimals}")
            
            # Check token balance
            token_balance = await token_contract.functions.balanceOf(self.account.address).call()
            token_balance_human = token_balance / (10 ** decimals)
            logger.info(f"Token balance: {token_balance_human} ({token_balance} wei)")
            
            # Convert amount to wei
            amount_wei = int(amount * (10 ** decimals))
            logger.info(f"Amount in wei: {amount_wei}")
            
            if token_balance < amount_wei:
                logger.error(f"Insufficient token balance: {token_balance_human} < {amount}")
                return None
            
            # Initialize router contract with ABI
            router_abi = json.loads('''[
                {"inputs":[{"components":[{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint24","name":"fee","type":"uint24"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMinimum","type":"uint256"},{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"}],"internalType":"struct ISwapRouter.ExactInputSingleParams","name":"params","type":"tuple"}],"name":"exactInputSingle","outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"}],"stateMutability":"payable","type":"function"},
                {"inputs":[{"components":[{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint24","name":"fee","type":"uint24"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint256","name":"amountInMaximum","type":"uint256"},{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"}],"internalType":"struct ISwapRouter.ExactOutputSingleParams","name":"params","type":"tuple"}],"name":"exactOutputSingle","outputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"}],"stateMutability":"payable","type":"function"}
            ]''')
            
            router_contract = self.async_w3.eth.contract(
                address=self.async_w3.to_checksum_address(UNISWAP_V3_ROUTER),
                abi=router_abi
            )
            
            # Check if token approval is needed
            allowance = token_contract.functions.allowance(
                self.account.address, 
                self.async_w3.to_checksum_address(UNISWAP_V3_ROUTER)
            ).call()
            
            logger.info(f"Current allowance: {allowance / (10 ** decimals)} ({allowance} wei)")
            
            if allowance < amount_wei:
                logger.info(f"Approving router to spend {amount} tokens...")
                # Need to approve the router to spend tokens
                approve_tx = token_contract.functions.approve(
                    self.async_w3.to_checksum_address(UNISWAP_V3_ROUTER),
                    amount_wei * 2  # Approve more than needed
                ).build_transaction({
                    'from': self.account.address,
                    'nonce': await self.async_w3.eth.get_transaction_count(self.account.address),
                    'gas': 100000,  # Standard approval gas
                    'gasPrice': await self.async_w3.eth.gas_price,
                    'chainId': await self.async_w3.eth.chain_id
                })
                
                # Sign and send approval transaction
                signed_tx = self.async_w3.eth.account.sign_transaction(approve_tx, self.wallet_private_key)
                approve_tx_hash = await self.async_w3.eth.send_raw_transaction(signed_tx.rawTransaction)
                
                # Wait for approval
                logger.info(f"Waiting for approval transaction {approve_tx_hash.hex()} to be confirmed...")
                tx_receipt = await self.async_w3.eth.wait_for_transaction_receipt(approve_tx_hash)
                logger.info(f"tx_receipt: {tx_receipt}")
            
            # Convert deadline from seconds to timestamp
            deadline_timestamp = int(time.time()) + deadline
            logger.info(f"Using deadline: {deadline_timestamp} (current time + {deadline} seconds)")
            
            
            
            # For WETH to USDC
            if not is_token0:  # WETH is token1
                expected_output = amount_wei * await self.get_current_price(self.metadata['token1'].symbol) / (10**6)  # Convert to USDC decimals
            else:  # WETH is token0
                expected_output = amount_wei * await self.get_current_price(self.metadata['token0'].symbol) / (10**6)
                
            # Apply slippage - 50% more than requested for safety
            real_slippage = slippage * 1.5
            min_output = int(expected_output * (1 - real_slippage))
            
            logger.info(f"Exact input swap: {amount} tokens in, expecting ~{expected_output/(10**6)} out")
            logger.info(f"Min output with {real_slippage*100}% effective slippage: {min_output/(10**6)}")
            
            # Create transaction parameters
            params = (
                token_in,                # tokenIn
                token_out,               # tokenOut
                self.fee_tier,           # fee
                self.account.address,     # recipient
                deadline_timestamp,      # deadline
                amount_wei,              # amountIn
                min_output,              # amountOutMinimum
                0                        # sqrtPriceLimitX96 (0 = no limit)
            )
            
            # Build transaction
            tx = router_contract.functions.exactInputSingle(params).build_transaction({
                'from': self.account.address,
                'nonce': await self.async_w3.eth.get_transaction_count(self.account.address),
                'gas': 300000,  # Safe starting value
                'value': 0,     # Not sending ETH directly
                'chainId': await self.async_w3.eth.chain_id
            })
            
            # Try to get gas estimate
            try:
                estimated_gas = await self.async_w3.eth.estimate_gas(tx)
                tx['gas'] = estimated_gas
                logger.info(f"Gas estimation successful: {estimated_gas}")
            except Exception as e:
                logger.error(f"Gas estimation failed: {str(e)}")
                # Continue with default gas if estimation fails
            
            # Sign and send transaction
            signed_tx = self.async_w3.eth.account.sign_transaction(tx, self.wallet_private_key)
            tx_hash = await self.async_w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            logger.info(f"Swap transaction submitted: {tx_hash.hex()}")
            
            return tx_hash.hex()
            
        except Exception as e:
            logger.error(f"Stack Trace: {traceback.format_exc()}")
            logger.error(f"Swap failed: {str(e)}")
            return None
        
    async def execute_trade(self, trade_direction, trade_size):
        """
        Execute a trade.
        """
        token_in_address = self.get_asset_address(self.base_asset)
        if trade_direction.upper() == "SELL":
            token_in_address = self.get_asset_address(self.quote_asset)

        return await self.swap_instrument(trade_size, token_in_address)
    
    async def pre_validate_transfers(self, asset: str, amount: float, max_transfer_time_seconds: int = 10) -> bool:
        """
        Check if network is healthy and if the asset is supported by the pool.
        """
        network_check = self.token_monitor.check_network_conditions()
        if network_check['status'] != 'ok':
            logger.warning(f"Network is not healthy, transfer may fail")
            return False
        
        transfer_status = self.token_monitor.estimate_transfer_time()
        if transfer_status['is_congested']:
            logger.warning(f"Network is congested, transfer may fail")
            return False
        
        if transfer_status['estimate_seconds'] > max_transfer_time_seconds:
            logger.warning(f"Transfer may take too long, consider increasing max_transfer_time_seconds (estimated_transfer_time: {transfer_status['estimate_seconds']} seconds, max_transfer_time by config: {max_transfer_time_seconds} seconds)")
            return False
        
        return True
    
    async def _compute_slippage_cost(self, asset: str, amount: float, direction: str) -> float:
        """
        Compute the slippage cost in basis points (bps) for a specific asset and amount.
        
        This calculates the price impact of executing a trade of the given size by comparing
        the effective execution price to the current market price.
        
        Args:
            asset (str): Asset symbol to compute slippage for
            amount (float): Amount of asset to trade
            direction (str): Trade direction - either 'buy' or 'sell'
            
        Returns:
            float: Slippage cost in basis points (bps). Positive values indicate worse prices.
        """
        # Get current market price from sqrt price
        token0_price, token1_price = self.compute_price_from_sqrt_price_x96()
        current_market_price = token1_price  # Standard quote is token0 per token1
        
        # Determine if this is an upward or downward swap based on asset and direction
        is_token0 = (asset == self.metadata['token0'].symbol)
        
        # For token0 (e.g. USDC):
        # - Buying token0 = downward swap (selling token1)
        # - Selling token0 = upward swap (buying token1)
        
        # For token1 (e.g. WETH):
        # - Buying token1 = upward swap
        # - Selling token1 = downward swap
        
        if is_token0:
            # Get max amounts for both directions
            token0_out_max, token1_in_max = self.compute_upward_swap_within_tick_boundaries()
            token0_in_max, token1_out_max = self.compute_downward_swap_within_tick_boundaries()
            
            # Calculate effective prices
            upward_effective_price = token0_in_max / token1_out_max if token1_out_max > 0 else 0
            downward_effective_price = token0_out_max / token1_in_max if token1_in_max > 0 else 0
            
            # For selling token0 (upward swap)
            upward_price_impact = ((upward_effective_price / current_market_price) - 1) * 10_000
            # For buying token0 (downward swap) 
            downward_price_impact = (1 - (downward_effective_price / current_market_price)) * 10_000
            
            return upward_price_impact if direction == 'sell' else downward_price_impact
            
        else:
            # Same logic but reversed for token1
            token0_out_max, token1_in_max = self.compute_upward_swap_within_tick_boundaries()
            token0_in_max, token1_out_max = self.compute_downward_swap_within_tick_boundaries()
            
            upward_effective_price = token0_in_max / token1_out_max if token1_out_max > 0 else 0
            downward_effective_price = token0_out_max / token1_in_max if token1_in_max > 0 else 0
            
            # For buying token1 (upward swap)
            upward_price_impact = ((upward_effective_price / current_market_price) - 1) * 10_000
            # For selling token1 (downward swap)
            downward_price_impact = (1 - (downward_effective_price / current_market_price)) * 10_000
            
            return upward_price_impact if direction == 'buy' else downward_price_impact
    
    async def _compute_wrap_cost(self, asset: str) -> float:
        """
        Compute the wrap cost for a specific asset.
        """
        if asset != 'ETH':
            return 0
        
        cost_info = await self.token_monitor.get_eth_wrap_gas()
        cost_in_asset = cost_info['cost_wei'] / self.get_asset_decimals(asset)
        return cost_in_asset
    
    async def _compute_unwrap_cost(self, asset: str) -> float:
        """
        Compute the unwrap cost for a specific asset.
        """
        if asset != 'WETH':
            return 0
        
        cost_info = await self.token_monitor.get_eth_unwrap_gas()
        cost_in_asset = cost_info['cost_wei'] / self.get_asset_decimals(asset)
        return cost_in_asset
    
    async def _compute_transfer_cost(self, asset: str) -> float:
        """
        Compute the transfer cost for a specific asset.
        """
        cost_info = await self.token_monitor.get_token_transfer_gas(self.get_asset_address(asset))
        cost_in_asset = cost_info['cost_wei'] / self.get_asset_decimals(asset)
        return cost_in_asset
    
    async def _compute_pool_swap_gas_cost(self, asset: str) -> float:
        """
        Compute the pool swap gas cost for a specific asset.
        """
        cost_info = await self.token_monitor.get_v3_swap_gas(self.pool_address)
        cost_in_asset = cost_info['cost_wei'] / self.get_asset_decimals(asset)
        return cost_in_asset
    
    async def compute_buy_and_transfer_costs(self, asset: str, amount: float) -> float:
        """
        Compute the buy and transfer costs for a specific asset.
            Costs
             1. Slippage cost
             2. Pool fee (fixed)
             3. Pool swap gas cost
             4. Wrapping gas cost
             5. Transfer gas cost
        Args:
            asset (str): The asset code (e.g., 'ETH')
            amount (float): The amount of the buy order
        Returns:
            float: The execution costs in BPS
        """
        # Calculate slippage and fee costs precisely
        # First apply pool fee, then calculate slippage on remaining amount
        fee_cost = amount * (self.metadata['pool_fee']/1_000_000)
        amount_after_fee = amount - fee_cost
        slippage_cost = await self._compute_slippage_cost(asset, amount_after_fee, 'buy') * amount_after_fee / 10_000

        swap_gas_cost = await self._compute_pool_swap_gas_cost(asset)
        execution_cost = fee_cost + slippage_cost + swap_gas_cost
        
        wrap_cost = await self._compute_wrap_cost(asset)
        transfer_cost = await self._compute_transfer_cost(asset)
        
        logger.info(f'slippage_cost: {slippage_cost}, swap_gas_cost: {swap_gas_cost}, wrap_cost: {wrap_cost}, transfer_cost: {transfer_cost}')
        total_cost = execution_cost + wrap_cost + transfer_cost
        return total_cost / amount * 10_000
    
    async def compute_sell_costs(self, asset: str, amount: float) -> float:
        """
        Compute the sell costs for a specific asset.
        Costs
             1. Slippage cost
             2. Pool fee (fixed)
             3. Pool swap gas cost
             4. Unwrapping gas cost
        """
        # Calculate slippage and fee costs precisely
        # First apply pool fee, then calculate slippage on remaining amount
        fee_cost = amount * (self.metadata['pool_fee']/1_000_000)
        amount_after_fee = amount - fee_cost
        slippage_cost = await self._compute_slippage_cost(asset, amount_after_fee, 'sell') * amount_after_fee / 10_000

        swap_gas_cost = await self._compute_pool_swap_gas_cost(asset)
        execution_cost = fee_cost + slippage_cost + swap_gas_cost
        
        unwrap_cost = await self._compute_unwrap_cost(asset)
        logger.info(f'slippage_cost: {slippage_cost}, swap_gas_cost: {swap_gas_cost}, unwrap_cost: {unwrap_cost}')
        return execution_cost + unwrap_cost
    
    async def get_max_executible_size(self, asset: str, direction: str) -> float:
        """
        Get the maximum size that can be executed for a specific asset and direction without crossing the tick boundaries.
        """
        # For upward price movement (selling token1, buying token0)
        token0_out_max, token1_in_max = self.compute_upward_swap_within_tick_boundaries()

        # For downward price movement (selling token1, buying token0)
        token0_in_max, token1_out_max = self.compute_downward_swap_within_tick_boundaries()

        if self.metadata['token0'].symbol == asset and direction == 'buy':
            return token0_in_max
        elif self.metadata['token0'].symbol == asset and direction == 'sell':
            return token0_out_max
        elif self.metadata['token1'].symbol == asset and direction == 'buy':
            return token1_in_max
        elif self.metadata['token1'].symbol == asset and direction == 'sell':
            return token1_out_max
        else:
            raise ValueError(f"Unsupported asset: {asset} and direction: {direction}")
    
    async def get_base_asset_price(self) -> float:
        """
        Get the current price of the base asset.
        """
        return await self.get_current_price(self.base_asset)
    
    async def get_base_asset_deposit_address(self) -> str:
        """
        Get the deposit address for the base asset.
        """
        return await self.get_deposit_address(self.base_asset)
    
    async def get_base_asset_balance(self) -> float:
        """
        Get the current balance of the base asset.
        """
        return await self.get_balance(self.base_asset)
    
    async def wrap_asset(self, asset: str, amount: float) -> float:
        """
        Wrap the asset.
        """
        if asset != 'ETH':
            logger.warning(f"Asset {asset} does not need to be wrapped")
            return amount
        
        # Get the wrapped asset symbol
        wrapped_asset = 'WETH'
        logger.info(f"Wrapping {amount} {asset} to {wrapped_asset}")

        # Convert amount to wei
        amount_wei = await self.async_w3.to_wei(amount, 'ether')

        try:
            # Build the deposit transaction
            contract = self.async_w3.eth.contract(address=self.get_asset_address(wrapped_asset), abi=self.ERC20_ABI)
            tx = contract.functions.deposit().build_transaction({
                'chainId': await self.async_w3.eth.chain_id,
                'gas': 100000,  # Standard gas limit for wrapping
                'maxPriorityFeePerGas': await self.web3.eth.max_priority_fee,
                'maxFeePerGas': await self.web3.eth.gas_price,
                'nonce': await self.web3.eth.get_transaction_count(self.wallet_address),
                'value': amount_wei,  # Amount to wrap in wei
            })

            # Sign and send the transaction
            signed_tx = self.web3.eth.account.sign_transaction(tx, self.wallet_private_key)
            tx_hash = await self.web3.eth.send_raw_transaction(signed_tx.rawTransaction)
            
            # Wait for transaction receipt
            tx_receipt = await self.web3.eth.wait_for_transaction_receipt(tx_hash)
            
            if tx_receipt['status'] == 1:
                logger.info(f"Successfully wrapped {amount} {asset} to {wrapped_asset}")
                logger.info(f"Transaction hash: {tx_hash.hex()}")
                return tx_receipt['value'] / self.get_asset_decimals(wrapped_asset)
            else:
                logger.error(f"Failed to wrap {asset}. Transaction reverted.")
                return 0

        except Exception as e:
            logger.error(f"Error wrapping {asset}: {str(e)}")
            logger.error(traceback.format_exc())
            return 0
    
    async def unwrap_asset(self, asset: str, amount: float) -> float:
        """
        Unwrap the asset.
        """
        if asset != 'WETH':
            logger.warning(f"Asset {asset} does not need to be unwrapped")
            return amount
        
        unwrapped_asset = 'ETH'
        logger.info(f"Unwrapping {amount} {asset} to {unwrapped_asset}")
        
        # Convert amount to wei
        amount_wei = await self.async_w3.to_wei(amount, 'ether')

        try:
            # Build the unwrap transaction
            contract = self.async_w3.eth.contract(address=self.get_asset_address(asset), abi=self.ERC20_ABI)
            tx = contract.functions.withdraw(amount_wei).build_transaction({
                'chainId': await self.async_w3.eth.chain_id,
                'gas': 100000,  # Standard gas limit for unwrapping
            })

            # Sign and send the transaction
            signed_tx = self.async_w3.eth.account.sign_transaction(tx, self.wallet_private_key)
            tx_hash = await self.async_w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            
            # Wait for transaction receipt
            tx_receipt = await self.async_w3.eth.wait_for_transaction_receipt(tx_hash)
            
            if tx_receipt['status'] == 1:
                logger.info(f"Successfully unwrapped {amount} {asset} to {unwrapped_asset}")
                return tx_receipt['value'] / self.get_asset_decimals(unwrapped_asset)
            else:
                logger.error(f"Failed to unwrap {asset}. Transaction reverted.")
                return 0

        except Exception as e:
            logger.error(f"Error unwrapping {asset}: {str(e)}")
            logger.error(traceback.format_exc())
            return 0

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
        base_asset = config.instrument_config[0]['base_asset']
        quote_asset = config.instrument_config[0]['quote_asset']

        # Initialize the connector
        uniswap = Uniswap(infura_url, infura_ws_url, pool_address, private_key, base_asset, quote_asset)
        await uniswap.init()
        logger.info(f"Pool contains {uniswap.metadata['token0'].symbol} and {uniswap.metadata['token1'].symbol}")
        
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