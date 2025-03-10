import asyncio
import json
import time
import datetime
import math
import traceback
from config import Config
import logging
import argparse
from web3 import Web3
from web3.middleware import geth_poa_middleware


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Default ETH/USDC pool address
DEFAULT_POOL_ADDRESS = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"  # ETH/USDC (0.05%)

TOKEN_ADDRESS_MAP = {
    "ETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    "DAI": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
    "WETH": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    "WBTC": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"
}


class PoolMonitor:
    """
    A class to monitor Uniswap V3 pool activity in real-time.
    
    This class provides functionality to track price changes and swap events
    for any Uniswap V3 pool using Web3 event subscriptions.
    """
    
    def __init__(self, pool_address, web3_provider_url, wallet_private_key, update_price=True, update_balance=True, intervals=(5, 15)):
        """
        Initialize the pool monitor.
        
        Args:
            pool_address (str): The address of the Uniswap V3 pool to monitor
            web3_provider_url (str): The WebSocket URL for the Web3 provider
            wallet_private_key (str): Private key for the wallet
            update_price (bool): Whether to start background price updates
            update_balance (bool): Whether to start background balance updates
            intervals (tuple): Intervals for price and balance updates (price_interval, balance_interval)
        """
        self.pool_address = pool_address.lower()
        self.web3_provider_url = web3_provider_url
        self.wallet_private_key = wallet_private_key
        
        # Initialize Web3 connection
        self.web3 = Web3(Web3.WebsocketProvider(self.web3_provider_url))
        self.web3.middleware_onion.inject(geth_poa_middleware, layer=0)
        
        # Get account from pool's wallet private key
        account = self.web3.eth.account.from_key(self.wallet_private_key)
        self.wallet_address = account.address
        
        # Initialize caches for this instance
        self._price_cache = {}
        self._token_cache = {}
        self._balance_cache = {}
        
        # Flag to control background updates
        self.background_updates_running = False
        self.balance_updates_running = False
        self.update_interval = intervals[0]  # Default update interval in seconds
        self.balance_update_interval = intervals[1]  # Default balance update interval in seconds
        self.background_task = None
        self.balance_task = None
        
        # Track the last tick value to detect changes
        self.last_tick = None
        self.last_tick_change_time = None
        
        # Initialize pool contract and token info
        self._initialize_pool()
        self._initialize_token_info()
        
        # Start background updates if requested
        if update_price:
            asyncio.create_task(self.start_background_updates(self.update_interval))
        if update_balance:
            asyncio.create_task(self.start_balance_updates(self.wallet_address, self.balance_update_interval))
        

    def _initialize_pool(self):
        """
        Initialize the Web3 connection and pool contract.
        
        This method sets up the Web3 connection, loads the pool contract,
        and initializes token information.
        """
        # ERC20 ABI for getting token information
        # Current minimal ERC20 ABI - only has symbol and decimals functions
        # erc20_abi = json.loads('''[
        #     {"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},
        #     {"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"}
        # ]''')

        # ABI with all functions
        erc20_abi = json.loads('''[
            {"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},
            {"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},
            {"constant":true,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},
            {"constant":false,"inputs":[{"name":"_to","type":"address"},{"name":"_value","type":"uint256"}],"name":"transfer","outputs":[{"name":"success","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},
            {"constant":false,"inputs":[{"name":"_spender","type":"address"},{"name":"_value","type":"uint256"}],"name":"approve","outputs":[{"name":"success","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},
            {"constant":true,"inputs":[{"name":"_owner","type":"address"},{"name":"_spender","type":"address"}],"name":"allowance","outputs":[{"name":"remaining","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},
            {"constant":false,"inputs":[{"name":"_from","type":"address"},{"name":"_to","type":"address"},{"name":"_value","type":"uint256"}],"name":"transferFrom","outputs":[{"name":"success","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},
            {"payable":true,"stateMutability":"payable","type":"fallback"},
            {"constant":false,"inputs":[],"name":"deposit","outputs":[],"payable":true,"stateMutability":"payable","type":"function"},
            {"constant":false,"inputs":[{"name":"wad","type":"uint256"}],"name":"withdraw","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"}
        ]''')
        
        # Initialize token contracts
        self.erc20_abi = erc20_abi
        
        # Load pool contract ABI
        pool_abi = json.loads('''[
            {"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"int256","name":"amount0","type":"int256"},{"indexed":false,"internalType":"int256","name":"amount1","type":"int256"},{"indexed":false,"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"indexed":false,"internalType":"uint128","name":"liquidity","type":"uint128"},{"indexed":false,"internalType":"int24","name":"tick","type":"int24"}],"name":"Swap","type":"event"},
            {"inputs":[],"name":"slot0","outputs":[{"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"internalType":"int24","name":"tick","type":"int24"},{"internalType":"uint16","name":"observationIndex","type":"uint16"},{"internalType":"uint16","name":"observationCardinality","type":"uint16"},{"internalType":"uint16","name":"observationCardinalityNext","type":"uint16"},{"internalType":"uint8","name":"feeProtocol","type":"uint8"},{"internalType":"bool","name":"unlocked","type":"bool"}],"stateMutability":"view","type":"function"},
            {"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},
            {"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},
            {"inputs":[],"name":"fee","outputs":[{"internalType":"uint24","name":"","type":"uint24"}],"stateMutability":"view","type":"function"},
            {"inputs":[],"name":"liquidity","outputs":[{"internalType":"uint128","name":"","type":"uint128"}],"stateMutability":"view","type":"function"},
            {"inputs":[],"name":"tickSpacing","outputs":[{"internalType":"int24","name":"","type":"int24"}],"stateMutability":"view","type":"function"},
            {"inputs":[{"internalType":"int24","name":"tick","type":"int24"}],"name":"ticks","outputs":[{"internalType":"uint128","name":"liquidityGross","type":"uint128"},{"internalType":"int128","name":"liquidityNet","type":"int128"},{"internalType":"uint256","name":"feeGrowthOutside0X128","type":"uint256"},{"internalType":"uint256","name":"feeGrowthOutside1X128","type":"uint256"},{"internalType":"int56","name":"tickCumulativeOutside","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityOutsideX128","type":"uint160"},{"internalType":"uint32","name":"secondsOutside","type":"uint32"},{"internalType":"bool","name":"initialized","type":"bool"}],"stateMutability":"view","type":"function"},
            {"inputs":[{"internalType":"int16","name":"wordPosition","type":"int16"}],"name":"tickBitmap","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
            {"inputs":[{"components":[{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint24","name":"fee","type":"uint24"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMinimum","type":"uint256"},{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"}],"internalType":"struct ISwapRouter.ExactInputSingleParams","name":"params","type":"tuple"}],"name":"exactInputSingle","outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"}],"stateMutability":"payable","type":"function"},
            {"inputs":[{"components":[{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint24","name":"fee","type":"uint24"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint256","name":"amountInMaximum","type":"uint256"},{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"}],"internalType":"struct ISwapRouter.ExactOutputSingleParams","name":"params","type":"tuple"}],"name":"exactOutputSingle","outputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"}],"stateMutability":"payable","type":"function"},
            {"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"mint","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},
            {"inputs":[{"internalType":"uint256","name":"amount0Desired","type":"uint256"},{"internalType":"uint256","name":"amount1Desired","type":"uint256"},{"internalType":"uint256","name":"amount0Min","type":"uint256"},{"internalType":"uint256","name":"amount1Min","type":"uint256"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"burn","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"stateMutability":"nonpayable","type":"function"}
        ]''')
        
        # Convert pool address to checksum address
        checksum_pool_address = self.web3.toChecksumAddress(self.pool_address)
        
        # Initialize pool contract
        self.pool_contract = self.web3.eth.contract(address=checksum_pool_address, abi=pool_abi)
        
        # Initialize the contract attribute for swap operations
        self.contract = self.pool_contract
        self.weth_contract = self.web3.eth.contract(address=TOKEN_ADDRESS_MAP["WETH"], abi=self.erc20_abi)
        self.usdc_contract = self.web3.eth.contract(address=TOKEN_ADDRESS_MAP["USDC"], abi=self.erc20_abi)
        
        # Get token addresses
        self.token0_address = self.pool_contract.functions.token0().call()
        self.token1_address = self.pool_contract.functions.token1().call()

        assert self.token0_address == self.usdc_contract.address
        assert self.token1_address == self.weth_contract.address
        
        # Get fee tier
        try:
            self.fee_tier = self.pool_contract.functions.fee().call()
        except Exception as e:
            logger.error(f"Error getting fee tier: {e}")
            # Default to 0.3% fee tier
            self.fee_tier = 3000
            
        # Get tick spacing
        try:
            self.tick_spacing = self.pool_contract.functions.tickSpacing().call()
        except Exception as e:
            logger.warning(f"Error getting tick spacing: {e}")
            # Default tick spacing based on fee tier
            if self.fee_tier == 500:  # 0.05%
                self.tick_spacing = 10
            elif self.fee_tier == 3000:  # 0.3%
                self.tick_spacing = 60
            elif self.fee_tier == 10000:  # 1%
                self.tick_spacing = 200
            else:
                self.tick_spacing = 60  # Default
        
        # Initialize token information
        self._initialize_token_info()
        
        # Set up event filter for Swap events
        swap_event_signature_hash = self.web3.keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)").hex()
        self.swap_filter = self.web3.eth.filter({
            'address': checksum_pool_address,
            'topics': [swap_event_signature_hash]
        })
    
    def _initialize_token_info(self):
        """
        Initialize token information for the pool.
        
        This method loads token symbols and decimals for both tokens in the pool.
        """
        token0_contract = self.web3.eth.contract(address=self.token0_address, abi=self.erc20_abi)
        token1_contract = self.web3.eth.contract(address=self.token1_address, abi=self.erc20_abi)
        
        # Get token symbols
        try:
            self.token0_symbol = token0_contract.functions.symbol().call()
            self.token1_symbol = token1_contract.functions.symbol().call()
        except Exception as e:
            logger.error(f"Error getting token symbols: {e}")
            # Use addresses as fallback
            self.token0_symbol = self.token0_address[:6] + "..." + self.token0_address[-4:]
            self.token1_symbol = self.token1_address[:6] + "..." + self.token1_address[-4:]
        
        # Get token decimals
        try:
            self.token0_decimals = token0_contract.functions.decimals().call()
            self.token1_decimals = token1_contract.functions.decimals().call()
        except Exception as e:
            logger.error(f"Error getting token decimals: {e}")
            # Use 18 as fallback (common for most tokens)
            self.token0_decimals = 18
            self.token1_decimals = 18
        
        # Store token info in cache
        self._token_cache[self.token0_address] = {
            'symbol': self.token0_symbol,
            'decimals': self.token0_decimals
        }
        self._token_cache[self.token1_address] = {
            'symbol': self.token1_symbol,
            'decimals': self.token1_decimals
        }
    
    def get_token_address(self, symbol: str) -> str:
        """
        Get the token address for a given symbol.
        Args:
            symbol (str): The symbol of the token (e.g., 'ETH', 'USDC')
        Returns:
            str: The address of the token if found, None otherwise.
        """
        if symbol.upper() == self.token0_symbol.upper():
            return self.token0_address
        elif symbol.upper() == self.token1_symbol.upper():
            return self.token1_address
        else:
            return None
    
    def calculate_price_from_tick(self, tick):
        """
        Calculate the price from a tick value.
        
        In Uniswap V3, price = 1.0001^tick
        
        Args:
            tick (int): The tick value
            
        Returns:
            float: The calculated price
        """
        return 1.0001 ** tick
    
    def tick_to_sqrt_price_x96(self, tick):
        """
        Converts a tick to sqrtPriceX96.
        
        Args:
            tick (int): The tick value
            
        Returns:
            int: The sqrtPriceX96 value
        """
        return int(math.sqrt(1.0001**tick) * (2**96))
    
    def get_tick_boundaries_sqrt_price_x96(self, target_tick):
        """
        Gets sqrtPriceX96 values for tick boundaries.
        
        Args:
            target_tick (int): The target tick
            
        Returns:
            tuple: (sqrt_price_lower, sqrt_price_upper) - The sqrtPriceX96 values for the boundaries
        """
        try:
            # Get the tick spacing
            tick_spacing = self.tick_spacing
            
            # Calculate the lower and upper tick boundaries
            lower_tick = (target_tick // tick_spacing) * tick_spacing
            upper_tick = lower_tick + tick_spacing
            
            # Calculate sqrtPriceX96 values for the tick boundaries
            sqrt_price_lower = self.tick_to_sqrt_price_x96(lower_tick)
            sqrt_price_upper = self.tick_to_sqrt_price_x96(upper_tick)
            
            return lower_tick, upper_tick, sqrt_price_lower, sqrt_price_upper
        except Exception as e:
            logger.error(f"Error getting tick boundaries: {e}")
            # Fall back to approximated values
            lower_tick = math.floor(target_tick / self.tick_spacing) * self.tick_spacing
            upper_tick = lower_tick + self.tick_spacing
            sqrt_price_lower = self.tick_to_sqrt_price_x96(lower_tick)
            sqrt_price_upper = self.tick_to_sqrt_price_x96(upper_tick)
            return lower_tick, upper_tick, sqrt_price_lower, sqrt_price_upper
    
    def calculate_reserves_from_liquidity(self, liquidity, sqrt_price_x96, current_tick):
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
            liquidity = float(liquidity)
            sqrt_price_x96 = float(sqrt_price_x96)
            
            # Calculate current sqrt price (P)
            sqrt_price = sqrt_price_x96 / (2**96)
            
            # Get the tick boundaries and their sqrtPriceX96 values
            lower_tick, upper_tick, sqrt_price_a_x96, sqrt_price_b_x96 = self.get_tick_boundaries_sqrt_price_x96(current_tick)
            
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
            reserve0_hr = reserve0 / (10 ** self.token0_decimals)
            reserve1_hr = reserve1 / (10 ** self.token1_decimals)
            
            # Log calculations for debugging
            logger.debug(f"Reserve calculation details:")
            logger.debug(f"  Liquidity: {liquidity}")
            logger.debug(f"  Current tick: {current_tick}")
            logger.debug(f"  Tick spacing: {self.tick_spacing}")
            logger.debug(f"  Lower tick: {lower_tick}, Upper tick: {upper_tick}")
            logger.debug(f"  sqrt_price: {sqrt_price}")
            logger.debug(f"  sqrt_price_a: {sqrt_price_a} (sqrtPriceAX96: {sqrt_price_a_x96})")
            logger.debug(f"  sqrt_price_b: {sqrt_price_b} (sqrtPriceBX96: {sqrt_price_b_x96})")
            logger.debug(f"  reserve0: {reserve0}")
            logger.debug(f"  reserve1: {reserve1}")
            logger.debug(f"  reserve0_hr: {reserve0_hr}")
            logger.debug(f"  reserve1_hr: {reserve1_hr}")
            
            return reserve0, reserve1, reserve0_hr, reserve1_hr
        except Exception as e:
            logger.error(f"Error calculating reserves: {e}")
            # For backward compatibility, return simplified calculation on error
            try:
                # Fallback simplified calculation
                sqrt_price = sqrt_price_x96 / (2**96)
                reserve0 = liquidity / sqrt_price
                reserve1 = liquidity * sqrt_price
                
                # Convert to human-readable format with proper decimals
                reserve0_hr = reserve0 / (10 ** self.token0_decimals)
                reserve1_hr = reserve1 / (10 ** self.token1_decimals)
                
                return reserve0, reserve1, reserve0_hr, reserve1_hr
            except:
                return 0, 0, 0, 0
    
    def calculate_price_from_sqrtprice(self, sqrt_price_x96, token0_decimals, token1_decimals):
        """
        Calculate the price directly from sqrtPriceX96.
        
        Args:
            sqrt_price_x96 (int): The square root price in X96 format
            token0_decimals (int): Decimals for token0
            token1_decimals (int): Decimals for token1
            
        Returns:
            tuple: (price_token0_in_token1, price_token1_in_token0)
                price_token0_in_token1: Price of token0 in terms of token1
                price_token1_in_token0: Price of token1 in terms of token0
        """
        try:
            # Convert to safer float calculation
            sqrt_price_x96 = float(sqrt_price_x96)
            
            # Calculate price of token0 in terms of token1
            # First calculate raw price: (sqrtPriceX96 / 2^96)^2
            raw_price = (sqrt_price_x96 / (2**96))**2
            
            # Adjust for decimal differences
            decimal_adjustment = 10**token1_decimals / 10**token0_decimals
            price_token0_in_token1 = raw_price / decimal_adjustment
            
            # Calculate price of token1 in terms of token0 (inverse)
            price_token1_in_token0 = 1 / price_token0_in_token1 if price_token0_in_token1 != 0 else 0
            
            return price_token0_in_token1, price_token1_in_token0
            
        except Exception as e:
            logger.error(f"Error calculating price from sqrtPriceX96: {e}")
            return 0, 0
    
    def _update_price_cache(self):
        """
        Update the price cache with the latest data from the pool.
        
        This method fetches the current tick from the pool and calculates
        the corresponding price.
        
        Returns:
            dict: Updated price data
        """
        try:
            # Get current slot0 data which contains the current tick and sqrtPriceX96
            s_time = time.time()
            slot0_data = self.pool_contract.functions.slot0().call()
            current_tick = slot0_data[1]
            sqrt_price_x96 = slot0_data[0]
            e_time = time.time()
            
            # Try to get current liquidity
            try:
                liquidity = self.pool_contract.functions.liquidity().call()
                logger.debug(f"Current liquidity: {liquidity}")
            except Exception as e:
                logger.warning(f"Failed to get liquidity: {e}")
                liquidity = 0
                
            # Log tick spacing for debugging
            logger.debug(f"Tick spacing: {self.tick_spacing}")
            
            # Get the tick boundaries and their sqrtPriceX96 values
            lower_tick, upper_tick, sqrt_price_a_x96, sqrt_price_b_x96 = self.get_tick_boundaries_sqrt_price_x96(current_tick)
            
            # Convert to floating point for logging
            sqrt_price_a = sqrt_price_a_x96 / (2**96)
            sqrt_price_b = sqrt_price_b_x96 / (2**96)
            
            # Log the price boundaries
            logger.info(f"Price boundaries: lower_tick={lower_tick}, upper_tick={upper_tick}")
            logger.info(f"sqrtPriceAX96: {sqrt_price_a_x96}, sqrtPriceBX96: {sqrt_price_b_x96}")
            
            # Check if tick has changed
            tick_changed = False
            if self.last_tick is not None and current_tick != self.last_tick:
                tick_changed = True
                self.last_tick_change_time = time.time()
                logger.info(f"Tick changed from {self.last_tick} to {current_tick}")
            
            # Update last tick
            self.last_tick = current_tick
            
            # If this is the first time we're getting the tick, set the change time
            if self.last_tick_change_time is None:
                self.last_tick_change_time = time.time()
            
            # Calculate time since last tick change
            time_since_last_change = time.time() - self.last_tick_change_time
            
            logger.info(f'Time taken to get current tick: {e_time - s_time:.6f} seconds | Tick changed: {tick_changed}')
            
            # Calculate prices using both methods
            
            # 1. Calculate price from sqrtPriceX96
            sqrt_price_token0, sqrt_price_token1 = self.calculate_price_from_sqrtprice(
                sqrt_price_x96, 
                self.token0_decimals, 
                self.token1_decimals
            )
            
            # 2. Calculate price from tick
            raw_price = self.calculate_price_from_tick(current_tick)  # Raw price from tick
            decimal_adjustment = 10 ** (self.token0_decimals - self.token1_decimals)
            tick_price = raw_price * decimal_adjustment  # Price from tick calculation
            tick_price_token0 = 1 / tick_price if tick_price != 0 else 0  # Inverse price from tick
            
            # Calculate token reserves using the whitepaper formula
            reserve0, reserve1, reserve0_hr, reserve1_hr = self.calculate_reserves_from_liquidity(
                liquidity, sqrt_price_x96, current_tick
            )
            
            # Log the reserves for debugging
            logger.info(f"Calculated reserves using whitepaper formula:")
            logger.info(f"Hourly {self.token0_symbol} reserves: {reserve0_hr:.2f}")
            logger.info(f"Hourly {self.token1_symbol} reserves: {reserve1_hr:.2f}")
            
            # Update the cache
            timestamp = int(time.time())
            self._price_cache[self.pool_address] = {
                'tick': current_tick,
                'raw_price': raw_price,
                # From tick calculation
                'tick_price': tick_price,  # Price of token1 in terms of token0 (from tick)
                'tick_price_token0': tick_price_token0,  # Price of token0 in terms of token1 (from tick)
                # From sqrtPriceX96 calculation
                'sqrt_price_token0': sqrt_price_token0,  # Price of token0 in terms of token1
                'sqrt_price_token1': sqrt_price_token1,  # Price of token1 in terms of token0
                # For backward compatibility
                'price': sqrt_price_token1,  # Price of token1 in terms of token0
                'token0_price': sqrt_price_token0,  # Price of token0 in terms of token1
                'token1_price': sqrt_price_token1,  # Price of token1 in terms of token0
                # Metadata
                'timestamp': timestamp,
                'block_timestamp': timestamp,
                'tick_changed': tick_changed,
                'time_since_last_change': time_since_last_change,
                'sqrt_price_x96': sqrt_price_x96,
                'lower_tick': lower_tick,
                'upper_tick': upper_tick,
                'sqrt_price_a_x96': sqrt_price_a_x96,  # sqrtRatioAX96
                'sqrt_price_b_x96': sqrt_price_b_x96,  # sqrtRatioBX96
                'liquidity': liquidity,
                'fee_tier': self.fee_tier,
                'tick_spacing': self.tick_spacing,
                # Reserves
                'reserve0': reserve0,
                'reserve1': reserve1,
                'reserve0_hr': reserve0_hr,
                'reserve1_hr': reserve1_hr
            }
            
            return self._price_cache[self.pool_address]
        except Exception as e:
            logger.error(f"Error updating price cache: {e}")
            return None
    
    def get_current_price(self, live=False):
        """
        Get current price from the cache or update if needed.
        
        Args:
            live (bool): If True, force a refresh of the price data before returning
                         If False, return the cached price data without refreshing
        
        Returns:
            dict: Price information including tick, price and timestamp
        """
        # If live=True, force a refresh of the price data
        if live:
            return self._update_price_cache()
        
        # Otherwise, return the cached price data if available
        if self.pool_address in self._price_cache:
            return self._price_cache[self.pool_address]
        
        # If no cache entry exists, update it
        return self._update_price_cache()
    
    def get_current_base_price(self):
        """
        Get the current price of the base token in the pool.
        
        Returns:
            float: The current price of the base token
        """
        return self.get_current_price()['token1_price']
    
    def _format_number(self, number):
        """
        Format large numbers in a more readable way.
        
        Args:
            number (float): The number to format
            
        Returns:
            str: Formatted number string
        """
        if number >= 1_000_000_000_000:  # Trillion
            return f"{number / 1_000_000_000_000:.2f}T"
        elif number >= 1_000_000_000:  # Billion
            return f"{number / 1_000_000_000:.2f}B"
        elif number >= 1_000_000:  # Million
            return f"{number / 1_000_000:.2f}M"
        elif number >= 1_000:  # Thousand
            return f"{number / 1_000:.2f}K"
        else:
            return f"{number:.2f}"
    
    def _format_time_duration(self, seconds):
        """
        Format a time duration in seconds to a human-readable string.
        
        Args:
            seconds (float): Time duration in seconds
            
        Returns:
            str: Formatted time duration
        """
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f}m"
        elif seconds < 86400:
            hours = seconds / 3600
            return f"{hours:.1f}h"
        else:
            days = seconds / 86400
            return f"{days:.1f}d"
    
    async def start_background_updates(self, interval=5):
        """
        Start a background task to periodically update the price cache.
        
        Args:
            interval (int): The interval in seconds between price updates
        """
        if self.background_updates_running:
            logger.info("Background price updates already running")
            return
        
        self.update_interval = interval
        self.background_updates_running = True
        logger.info(f"Starting background price updates every {interval} seconds")
        
        # Create and start the background task
        self.background_task = asyncio.create_task(self._background_update_loop())
    
    async def _background_update_loop(self):
        """
        Background loop to periodically update the price cache.
        
        This method runs in a separate task and updates the price cache
        at regular intervals.
        """
        try:
            while self.background_updates_running:
                try:
                    # Update the price cache
                    self._update_price_cache()
                    
                    # Sleep for the update interval
                    await asyncio.sleep(self.update_interval)
                except Exception as e:
                    logger.error(f"Error in background price update: {e}")
                    # Sleep for a short time before retrying
                    await asyncio.sleep(1)
        except asyncio.CancelledError:
            # Task was cancelled, clean up
            logger.info("Background price update task cancelled")
        except Exception as e:
            logger.error(f"Error in background price update loop: {e}")
            self.background_updates_running = False
    
    async def get_base_token_decimals(self):
        """
        Get the decimals of the base token.
        
        Returns:
            int: The number of decimals of the base token
        """
        return self.token1_decimals
    
    async def process_swap_event(self, event):
        """
        Process a swap event from the pool.
        
        Args:
            event (dict): The swap event data
        """
        try:
            # Decode the event data
            event_data = self.pool_contract.events.Swap().processLog(event)
            
            # Extract relevant data
            amount0 = event_data['args']['amount0']
            amount1 = event_data['args']['amount1']
            sqrt_price_x96 = event_data['args']['sqrtPriceX96']
            liquidity = event_data['args']['liquidity']
            tick = event_data['args']['tick']
            
            # Check if tick has changed
            tick_changed = False
            if self.last_tick is not None and tick != self.last_tick:
                tick_changed = True
                self.last_tick_change_time = time.time()
                logger.info(f"Tick changed from {self.last_tick} to {tick} (from swap event)")
            
            # Update last tick
            self.last_tick = tick
            
            # Get the tick boundaries and their sqrtPriceX96 values
            lower_tick, upper_tick, sqrt_price_a_x96, sqrt_price_b_x96 = self.get_tick_boundaries_sqrt_price_x96(tick)
            
            # Log the price boundaries
            logger.info(f"Swap event - Price boundaries: lower_tick={lower_tick}, upper_tick={upper_tick}")
            logger.info(f"Swap event - sqrtPriceAX96: {sqrt_price_a_x96}, sqrtPriceBX96: {sqrt_price_b_x96}")
            
            # Calculate prices using both methods
            # 1. Using sqrtPriceX96
            sqrt_price_token0, sqrt_price_token1 = self.calculate_price_from_sqrtprice(
                sqrt_price_x96, 
                self.token0_decimals, 
                self.token1_decimals
            )
            
            # 2. Using tick
            raw_price = self.calculate_price_from_tick(tick)  # Raw price from tick
            decimal_adjustment = 10 ** (self.token0_decimals - self.token1_decimals)
            tick_price = raw_price * decimal_adjustment  # Price from tick calculation
            tick_price_token0 = 1 / tick_price if tick_price != 0 else 0  # Inverse price from tick
            
            # Determine swap direction
            if amount0 > 0 and amount1 < 0:
                # Token0 in, Token1 out
                token_in = self.token0_symbol
                token_out = self.token1_symbol
                amount_in = abs(amount0) / (10 ** self.token0_decimals)
                amount_out = abs(amount1) / (10 ** self.token1_decimals)
                direction = f"{amount_in:.6f} {token_in} → {amount_out:.6f} {token_out}"
            else:
                # Token1 in, Token0 out
                token_in = self.token1_symbol
                token_out = self.token0_symbol
                amount_in = abs(amount1) / (10 ** self.token1_decimals)
                amount_out = abs(amount0) / (10 ** self.token0_decimals)
                direction = f"{amount_in:.6f} {token_in} → {amount_out:.6f} {token_out}"
            
            # Update price cache with the new data
            timestamp = int(time.time())  # Use current time as fallback
            try:
                # Try to get the block timestamp if possible
                block_timestamp = self.web3.eth.get_block(event['blockNumber']).timestamp
                timestamp = block_timestamp
            except Exception:
                # If we can't get the block timestamp, use current time
                pass
            
            # Calculate time since last tick change
            time_since_last_change = time.time() - self.last_tick_change_time
            
            # Calculate token reserves using the whitepaper formula
            reserve0, reserve1, reserve0_hr, reserve1_hr = self.calculate_reserves_from_liquidity(
                liquidity, sqrt_price_x96, tick
            )
            
            # Log the reserves for debugging
            logger.info(f"Swap event - Calculated reserves:")
            logger.info(f"  {self.token0_symbol} reserves: {reserve0_hr:.2f}")
            logger.info(f"  {self.token1_symbol} reserves: {reserve1_hr:.2f}")
            
            # Update the price cache
            self._price_cache[self.pool_address] = {
                'tick': tick,
                'raw_price': raw_price,
                # From tick calculation
                'tick_price': tick_price,  # Price of token1 in terms of token0 (from tick)
                'tick_price_token0': tick_price_token0,  # Price of token0 in terms of token1 (from tick)
                # From sqrtPriceX96 calculation
                'sqrt_price_token0': sqrt_price_token0,  # Price of token0 in terms of token1
                'sqrt_price_token1': sqrt_price_token1,  # Price of token1 in terms of token0
                # For backward compatibility
                'price': sqrt_price_token1,  # Price of token1 in terms of token0
                'token0_price': sqrt_price_token0,  # Price of token0 in terms of token1
                'token1_price': sqrt_price_token1,  # Price of token1 in terms of token0
                # Metadata
                'timestamp': timestamp,
                'block_timestamp': timestamp,
                'tick_changed': tick_changed,
                'time_since_last_change': time_since_last_change,
                'sqrt_price_x96': sqrt_price_x96,
                'lower_tick': lower_tick,
                'upper_tick': upper_tick,
                'sqrt_price_a_x96': sqrt_price_a_x96,
                'sqrt_price_b_x96': sqrt_price_b_x96,
                'liquidity': liquidity,
                'fee_tier': self.fee_tier,
                'tick_spacing': self.tick_spacing,
                # Reserves
                'reserve0': reserve0,
                'reserve1': reserve1,
                'reserve0_hr': reserve0_hr,
                'reserve1_hr': reserve1_hr
            }
            
            # Create a swap data object
            swap_data = {
                'block_number': event['blockNumber'],
                'transaction_hash': event['transactionHash'].hex(),
                'token_in': token_in,
                'token_out': token_out,
                'amount_in': amount_in,
                'amount_out': amount_out,
                'direction': direction,
                'sqrt_price_token1': sqrt_price_token1,  # WETH price in USDC (for USDC/WETH pool)
                'tick_price': tick_price,  # Price from tick
                'tick': tick,
                'lower_tick': lower_tick,
                'upper_tick': upper_tick,
                'sqrt_price_a_x96': sqrt_price_a_x96,
                'sqrt_price_b_x96': sqrt_price_b_x96,
                'liquidity': liquidity,
                'timestamp': timestamp,
                'tick_changed': tick_changed,
                'reserve0_hr': reserve0_hr,
                'reserve1_hr': reserve1_hr
            }
            
            # Display the swap data
            self._display_swap_data(swap_data)
            
            return swap_data
        except Exception as e:
            logger.error(f"Error processing swap event: {e}")
            return None
            
    def _display_swap_data(self, swap_data):
        """
        Display formatted swap event data.
        
        Args:
            swap_data (dict): The processed swap event data
        """
        if not swap_data:
            logger.info("\n=== Error processing swap event ===")
            return
            
        logger.info(f"\n=== New swap detected in block {swap_data.get('block_number', 'unknown')} ===")
        
        # Display price from sqrtPriceX96
        if 'sqrt_price_token1' in swap_data:
            logger.info(f"  Price (from sqrtPriceX96): 1 {self.token1_symbol} = {swap_data['sqrt_price_token1']:.8f} {self.token0_symbol}")
        
        # Display price from tick
        if 'tick_price' in swap_data:
            logger.info(f"  Price (from tick): 1 {self.token1_symbol} = {swap_data['tick_price']:.8f} {self.token0_symbol}")
            
        # Display tick and boundaries
        if 'tick' in swap_data:
            logger.info(f"  Current Tick: {swap_data['tick']}")
            
        if 'lower_tick' in swap_data and 'upper_tick' in swap_data:
            logger.info(f"  Tick Range: {swap_data['lower_tick']} to {swap_data['upper_tick']}")
            
        # Display swap direction
        if 'direction' in swap_data:
            logger.info(f"  Swap: {swap_data['direction']}")
        else:
            logger.info(f"  Swap: Direction not available")
        
        # Display reserves if available
        if 'reserve0_hr' in swap_data and 'reserve1_hr' in swap_data:
            logger.info(f"  Reserves: {swap_data['reserve0_hr']:.2f} {self.token0_symbol}, {swap_data['reserve1_hr']:.2f} {self.token1_symbol}")
        
        if 'tick_changed' in swap_data and swap_data['tick_changed']:
            logger.info(f"  Tick changed: Yes")
            
        # Display sqrtPriceX96 values
        if 'sqrt_price_a_x96' in swap_data and 'sqrt_price_b_x96' in swap_data:
            logger.info(f"  sqrtPriceAX96: {swap_data['sqrt_price_a_x96']}")
            logger.info(f"  sqrtPriceBX96: {swap_data['sqrt_price_b_x96']}")
        
        logger.info("--------------------------------------------------")

    def get_eth_balance(self, address):
        """
        Get ETH balance for an address.
        
        Args:
            address (str): Ethereum address to check balance for
            
        Returns:
            float: ETH balance in ETH units (not wei)
        """
        try:
            # Convert address to checksum address
            checksum_address = self.web3.toChecksumAddress(address)
            
            # Get the balance in wei
            balance_wei = self.web3.eth.get_balance(checksum_address)
            
            # Convert to ETH
            balance_eth = self.web3.fromWei(balance_wei, 'ether')
            
            return float(balance_eth)
        except Exception as e:
            logger.error(f"Error getting ETH balance: {e}")
            return 0.0
    
    def get_token_balance(self, address, token_address):
        """
        Get ERC20 token balance for an address.
        
        Args:
            address (str): Ethereum address to check balance for
            token_address (str): ERC20 token contract address
            
        Returns:
            float: Token balance in token units (adjusted for decimals)
        """
        try:
            # Convert addresses to checksum
            checksum_address = self.web3.toChecksumAddress(address)
            checksum_token_address = self.web3.toChecksumAddress(token_address)
            
            # Load ERC20 ABI
            erc20_abi = json.loads('''[
                {"constant":true,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},
                {"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},
                {"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"}
            ]''')
            
            # Initialize token contract
            token_contract = self.web3.eth.contract(address=checksum_token_address, abi=erc20_abi)
            
            # Get token decimals and symbol (and cache them)
            if token_address not in self._token_cache:
                try:
                    symbol = token_contract.functions.symbol().call()
                    decimals = token_contract.functions.decimals().call()
                    self._token_cache[token_address] = {
                        'symbol': symbol,
                        'decimals': decimals
                    }
                except Exception as e:
                    logger.error(f"Error getting token info: {e}")
                    # Default values
                    symbol = token_address[:6] + "..." + token_address[-4:]
                    decimals = 18
                    self._token_cache[token_address] = {
                        'symbol': symbol,
                        'decimals': decimals
                    }
            else:
                decimals = self._token_cache[token_address]['decimals']
            
            # Get token balance
            balance_raw = token_contract.functions.balanceOf(checksum_address).call()
            
            # Convert to token units
            balance = balance_raw / (10 ** decimals)
            
            return float(balance)
        except Exception as e:
            logger.error(f"Error getting token balance: {e}")
            return 0.0
    
    def update_balance_cache(self, address, token_addresses=None):
        """
        Update the balance cache for an address.
        
        Args:
            address (str): Ethereum address to check balances for
            token_addresses (list, optional): List of token addresses to check. If None, 
                                              will check the pool tokens and ETH.
        
        Returns:
            dict: Dictionary with updated balance data
        """
        # Get current time for cache timestamp
        timestamp = int(time.time())
        
        # Default tokens to check: ETH, token0, token1
        if token_addresses is None:
            token_addresses = [self.token0_address, self.token1_address]
        
        # Get ETH balance
        eth_balance = self.get_eth_balance(address)
        
        # Initialize balances dictionary
        balances = {
            'ETH': eth_balance
        }
        
        # Get token balances
        for token_address in token_addresses:
            token_balance = self.get_token_balance(address, token_address)
            
            # Get token symbol from cache
            if token_address in self._token_cache:
                symbol = self._token_cache[token_address]['symbol']
            else:
                # If not in cache, use address as fallback (shouldn't happen)
                symbol = token_address[:6] + "..." + token_address[-4:]
            
            balances[symbol] = token_balance
        
        # Update the balance cache
        self._balance_cache[address.lower()] = {
            'balances': balances,
            'timestamp': timestamp
        }
        
        logger.info(f"Updated balance cache for {address}")
        for symbol, balance in balances.items():
            logger.info(f"  {symbol}: {balance}")
        
        return self._balance_cache[address.lower()]
    
    def get_balances(self, token_addresses=None, live=False):
        """
        Get token balances for the stored wallet address from cache or update if needed.
        
        Args:
            token_addresses (list, optional): List of token addresses to check balances for
            live (bool): If True, force a refresh of balances before returning
        
        Returns:
            dict: Dictionary with token balances
        """
        return self.get_balances_for_address(self.wallet_address, token_addresses, live)

    def get_balances_for_address(self, address, token_addresses=None, live=False):
        """
        Get token balances for a specific address from cache or update if needed.
        
        Args:
            address (str): Ethereum address to check balances for
            token_addresses (list, optional): List of token addresses to check balances for
            live (bool): If True, force a refresh of balances before returning
        
        Returns:
            dict: Dictionary with token balances
        """
        address_lower = address.lower()
        
        # If live=True, force a refresh of balances
        if live:
            return self.update_balance_cache(address, token_addresses)
        
        # Otherwise, return the cached balances if available
        if address_lower in self._balance_cache:
            # Check if cache is expired (older than 5 minutes)
            cache_age = int(time.time()) - self._balance_cache[address_lower].get('timestamp', 0)
            if cache_age > 300:  # 5 minutes
                logger.debug(f"Balance cache expired for {address} (age: {cache_age}s)")
                return self.update_balance_cache(address, token_addresses)
            else:
                return self._balance_cache[address_lower]
        
        # If no cache entry exists, update it
        return self.update_balance_cache(address, token_addresses)

    async def _balance_update_loop(self, address):
        """
        Background loop to periodically update the balance cache.
        
        Args:
            address (str): Ethereum address to check balances for
        """
        try:
            while self.balance_updates_running:
                try:
                    # Update the balance cache
                    balance_data = self.update_balance_cache(address)
                    
                    # Log the balances
                    logger.info(f"Current balances for {address}:")
                    for symbol, balance in balance_data['balances'].items():
                        logger.info(f"  {symbol}: {balance}")
                    
                    # Sleep for the balance update interval
                    await asyncio.sleep(self.balance_update_interval)
                except Exception as e:
                    logger.error(f"Error in background balance update: {e}")
                    # Sleep for a short time before retrying
                    await asyncio.sleep(1)
        except asyncio.CancelledError:
            # Task was cancelled, clean up
            logger.info("Background balance update task cancelled")
        except Exception as e:
            logger.error(f"Error in background balance update loop: {e}")
            self.balance_updates_running = False

    async def start_balance_updates(self, address, interval=15):
        """
        Start a background task to periodically update the balance cache.
        
        Args:
            address (str): Ethereum address to check balances for
            interval (int): The interval in seconds between balance updates
        """
        if self.balance_updates_running:
            logger.info("Background balance updates already running")
            return
        
        self.balance_update_interval = interval
        self.balance_updates_running = True
        logger.info(f"Starting background balance updates every {interval} seconds for {address}")
        
        # Create and start the balance update task
        self.balance_task = asyncio.create_task(self._balance_update_loop(address))

    async def stop_background_updates(self):
        """
        Stop the background price update task.
        """
        if not self.background_updates_running:
            logger.info("No background price updates running")
            return
        
        logger.info("Stopping background price updates")
        self.background_updates_running = False
        
        # Cancel the background task if it exists
        if self.background_task:
            self.background_task.cancel()
            try:
                await self.background_task
            except asyncio.CancelledError:
                pass
            self.background_task = None

    async def stop_balance_updates(self):
        """
        Stop the background balance update task.
        """
        if not self.balance_updates_running:
            logger.info("No background balance updates running")
            return
        
        logger.info("Stopping background balance updates")
        self.balance_updates_running = False
        
        # Cancel the balance task if it exists
        if self.balance_task:
            self.balance_task.cancel()
            try:
                await self.balance_task
            except asyncio.CancelledError:
                pass
            self.balance_task = None
    
    async def transfer_from_pool(self, to_address: str, amount: float, token_address: str = None):
        """
        Transfer assets from the pool's connected wallet to another address.
        Supports both native ETH and ERC20 token transfers.
        
        This method is part of the PoolMonitor class and uses the pool's web3 connection
        and wallet configuration to execute transfers.
        
        Args:
            to_address (str): Destination wallet address
            amount (float): Amount to transfer
            token_address (str, optional): ERC20 token address. If None, transfers native ETH.
        
        Returns:
            str: Transaction hash if successful, None if failed
            
        Why this implementation:
        - Integrated with PoolMonitor's existing web3 connection and wallet setup
        - Handles both ETH and ERC20 transfers in a single method
        - Validates inputs and handles errors gracefully
        - Uses async/await for non-blocking blockchain interactions
        - Follows gas estimation best practices
        """
        try:
            # Validate addresses
            to_address = self.web3.toChecksumAddress(to_address)
            if token_address:
                token_address = self.web3.toChecksumAddress(token_address)
                
            # Get account from pool's wallet private key
            account = self.web3.eth.account.from_key(self.wallet_private_key)
            from_address = account.address
            
            # Get nonce for transaction
            nonce = self.web3.eth.get_transaction_count(from_address)
            
            if token_address:
                # ERC20 Transfer
                token_contract = self.web3.eth.contract(address=token_address, abi=self.erc20_abi)
                decimals = token_contract.functions.decimals().call()
                amount_wei = int(amount * (10 ** decimals))
                
                # Build transaction
                tx_data = token_contract.encodeABI(
                    fn_name="transfer", 
                    args=[to_address, amount_wei]
                )
                
                tx = {
                    'from': from_address,
                    'to': token_address,
                    'data': tx_data,
                    'nonce': nonce,
                    'chainId': await self.web3.eth.chain_id
                }
            else:
                # Native ETH transfer
                amount_wei = self.web3.toWei(amount, 'ether')
                tx = {
                    'from': from_address,
                    'to': to_address,
                    'value': amount_wei,
                    'nonce': nonce,
                    'chainId': await self.web3.eth.chain_id
                }
            
            # Estimate gas and get gas price
            gas = await self.web3.eth.estimate_gas(tx)
            gas_price = await self.web3.eth.gas_price
            
            # Add gas details to transaction
            tx['gas'] = gas
            tx['gasPrice'] = gas_price
            
            # Sign and send transaction
            signed_tx = self.web3.eth.account.sign_transaction(tx, self.wallet_private_key)
            tx_hash = await self.web3.eth.send_raw_transaction(signed_tx.rawTransaction)
            
            logger.info(f"Pool transfer initiated. Transaction hash: {tx_hash.hex()}")
            return tx_hash.hex()
            
        except Exception as e:
            logger.error(f"Stack Trace: {traceback.format_exc()}")
            logger.error(f"Pool transfer failed: {str(e)}")
            return None
    
    def _deadline(self):
        return int(time.time()) + 60 * 10
    
    def wrap_eth(self, amount: float = None):
        if amount is None:
            amount = self.get_eth_balance(self.wallet_address) * 0.8

        # Convert amount to wei
        amount_wei = self.web3.toWei(amount, 'ether')

        # wrap eth
        tx = self.weth_contract.functions.deposit().build_transaction({
            'chainId': self.web3.eth.chain_id,
            'gas': 2000000,
            "maxPriorityFeePerGas": self.web3.eth.max_priority_fee,
            "maxFeePerGas": 100 * 10**9,
            'nonce': self.web3.eth.get_transaction_count(self.wallet_address),
            'value': amount_wei,  # wrap amount in wei
        })

        signed_transaction = self.web3.eth.account.sign_transaction(tx, self.wallet_private_key)
        tx_hash = self.web3.eth.send_raw_transaction(signed_transaction.rawTransaction)

        tx_receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
        print(f"tx hash: {self.web3.to_hex(tx_hash)}, gas used: {tx_receipt['gasUsed']}")

        weth_balance = self.weth_contract.functions.balanceOf(self.wallet_address).call()
        print(f"weth balance: {weth_balance / 10**18}")
    
    def unwrap_eth(self, amount: float = None):
        if amount is None:
            amount = self.get_eth_balance(self.wallet_address)
        return self.web3.fromWei(amount, 'ether')
    
    async def swap_instrument(self, amount: float, token_address: str, is_exact_input: bool = True, slippage: float = 0.005) -> str:
        """
        Execute a swap transaction in the current Uniswap pool.
        
        This method allows swapping between the two tokens in the pool, handling both exact input 
        and exact output swaps with slippage protection.
        
        Args:
            amount (float): The amount to swap (interpreted as input amount if is_exact_input=True, 
                        otherwise as desired output amount)
            token_address (str): Address of the token being sent (for input) or received (for output)
            is_exact_input (bool): If True, amount is the exact input amount. If False, amount is 
                                the desired output amount.
            slippage (float): Maximum acceptable slippage as a decimal (e.g., 0.005 for 0.5%)
        
        Returns:
            str: Transaction hash if successful, None if failed
            
        Raises:
            ValueError: If token_address is not one of the pool's tokens
        """
        try:
            # Verify token is in pool
            token0 = self.token0_address
            token1 = self.token1_address
            token_address = self.web3.toChecksumAddress(token_address)
            
            if token_address not in [token0, token1]:
                raise ValueError("Token address must be one of the pool's tokens")
            
            current_price = self.get_current_price(live=True)

            # Determine which token is being swapped in/out
            is_token0 = False
            if token_address == token0:
                is_token0 = True
                current_price = current_price['token0_price']
            else:
                current_price = current_price['token1_price']
                
            token_contract = self.web3.eth.contract(address=token_address, abi=self.erc20_abi)
            decimals = token_contract.functions.decimals().call()
            
            # Convert amount to wei
            amount_wei = int(amount * (10 ** decimals))
            
            # Calculate minimum output or maximum input based on slippage
            if is_exact_input:
                min_output = int(amount_wei * current_price * (1 - slippage))
                swap_params = {
                    'tokenIn': token0 if is_token0 else token1,
                    'tokenOut': token1 if is_token0 else token0,
                    'fee': self.contract.functions.fee().call(),
                    'recipient': self.wallet_address,
                    'deadline': self._deadline(),
                    'amountIn': amount_wei,
                    'amountOutMinimum': min_output,
                    'sqrtPriceLimitX96': 0  # No price limit
                }
            else:
                max_input = int(amount_wei / current_price * (1 + slippage))
                swap_params = {
                    'tokenIn': token0 if is_token0 else token1,
                    'tokenOut': token1 if is_token0 else token0,
                    'fee': self.contract.functions.fee().call(),
                    'recipient': self.wallet_address,
                    'deadline': self._deadline(),
                    'amountOut': amount_wei,
                    'amountInMaximum': max_input,
                    'sqrtPriceLimitX96': 0  # No price limit
                }

            # Build swap transaction
            nonce = self.web3.eth.get_transaction_count(self.wallet_address)

            # Encode the swap function call
            fn_name = "exactInputSingle" if is_exact_input else "exactOutputSingle"
            if is_exact_input:
                swap_data = self.contract.encodeABI(fn_name=fn_name, args=[(
                    swap_params['tokenIn'],
                    swap_params['tokenOut'],
                    swap_params['fee'],
                    swap_params['recipient'],
                    swap_params['deadline'],
                    swap_params['amountIn'],
                    swap_params['amountOutMinimum'],
                    swap_params['sqrtPriceLimitX96']
                )])
            else:
                swap_data = self.contract.encodeABI(fn_name=fn_name, args=[(
                    swap_params['tokenIn'],
                    swap_params['tokenOut'],
                    swap_params['fee'],
                    swap_params['recipient'],
                    swap_params['deadline'],
                    swap_params['amountOut'],
                    swap_params['amountInMaximum'],
                    swap_params['sqrtPriceLimitX96']
                )])
            
            # Build transaction
            tx = {
                'from': self.wallet_address,
                'to': self.contract.address,
                'data': swap_data,
                'nonce': nonce,
                'chainId': self.web3.eth.chain_id
            }
            
            # Estimate gas and get gas price
            gas = await self.web3.eth.estimate_gas(tx)
            gas_price = await self.web3.eth.gas_price
            
            tx['gas'] = gas
            tx['gasPrice'] = gas_price
            
            # Sign and send transaction
            signed_tx = self.web3.eth.account.sign_transaction(tx, self.wallet_private_key)
            tx_hash = await self.web3.eth.send_raw_transaction(signed_tx.rawTransaction)
            
            logger.info(f"Swap transaction initiated. Hash: {tx_hash.hex()}")
            return tx_hash.hex()
            
        except Exception as e:
            logger.error(f'Stack Trace: {traceback.format_exc()}')
            logger.error(f"Swap failed: {str(e)}")
            return None

    async def get_tob_size(self, direction: str):
        """
        Get the top-of-book (TOB) size for a given trading pair symbol.
        
        WHY: We need to estimate the maximum trade size that can be executed at the current price
        with minimal price impact. This helps determine optimal arbitrage sizes.
        
        Args:
            symbol (str): Trading pair symbol (e.g. "ETHUSDC")
            
        Returns:
            float: Estimated TOB size in base token units
        """
        try:
            # WHY: Using cached values avoids unnecessary RPC calls and calculations
            # If not in cache, we return 0 as a safe default
            price_cache = self.get_current_price()
            reserve0, reserve1 = price_cache.get("reserve0"), price_cache.get("reserve1")
            if reserve0 is None or reserve1 is None:
                return 0
            
            token_we_are_getting = self.token0_address if direction == 'sell' else self.token1_address
            is_base_token = token_we_are_getting == self.token0_address
                
            if is_base_token:
                tob_size = reserve0 * 0.001
            else:
                tob_size = reserve1 * 0.001
            
            # Cap at reasonable size
            MAX_TOB = 10.0  # Maximum 10 base tokens
            tob_size = min(tob_size, MAX_TOB)

            # convert to base token units
            if is_base_token:
                current_price = self.get_current_price()['token0_price']
                tob_size = tob_size / (10 ** self.token0_decimals) / current_price
            else:
                tob_size = tob_size / (10 ** self.token1_decimals)
            
            return tob_size
            
        except Exception as e:
            logger.error(f"Stack Trace: {traceback.format_exc()}")
            logger.error(f"Error getting TOB size: {e}")
            return 0

    async def execute_trade(self, trade_direction, trade_size, arb_instrument):
        """
        Execute a trade on Uniswap.

        Args:
            trade_direction (str): Direction of the trade ('buy' or 'sell')
            trade_size (float): Size of the trade
            arb_instrument (str): Trading instrument (e.g., 'ETHUSDT')

        Returns:
            dict: Trade details
        """
        # Implement trade execution logic here
        # This is a placeholder implementation
        return {"status": "success", "trade_size": trade_size}

    async def confirm_trade(self, trade):
        """
        Confirm that a trade was executed successfully.

        Args:
            trade (dict): Trade details

        Returns:
            float: Confirmed trade size
        """
        # Implement trade confirmation logic here
        # This is a placeholder implementation
        return trade["trade_size"]

    async def initiate_transfer(self, transfer_direction, transfer_amount, from_address, to_address):
        """
        Initiate a transfer of assets.

        Args:
            transfer_direction (str): Direction of the transfer ('withdraw' or 'deposit')
            transfer_amount (float): Amount to transfer
            from_address (str): Source address
            to_address (str): Destination address

        Returns:
            dict: Transfer details
        """
        # Implement transfer initiation logic here
        # This is a placeholder implementation
        return {"status": "initiated", "transfer_amount": transfer_amount}

    async def confirm_transfer(self, transfer):
        """
        Confirm that a transfer was completed successfully.

        Args:
            transfer (dict): Transfer details

        Returns:
            float: Confirmed transfer size
        """
        # Implement transfer confirmation logic here
        # This is a placeholder implementation
        return transfer["transfer_amount"]

    async def get_withdraw_address(self, arb_instrument):
        """
        Get the address to withdraw assets from.

        Args:
            arb_instrument (str): Trading instrument

        Returns:
            str: Withdraw address
        """
        # Implement logic to get withdraw address
        return self.wallet_address

    async def get_deposit_address(self, arb_instrument):
        """
        Get the address to deposit assets to.

        Args:
            arb_instrument (str): Trading instrument

        Returns:
            str: Deposit address
        """
        # Implement logic to get deposit address
        return self.wallet_address

    async def get_balance(self, arb_instrument):
        """
        Get the current balance of a specific asset.

        Args:
            arb_instrument (str): Trading instrument

        Returns:
            float: Balance of the asset
        """
        # Implement logic to get balance
        return self.get_token_balance(self.wallet_address, self.get_token_address(arb_instrument))


async def test_price_updates(pool_monitor, update_interval=5, track_balances=False):
    """
    Test function to verify that the price updates are working properly.
    Uses an existing PoolMonitor instance to continuously log prices.
    
    Args:
        pool_monitor (PoolMonitor): An initialized PoolMonitor instance
        update_interval (int): Interval in seconds between price updates
        track_balances (bool): Whether to also track and log balances
    """
    logger.info("Testing price updates using PoolMonitor class...")
    
    # Start background price updates
    await pool_monitor.start_background_updates(update_interval)
    
    # Start background balance updates if requested
    address = None
    if track_balances:
        config = Config()
        if hasattr(config, 'private_key') and config.private_key:
            account = pool_monitor.web3.eth.account.from_key(config.private_key)
            address = account.address
            logger.info(f"Will track balances for account: {address}")
            
            # Initialize balance cache
            balance_data = pool_monitor.update_balance_cache(address)
            logger.info(f"Initial balances for {address}:")
            for symbol, balance in balance_data['balances'].items():
                logger.info(f"  {symbol}: {balance}")
            
            # Start balance updates (every 15 seconds by default)
            await pool_monitor.start_balance_updates(address, 15)
        else:
            logger.warning("No private key found in config. Cannot track balances.")
    
    logger.info(f"Monitoring {pool_monitor.token0_symbol}/{pool_monitor.token1_symbol} pool prices...")
    logger.info(f"Update interval: {update_interval} seconds")
    
    # Continuous loop to fetch and log prices
    counter = 0
    try:
        while True:
            try:
                # Get current price data from cache (updated by background task)
                s_time = time.time()
                
                # Alternate between live and cached price to demonstrate the difference
                if counter % 10 == 0:
                    price_data = pool_monitor.get_current_price(live=True)
                    source = "Live RPC call"
                else:
                    price_data = pool_monitor.get_current_price(live=False)
                    source = "Cache"
                
                e_time = time.time()
                
                if price_data and 'price' in price_data:
                    # Calculate cache age
                    cache_age = int(time.time()) - price_data.get('timestamp', 0)
                    
                    # Format time since last tick change
                    time_since_change = price_data.get('time_since_last_change', 0)
                    formatted_time_since_change = pool_monitor._format_time_duration(time_since_change)
                    
                    # Get current timestamp
                    current_time = datetime.datetime.now().strftime("%H:%M:%S")
                    
                    # For USDC/WETH pool, show prices in both directions for clarity
                    if pool_monitor.token0_symbol == 'USDC' and pool_monitor.token1_symbol == 'WETH':
                        # Get prices calculated from sqrtPriceX96
                        usdc_per_weth = price_data['sqrt_price_token1']  # WETH price in USDC
                        weth_per_usdc = price_data['sqrt_price_token0']  # USDC price in WETH
                        
                        price_display = f"1 WETH = {usdc_per_weth:.2f} USDC | 1 USDC = {weth_per_usdc:.8f} WETH"
                    else:
                        # For other pairs, use the token prices directly
                        price_display = f"1 {pool_monitor.token1_symbol} = {price_data['token1_price']:.8f} {pool_monitor.token0_symbol}"
                    
                    # Log basic price info
                    logger.info(
                        f"[{current_time}] {pool_monitor.token0_symbol}/{pool_monitor.token1_symbol} | {price_display} | "
                        f"Tick: {price_data['tick']} | Source: {source} | Fetch time: {(e_time - s_time):.6f}s | "
                        f"Cache age: {cache_age}s | Last tick change: {formatted_time_since_change} ago"
                    )
                    
                    # Display reserves
                    if 'reserve0_hr' in price_data and 'reserve1_hr' in price_data:
                        logger.info(
                            f"Reserves: {price_data['reserve0_hr']:.2f} {pool_monitor.token0_symbol}, "
                            f"{price_data['reserve1_hr']:.2f} {pool_monitor.token1_symbol}"
                        )
                    
                    # Display balances if tracking them and address is available
                    if track_balances and address and counter % 10 == 0:
                        # Get balances from cache
                        balance_data = pool_monitor.get_balances(address)
                        if balance_data and 'balances' in balance_data:
                            current_balances = balance_data['balances']
                            # Only show token0, token1, and ETH in regular updates
                            relevant_balances = {
                                'ETH': current_balances.get('ETH', 0),
                                pool_monitor.token0_symbol: current_balances.get(pool_monitor.token0_symbol, 0),
                                pool_monitor.token1_symbol: current_balances.get(pool_monitor.token1_symbol, 0)
                            }
                            logger.info(f"Current balances: ETH: {relevant_balances['ETH']:.6f}, "
                                        f"{pool_monitor.token0_symbol}: {relevant_balances[pool_monitor.token0_symbol]:.6f}, "
                                        f"{pool_monitor.token1_symbol}: {relevant_balances[pool_monitor.token1_symbol]:.6f}")
                    
                    # Detailed update every 5 cycles
                    counter += 1
                    if counter % 5 == 0:
                        # For easier reading, we'll use these variable names
                        token0 = pool_monitor.token0_symbol
                        token1 = pool_monitor.token1_symbol
                        
                        logger.info(f"\n=== Detailed Pool State ===")
                        
                        # For USDC/WETH, display prices in a clear format
                        if token0 == 'USDC' and token1 == 'WETH':
                            # Get prices calculated from sqrtPriceX96
                            weth_in_usdc_sqrt = price_data['sqrt_price_token1']  # WETH price in USDC
                            usdc_in_weth_sqrt = price_data['sqrt_price_token0']  # USDC price in WETH
                            
                            # Get prices calculated from tick
                            weth_in_usdc_tick = price_data['tick_price']  # WETH price in USDC
                            usdc_in_weth_tick = price_data['tick_price_token0']  # USDC price in WETH
                            
                            # Display both calculation methods
                            logger.info(f"  === Prices from sqrtPriceX96 ===")
                            logger.info(f"  1 WETH = {weth_in_usdc_sqrt:.2f} USDC")
                            logger.info(f"  1 USDC = {usdc_in_weth_sqrt:.8f} WETH")
                            
                            logger.info(f"\n  === Prices from tick ===")
                            logger.info(f"  1 WETH = {weth_in_usdc_tick:.2f} USDC")
                            logger.info(f"  1 USDC = {usdc_in_weth_tick:.8f} WETH")
                        else:
                            # For other pairs, show similar information
                            if 'sqrt_price_token0' in price_data and 'sqrt_price_token1' in price_data:
                                logger.info(f"  === Prices from sqrtPriceX96 ===")
                                logger.info(f"  1 {token0} = {price_data['sqrt_price_token0']:.8f} {token1}")
                                logger.info(f"  1 {token1} = {price_data['sqrt_price_token1']:.8f} {token0}")
                                
                                logger.info(f"\n  === Prices from tick ===")
                                logger.info(f"  1 {token1} = {price_data['tick_price']:.8f} {token0}")
                                logger.info(f"  1 {token0} = {price_data['tick_price_token0']:.8f} {token1}")
                        
                        logger.info(f"\n  === Pool Details ===")
                        logger.info(f"  Current Tick: {price_data['tick']}")
                        logger.info(f"  sqrt_price_x96: {price_data.get('sqrt_price_x96', 0)}")
                        logger.info(f"  Time since last tick change: {formatted_time_since_change}")
                        logger.info(f"  Liquidity: {pool_monitor._format_number(price_data.get('liquidity', 0))}")
                        
                        # Add reserves in detail view
                        if 'reserve0_hr' in price_data and 'reserve1_hr' in price_data:
                            logger.info(f"  {token0} Reserves: {price_data['reserve0_hr']:.2f}")
                            logger.info(f"  {token1} Reserves: {price_data['reserve1_hr']:.2f}")
                        
                        # Add detailed balance info if tracking
                        if track_balances and address:
                            balance_data = pool_monitor.get_balances(address)
                            if balance_data and 'balances' in balance_data:
                                logger.info(f"\n  === Account Balances ===")
                                for symbol, balance in balance_data['balances'].items():
                                    # Filter out zero balances except for ETH, token0, and token1
                                    if balance > 0 or symbol == 'ETH' or symbol == token0 or symbol == token1:
                                        logger.info(f"  {symbol}: {balance}")
                        
                        logger.info("--------------------------------------------------")
                else:
                    logger.warning("Failed to get price data")
                
                # Sleep for 1 second between logs (independent of the background update interval)
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"Error fetching price: {e}")
                await asyncio.sleep(1)  # Brief pause on error
                
    except KeyboardInterrupt:
        logger.info("Price monitoring stopped by user.")
        # Stop background updates
        await pool_monitor.stop_background_updates()
        if track_balances:
            await pool_monitor.stop_balance_updates()
        return True


# Test function for swap_instrument
async def test_swap_instrument(pool_monitor):
    """
    Test suite for swap_instrument function.
    """
    try:
        # Get WETH address using symbol
        weth_address = pool_monitor.get_token_address("WETH")
        if not weth_address:
            raise ValueError("WETH address not found")

        # Execute swap using WETH
        tx_hash = await pool_monitor.swap_instrument(0.36, weth_address, is_exact_input=True)

        # Assert
        if tx_hash:
            logger.info(f"✓ Swap successful. Transaction hash: {tx_hash}")
        else:
            logger.error("✗ Swap failed")

    except Exception as e:
        logger.error(f"Stack Trace: {traceback.format_exc()}")
        logger.error(f"Test failed with error: {e}")


# Test function for transfer_from_pool
async def test_transfer_from_pool(pool_monitor):
    """
    Test suite for transfer_from_pool function.
    """
    try:
        # Get USDC address using symbol
        eth_address = pool_monitor.get_token_address("WETH")
        if not eth_address:
            raise ValueError("ETH address not found")

        # Execute transfer using USDC
        eth_balance = pool_monitor.get_balances(token_addresses=eth_address, live=True)['balances']['ETH']
        logger.info(f"ETH balance: {eth_balance}")
        tx_hash = await pool_monitor.transfer_from_pool('0x3e6b04c2f793d77d6414075aae1d44ef474b483e', eth_balance, eth_address)

        # Assert
        if tx_hash:
            logger.info(f"✓ Transfer successful. Transaction hash: {tx_hash}")
        else:
            logger.error("✗ Transfer failed")

    except Exception as e:
        logger.error(f"Stack Trace: {traceback.format_exc()}")
        logger.error(f"Test failed with error: {e}")


async def main():
    """
    Main entry point for the script.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Monitor Uniswap V3 pool activity in real-time')
    parser.add_argument('--pool', type=str, default=DEFAULT_POOL_ADDRESS,
                        help=f'Uniswap V3 pool address (default: {DEFAULT_POOL_ADDRESS} - ETH/USDC)')
    parser.add_argument('--interval', type=int, default=5,
                        help='Price update interval in seconds (default: 5)')
    parser.add_argument('--verbose', action='store_true',
                        help='Enable verbose logging')
    parser.add_argument('--trade-test', action='store_true',
                        help='Run the swap_instrument test')
    parser.add_argument('--transfer-test', action='store_true',
                        help='Run the transfer_from_pool test')
    parser.add_argument('--wrap-eth', action='store_true',
                        help='Wrap ETH')
    parser.add_argument('--unwrap-eth', action='store_true',
                        help='Unwrap ETH')
    
    args = parser.parse_args()
    
    # Set logging level based on verbose flag
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Validate pool address
    if not args.pool.startswith('0x') or len(args.pool) != 42:
        logger.warning(f"Invalid pool address format: {args.pool}")
        logger.warning(f"Using default ETH/USDC pool: {DEFAULT_POOL_ADDRESS}")
        pool_address = DEFAULT_POOL_ADDRESS
    else:
        pool_address = args.pool
    
    logger.info(f"Pool address: {pool_address}")
    config = Config()
    
    # Initialize pool monitor with default update settings
    pool_monitor = PoolMonitor(pool_address, config.infura_ws_url, config.wallet_private_key)

    # Run tests based on command-line arguments
    if args.trade_test:
        await test_swap_instrument(pool_monitor)
    elif args.transfer_test:
        await test_transfer_from_pool(pool_monitor)
    elif args.wrap_eth:
        wrapped_eth =  pool_monitor.wrap_eth()
        logger.info(f"Wrapped ETH: {wrapped_eth}")
    elif args.unwrap_eth:
        unwrapped_eth = pool_monitor.unwrap_eth()
        logger.info(f"Unwrapped ETH: {unwrapped_eth}")
    else:
        # Run the normal price monitoring
        await test_price_updates(pool_monitor, args.interval)


# Run the main function
if __name__ == "__main__":
    asyncio.run(main())