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

INSTRUMENT_TO_POOL_MAP = {
    "ETH/USDC": {
        "pool_address": DEFAULT_POOL_ADDRESS,   
        "base_asset": "USDC",
        "quote_asset": "ETH"
    },
    "USDC/ETH": {
        "pool_address": DEFAULT_POOL_ADDRESS,
        "base_asset": "WETH",
        "quote_asset": "USDC"
    },
    "USDT/ETH": {
        "pool_address": DEFAULT_POOL_ADDRESS,
        "base_asset": "ETH",
        "quote_asset": "USDT"
    },
    "DAI/ETH": {
        "pool_address": DEFAULT_POOL_ADDRESS,
        "base_asset": "ETH",
        "quote_asset": "DAI"
    },
    "WETH/ETH": {
        "pool_address": DEFAULT_POOL_ADDRESS,
        "base_asset": "ETH",
        "quote_asset": "WETH"
    },
    "WBTC/ETH": {
        "pool_address": DEFAULT_POOL_ADDRESS,
        "base_asset": "ETH",
        "quote_asset": "WBTC"
    }
}

class PoolMonitor:
    """
    A class to monitor Uniswap V3 pool activity in real-time.
    
    This class provides functionality to track price changes and swap events
    for any Uniswap V3 pool using Web3 event subscriptions.
    """
    
    def __init__(self, instrument, web3_provider_url, wallet_private_key, update_price=True, update_balance=True, intervals=(5, 15)):
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
        if instrument not in INSTRUMENT_TO_POOL_MAP:
            raise ValueError(f"{instrument} not found in INSTRUMENT_TO_POOL_MAP (uniswap_connector.py), please add it to the map to use this instrument.")
        
        pool_info = INSTRUMENT_TO_POOL_MAP[instrument]
        self.instrument = instrument
        self.pool_address = pool_info["pool_address"]
        self.base_asset = pool_info["base_asset"]
        self.quote_asset = pool_info["quote_asset"]

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
        
        # Initialize pool 
        self._initialize_pool()
        
        # Start background updates if requested
        if update_price:
            asyncio.create_task(self.start_price_updates(self.update_interval))
        if update_balance:
            asyncio.create_task(self.start_balance_updates(self.balance_update_interval))
        

    def _initialize_pool(self):
        """
        Initialize the Web3 connection and pool contract.
        
        This method sets up the Web3 connection, loads the pool contract,
        and initializes token information.
        """
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
        self.fee_tier = self.pool_contract.functions.fee().call()
        self.tick_spacing = self.pool_contract.functions.tickSpacing().call()
        
        # Initialize token information
        self._initialize_token_info()
        
    
    def _initialize_token_info(self):
        """
        Initialize token information for the pool.
        
        This method loads token symbols and decimals for both tokens in the pool.
        """
        token0_address = self.pool_contract.functions.token0().call()
        token1_address = self.pool_contract.functions.token1().call()
        token0_contract = self.web3.eth.contract(address=token0_address, abi=self.erc20_abi)
        token1_contract = self.web3.eth.contract(address=token1_address, abi=self.erc20_abi)

        token0_symbol = token0_contract.functions.symbol().call()
        token1_symbol = token1_contract.functions.symbol().call()

        base_token_contract = token0_contract
        quote_token_contract = token1_contract

        if token0_symbol.lower() == self.base_asset.lower():
            assert token1_symbol.lower() == self.quote_asset.lower(), f'{self.instrument} info in INSTRUMENT_TO_POOL_MAP is incorrect (quote token: {token1_symbol} from pool is not the same as the quote asset: {self.quote_asset})'
            base_token_contract, quote_token_contract = token0_contract, token1_contract
        else:
            assert token0_symbol.lower() == self.quote_asset.lower(), f'{self.instrument} info in INSTRUMENT_TO_POOL_MAP is incorrect (base token: {token0_symbol} from pool is not the same as the base asset: {self.base_asset})'
            assert token1_symbol.lower() == self.base_asset.lower(), f'{self.instrument} info in INSTRUMENT_TO_POOL_MAP is incorrect (quote token: {token1_symbol} from pool is not the same as the quote asset: {self.quote_asset})'
            base_token_contract, quote_token_contract = token1_contract, token0_contract
        
            
        self.base_token_info = {
            'address': base_token_contract.address,
            'symbol': base_token_contract.functions.symbol().call(),
            'decimals': base_token_contract.functions.decimals().call(),
            'contract': base_token_contract
        }
        
        self.quote_token_info = {
            'address': quote_token_contract.address,
            'symbol': quote_token_contract.functions.symbol().call(),
            'decimals': quote_token_contract.functions.decimals().call(),
            'contract': quote_token_contract
        }

        self.token0_info = {
            'address': token0_address,
            'symbol': token0_symbol,
            'decimals': token0_contract.functions.decimals().call(),
            'contract': token0_contract
        }

        self.token1_info = {
            'address': token1_address,
            'symbol': token1_symbol,
            'decimals': token1_contract.functions.decimals().call(),
            'contract': token1_contract
        }
    
    def get_token_address(self, symbol: str) -> str:
        """
        Get the token address for a given symbol.
        Args:
            symbol (str): The symbol of the token (e.g., 'ETH', 'USDC')
        Returns:
            str: The address of the token if found, None otherwise.
        """
        if symbol.lower() == self.base_asset.lower():
            return self.base_token_info['address']
        elif symbol.lower() == self.quote_asset.lower():
            return self.quote_token_info['address']
        else:
            raise ValueError(f"{symbol} not part of the pool")
    
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
            reserve0_hr = reserve0 / (10 ** self.token0_info['decimals'])
            reserve1_hr = reserve1 / (10 ** self.token1_info['decimals'])
            
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
                reserve0_hr = reserve0 / (10 ** self.token0_info['decimals'])
                reserve1_hr = reserve1 / (10 ** self.token1_info['decimals'])
                
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
                self.token0_info['decimals'], 
                self.token1_info['decimals']
            )
            
            # 2. Calculate price from tick
            raw_price = self.calculate_price_from_tick(current_tick)  # Raw price from tick
            decimal_adjustment = 10 ** (self.token0_info['decimals'] - self.token1_info['decimals'])
            tick_price = raw_price * decimal_adjustment  # Price from tick calculation
            tick_price_token0 = 1 / tick_price if tick_price != 0 else 0  # Inverse price from tick
            
            # Calculate token reserves using the whitepaper formula
            reserve0, reserve1, reserve0_hr, reserve1_hr = self.calculate_reserves_from_liquidity(
                liquidity, sqrt_price_x96, current_tick
            )
            
            # Log the reserves for debugging
            logger.info(f"Calculated reserves using whitepaper formula:")
            logger.info(f"Hourly {self.token0_info['symbol']} reserves: {reserve0_hr:.2f}")
            logger.info(f"Hourly {self.token1_info['symbol']} reserves: {reserve1_hr:.2f}")
            
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
        if self.base_token_info['symbol'] == self.token1_info['symbol']:
            return self.get_current_price()['token1_price']
        else:
            return self.get_current_price()['token0_price']
    
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
    
    async def start_price_updates(self, interval=5):
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
        self.background_task = asyncio.create_task(self._backgroup_price_update_loop())
    
    async def _backgroup_price_update_loop(self):
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
    
    def update_balance_cache(self):
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
        for symbol_info in [self.base_token_info, self.quote_token_info]:
            symbol_contract = symbol_info['contract']
            symbol = symbol_info['symbol']
            decimals = symbol_info['decimals']
            balance = symbol_contract.functions.balanceOf(self.wallet_address).call()
            symbol_balance = balance / (10 ** decimals)
            self._balance_cache[symbol.lower()] = symbol_balance
        
        return self._balance_cache
    
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

    async def _balance_update_loop(self):
        """
        Background loop to periodically update the balance cache.
        
        Args:
            address (str): Ethereum address to check balances for
        """
        try:
            while self.balance_updates_running:
                try:
                    # Update the balance cache
                    balance_data = self.update_balance_cache()
                    
                    # Log the balances
                    logger.info(f"Current balances for {self.wallet_address}:")
                    for symbol, balance in balance_data.items():
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

    async def start_balance_updates(self, interval=15):
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
        logger.info(f"Starting background balance updates every {interval} seconds")
        
        # Create and start the balance update task
        self.balance_task = asyncio.create_task(self._balance_update_loop())

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
            
            logger.info(f"Transfer initiated: From {from_address} to {to_address}")
            
            # Check sender balance first
            if token_address:
                # ERC20 Transfer - Check token balance first
                token_contract = self.web3.eth.contract(address=token_address, abi=self.erc20_abi)
                decimals = token_contract.functions.decimals().call()
                amount_wei = int(amount * (10 ** decimals))
                
                # Check sender's token balance
                sender_balance = token_contract.functions.balanceOf(from_address).call()
                logger.info(f"Token transfer: Amount={amount} ({amount_wei} wei), Sender balance={sender_balance / (10 ** decimals)} ({sender_balance} wei)")
                
                if sender_balance < amount_wei:
                    logger.error(f"Insufficient token balance: Have {sender_balance / (10 ** decimals)}, need {amount}")
                    return None
                
                # Check allowance (if relevant)
                allowance = token_contract.functions.allowance(from_address, token_address).call()
                logger.info(f"Token allowance: {allowance / (10 ** decimals)}")
                
            else:
                # Native ETH transfer - Check ETH balance first
                amount_wei = self.web3.toWei(amount, 'ether')
                sender_balance = self.web3.eth.get_balance(from_address)
                logger.info(f"ETH transfer: Amount={amount} ETH ({amount_wei} wei), Sender balance={self.web3.fromWei(sender_balance, 'ether')} ETH ({sender_balance} wei)")
                
                if sender_balance < amount_wei:
                    logger.error(f"Insufficient ETH balance: Have {self.web3.fromWei(sender_balance, 'ether')} ETH, need {amount} ETH")
                    return None
            
            # Get nonce for transaction
            nonce = self.web3.eth.get_transaction_count(from_address)
            logger.info(f"Using nonce: {nonce}")
            
            if token_address:
                # ERC20 Transfer
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
                    'chainId': self.web3.eth.chain_id
                }
                logger.info(f"Created ERC20 transfer transaction for token: {token_address}")
            else:
                # Native ETH transfer
                tx = {
                    'from': from_address,
                    'to': to_address,
                    'value': amount_wei,
                    'nonce': nonce,
                    'chainId': self.web3.eth.chain_id
                }
                logger.info(f"Created native ETH transfer transaction")
            
            # Get gas price first
            gas_price = await self.web3.eth.gas_price
            logger.info(f"Current gas price: {self.web3.fromWei(gas_price, 'gwei')} gwei")
            
            # Add gas price to transaction to help with gas estimation
            tx['gasPrice'] = gas_price
            
            # Try to estimate gas with detailed error handling
            try:
                logger.info(f"Estimating gas for transaction: {tx}")
                gas = await self.web3.eth.estimate_gas(tx)
                logger.info(f"Gas estimation successful: {gas} gas units")
            except Exception as gas_error:
                logger.error(f"Gas estimation failed with error: {str(gas_error)}")
                logger.error(f"Gas estimation stack trace: {traceback.format_exc()}")
                
                # Add a fallback gas limit and warn about it
                if token_address:
                    fallback_gas = 100000  # Higher fallback for token transfers
                else:
                    fallback_gas = 21000   # Standard ETH transfer gas
                
                logger.warning(f"Using fallback gas limit of {fallback_gas}. Transaction may fail.")
                gas = fallback_gas
            
            # Add gas details to transaction
            tx['gas'] = gas
            
            # Sign and send transaction
            logger.info(f"Signing transaction with gas={gas}, gasPrice={gas_price}")
            signed_tx = self.web3.eth.account.sign_transaction(tx, self.wallet_private_key)
            
            try:
                tx_hash = await self.web3.eth.send_raw_transaction(signed_tx.rawTransaction)
                logger.info(f"Pool transfer initiated. Transaction hash: {tx_hash.hex()}")
                return tx_hash.hex()
            except Exception as send_error:
                logger.error(f"Transaction sending failed: {str(send_error)}")
                logger.error(f"Transaction details: {tx}")
                return None
            
        except Exception as e:
            logger.error(f"Stack Trace: {traceback.format_exc()}")
            logger.error(f"Pool transfer failed: {str(e)}")
            return None
    
    def _deadline(self):
        return int(time.time()) + 5
    
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
    
    async def swap_instrument(self, amount: float, token_address: str, is_exact_input: bool = True, slippage: float = 0.005, deadline: int = 60) -> str:
        """
        Execute a swap transaction using Uniswap V3 SwapRouter.
        """
        try:
            # Define Uniswap V3 Router address
            UNISWAP_V3_ROUTER = "0xE592427A0AEce92De3Edee1F18E0157C05861564"  # Mainnet SwapRouter
            
            # Verify token is in pool
            token0 = self.token0_info['address']
            token1 = self.token1_info['address']
            token_address = self.web3.toChecksumAddress(token_address)
            
            logger.info(f"Swap parameters: amount={amount}, token_address={token_address}, is_exact_input={is_exact_input}")
            logger.info(f"Pool tokens: token0={token0}, token1={token1}")
            
            if token_address not in [token0, token1]:
                raise ValueError(f"Token address {token_address} must be one of the pool's tokens ({token0} or {token1})")
            
            # Determine which token is being swapped in/out
            is_token0 = (token_address == token0)
            token_in = token0 if is_token0 else token1
            token_out = token1 if is_token0 else token0
            
            logger.info(f"Token in: {token_in}, Token out: {token_out}, Is token0: {is_token0}")
            
            # Get token contract and decimals
            token_contract = self.web3.eth.contract(address=token_address, abi=self.erc20_abi)
            decimals = token_contract.functions.decimals().call()
            logger.info(f"Token decimals: {decimals}")
            
            # Check token balance
            token_balance = token_contract.functions.balanceOf(self.wallet_address).call()
            token_balance_human = token_balance / (10 ** decimals)
            logger.info(f"Token balance: {token_balance_human} ({token_balance} wei)")
            
            # Convert amount to wei
            amount_wei = int(amount * (10 ** decimals))
            logger.info(f"Amount in wei: {amount_wei}")
            
            if is_exact_input and token_balance < amount_wei:
                logger.error(f"Insufficient token balance: {token_balance_human} < {amount}")
                return None
            
            # Initialize router contract with ABI
            router_abi = json.loads('''[
                {"inputs":[{"components":[{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint24","name":"fee","type":"uint24"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"amountOutMinimum","type":"uint256"},{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"}],"internalType":"struct ISwapRouter.ExactInputSingleParams","name":"params","type":"tuple"}],"name":"exactInputSingle","outputs":[{"internalType":"uint256","name":"amountOut","type":"uint256"}],"stateMutability":"payable","type":"function"},
                {"inputs":[{"components":[{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint24","name":"fee","type":"uint24"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint256","name":"amountOut","type":"uint256"},{"internalType":"uint256","name":"amountInMaximum","type":"uint256"},{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"}],"internalType":"struct ISwapRouter.ExactOutputSingleParams","name":"params","type":"tuple"}],"name":"exactOutputSingle","outputs":[{"internalType":"uint256","name":"amountIn","type":"uint256"}],"stateMutability":"payable","type":"function"}
            ]''')
            
            router_contract = self.web3.eth.contract(
                address=self.web3.toChecksumAddress(UNISWAP_V3_ROUTER),
                abi=router_abi
            )
            
            # Check if token approval is needed
            allowance = token_contract.functions.allowance(
                self.wallet_address, 
                self.web3.toChecksumAddress(UNISWAP_V3_ROUTER)
            ).call()
            
            logger.info(f"Current allowance: {allowance / (10 ** decimals)} ({allowance} wei)")
            
            if allowance < amount_wei:
                logger.info(f"Approving router to spend {amount} tokens...")
                # Need to approve the router to spend tokens
                approve_tx = token_contract.functions.approve(
                    self.web3.toChecksumAddress(UNISWAP_V3_ROUTER),
                    amount_wei * 2  # Approve more than needed
                ).build_transaction({
                    'from': self.wallet_address,
                    'nonce': self.web3.eth.get_transaction_count(self.wallet_address),
                    'gas': 100000,  # Standard approval gas
                    'gasPrice': self.web3.eth.gas_price,
                    'chainId': self.web3.eth.chain_id
                })
                
                # Sign and send approval transaction
                signed_tx = self.web3.eth.account.sign_transaction(approve_tx, self.wallet_private_key)
                approve_tx_hash = self.web3.eth.send_raw_transaction(signed_tx.rawTransaction)
                
                # Wait for approval
                logger.info(f"Waiting for approval transaction {approve_tx_hash.hex()} to be confirmed...")
                self.web3.eth.wait_for_transaction_receipt(approve_tx_hash)
                logger.info("Approval confirmed!")
            
            # Convert deadline from seconds to timestamp
            deadline_timestamp = int(time.time()) + deadline
            logger.info(f"Using deadline: {deadline_timestamp} (current time + {deadline} seconds)")
            
            # Calculate output with slippage or input with slippage
            current_price = self.get_current_price(live=True)
            
            if is_exact_input:
                # For WETH to USDC
                if not is_token0:  # WETH is token1
                    expected_output = amount_wei * current_price['token1_price'] / (10**6)  # Convert to USDC decimals
                else:  # WETH is token0
                    expected_output = amount_wei * current_price['token0_price'] / (10**6)
                    
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
                    self.wallet_address,     # recipient
                    deadline_timestamp,      # deadline
                    amount_wei,              # amountIn
                    min_output,              # amountOutMinimum
                    0                        # sqrtPriceLimitX96 (0 = no limit)
                )
                
                # Build transaction
                tx = router_contract.functions.exactInputSingle(params).build_transaction({
                    'from': self.wallet_address,
                    'nonce': self.web3.eth.get_transaction_count(self.wallet_address),
                    'gas': 300000,  # Safe starting value
                    'value': 0,     # Not sending ETH directly
                    'chainId': self.web3.eth.chain_id
                })
            else:
                # Exact output logic (similar but for exact output)
                # ... (code omitted for brevity)
                return None  # Not yet implemented
            
            # Try to get gas estimate
            try:
                estimated_gas = self.web3.eth.estimate_gas(tx)
                tx['gas'] = estimated_gas
                logger.info(f"Gas estimation successful: {estimated_gas}")
            except Exception as e:
                logger.error(f"Gas estimation failed: {str(e)}")
                # Continue with default gas if estimation fails
            
            # Sign and send transaction
            signed_tx = self.web3.eth.account.sign_transaction(tx, self.wallet_private_key)
            tx_hash = self.web3.eth.send_raw_transaction(signed_tx.rawTransaction)
            logger.info(f"Swap transaction submitted: {tx_hash.hex()}")
            
            return tx_hash.hex()
            
        except Exception as e:
            logger.error(f"Stack Trace: {traceback.format_exc()}")
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
            
            token_we_are_getting = self.token0_info['address'] if direction == 'sell' else self.token1_info['address']
            is_base_token = token_we_are_getting == self.token0_info['address']
                
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
                tob_size = tob_size / (10 ** self.token0_info['decimals']) / current_price
            else:
                tob_size = tob_size / (10 ** self.token1_info['decimals'])
            
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
        if arb_instrument not in [self.token0_info['symbol'], self.token1_info['symbol']]:
            raise ValueError(f"Invalid instrument: {arb_instrument}, instruemnt not found in pool. (pool: {self.instrument})")
        
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
        tx_hash = await pool_monitor.swap_instrument(0.002, weth_address, is_exact_input=True, slippage=0.1, deadline=600)

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

async def test_price_updates(interval):
    """
    Simple test function to test the balance cache.
    """
    config = Config()
    pool_monitor = PoolMonitor("USDC/ETH", config.infura_ws_url, config.wallet_private_key)
    while True:
        logger.info(f'Base token price: {pool_monitor.get_current_base_price()}')
        logger.info(f'Balance cache: {pool_monitor._balance_cache}')
        logger.info(f'Price cache: {pool_monitor._price_cache}')
        await asyncio.sleep(interval)

async def main():
    """
    Main entry point for the script.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Monitor Uniswap V3 pool activity in real-time')
    parser.add_argument('--pool', type=str, default="USDC/ETH",
                        help=f'Uniswap V3 pool address (default: USDC/ETH)')
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
    
    pool = args.pool
    logger.info(f"Pool: {pool}")
    config = Config()
    
    # Initialize pool monitor with default update settings
    pool_monitor = PoolMonitor("USDC/ETH", config.infura_ws_url, config.wallet_private_key)

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
        await test_price_updates(args.interval)


# Run the main function
if __name__ == "__main__":
    asyncio.run(main())