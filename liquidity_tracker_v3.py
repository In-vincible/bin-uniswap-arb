import logging
import asyncio
import sys
from typing import Optional, Dict, Tuple, List, Any
from uniswap_subgraph import UniswapSubgraph

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("liquidity_tracker")

class LiquidityTracker:
    """
    Tracks liquidity positions keyed by (tickLower, tickUpper) and owner.
    Also stores the current pool state (tick and sqrtPriceX96) for calculating active liquidity.
    Initializes from Uniswap subgraph data and tracks the last synced block for real-time event subscriptions.
    """
    def __init__(self, current_tick: Optional[int] = None, current_sqrtPriceX96: Optional[int] = None):
        """
        Initialize the LiquidityTracker.
        
        Args:
            current_tick: Current tick value for the pool (optional)
            current_sqrtPriceX96: Current sqrt price X96 value for the pool (optional)
        """
        # Main positions data structure:
        # First key: (tickLower, tickUpper)
        # Second key: owner address
        # Value: liquidity amount (integer)
        self.positions = {}
        
        # Track total liquidity by range for quick lookups
        # Key: (tickLower, tickUpper)
        # Value: total liquidity across all owners
        self.range_totals = {}
        
        # Set current pool state
        self.current_tick = current_tick
        self.current_sqrtPriceX96 = current_sqrtPriceX96
        
        if current_tick is not None:
            logger.info(f"Initialized with current tick: {current_tick}")
        if current_sqrtPriceX96 is not None:
            logger.info(f"Initialized with current sqrtPriceX96: {current_sqrtPriceX96}")
        
        # Track subgraph data freshness and synchronization
        self.last_synced_block = None
        self.last_synced_block_timestamp = None
        self.pool_id = None
        self._subgraph = None
    
    async def init_from_subgraph(self, pool_id: str, min_liquidity: int = 0, api_key: str = "232256ca200587119cbca5c3583dc5fb") -> bool:
        """
        Initialize the LiquidityTracker with a connection to the subgraph and pool data.
        This method encapsulates both connection and data initialization.
        
        Args:
            pool_id: The Uniswap pool ID (contract address)
            min_liquidity: Minimum liquidity threshold for positions
            api_key: The Graph API key for the gateway
            
        Returns:
            True if initialization was successful, False otherwise
            
        Raises:
            ConnectionError: If subgraph connection fails
        """
        logger.info(f"Initializing LiquidityTracker for pool {pool_id}...")
        
        try:
            # Step 1: Connect to the subgraph
            await self.connect_to_subgraph(api_key)
            
            # Step 2: Initialize data from the subgraph
            success = await self.initialize_from_subgraph(pool_id, min_liquidity)
            
            if not success:
                logger.error(f"Failed to initialize from subgraph for pool {pool_id}")
                return False
                
            logger.info(f"Successfully initialized LiquidityTracker for pool {pool_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error during LiquidityTracker initialization: {str(e)}")
            # Clean up resources if initialization fails
            await self.close()
            raise

    async def connect_to_subgraph(self, api_key: str = "232256ca200587119cbca5c3583dc5fb") -> None:
        """
        Initialize the connection to the Uniswap subgraph.
        
        Args:
            api_key: The Graph API key for the gateway
        """
        self._subgraph = UniswapSubgraph(api_key=api_key)
        
        # Test connection by retrieving the latest block
        latest_block = await self._subgraph.get_latest_block()
        if not latest_block:
            raise ConnectionError("Failed to connect to Uniswap subgraph. Check API key and network connection.")
        
        logger.info(f"Connected to Uniswap subgraph. Latest block: {self._subgraph.format_block_time(latest_block)}")

    async def initialize_from_subgraph(self, pool_id: str, min_liquidity: int = 0) -> bool:
        """
        Initialize the tracker with positions from the Uniswap subgraph.
        
        Args:
            pool_id: The Uniswap pool ID (contract address)
            min_liquidity: Minimum liquidity threshold for positions
            
        Returns:
            True if initialization was successful, False otherwise
        """
        if not self._subgraph:
            logger.error("Subgraph client not initialized. Call connect_to_subgraph first.")
            return False
            
        self.pool_id = pool_id
        
        # Check if pool exists
        pool_exists = await self._subgraph.check_pool_exists(pool_id)
        if not pool_exists:
            logger.error(f"Pool {pool_id} does not exist in the subgraph")
            return False
            
        # Fetch pool details to get current state
        pool_result = await self._subgraph.get_pool_details(pool_id)
        pool_data = pool_result["pool"]
        
        if not pool_data:
            logger.error(f"Failed to fetch details for pool {pool_id}")
            return False
            
        logger.info(f"Fetching positions for pool {pool_id} (minimum liquidity: {min_liquidity})...")
        
        # Fetch all positions for the pool
        result = await self._subgraph.get_pool_positions(
            pool_id, 
            min_liquidity=min_liquidity
        )
        
        positions = result["positions"]
        block_data = result["block"]
        
        # Store the block information for later use with real-time events
        if block_data and "number" in block_data:
            self.last_synced_block = int(block_data["number"])
            if "timestamp" in block_data:
                self.last_synced_block_timestamp = int(block_data["timestamp"])
            logger.info(f"Positions synced from {self._subgraph.format_block_time(block_data)}")
        
        # Initialize positions from subgraph data
        self.initialize_from_existing_positions(positions)
        
        logger.info(f"Successfully initialized {len(self.positions)} position ranges from the subgraph")
        return True

    def initialize_from_existing_positions(self, existing_positions):
        """
        Initialize the in-memory data structure using existing positions.
        Each position in existing_positions should be a dict with keys:
          - tickLower
          - tickUpper
          - liquidity
          - owner (if available)
        """
        # Clear any existing positions
        self.positions = {}
        self.range_totals = {}
        
        for pos in existing_positions:
            tick_lower = int(pos['tickLower']['tickIdx'])
            tick_upper = int(pos['tickUpper']['tickIdx'])
            key = (tick_lower, tick_upper)
            liquidity = int(pos['liquidity'])
            
            # Use position owner if available, otherwise use a default owner
            owner = pos.get('owner', 'unknown_owner')
            
            # Ensure nested dictionaries exist
            if key not in self.positions:
                self.positions[key] = {}
            
            # Add liquidity for this owner
            self.positions[key][owner] = self.positions[key].get(owner, 0) + liquidity
            
            # Update range totals
            self.range_totals[key] = self.range_totals.get(key, 0) + liquidity
            
            logger.debug(f"Initialized position: ({tick_lower}, {tick_upper}) with liquidity {liquidity} for owner {owner[:8]}...")
        
        logger.info(f"Initialized {len(self.positions)} unique position ranges")
        logger.info(f"Total unique owner-range combinations: {sum(len(owners) for owners in self.positions.values())}")

    def add_position(self, tick_lower: int, tick_upper: int, liquidity: int, owner: str = 'unknown_owner'):
        """
        Add liquidity to a position range for a specific owner.
        
        Args:
            tick_lower: Lower tick boundary
            tick_upper: Upper tick boundary
            liquidity: Amount of liquidity to add
            owner: Address of the position owner
        """
        key = (tick_lower, tick_upper)
        
        # Ensure nested dict exists
        if key not in self.positions:
            self.positions[key] = {}
        
        # Add liquidity for this owner
        self.positions[key][owner] = self.positions[key].get(owner, 0) + liquidity
        
        # Update range totals
        self.range_totals[key] = self.range_totals.get(key, 0) + liquidity
        
        logger.info(f"Added liquidity for {key} owner {owner[:8]}...: new owner total = {self.positions[key][owner]}")
        logger.info(f"Range total liquidity: {self.range_totals[key]}")

    def remove_position(self, tick_lower: int, tick_upper: int, liquidity: int, owner: str = 'unknown_owner'):
        """
        Remove liquidity from a position range for a specific owner.
        
        Args:
            tick_lower: Lower tick boundary
            tick_upper: Upper tick boundary
            liquidity: Amount of liquidity to remove
            owner: Address of the position owner
        """
        key = (tick_lower, tick_upper)
        
        if key not in self.positions:
            logger.warning(f"Attempted to remove liquidity from non-existent position range {key}.")
            return
            
        if owner not in self.positions[key]:
            logger.warning(f"Attempted to remove liquidity from non-existent owner {owner[:8]}... for position {key}.")
            return
        
        # Remove liquidity for this owner
        self.positions[key][owner] -= liquidity
        
        # Update range total
        self.range_totals[key] -= liquidity
        
        logger.info(f"Removed liquidity for {key} owner {owner[:8]}...: new owner total = {self.positions[key][owner]}")
        logger.info(f"Range total liquidity: {self.range_totals[key]}")
        
        # Clean up if no liquidity left for this owner
        if self.positions[key][owner] <= 0:
            del self.positions[key][owner]
            logger.info(f"Owner {owner[:8]}... removed from position {key} as their liquidity is zero or negative.")
            
        # Clean up if no owners left for this range
        if not self.positions[key]:
            del self.positions[key]
            del self.range_totals[key]
            logger.info(f"Position range {key} removed as it has no owners left.")

    def update_pool_state(self, tick: int, sqrtPriceX96: int):
        """
        Update the current pool state.
        
        Args:
            tick: Current tick
            sqrtPriceX96: Current sqrtPriceX96
        """
        self.current_tick = tick
        self.current_sqrtPriceX96 = sqrtPriceX96
        logger.info(f"Updated pool state: tick={tick}, sqrtPriceX96={sqrtPriceX96}")

    def get_active_liquidity(self) -> int:
        """
        Computes effective liquidity: sum liquidity for positions that are active at the current tick.
        A position is active if tickLower <= current_tick <= tickUpper.
        
        Returns:
            Total active liquidity at the current tick
        """
        if self.current_tick is None:
            logger.warning("Current tick is not set; cannot compute active liquidity.")
            return 0

        active_liquidity = 0
        for (tick_lower, tick_upper), total_liquidity in self.range_totals.items():
            if tick_lower <= self.current_tick <= tick_upper:
                active_liquidity += total_liquidity
                logger.debug(f"Active position: ({tick_lower}, {tick_upper}) with liquidity {total_liquidity}")
        
        logger.info(f"Total active liquidity at tick {self.current_tick}: {active_liquidity}")
        return active_liquidity
    
    def get_position_liquidity(self, tick_lower: int, tick_upper: int, owner: Optional[str] = None) -> int:
        """
        Get liquidity for a specific position range, optionally filtered by owner.
        
        Args:
            tick_lower: Lower tick boundary
            tick_upper: Upper tick boundary
            owner: Optional owner address to filter by
            
        Returns:
            Total liquidity for the position range (and owner if specified)
        """
        key = (tick_lower, tick_upper)
        
        if key not in self.positions:
            return 0
            
        if owner is not None:
            # Return liquidity for specific owner
            return self.positions[key].get(owner, 0)
        else:
            # Return total liquidity for this range
            return self.range_totals[key]
    
    def simulate_swap(self, amount_in_token0: float, direction: str = 'upward') -> Tuple[float, int]:
        """
        Simulate a swap across multiple ticks using a tick-by-tick approach.
        
        Args:
            amount_in_token0: Amount of token0 to swap (in token0 units)
            direction: 'upward' for selling token1 to buy token0, 'downward' for the opposite
            
        Returns:
            Tuple of (total_dy_out, ending_tick) where:
            - total_dy_out: Total amount of token1 received (in token1 units)
            - ending_tick: The final tick after the swap
        """
        if self.current_tick is None or self.current_sqrtPriceX96 is None:
            logger.warning("Current tick or sqrtPriceX96 is not set; cannot simulate swap.")
            return 0.0, 0

        amount_remaining = amount_in_token0
        sqrtP = self.current_sqrtPriceX96
        L = self.get_active_liquidity()
        tick = self.current_tick
        total_dy_out = 0.0

        while amount_remaining > 0:
            sqrtP_next = self.sqrt_price_at_next_tick(tick + 1)
            dx_max = L * (1/sqrtP - 1/sqrtP_next)

            if amount_remaining < dx_max:
                sqrtP_new = 1 / (1/sqrtP - amount_remaining / L)
                dy = L * (sqrtP_new - sqrtP)
                sqrtP = sqrtP_new
                amount_remaining = 0
            else:
                dy = L * (sqrtP_next - sqrtP)
                amount_remaining -= dx_max
                sqrtP = sqrtP_next
                tick += 1
                L += self.tick_liquidity_delta(tick)

            total_dy_out += dy

        logger.info(f"Completed swap: total_dy_out={total_dy_out}, ending_tick={tick}")
        return total_dy_out, tick

    def sqrt_price_at_next_tick(self, tick: int) -> float:
        """
        Calculate the sqrt price at the next tick.
        
        Args:
            tick: The tick for which to calculate the sqrt price
            
        Returns:
            The sqrt price at the given tick
        """
        return 1.0001 ** (tick / 2)

    def tick_liquidity_delta(self, tick: int) -> float:
        """
        Calculate the change in liquidity at a given tick.
        
        Args:
            tick: The tick for which to calculate the liquidity change
            
        Returns:
            The change in liquidity at the given tick
        """
        # Placeholder implementation, should be replaced with actual logic
        return 0.0
    
    def calculate_effective_price(self, token0_amount: float, token1_amount: float) -> Tuple[float, float]:
        """
        Calculate the effective price based on token amounts.
        
        Args:
            token0_amount: Amount of token0 (in token0 units)
            token1_amount: Amount of token1 (in token1 units)
            
        Returns:
            Tuple of (token0_price, token1_price) where:
            - token0_price: Price of token0 in terms of token1 (token1/token0)
            - token1_price: Price of token1 in terms of token0 (token0/token1)
        """
        if token0_amount <= 0 or token1_amount <= 0:
            return 0.0, 0.0
            
        token0_price = token1_amount / token0_amount  # Price of token0 in terms of token1
        token1_price = token0_amount / token1_amount  # Price of token1 in terms of token0
        
        return token0_price, token1_price
    
    def get_sync_block_info(self) -> Dict[str, Any]:
        """
        Get information about the last synchronized block.
        Useful for continuing real-time event subscriptions from this point.
        
        Returns:
            Dictionary with block number and timestamp
        """
        return {
            "number": self.last_synced_block,
            "timestamp": self.last_synced_block_timestamp,
            "pool_id": self.pool_id
        }
    
    async def close(self):
        """
        Close the subgraph connection and clean up resources.
        """
        if self._subgraph:
            await self._subgraph.close()
            logger.info("Closed subgraph connection")

    def get_liquidity_by_tick(self, tick: int) -> int:
        """
        Get the total liquidity for a specific tick.
        
        Args:
            tick: The tick for which to get the total liquidity.
            
        Returns:
            Total liquidity at the specified tick.
        """
        total_liquidity = 0
        for (tick_lower, tick_upper), liquidity in self.range_totals.items():
            if tick_lower <= tick <= tick_upper:
                total_liquidity += liquidity
        return total_liquidity

    def get_liquidity_by_tick_range(self, tick_lower: int, tick_upper: int) -> int:
        """
        Get the total liquidity for a specific tick range.
        
        Args:
            tick_lower: The lower bound of the tick range.
            tick_upper: The upper bound of the tick range.
            
        Returns:
            Total liquidity within the specified tick range.
        """
        total_liquidity = 0
        for (range_lower, range_upper), liquidity in self.range_totals.items():
            if range_lower <= tick_upper and range_upper >= tick_lower:
                total_liquidity += liquidity
        return total_liquidity


# Example usage
async def main():
    # Initialize the tracker with current tick and sqrtPriceX96
    example_tick = -200000  # Just an example value
    example_sqrtPriceX96 = 1234567890  # Just a placeholder
    
    # Create tracker with initial state
    tracker = LiquidityTracker(current_tick=example_tick, current_sqrtPriceX96=example_sqrtPriceX96)
    
    try:
        # Initialize from subgraph
        pool_id = "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8"  # USDC/ETH 0.3%
        success = await tracker.init_from_subgraph(pool_id, min_liquidity=1000000)
        
        if success:
            # Calculate active liquidity based on the current tick
            active_liquidity = tracker.get_active_liquidity()
            
            # Get information about the block we're synced to
            sync_info = tracker.get_sync_block_info()
            logger.info(f"Synced to block #{sync_info['number']}")
        else:
            logger.error("Failed to initialize from subgraph")
            sys.exit(1)
        
    except ConnectionError as e:
        logger.error(f"Connection error: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)
    finally:
        # Always close the connection
        await tracker.close()


if __name__ == "__main__":
    asyncio.run(main())