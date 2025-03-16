import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("liquidity_tracker")

class LiquidityTracker:
    """
    Tracks liquidity positions keyed by (tickLower, tickUpper).
    Also stores the current pool state (tick and sqrtPriceX96) for calculating active liquidity.
    """
    def __init__(self):
        # Key: (tickLower, tickUpper), Value: total liquidity (an integer)
        self.positions = {}
        self.current_tick = None
        self.current_sqrtPriceX96 = None

    def initialize_from_existing_positions(self, existing_positions):
        """
        Initialize the in-memory data structure using existing positions.
        Each position in existing_positions should be a dict with keys:
          - tickLower
          - tickUpper
          - liquidity
        """
        for pos in existing_positions:
            key = (int(pos['tickLower']), int(pos['tickUpper']))
            liquidity = int(pos['liquidity'])
            self.positions[key] = self.positions.get(key, 0) + liquidity
            logger.info(f"Initialized position {key}: liquidity = {self.positions[key]}")

    def add_position(self, tick_lower: int, tick_upper: int, liquidity: int):
        key = (tick_lower, tick_upper)
        self.positions[key] = self.positions.get(key, 0) + liquidity
        logger.info(f"Added liquidity for {key}: new total = {self.positions[key]}")

    def remove_position(self, tick_lower: int, tick_upper: int, liquidity: int):
        key = (tick_lower, tick_upper)
        if key in self.positions:
            self.positions[key] -= liquidity
            logger.info(f"Removed liquidity for {key}: new total = {self.positions[key]}")
            if self.positions[key] <= 0:
                del self.positions[key]
                logger.info(f"Position {key} removed as liquidity is zero or negative.")
        else:
            logger.warning(f"Attempted to remove liquidity from non-existent position {key}.")

    def update_pool_state(self, tick: int, sqrtPriceX96: int):
        self.current_tick = tick
        self.current_sqrtPriceX96 = sqrtPriceX96
        logger.info(f"Updated pool state: tick={tick}, sqrtPriceX96={sqrtPriceX96}")

    def get_active_liquidity(self):
        """
        Computes effective liquidity: sum liquidity for positions that are active at the current tick.
        A position is active if tickLower <= current_tick <= tickUpper.
        """
        if self.current_tick is None:
            logger.warning("Current tick is not set; cannot compute active liquidity.")
            return 0

        active_liquidity = 0
        for (tick_lower, tick_upper), liquidity in self.positions.items():
            if tick_lower <= self.current_tick <= tick_upper:
                active_liquidity += liquidity
        return active_liquidity