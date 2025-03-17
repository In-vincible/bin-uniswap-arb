import asyncio
from typing import Dict, Any

from strategies.base_strategy import BaseStrategy
from exchanges import Binance, Uniswap
from execution_engine import ExecutionEngine
from config import Config
from utils.log_utils import get_logger

logger = get_logger(__name__)

class ArbitrageStrategy(BaseStrategy):
    """
    Arbitrage strategy that identifies and executes trading opportunities
    between Binance and Uniswap.
    
    This strategy monitors price differences between centralized (Binance) and 
    decentralized (Uniswap) exchanges to find profitable arbitrage opportunities,
    validates them, and executes trades when conditions are favorable.
    """
    
    def __init__(self, config: Config):
        """
        Initialize the arbitrage strategy with configuration parameters.
        
        Args:
            config: Dictionary containing strategy configuration parameters
        """
        super().__init__(config)
        
        # Load configuration
        self.instrument_config = config.instrument_config[0]
        self.arb_config = config.arb_config
        
        # Set up instrument parameters
        self.binance_instrument = self.instrument_config['binance_instrument']
        self.uniswap_instrument = self.instrument_config['uniswap_instrument']

        # Initialize exchange connections
        self.binance = Binance(
            config.binance_api_key,
            config.binance_api_secret,
            self.instrument_config['binance_instrument'],
            self.instrument_config['binance_base_asset'],
            self.instrument_config['binance_quote_asset']
        )
        self.uniswap = Uniswap(
            config.infura_url,
            config.infura_ws_url,
            self.instrument_config['pool_address'],
            config.wallet_private_key,
            self.instrument_config['uniswap_base_asset'],
            self.instrument_config['uniswap_quote_asset']
        )
    
    async def initialize(self) -> None:
        """
        Initialize exchange connections and any other required resources.
        
        Returns:
            None
        """
        self.logger.info("Initializing exchanges")
        await self.binance.init()
        await self.uniswap.init()
    
    async def analyze(self) -> Dict[str, Any]:
        """
        Fetch current prices from both exchanges and analyze for arbitrage opportunities.
        
        Returns:
            Dictionary containing analysis results including prices and initial signals
        """
        binance_price = await self.binance.get_base_asset_price()
        uniswap_price = await self.uniswap.get_base_asset_price()
        
        self.logger.info(f"Binance price: {binance_price}, Uniswap price: {uniswap_price}")
        
        # Check if we have valid prices from both exchanges
        if not (binance_price and uniswap_price):
            return {
                "binance_price": binance_price,
                "uniswap_price": uniswap_price,
                "should_execute": False,
                "reason": "Invalid or missing price data"
            }
        
        # Calculate initial price dislocation (in basis points)
        price_difference = abs(uniswap_price - binance_price) / min(uniswap_price, binance_price) * 10_000
        uniswap_trade_direction = self._uniswap_trade_direction(uniswap_price, binance_price)
        
        # Preliminary check based on execution mode and price dislocation
        basic_opportunity = self.is_price_dislocated(uniswap_price, binance_price)
        arb_size = await self.compute_arb_size(uniswap_price, binance_price, uniswap_trade_direction)
        
        return {
            "binance_price": binance_price,
            "uniswap_price": uniswap_price,
            "price_dislocation_bps": round(price_difference, 2),
            "uniswap_trade_direction": uniswap_trade_direction,
            "basic_opportunity": basic_opportunity,
            "should_execute": False,  # Will be set to True after validation if opportunity is valid
            "arb_size": arb_size
        }
    
    async def run_pre_validations(self, analysis_result: Dict[str, Any]) -> bool:
        """
        Run pre-validation checks for the opportunity.
        """
        # Check for capital availability in buy exchange
        buy_exchange = self.uniswap if analysis_result["uniswap_trade_direction"] == "buy" else self.binance
        expected_balance = analysis_result["arb_size"] * await buy_exchange.get_base_asset_price()
        quote_asset_balance = await buy_exchange.get_balance(buy_exchange.quote_asset)
        if quote_asset_balance < expected_balance:
            self.logger.info(f"Insufficient balance in {buy_exchange.quote_asset} on {buy_exchange.__class__.__name__} (current balance: {quote_asset_balance}, expected balance: {expected_balance})")
            return False

        # Preliminary checks for transfers and network conditions/block congestions
        if not await self.binance.pre_validate_transfers(self.binance.base_asset, analysis_result["arb_size"]):
            self.logger.info("Binance transfer validation failed")
            return False
        
        if not await self.uniswap.pre_validate_transfers(self.uniswap.base_asset, analysis_result["arb_size"]):
            self.logger.info("Uniswap transfer validation failed")
            return False
    
    async def verify_profitability_against_costs(self, analysis_result: Dict[str, Any]) -> bool:
        """
        Verify profitability against costs.
        """
        buy_exchange = self.uniswap if analysis_result["uniswap_trade_direction"] == "buy" else self.binance
        sell_exchange = self.binance if analysis_result["uniswap_trade_direction"] == "buy" else self.uniswap

        # Calculate buy costs and resulting size after fees
        buy_costs_bps = await buy_exchange.compute_buy_and_transfer_costs(buy_exchange.base_asset, analysis_result["arb_size"])
        buy_and_transfer_costs = analysis_result["arb_size"] * (buy_costs_bps / 10000)
        size_after_buy = analysis_result["arb_size"] * (1 - (buy_costs_bps / 10000))
        
        # Calculate sell costs on remaining size
        sell_costs_bps = await sell_exchange.compute_sell_costs(sell_exchange.base_asset, size_after_buy)
        sell_costs = size_after_buy * (sell_costs_bps / 10000)
        
        # Calculate final profit in bps after all costs
        buy_accrued = buy_exchange.get_base_asset_price() * analysis_result["arb_size"]
        sell_accrued = sell_exchange.get_base_asset_price() * size_after_buy
        profit = sell_accrued - buy_accrued - buy_and_transfer_costs - sell_costs
        self.logger.info(f"Final expected profit: {profit}")

        if profit/analysis_result["arb_size"] < self.arb_config["min_profit_bps"] / 10_000:
            self.logger.info(f"Profit is too low: {profit/analysis_result['arb_size']}")
            return False    

        return True
    
    async def execute(self, analysis_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the arbitrage opportunity using the execution engine.
        
        Args:
            analysis_result: Validated analysis result containing all 
                            parameters needed for execution
        
        Returns:
            Dictionary containing execution results
        """
        uniswap_price = analysis_result["uniswap_price"]
        binance_price = analysis_result["binance_price"]
        
        # Get parameters for execution
        arb_parameters = await self.get_arb_parameters(uniswap_price, binance_price)
        
        # Execute the arbitrage
        self.logger.info(f"Executing arbitrage: buy on {arb_parameters['buy_exchange'].__class__.__name__}, "
                         f"sell on {arb_parameters['sell_exchange'].__class__.__name__}, "
                         f"size: {arb_parameters['arb_size']} {arb_parameters['arb_instrument']}")
        
        execution_result = await ExecutionEngine.execute_arb(
            arb_parameters['buy_exchange'],
            arb_parameters['sell_exchange'],
            arb_parameters['arb_size'],
            arb_parameters['arb_instrument'],
            arb_parameters['min_rollback_size']
        )
        
        return {
            "success": True,
            "arb_parameters": arb_parameters,
            "execution_details": execution_result
        }
    
    async def get_state(self) -> Dict[str, Any]:
        """
        Get the current state of the arbitrage strategy.
        
        Returns:
            Dictionary containing the current state of the strategy
        """
        # Get states from exchanges
        binance_state = await self.binance.get_state() if hasattr(self.binance, "get_state") else {}
        uniswap_state = await self.uniswap.get_state() if hasattr(self.uniswap, "get_state") else {}
        
        return {
            "is_running": self.is_running,
            "binance_instrument": self.binance_instrument,
            "uniswap_instrument": self.uniswap_instrument,
            "base_token": self.base_token,
            "quote_token": self.quote_token,
            "binance_state": binance_state,
            "uniswap_state": uniswap_state
        }
    
    def is_price_dislocated(self, uniswap_price: float, binance_price: float) -> bool:
        """
        Check if the price difference is profitable based on execution mode.
        
        Args:
            uniswap_price: Current price on Uniswap
            binance_price: Current price on Binance
            
        Returns:
            Boolean indicating whether price dislocation exceeds threshold
        """
        price_difference = abs(uniswap_price - binance_price) / min(uniswap_price, binance_price) * 10_000
        self.logger.info(f"Price dislocation (bps): {round(price_difference, 2)}")
        
        if self.arb_config['execution_mode'] == 'both':
            return price_difference > self.arb_config['min_price_dislocation_bps']
        elif self.arb_config['execution_mode'] == 'binance_to_uniswap':
            return uniswap_price > binance_price * (1 + self.arb_config['min_price_dislocation_bps'] / 10_000)
        elif self.arb_config['execution_mode'] == 'uniswap_to_binance':
            return binance_price > uniswap_price * (1 + self.arb_config['min_price_dislocation_bps'] / 10_000)
        
        return False
    
    async def compute_arb_size(self, uniswap_price: float, binance_price: float, uniswap_trade_direction: str) -> float:
        """
        Compute the optimal size for the arbitrage trade based on available liquidity.
        
        Args:
            uniswap_price: Current price on Uniswap
            binance_price: Current price on Binance
            uniswap_trade_direction: Direction of trade on Uniswap ('buy' or 'sell')
            
        Returns:
            Float representing the optimal size for the arbitrage
        """
        binance_trade_direction = 'buy' if uniswap_trade_direction == 'sell' else 'sell'
        theoretical_binance_max_executible_size = await self.binance.get_max_executible_size(self.binance_instrument, binance_trade_direction)
        theoretical_uniswap_max_executible_size = await self.uniswap.get_max_executible_size(self.uniswap_instrument, uniswap_trade_direction)
        
        self.logger.info(f"Binance max executible size: {theoretical_binance_max_executible_size}, Uniswap max executible size: {theoretical_uniswap_max_executible_size}")
        theoretical_arb_size = min(theoretical_binance_max_executible_size, theoretical_uniswap_max_executible_size)
        scaled_arb_size = theoretical_arb_size * self.arb_config['size_scale_factor']
        final_arb_size = min(scaled_arb_size, self.arb_config['order_size_limit'])
        self.logger.info(f"Computed arb size: {final_arb_size}")
        return final_arb_size
    
    def _uniswap_trade_direction(self, uniswap_price: float, binance_price: float) -> str:
        """
        Determine the direction of trade on Uniswap based on price comparison.
        
        Args:
            uniswap_price: Current price on Uniswap
            binance_price: Current price on Binance
            
        Returns:
            String representing trade direction ('buy' or 'sell')
        """
        if uniswap_price > binance_price:
            return 'sell'
        else:
            return 'buy'
    
    async def get_arb_parameters(self, uniswap_price: float, binance_price: float) -> Dict[str, Any]:
        """
        Get parameters for arbitrage execution.
        
        Args:
            uniswap_price: Current price on Uniswap
            binance_price: Current price on Binance
            
        Returns:
            Dictionary containing parameters for execution
        """
        uniswap_trade_direction = self._uniswap_trade_direction(uniswap_price, binance_price)
        arb_size = await self.compute_arb_size(uniswap_price, binance_price, uniswap_trade_direction)
        
        if uniswap_trade_direction == 'sell':
            return {
                'buy_exchange': self.binance,
                'sell_exchange': self.uniswap,
                'arb_size': arb_size,
                'arb_instrument': self.base_token,
                'min_rollback_size': self.arb_config['min_rollback_order_size']
            }
        else:
            return {
                'buy_exchange': self.uniswap,
                'sell_exchange': self.binance,
                'arb_size': arb_size,
                'arb_instrument': self.base_token,
                'min_rollback_size': self.arb_config['min_rollback_order_size']
            }

    async def cleanup(self) -> None:
        """
        Clean up resources before stopping the strategy.
        
        Returns:
            None
        """
        self.logger.info("Cleaning up arbitrage strategy resources")
        await ExecutionEngine.rollback_trades_and_transfers(self.binance, self.uniswap, self.base_token, self.arb_config['min_rollback_order_size'])

# Example usage
async def main():
    """
    Run the arbitrage strategy as a standalone script.
    """
    config = Config()
    strategy = ArbitrageStrategy(config)
    await strategy.start(interval_seconds=1.0)
    
    try:
        # Keep the main task alive
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("Stopping strategy...")
    finally:
        await strategy.stop()


if __name__ == "__main__":
    asyncio.run(main()) 