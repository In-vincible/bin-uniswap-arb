import asyncio
from typing import Dict, Any

from strategies.base_strategy import BaseStrategy
from exchanges import Binance, Uniswap
from execution_engine import ExecutionEngine
from config import Config
import logging

logging.basicConfig(level=logging.INFO)


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
        
        # Set up trading parameters
        self.binance_instrument = self.instrument_config['binance_instrument']
        self.uniswap_instrument = self.instrument_config['uniswap_instrument']
        self.base_token = self.instrument_config['base_token']
        self.quote_token = self.instrument_config['quote_token']
        self.base_token_address = self.instrument_config['base_token_address']
        self.quote_token_address = self.instrument_config['quote_token_address']
        
        # Initialize exchange connections
        self.binance = Binance(
            config.binance_api_key,
            config.binance_api_secret,
            self.instrument_config['binance_instrument']
        )
        self.uniswap = Uniswap(
            config.infura_url,
            config.infura_ws_url,
            self.instrument_config['pool_address'],
            config.wallet_private_key
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
        binance_price = await self.binance.get_current_price(self.base_token)
        uniswap_price = await self.uniswap.get_current_price(self.base_token)
        
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
        
        return {
            "binance_price": binance_price,
            "uniswap_price": uniswap_price,
            "price_dislocation_bps": round(price_difference, 2),
            "uniswap_trade_direction": uniswap_trade_direction,
            "basic_opportunity": basic_opportunity,
            "should_execute": False  # Will be set to True after validation if opportunity is valid
        }
    
    async def validate_opportunity(self, analysis_result: Dict[str, Any]) -> bool:
        """
        Validate a potential arbitrage opportunity through multiple checks.
        
        Args:
            analysis_result: Output from the analyze method containing prices
                            and initial opportunity assessment
                            
        Returns:
            Boolean indicating whether the opportunity is valid and profitable
        """
        # Skip validation if basic opportunity check failed
        if not analysis_result.get("basic_opportunity", False):
            return False
        
        uniswap_price = analysis_result["uniswap_price"]
        binance_price = analysis_result["binance_price"]
        uniswap_trade_direction = analysis_result["uniswap_trade_direction"]
        
        # Calculate optimal arbitrage size
        arb_size = await self.compute_arb_size(uniswap_price, binance_price, uniswap_trade_direction)
        if arb_size == 0:
            self.logger.info("No arbitrage size found")
            return False
        
        # Store arb_size in analysis result for later use in execute
        analysis_result["arb_size"] = arb_size
        
        # Validate network conditions, gas costs, and slippage
        expected_profit = arb_size * abs(uniswap_price - binance_price)
        analysis_result["expected_profit"] = expected_profit
        
        if not await self._validate_network_congestion_gas_costs_and_possible_slippage(
            arb_size, uniswap_trade_direction, expected_profit
        ):
            self.logger.info("Gas costs or slippage validation failed")
            return False
        
        # Verify Binance transfer availability
        if not await self._verify_binance_transfer_open(uniswap_price, binance_price):
            self.logger.info("Binance transfer check failed")
            return False
        
        # Simulate the transaction
        if not await self._simulate_transaction(arb_size, uniswap_trade_direction, expected_profit):
            self.logger.info("Transaction simulation validation failed")
            return False
        
        # If all validations pass, this is a valid opportunity
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
    
    async def cleanup(self) -> None:
        """
        Clean up resources before stopping the strategy.
        
        Returns:
            None
        """
        self.logger.info("Cleaning up arbitrage strategy resources")
        await ExecutionEngine.rollback_trades_and_transfers(self.binance, self.uniswap, self.base_token, self.arb_config['min_rollback_order_size'])
    
    
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
        binance_tob_size = await self.binance.get_tob_size(self.binance_instrument, binance_trade_direction)
        uniswap_tob_size = await self.uniswap.get_tob_size(uniswap_trade_direction)
        
        self.logger.info(f"Binance TOB size: {binance_tob_size}, Uniswap TOB size: {uniswap_tob_size}")
        
        exchange_size = min(binance_tob_size, uniswap_tob_size)
        arb_size = min(exchange_size, self.arb_config['order_size_limit'])
        
        return arb_size
    
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
    
    def _binance_transfer_direction(self, uniswap_price: float, binance_price: float) -> str:
        """
        Determine the direction of token transfer on Binance based on price comparison.
        
        Args:
            uniswap_price: Current price on Uniswap
            binance_price: Current price on Binance
            
        Returns:
            String representing transfer direction ('deposit' or 'withdraw')
        """
        if uniswap_price > binance_price:
            return 'deposit'
        else:
            return 'withdraw'
    
    async def _verify_binance_transfer_open(self, uniswap_price: float, binance_price: float) -> bool:
        """
        Verify that token transfer is open on Binance for the trade direction.
        
        Args:
            uniswap_price: Current price on Uniswap
            binance_price: Current price on Binance
            
        Returns:
            Boolean indicating whether token transfer is open
        """
        transfer_direction = self._binance_transfer_direction(uniswap_price, binance_price)
        if transfer_direction == 'deposit':
            return await self.binance.verify_deposit_open(self.base_token)
        else:
            return await self.binance.verify_withdrawal_open(self.base_token)
    
    async def _validate_network_congestion_gas_costs_and_possible_slippage(
        self, arb_size: float, uniswap_trade_direction: str, expected_profit: float
    ) -> bool:
        """
        Validate network conditions for trade execution.
        
        Args:
            arb_size: Size of the arbitrage trade
            uniswap_trade_direction: Direction of trade on Uniswap
            expected_profit: Expected profit from the trade
            
        Returns:
            Boolean indicating whether network conditions are favorable
        """
        # In the original implementation, this is a placeholder
        # In a real implementation, this would check gas prices, network congestion, etc.
        return True
    
    async def _simulate_transaction(
        self, arb_size: float, uniswap_trade_direction: str, expected_profit: float
    ) -> bool:
        """
        Simulate the transaction to verify profitability after costs.
        
        Args:
            arb_size: Size of the arbitrage trade
            uniswap_trade_direction: Direction of trade on Uniswap
            expected_profit: Expected profit from the trade
            
        Returns:
            Boolean indicating whether the simulated transaction is profitable
        """
        # In the original implementation, this is a placeholder
        # In a real implementation, this would use a service like Blocknative
        # to simulate the transaction and verify profitability
        return True
    
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