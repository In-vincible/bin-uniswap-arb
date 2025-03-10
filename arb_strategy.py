import asyncio
from binance_connector import Binance
from blocknative_simulator import BlocknativeSimulator
from execution_engine import ExecutionEngine
from uniswap_connector import PoolMonitor
from config import Config
from token_monitoring import TokenMonitor, Transaction
import logging

logger = logging.getLogger(__name__)

class ArbitrageStrategy:
    def __init__(self):
        """
        Initialize the arbitrage strategy with Binance and Uniswap connectors.

        Args:
            instrument (str): Trading instrument (e.g., 'ETHUSDT')
            pool_address (str): Address of the Uniswap pool
            token_addresses (list): List of token addresses to monitor
        """
        config = Config()
        instrument_config = config.instrument_config[0]
        self.binance_instrument = instrument_config['binance_instrument']
        self.uniswap_instrument = instrument_config['uniswap_instrument']
        self.base_token = instrument_config['base_token']
        self.quote_token = instrument_config['quote_token']
        self.base_token_address = instrument_config['base_token_address']
        self.quote_token_address = instrument_config['quote_token_address']
        self.arb_config = config.arb_config
        self.binance = Binance(config.binance_api_key, config.binance_api_secret, [instrument_config['binance_instrument']])
        self.uniswap = PoolMonitor(instrument_config['uniswap_instrument'], config.infura_ws_url, config.wallet_private_key)
        self.token_monitor = TokenMonitor([instrument_config['base_token_address'], instrument_config['quote_token_address']], config.infura_url)
        self.blocknative_simulator = BlocknativeSimulator(config.blocknative_api_key)
    
    def is_price_dislocated(self, uniswap_price: float, binance_price: float):
        """
        Check if the price difference is profitable.
        """
        price_difference = abs(uniswap_price - binance_price) / min(uniswap_price, binance_price) * 10_000
        logger.info(f"Price dislocation (bps): {round(price_difference, 2)}")
        if self.arb_config['execution_mode'] == 'both':
            return price_difference > self.arb_config['min_price_dislocation_bps']
        elif self.arb_config['execution_mode'] == 'binance_to_uniswap':
            return uniswap_price > binance_price * (1 + self.arb_config['min_price_dislocation_bps'] / 100)
        elif self.arb_config['execution_mode'] == 'uniswap_to_binance':
            return binance_price > uniswap_price * (1 + self.arb_config['min_price_dislocation_bps'] / 100)
    
    async def compute_arb_size(self, uniswap_price: float, binance_price: float, uniswap_trade_direction: str):
        binance_trade_direction = 'buy' if uniswap_trade_direction == 'sell' else 'sell'
        binance_tob_size = await self.binance.get_tob_size(self.binance_instrument, binance_trade_direction)
        uniswap_tob_size = await self.uniswap.get_tob_size(uniswap_trade_direction)
        logger.info(f"Binance TOB size: {binance_tob_size}, Uniswap TOB size: {uniswap_tob_size}")
        exchange_size = min(binance_tob_size, uniswap_tob_size)
        arb_size = min(exchange_size, self.arb_config['order_size_limit'])
        return arb_size
    
    def _uniswap_trade_direction(self, uniswap_price: float, binance_price: float):
        if uniswap_price > binance_price:
            return 'sell'
        else:
            return 'buy'
    
    def _binance_transfer_direction(self, uniswap_price: float, binance_price: float):
        if uniswap_price > binance_price:
            return 'deposit'
        else:
            return 'withdraw'
    
    async def _verify_binance_transfer_open(self, uniswap_price: float, binance_price: float):
        transfer_direction = self._binance_transfer_direction(uniswap_price, binance_price)
        if transfer_direction == 'deposit':
            return await self.binance.verify_deposit_open(self.base_token)
        else:
            return await self.binance.verify_withdrawal_open(self.base_token)

    async def _compute_expected_transfer_tx(self, arb_size: float, uniswap_trade_direction: str):
        from_address = self.uniswap.wallet_address
        to_address = await self.binance.get_deposit_address(self.base_token)
        logger.info(f"uniswap wallet address: {from_address}, binance deposit address: {to_address}")
        if uniswap_trade_direction == 'buy':
            from_address, to_address = to_address, from_address
        tx = Transaction(
                            from_address=from_address,
                            to_address=to_address,
                            value=arb_size,
                            data="0x"  # Empty data for this example
                        )
        return tx
    
    async def _validate_network_congestion_gas_costs_and_possible_slippage(self, arb_size: float, uniswap_trade_direction: str, expected_profit: float):
        if self.arb_config['disable_network_level_validations']:
            logger.info("Network level validations disabled")
            return True
        
        tx = await self._compute_expected_transfer_tx(arb_size, uniswap_trade_direction)
        opportunity = await self.token_monitor.validate_arbitrage_opportunity(
                        token_address=self.base_token_address,
                        amount=arb_size,
                        gas_limit=self.arb_config['gas_fee_limit'],
                        expected_profit=expected_profit,
                        max_wait_time=self.arb_config['max_transfer_time_seconds'],
                        transaction=tx
                    )
        logger.info(f"Token monitor validation: {opportunity}")
        if not opportunity['valid']:
            logger.info(f"Token monitor validation failed, reason: {opportunity['reason']}")
            return False
        
        logger.info(f"Arbitrage opportunity valid! Use gas price: {opportunity['gas_price']} wei")
        logger.info(f"Expected profit after gas: {opportunity['profit_after_gas']} wei")
        logger.info(f"Estimated wait time: {opportunity['estimated_time']} seconds")
        return True
    
    async def _simulate_transaction(self, arb_size: float, uniswap_trade_direction: str, expected_profit: float):
        if self.arb_config['disable_blocknative_simulation']:
            return True
        
        # Simulate the transaction
        if uniswap_trade_direction == 'sell':
            tx_data = {
                "from": self.base_token_address,
                "to": self.uniswap.wallet_address,
                "value": arb_size,
                "gas": self.arb_config['gas_fee_limit'],
                "gasPrice": self.arb_config['gas_fee_limit']
            }
        else:
            tx_data = {
                "from": self.uniswap.wallet_address,
                "to": self.base_token_address,
                "value": arb_size,
                "gas": self.arb_config['gas_fee_limit'],
                "gasPrice": self.token_monitor.current_gas_price
            }
        simulation_result = self.blocknative_simulator.simulate_transaction(tx_data)
        simulated_transaction_cost = simulation_result['total_estimated_cost']
        base_token_decimals = await self.uniswap.get_base_token_decimals()
        trade_size_wei = arb_size * (10**base_token_decimals)
        transaction_cost_bps = simulated_transaction_cost / trade_size_wei * 10_000
        logger.info(f'base token decimals: {base_token_decimals}, trade size wei: {trade_size_wei}, transaction cost: {simulated_transaction_cost}')
        if transaction_cost_bps > self.arb_config['max_transaction_cost_bps']:
            logger.info(f"Transaction cost (bps): {transaction_cost_bps} above threshold: {self.arb_config['max_transaction_cost_bps']}")
            return False
        
        return True
        
    
    async def validate_arbitrage_opportunity(self, uniswap_price: float, binance_price: float):
        """
        Validate an arbitrage opportunity by checking multiple conditions.

        Steps:
        1. Check if the price difference exceeds minimum profit threshold based on execution mode
        2. Calculate optimal arbitrage size considering both exchange liquidity limits
        3. Validate token-specific metrics (gas costs, network congestion, etc.)
        4. Verify Binance deposit/withdrawal status for the base token
        5. Simulate the transaction using Blocknative to verify profitability
           after gas costs, slippage and transfer fees

        Args:
            uniswap_price (float): Current price on Uniswap
            binance_price (float): Current price on Binance

        Returns:
            bool: True if arbitrage opportunity is valid and profitable,
                 False otherwise
        """
        # Fundamental check to skip unnecessary checks
        if not self.is_price_dislocated(uniswap_price, binance_price):
            return False
        
        uniswap_trade_direction = self._uniswap_trade_direction(uniswap_price, binance_price)
        arb_size = await self.compute_arb_size(uniswap_price, binance_price, uniswap_trade_direction)

        if arb_size == 0:
            logger.info("No arbitrage size found")
            return False
        
        # Here we verify network congestion/gas costs/slippage thresholds/possible transfer issues with network and token
        expected_profit = arb_size * (uniswap_price - binance_price)
        if not await self._validate_network_congestion_gas_costs_and_possible_slippage(arb_size, uniswap_trade_direction, expected_profit):
            logger.info("Gas costs or slippage validation failed")
            return False
        
        # Binance transfer check
        if not await self._verify_binance_transfer_open(uniswap_price, binance_price):
            logger.info("Binance transfer check failed")
            return False
        
        # Simulate the transaction
        if not await self._simulate_transaction(arb_size, uniswap_trade_direction, expected_profit):
            logger.info("Transaction simulation validation failed")
            return False
        
        return True
    
    async def get_arb_parameters(self, uniswap_price: float, binance_price: float):
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
    
    async def execute_arb(self, binance_price: float, uniswap_price: float):
        arb_parameters = await self.get_arb_parameters(binance_price, uniswap_price)
        await ExecutionEngine.execute_arb(arb_parameters['buy_exchange'], arb_parameters['sell_exchange'], arb_parameters['arb_size'], arb_parameters['arb_instrument'], arb_parameters['min_rollback_size'])
    
    async def monitor_prices(self):
        """
        Monitor prices on Binance and Uniswap and detect arbitrage opportunities.
        """
        await self.binance.init()
        await self.token_monitor.start_monitoring()

        while True:
            binance_price = self.binance.get_current_base_price(self.binance_instrument)
            uniswap_price = self.uniswap.get_current_base_price()
            logger.info(f"Binance price: {binance_price}, Uniswap price: {uniswap_price}")
            if binance_price and uniswap_price:
                if await self.validate_arbitrage_opportunity(uniswap_price, binance_price):
                    await self.execute_arb(binance_price, uniswap_price)

            await asyncio.sleep(1)  # Adjust the frequency as needed

    async def close(self):
        """
        Close connections to Binance and Uniswap.
        """
        await self.binance.close()
        await self.uniswap.stop_background_updates()
        await self.token_monitor.stop_monitoring()

# Example usage
async def main():
    strategy = ArbitrageStrategy()
    await strategy.monitor_prices()

if __name__ == "__main__":
    asyncio.run(main())
