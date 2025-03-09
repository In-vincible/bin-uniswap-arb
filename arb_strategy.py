import asyncio
from binance_connector import Binance
from blocknative_simulator import BlocknativeSimulator
from uniswap_connector import PoolMonitor
from config import Config
from token_monitoring import TokenMonitor

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
        self.instrument = instrument_config['instrument']
        self.base_token = instrument_config['base_token']
        self.quote_token = instrument_config['quote_token']
        self.base_token_address = instrument_config['base_token_address']
        self.quote_token_address = instrument_config['quote_token_address']
        self.arb_config = config.arb_config
        self.binance = Binance(config.binance_api_key, config.binance_api_secret, [instrument_config['instrument']])
        self.uniswap = PoolMonitor(instrument_config['pool_address'], config.infura_url, config.wallet_private_key)
        self.token_monitor = TokenMonitor([instrument_config['base_token_address'], instrument_config['quote_token_address']], config.infura_url)
        self.blocknative_simulator = BlocknativeSimulator(config.blocknative_api_key)
    
    def is_profitable(self, uniswap_price: float, binance_price: float):
        """
        Check if the price difference is profitable.
        """
        if self.arb_config['execution_mode'] == 'both':
            price_difference = abs(uniswap_price - binance_price) / min(uniswap_price, binance_price)
            return price_difference > self.arb_config['min_profit_threshold']
        elif self.arb_config['execution_mode'] == 'binance_to_uniswap':
            return uniswap_price > binance_price * (1 + self.arb_config['min_profit_threshold'] / 100)
        elif self.arb_config['execution_mode'] == 'uniswap_to_binance':
            return binance_price > uniswap_price * (1 + self.arb_config['min_profit_threshold'] / 100)
    
    async def compute_arb_size(self, uniswap_price: float, binance_price: float, uniswap_trade_direction: str):
        binance_tob_size = await self.binance.get_tob_size(self.instrument)
        uniswap_tob_size = await self.uniswap.get_tob_size('sell')
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
        if not self.is_profitable(uniswap_price, binance_price):
            return False
        
        uniswap_trade_direction = self._uniswap_trade_direction(uniswap_price, binance_price)
        arb_size = await self.compute_arb_size(uniswap_price, binance_price, uniswap_trade_direction)

        # Here we verify network congestion/gas costs/slippage thresholds/possible transfer issues with network and token
        expected_profit = arb_size * (uniswap_price - binance_price)
        opportunity = self.token_monitor.validate_arbitrage_opportunity(
                        token_address=self.base_token_address,
                        amount=arb_size,
                        expected_profit=expected_profit
                    )
        if not opportunity['is_valid']:
            return False
        
        # Binance transfer check
        if not await self._verify_binance_transfer_open(uniswap_price, binance_price):
            return False
        
        # Simulate the transaction
        if uniswap_trade_direction == 'sell':
            tx_data = {
                "from": self.base_token_address,
                "to": self.wallet_address,
                "value": arb_size,
                "gas": self.arb_config['gas_fee_limit'],
                "gasPrice": self.arb_config['gas_fee_limit']
            }
        else:
            tx_data = {
                "from": self.wallet_address,
                "to": self.base_token_address,
                "value": arb_size,
                "gas": self.arb_config['gas_fee_limit'],
                "gasPrice": self.token_monitor.current_gas_price
            }
        simulation_result = self.blocknative_simulator.simulate_transaction(tx_data)
        simulated_profit = None
        if uniswap_trade_direction == 'sell':
            simulated_profit = (simulation_result['estimated_price'] - expected_profit) / min(simulation_result['estimated_price'], expected_profit)
        else:
            simulated_profit = (expected_profit - simulation_result['estimated_price']) / min(simulation_result['estimated_price'], expected_profit)
        if simulated_profit < self.arb_config['min_profit_threshold']:
            return False
        
        return True
        

    async def monitor_prices(self):
        """
        Monitor prices on Binance and Uniswap and detect arbitrage opportunities.
        """
        await self.binance.init()
        await self.uniswap.start_background_updates()
        await self.token_monitor.start_monitoring()

        while True:
            binance_price = self.binance.get_latest_price('ETHUSDT')
            uniswap_price = self.uniswap.get_current_price()

            if binance_price and uniswap_price:
                if await self.validate_arbitrage_opportunity(uniswap_price, binance_price):
                    print("Arbitrage opportunity detected: Buy on Binance, Sell on Uniswap")

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
    strategy = ArbitrageStrategy('ETHUSDT', 'uniswap_pool_address', ['token_address'])
    await strategy.monitor_prices()

if __name__ == "__main__":
    asyncio.run(main())
