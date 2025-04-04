import asyncio
import logging
import math
import time
import traceback
from typing import Any, Dict
from binance import AsyncClient, BinanceSocketManager
from config import Config
from exchanges.base_connector import BaseExchange

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("binance_connector")

class Binance(BaseExchange):
    def __init__(self, api_key: str, secret_key: str, instrument: str, base_asset: str, quote_asset: str, balance_update_interval: int = 10, deposit_withdraw_check_interval: int = 10):
        self.api_key = api_key
        self.secret_key = secret_key
        self.instrument = instrument
        self.base_asset = base_asset
        self.quote_asset = quote_asset
        self.client = None
        self.bsm = None
        self.market_data = {}
        self.balances = {}
        self.deposit_addresses = {}
        self.assets_to_track = []
        self.deposit_status = {}
        self.withdrawal_status = {}
        self.withdrawal_fee = {}
        self.min_withdrawal_amount = {}
        self.balance_update_interval = balance_update_interval
        self.deposit_withdraw_check_interval = deposit_withdraw_check_interval
        self.taker_fee_bps = 10

    async def init(self):
        """
        Initialize the async client and the socket manager.
        Also initializes the market_data dictionary with empty values for each instrument.
        """
        self.client = await AsyncClient.create(self.api_key, self.secret_key)
        self.bsm = BinanceSocketManager(self.client)
        await self.load_metadata()
        asyncio.create_task(self._task_loop(self._update_balance_cache, self.balance_update_interval))
        asyncio.create_task(self._task_loop(self._update_deposit_withdraw_status, self.deposit_withdraw_check_interval))
        # Initialize market data structure for each instrument
        self.market_data[self.instrument] = {
            'price': None,
            'timestamp': None,
            'last_update_time': None
        }
        self.subscribe_market_data()

    async def load_metadata(self):
        """
        Load the metadata for the instrument.
        """
        self.metadata = await self.client.get_symbol_info(self.instrument)
        self.assets_to_track = [self.metadata['baseAsset'], self.metadata['quoteAsset']]
        for asset in self.assets_to_track:
            self.deposit_addresses[asset] = await self.client.get_deposit_address(coin=asset)
    
    async def _task_loop(self, coroutine, interval):
        """
        Background loop to periodically run a coroutine.
        """
        while True:
            try:
                await coroutine()
            except Exception as e:
                logger.error(f"Error in {coroutine.__name__} loop: {e}")
                logger.error(traceback.format_exc())
            await asyncio.sleep(interval)
    
    async def _update_balance_cache(self):
        """
        Update the balance cache.
        """
        account = await self.client.get_account()
        balances = {}
        for balance in account.get("balances", []):
            if balance["asset"] in self.assets_to_track:
                balances[balance["asset"]] = balance["free"]
        self.balances = balances
        return self.balances
    
    async def _update_deposit_withdraw_status(self):
        """
        Update the deposit and withdraw status.
        """
        for asset in self.assets_to_track:
            asset_status = await self.get_asset_status(asset)
            self.deposit_status[asset] = asset_status.get('depositStatus', False)
            self.withdrawal_status[asset] = asset_status.get('withdrawStatus', False)
            self.withdrawal_fee[asset] = float(asset_status.get('withdrawFee', 0))
            self.min_withdrawal_amount[asset] = float(asset_status.get('minWithdrawAmount', 0))
    
    def subscribe_market_data(self, callback=None):
        """
        For each symbol in instrument_list, subscribes to the ticker websocket.
        Stores price and timestamp data in memory.
        If a callback is provided, it will be invoked with each market data message.
        
        The subscription tasks will run continuously until explicitly stopped.
        """
        
        socket = self.bsm.symbol_ticker_socket(self.instrument)
            
        async def handle_socket(sock, symbol):
            async with sock as stream:
                while True:
                    msg = await stream.recv()
                    # Update internal market data
                    self._update_market_data(symbol, msg)
                    # Call external callback if provided
                    if callback:
                        callback(msg)
        
        # Create task with the symbol passed as an argument
        asyncio.create_task(handle_socket(socket, self.instrument))
        
    
    def _update_market_data(self, symbol, msg):
        """
        Update the internal market data dictionary with the latest price information.
        
        Args:
            symbol (str): The trading pair symbol (e.g., 'BTCUSDT')
            msg (dict): The message received from the websocket
        """
        if symbol in self.market_data:
            self.market_data[symbol]['price'] = float(msg.get('c', 0))  # 'c' is the close price
            self.market_data[symbol]['timestamp'] = msg.get('E', 0)  # 'E' is the event time
            self.market_data[symbol]['best_bid'] = float(msg.get('b', 0))  # 'b' is the best bid price
            self.market_data[symbol]['best_bid_quantity'] = float(msg.get('B', 0))  # 'B' is the best bid quantity
            self.market_data[symbol]['best_ask'] = float(msg.get('a', 0))  # 'a' is the best ask price
            self.market_data[symbol]['best_ask_quantity'] = float(msg.get('A', 0))  # 'A' is the best ask quantity
            self.market_data[symbol]['last_update_time'] = time.time()
    
    def get_latest_price(self, symbol):
        """
        Get the latest price for a specific symbol.
        
        Args:
            symbol (str): The trading pair symbol (e.g., 'BTCUSDT')
            
        Returns:
            dict: A dictionary containing price and timestamp information
        """
        if symbol in self.market_data:
            return self.market_data[symbol]
        return None
    
    async def get_current_price(self, asset: str):
        """
        Get the current price of an asset.
        
        Args:
            asset (str): The asset code (e.g., 'ETH')
            
        Returns:
            float: The current price of the asset
        """
        if asset not in self.market_data:
            raise ValueError(f"Asset {asset} not found in market data")
        
        return self.get_latest_price(asset)['price']
    
    def get_all_prices(self):
        """
        Get the latest prices for all subscribed instruments.
        
        Returns:
            dict: A dictionary mapping symbols to their price information
        """
        return self.market_data

    async def place_order(self, symbol: str, side: str, order_type: str, quantity, price=None):
        """
        Place an order on Binance.
        """
        # Round quantity according to the asset's precision to avoid Binance API errors
        if symbol != self.metadata['symbol']:
            raise ValueError(f"Symbol {symbol} not found in metadata")
        
        # Get the quantity precision from the metadata
        lot_size_filter = next(filter(lambda x: x['filterType'] == 'LOT_SIZE', self.metadata['filters']))
        step_size = float(lot_size_filter['stepSize'])
        precision = len(str(step_size).split('.')[-1].rstrip('0'))
        
        # Round down to the correct precision
        quantity = math.floor(quantity * (10 ** precision)) / (10 ** precision)

        if order_type.upper() == "MARKET":
            order = await self.client.create_order(
                symbol=symbol,
                side=side.upper(),
                type=order_type.upper(),
                quantity=quantity
            )
        elif order_type.upper() == "LIMIT":
            order = await self.client.create_order(
                symbol=symbol,
                side=side.upper(),
                type=order_type.upper(),
                timeInForce="GTC",
                quantity=quantity,
                price=str(price)
            )
        else:
            raise ValueError("Unsupported order type. Use MARKET or LIMIT.")
        return order

    async def get_deposit_address(self, asset: str):
        """
        Get the deposit address for a specific asset.
        
        Args:
            asset (str): The cryptocurrency symbol (e.g., 'BTC', 'ETH')
            
        Returns:
            str: Deposit address as a string
            
        Raises:
            Exception: If the API call fails or the asset is not supported
        """
        return self.deposit_addresses[asset]['address']

    async def withdraw(self, asset: str, address: str, amount, network: str = "ETH"):
        params = {"coin": asset, "address": address, "amount": amount}
        if network:
            params["network"] = network
        return await self.client.withdraw(**params)

    async def get_deposit_history(self, asset: str):
        return await self.client.get_deposit_history(coin=asset)

    async def get_withdraw_history(self, asset: str):
        return await self.client.get_withdraw_history(coin=asset)
    
    async def get_withdrawal_fee(self, asset: str):
        return self.withdrawal_fee[asset]

    async def get_asset_status(self, asset: str):
        details = await self.client.get_asset_details()
        return details.get(asset)

    async def get_balances(self, live=False):
        """
        Retrieves the account balances for the specified assets.
        Returns a dictionary mapping each asset to its free and locked balances.
        """
        if live or len(self.balances) == 0:
            return await self._update_balance_cache()
        else:
            return self.balances
    
    async def get_max_executible_size(self, symbol: str, side: str):
        """
        Get the TOB size for a given symbol.
        """
        if side == 'buy':
            return self.market_data[symbol]['best_ask_quantity']
        elif side == 'sell':
            return self.market_data[symbol]['best_bid_quantity']
        else:
            raise ValueError(f"Invalid side: {side}. Must be 'buy' or 'sell'")
    
    async def verify_withdrawal_open(self, asset: str, live=False):
        """
        Verify if a withdrawal is currently open for a given asset.
        """
        if live:
            await self._update_deposit_withdraw_status()
        return self.withdrawal_status[asset]
    
    async def verify_deposit_open(self, asset: str, live=False):
        """
        Verify if a deposit is currently open for a given asset.
        """
        if live:
            await self._update_deposit_withdraw_status()
        return self.deposit_status[asset]

    async def execute_trade(self, trade_direction, trade_size):
        """
        Execute a trade on Binance.

        Args:
            trade_direction (str): Direction of the trade ('buy' or 'sell')
            trade_size (float): Size of the trade
            arb_instrument (str): Trading instrument (e.g., 'ETHUSDT')

        Returns:
            dict: Trade details
        """
        try:
            # Validate inputs
            if trade_direction not in ['buy', 'sell']:
                raise ValueError(f"Invalid trade direction: {trade_direction}. Must be 'buy' or 'sell'")
            if trade_size <= 0:
                raise ValueError(f"Invalid trade size: {trade_size}. Must be positive")

            # Convert direction to Binance order side
            side = trade_direction.upper()

            # Create order parameters
            order_params = {
                'symbol': self.instrument,
                'side': side,
                'type': 'MARKET',  # Using market orders for immediate execution
                'quantity': trade_size
            }

            # Execute the trade
            logger.info(f"Executing trade: {order_params}")
            order = await self.client.create_order(**order_params)

            # Return trade details
            return {
                'status': 'success',
                'order_id': order['orderId'],
                'trade_size': float(order['executedQty']),
                'avg_price': float(order['cummulativeQuoteQty']) / float(order['executedQty']),
                'timestamp': order['transactTime']
            }

        except Exception as e:
            logger.error(f"Failed to execute trade: {str(e)}")
            return {
                    'status': 'error',
                    'error': str(e),
                    'trade_size': 0
                }

    async def confirm_trade(self, trade):
        """
        Confirm that a trade was executed successfully.

        Args:
            trade (dict): Trade details

        Returns:
            float: Confirmed trade size
        """
        if trade and 'trade_size' in trade:
            return trade["trade_size"]
        else:
            return 0

    async def get_balance(self, symbol, live=False):
        """
        Get the current balance of a specific asset.

        Args:
            arb_instrument (str): Trading instrument

        Returns:
            float: Balance of the asset
        """
        balances = await self.get_balances(live=live)
        return float(balances[symbol])
    
    async def confirm_deposit(self, asset: str, amount: float, tolerance: float = 1e-6, timeout_seconds: int = 600) -> float:
        """
        Confirm that a deposit was completed and return the confirmed size.
        """
        logger.info(f"Confirming deposit for {asset} {amount}")

        start_time = time.time()
        while True:
            deposit_history = await self.get_deposit_history(asset)
            for deposit in deposit_history:
                if float(deposit['amount']) - float(amount) <= tolerance:
                    return float(deposit['amount'])
            await asyncio.sleep(2)
            if time.time() - start_time > timeout_seconds:
                return 0
        return 0
    
    async def confirm_withdrawal(self, withdrawal_info: Dict[str, Any]):
        """
        Confirm that a withdrawal was completed and return the confirmed size.
        """
        while True:
            withdrawal = await self.client.get_withdraw_history_id(withdrawal_info['id'])
            # 6: completed, 3: failed
            if withdrawal['status'] == 6:
                return withdrawal['amount']
            elif withdrawal['status'] == 3:
                return 0
            await asyncio.sleep(2)
    
    async def pre_validate_transfers(self, asset: str, amount: float, max_transfer_time_seconds: int = 10) -> bool:
        """
        Pre-validate transfers for a specific asset, verify if network is healthy and if the asset is supported by the pool.
        """
        if not await self.verify_deposit_open(asset):
            logger.warning(f"Deposit is not open for {asset}")
            return False
        
        if not await self.verify_withdrawal_open(asset):
            logger.warning(f"Withdrawal is not open for {asset}")
            return False
        
        if amount < self.min_withdrawal_amount[asset]:
            logger.warning(f"Amount ({amount}) is less than the minimum withdrawal ({self.min_withdrawal_amount[asset]}) amount for {asset}")
            return False
        
        return True
    
    async def compute_buy_and_transfer_costs(self, asset: str, amount: float) -> float:
        """
        Compute the buy and transfer costs for a specific asset.

        Args:
            asset: The asset code (e.g., 'ETH')

        Returns:
            float: The execution costs in BPS
        """
        slippage_bps = (await self.get_current_price(self.instrument) - self.market_data[self.instrument]['best_ask']) / self.market_data[self.instrument]['best_ask'] * 10_000
        slippage_cost = slippage_bps * amount / 10_000
        post_execution_amount = amount - slippage_cost

        # apply taker fee post execution
        taker_fee_cost = post_execution_amount * (self.taker_fee_bps / 10000)
        post_execution_amount = post_execution_amount - taker_fee_cost

        # apply withdrawal fee
        withdrawal_fee = await self.get_withdrawal_fee(asset)
        final_amount = post_execution_amount - withdrawal_fee
        
        logger.info(f"slippage_bps: {slippage_bps}, toker_fee_bps: {self.taker_fee_bps}, withdrawal_fee: {withdrawal_fee}")
        total_cost_bps = (1 - final_amount / amount) * 10_000
        return total_cost_bps
    
    async def compute_sell_costs(self, asset: str, amount: float) -> float:
        """
        Compute execution costs in BPS for a sell order.

        Args:
            asset: The asset code (e.g., 'ETH')
            amount: The amount of the sell order

        Returns:
            float: The execution costs in BPS
        """
        # For binance fee applies post execution
        ideal_sell_accrual = await self.get_current_price(self.instrument) * amount
        real_sell_accrual = self.market_data[self.instrument]['best_bid'] * amount
        slippage_cost_in_quote_asset = real_sell_accrual - ideal_sell_accrual

        fee_cost_in_quote_asset = real_sell_accrual * self.taker_fee_bps / 10_000
        total_cost_in_quote_asset = slippage_cost_in_quote_asset + fee_cost_in_quote_asset
        total_cost_bps = (total_cost_in_quote_asset / ideal_sell_accrual) * 10_000
        
        return total_cost_bps
    
    async def get_base_asset_price(self) -> float:
        """
        Get the current price of the base asset.
        """
        return await self.get_current_price(self.instrument)
    
    async def get_base_asset_deposit_address(self) -> str:
        """
        Get the deposit address for the base asset.
        """
        return await self.get_deposit_address(self.base_asset)
    
    async def get_base_asset_balance(self, live=False) -> float:
        """
        Get the current balance of the base asset.
        """
        return await self.get_balance(self.base_asset, live=live)
    
    async def wrap_asset(self, asset: str, amount: float) -> float:
        """
        Not required for Binance.
        """
        return amount
    
    async def unwrap_asset(self, asset: str, amount: float) -> float:
        """
        Not required for Binance.
        """
        return amount

# --- Async test code in __main__ ---
async def main():
    config = Config()
    instrument = "ETHUSDC"
    base_asset = "ETH"
    quote_asset = "USDC"
    connector = Binance(config.binance_api_key, config.binance_api_secret, instrument, base_asset, quote_asset)
    await connector.init()

    
    # Wait for some initial data to come in
    await asyncio.sleep(5)
    
    # Example: Get the latest price for BTCUSDT
    ethusdc_price = connector.get_latest_price("ETHUSDC")
    logger.info(f"Latest ETHUSDC price: {ethusdc_price}")
    
    # Example: Get all prices
    all_prices = connector.get_all_prices()
    logger.info(f"All prices: {all_prices}")

    # Example: Get deposit address
    deposit_address = await connector.get_deposit_address("USDC")
    logger.info(f"Deposit address (USDC): {deposit_address}")

    # Example: Place a market buy order for ETHUSDC (0.005 quantity)
    test_trade = False
    if test_trade:
        try:
            trade_response = await connector.place_order(
                symbol="ETHUSDC",
                side="BUY",
                order_type="MARKET",
                quantity=0.005
            )
            logger.info(f'Trade Order Response: {trade_response}')

            # Wait for the order to be filled
            await asyncio.sleep(1)

            # Get the order status
            ETH_BALANCE = await connector.get_balance(connector.base_asset, live=True)
            logger.info(f"ETH Balance: {ETH_BALANCE}")

            # Sell the ETH
            sell_response = await connector.place_order(
                symbol="ETHUSDC",
                side="SELL",
                order_type="MARKET",
                quantity=ETH_BALANCE
            )
            logger.info(f'Sell Order Response: {sell_response}')

        except Exception as e:
            logger.info(f"Error placing order: {e}")

    # Test withdrawals
    test_withdrawals = False
    if test_withdrawals:
        try:
            # Get deposit address for USDT
            address = "0x94A5d421375c9486061f6835a11c81196470290C"
            amount = ETH_BALANCE
            withdrawal_response = await connector.withdraw(asset="ETH", address=address, amount=amount)
            logger.info(f"Withdrawal Response: {withdrawal_response}")
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.info(f"Error withdrawing: {e}")


    # Example: Get deposit history for USDT
    try:
        deposits = await connector.get_deposit_history(asset="USDC")
        logger.info(f"USDC Deposit History: {deposits}")
        deposits = await connector.get_deposit_history(asset="ETH")
        logger.info(f"ETH Deposit History: {deposits}")
    except Exception as e:
        logger.info(f"Error fetching deposit history: {e}")

    # Example: Get withdrawal history for ETH
    try:
        withdrawals = await connector.get_withdraw_history(asset="ETH")
        logger.info(f"ETH Withdrawal History: {withdrawals}")
        withdrawals = await connector.get_withdraw_history(asset="USDC")
        logger.info(f"USDC Withdrawal History: {withdrawals}")
    except Exception as e:
        logger.info(f"Error fetching withdrawal history: {e}")

    # Example: Get asset status for USDT
    try:
        asset_status = await connector.get_asset_status("USDC")
        if asset_status:
            logger.info(f"USDC Asset Status: {asset_status}")
        else:
            logger.info("No asset status found for USDC.")
    except Exception as e:
        logger.info(f"Error fetching asset status: {e}")

    # Example: Get current balances for the asset_list provided
    try:
        balances = await connector.get_balances()
        logger.info(f"Current Balances for assets: {balances}")
    except Exception as e:
        logger.info(f"Error fetching balances: {e}")
    
    # Verify deposit and withdrawal status
    try:
        deposit_status = await connector.verify_deposit_open("USDC")
        withdrawal_status = await connector.verify_withdrawal_open("USDC")
        logger.info(f"USDC deposit status: {deposit_status}, withdrawal status: {withdrawal_status}")
        deposit_status = await connector.verify_deposit_open("ETH")
        withdrawal_status = await connector.verify_withdrawal_open("ETH")
        logger.info(f"ETH deposit status: {deposit_status}, withdrawal status: {withdrawal_status}")
    except Exception as e:
        logger.info(f"Error verifying deposit and withdrawal status: {e}")
    

    # Example: Confirm withdrawal
    try:
        withdrawal_info = {'id': '06376058b9554848b1e1f6c0f084bc1c'}
        confirmed_withdrawal = await connector.confirm_withdrawal(withdrawal_info)
        logger.info(f"Confirmed withdrawal: {confirmed_withdrawal}")
    except Exception as e:
        logger.info(f"Error confirming withdrawal: {e}")
    
    # Example: Confirm deposit
    try:
        asset = "ETH"
        amount = 0.004995930032057706
        confirmed_deposit = await connector.confirm_deposit(asset, amount)
        logger.info(f"Confirmed deposit: {confirmed_deposit}")
    except Exception as e:
        logger.info(f"Error confirming deposit: {e}")
    
    # Example: Pre-validate transfers
    try:
        pre_validate = await connector.pre_validate_transfers("ETH", 0.001)
        logger.info(f"Pre-validate transfers: {pre_validate}")
    except Exception as e:
        logger.info(f"Error pre-validating transfers: {e}")
    
    # Fetch withdrawal fee
    try:
        withdrawal_fee = await connector.get_withdrawal_fee("ETH")
        logger.info(f"Withdrawal fee: {withdrawal_fee}")
    except Exception as e:
        logger.info(f"Error fetching withdrawal fee: {e}")
        
    # Compute buy and transfer costs
    try:
        buy_and_transfer_costs = await connector.compute_buy_and_transfer_costs("ETH", 0.01)
        logger.info(f"Buy and transfer costs: {round(buy_and_transfer_costs, 2)} BPS")
    except Exception as e:
        logger.info(f"Error computing buy and transfer costs: {e}")
    
    # Compute sell costs
    try:
        sell_costs = await connector.compute_sell_costs("ETH", 0.01)
        logger.info(f"Sell costs: {round(sell_costs, 2)} BPS")
    except Exception as e:
        logger.info(f"Error computing sell costs: {e}")
        
    # Get updated prices
    while True:
        base_asset_price = await connector.get_base_asset_price()
        logger.info(f"{connector.base_asset} price: {base_asset_price}")
        logger.info(f"Balances: {await connector.get_balances()}")
        logger.info(f"Deposit status: {await connector.verify_deposit_open(connector.base_asset)}")
        logger.info(f"Withdrawal status: {await connector.verify_withdrawal_open(connector.base_asset)}")
        await asyncio.sleep(1)
    

if __name__ == "__main__":
    asyncio.run(main())
