import asyncio
import os
import time
from binance import AsyncClient, BinanceSocketManager
from config import Config

class Binance:
    def __init__(self, api_key: str, secret_key: str, instrument_list: list):
        self.api_key = api_key
        self.secret_key = secret_key
        self.instrument_list = instrument_list
        self.client = None
        self.bsm = None
        # Dictionary to store latest price data for each instrument
        self.market_data = {}
        # List to store subscription tasks
        self.subscription_tasks = []

    async def init(self):
        """
        Initialize the async client and the socket manager.
        Also initializes the market_data dictionary with empty values for each instrument.
        """
        self.client = await AsyncClient.create(self.api_key, self.secret_key)
        self.bsm = BinanceSocketManager(self.client)
        
        # Initialize market data structure for each instrument
        for symbol in self.instrument_list:
            self.market_data[symbol] = {
                'price': None,
                'timestamp': None,
                'last_update_time': None
            }
        self.subscribe_market_data()

    # 1. Subscribe to market data via websocket
    def subscribe_market_data(self, callback=None):
        """
        For each symbol in instrument_list, subscribes to the ticker websocket.
        Stores price and timestamp data in memory.
        If a callback is provided, it will be invoked with each market data message.
        
        The subscription tasks will run continuously until explicitly stopped.
        Tasks are stored internally in self.subscription_tasks.
        """
        self.subscription_tasks = []  # Clear any existing tasks
        
        for symbol in self.instrument_list:
            socket = self.bsm.symbol_ticker_socket(symbol)
            
            async def handle_socket(sock, sym):
                async with sock as stream:
                    while True:
                        msg = await stream.recv()
                        # Update internal market data
                        self._update_market_data(sym, msg)
                        # Call external callback if provided
                        if callback:
                            callback(msg)
            
            # Create task with the symbol passed as an argument
            task = asyncio.create_task(handle_socket(socket, symbol))
            self.subscription_tasks.append(task)
        
        # No need to return tasks as they're stored in self.subscription_tasks
    
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
    
    def get_current_base_price(self, symbol):
        """
        Get the current price of the base token in the pool.
        
        Returns:
            float: The current price of the base token
        """
        if symbol in self.market_data:
            return self.market_data[symbol]['price']
        return None
    
    def get_all_prices(self):
        """
        Get the latest prices for all subscribed instruments.
        
        Returns:
            dict: A dictionary mapping symbols to their price information
        """
        return self.market_data

    async def place_order(self, symbol: str, side: str, order_type: str, quantity, price=None):
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
        address = await self.client.get_deposit_address(coin=asset)
        return address['address']

    async def withdraw(self, asset: str, address: str, amount, network: str = None):
        params = {"asset": asset, "address": address, "amount": amount}
        if network:
            params["network"] = network
        return await self.client.withdraw(**params)

    # 5. Deposit and withdrawal histories
    async def get_deposit_history(self, asset: str):
        return await self.client.get_deposit_history(coin=asset)

    async def get_withdraw_history(self, asset: str):
        return await self.client.get_withdraw_history(coin=asset)

    # 6. Get asset status (if deposits/withdrawals are suspended)
    async def get_asset_status(self, asset: str):
        details = await self.client.get_asset_details()
        return details.get(asset)

    # 7. Get current balances for a given list of assets
    async def get_balances(self, assets: list):
        """
        Retrieves the account balances for the specified assets.
        Returns a dictionary mapping each asset to its free and locked balances.
        """
        account = await self.client.get_account()
        balances = {}
        for balance in account.get("balances", []):
            asset = balance["asset"]
            if asset in assets:
                balances[asset] = {"free": balance["free"], "locked": balance["locked"]}
        return balances
    
    def stop_market_data_subscriptions(self):
        """
        Stop all market data subscription tasks.
        """
        for task in self.subscription_tasks:
            if not task.done():
                task.cancel()
        self.subscription_tasks = []

    async def close(self):
        """
        Close the Binance client connection and cancel any running subscription tasks.
        """
        self.stop_market_data_subscriptions()
        await self.client.close_connection()
    
    async def get_tob_size(self, symbol: str, side: str):
        """
        Get the TOB size for a given symbol.
        """
        # Get the order book depth to find the top of book size
        depth = await self.client.get_order_book(symbol=symbol, limit=1)
        if side == 'buy':
            if depth and len(depth['bids']) > 0:
                # Return the size at the best bid
                return float(depth['bids'][0][1])
            return 0.0  # Return 0 if no valid depth data
        elif side == 'sell':
            if depth and len(depth['asks']) > 0:
                # Return the size at the best ask
                return float(depth['asks'][0][1])
        return 0.0  # Return 0 if no valid depth data
    
    async def verify_withdrawal_open(self, asset: str):
        """
        Verify if a withdrawal is currently open for a given asset.
        """
        asset_status = await self.get_asset_status(asset)
        if asset_status and asset_status.get('withdrawStatus'):
            return True
        else:
            return False
    
    async def verify_deposit_open(self, asset: str):
        """
        Verify if a deposit is currently open for a given asset.
        """
        asset_status = await self.get_asset_status(asset)
        return asset_status and asset_status.get('depositStatus', False)


# --- Async test code in __main__ ---
async def main():
    config = Config()
    instrument_list = ["ETHUSDC"]
    asset_list = ["ETH", "USDC"]

    connector = Binance(config.binance_api_key, config.binance_api_secret, instrument_list)
    await connector.init()


    # Start the market data subscription - it will run continuously
    connector.subscribe_market_data()
    print("Subscribed to market data. Running continuously...")
    
    # Wait for some initial data to come in
    await asyncio.sleep(5)
    
    # Example: Get the latest price for BTCUSDT
    ethusdc_price = connector.get_latest_price("ETHUSDC")
    print(f"Latest ETHUSDC price: {ethusdc_price}")
    
    # Example: Get all prices
    all_prices = connector.get_all_prices()
    print(f"All prices: {all_prices}")

    # Example: Place a market buy order for ETHUSDC (0.001 quantity)
    try:
        trade_response = await connector.place_order(
            symbol="ETHUSDC",
            side="BUY",
            order_type="MARKET",
            quantity=0.001
        )
        print("Trade Order Response:", trade_response)
    except Exception as e:
        print("Error placing order:", e)

    # Example: Get deposit address for USDT
    try:
        deposit_addr = await connector.get_deposit_address(asset="ETH")
        print("USDC Deposit Address:", deposit_addr)
    except Exception as e:
        print("Error fetching deposit address:", e)

    # Example: Get deposit history for USDT
    try:
        deposits = await connector.get_deposit_history(asset="USDC")
        print("USDC Deposit History:", deposits)
    except Exception as e:
        print("Error fetching deposit history:", e)

    # Example: Get withdrawal history for ETH
    try:
        withdrawals = await connector.get_withdraw_history(asset="ETH")
        print("ETH Withdrawal History:", withdrawals)
    except Exception as e:
        print("Error fetching withdrawal history:", e)

    # Example: Get asset status for USDT
    try:
        asset_status = await connector.get_asset_status("USDC")
        if asset_status:
            print("USDC Asset Status:", asset_status)
        else:
            print("No asset status found for USDC.")
    except Exception as e:
        print("Error fetching asset status:", e)

    # Example: Get current balances for the asset_list provided
    try:
        balances = await connector.get_balances(asset_list)
        print("Current Balances for assets:", balances)
    except Exception as e:
        print("Error fetching balances:", e)

    # Wait a bit longer to demonstrate continuous data collection
    print("Waiting for more market data...")
    await asyncio.sleep(10)
    
    # Get updated prices
    while True:
        updated_ethusdc_price = connector.get_latest_price("ETHUSDC")
        print(f"Updated ETHUSDC price: {updated_ethusdc_price}")
        await asyncio.sleep(1)
    
    # In a real application, you would keep the tasks running
    # For this example, we'll close the connection after demonstrating
    await connector.close()

if __name__ == "__main__":
    asyncio.run(main())
