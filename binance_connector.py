import asyncio
import logging
import time
from binance import AsyncClient, BinanceSocketManager
from config import Config

logger = logging.getLogger(__name__)

class Binance:
    def __init__(self, api_key: str, secret_key: str, instrument: str):
        self.api_key = api_key
        self.secret_key = secret_key
        self.instrument = instrument
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
        self.market_data[self.instrument] = {
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
        task = asyncio.create_task(handle_socket(socket, self.instrument))
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
        """
        Get the deposit address for a specific asset.
        
        Args:
            asset (str): The cryptocurrency symbol (e.g., 'BTC', 'ETH')
            
        Returns:
            str: Deposit address as a string
            
        Raises:
            Exception: If the API call fails or the asset is not supported
        """
        try:
            address_info = await self.client.get_deposit_address(coin=asset)
            # Return just the address string
            return address_info['address']
        except Exception as e:
            # Log the error for debugging purposes
            print(f"Error getting deposit address for {asset}: {str(e)}")
            raise

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
            dict: Transfer details including transaction ID and status
            
        Raises:
            ValueError: If parameters are invalid
            Exception: If API call fails or transfer cannot be initiated
        """
        # Input validation
        if not transfer_direction or transfer_direction not in ['withdraw', 'deposit']:
            raise ValueError(f"Invalid transfer direction: {transfer_direction}. Must be 'withdraw' or 'deposit'")
            
        if not transfer_amount or transfer_amount <= 0:
            raise ValueError(f"Invalid transfer amount: {transfer_amount}. Must be a positive number")
            
        try:
            if transfer_direction == 'withdraw':
                # For withdrawals, we need:
                # 1. The asset being withdrawn
                # 2. The destination address
                # 3. The amount
                
                # Extract asset from address (in a real implementation, this would be more robust)
                # This is a simplification - in practice you'd need proper asset identification
                asset = to_address.split(':')[0] if ':' in to_address else None
                
                if not asset:
                    raise ValueError(f"Could not determine asset from address: {to_address}")
                    
                # Check if withdrawal is possible for this asset
                withdrawal_status = await self.verify_withdrawal_open(asset)
                if not withdrawal_status.get('isWithdrawEnabled', False):
                    raise ValueError(f"Withdrawals are currently disabled for {asset}")
                    
                # Initialize withdrawal
                # In a production environment, this would call the exchange API
                # Example: withdrawal = await self.client.withdraw(asset, to_address, transfer_amount)
                
                # Simulating a transaction ID
                import uuid
                transaction_id = str(uuid.uuid4())
                
                return {
                    "status": "initiated",
                    "transfer_direction": transfer_direction,
                    "transfer_amount": transfer_amount,
                    "asset": asset,
                    "from_address": from_address, 
                    "to_address": to_address,
                    "transaction_id": transaction_id,
                    "timestamp": int(time.time())
                }
                
            elif transfer_direction == 'deposit':
                # For deposits, typically we just provide an address to the user
                # and they initiate the deposit from their external wallet
                
                # Extract asset from address (in a real implementation, this would be more robust)
                asset = from_address.split(':')[0] if ':' in from_address else None
                
                if not asset:
                    raise ValueError(f"Could not determine asset from address: {from_address}")
                
                # Check if deposits are enabled for this asset
                deposit_status = await self.verify_deposit_open(asset)
                if not deposit_status.get('isDepositEnabled', False):
                    raise ValueError(f"Deposits are currently disabled for {asset}")
                
                # Get deposit address
                deposit_info = await self.get_deposit_address(asset)
                
                return {
                    "status": "ready_for_deposit",
                    "transfer_direction": transfer_direction,
                    "transfer_amount": transfer_amount,
                    "asset": asset,
                    "deposit_address": deposit_info,
                    "timestamp": int(time.time())
                }
        except Exception as e:
            print(f"Error initiating {transfer_direction}: {str(e)}")
            raise

    async def confirm_transfer(self, transfer):
        """
        Confirm that a transfer was completed successfully.

        Args:
            transfer (dict): Transfer details containing at minimum 'transfer_amount',
                            'transaction_id', and asset information

        Returns:
            float: Confirmed transfer size
            
        Raises:
            ValueError: If transfer object is invalid
            Exception: If API call fails or transfer cannot be confirmed
        """
        if not transfer or not isinstance(transfer, dict):
            raise ValueError("Invalid transfer object")
            
        if "transfer_amount" not in transfer or "asset" not in transfer:
            raise ValueError("Transfer object missing required fields")
            
        try:
            # In a real implementation, we would:
            # 1. Check the transaction status on the blockchain or exchange API
            # 2. Verify that the transaction has enough confirmations
            # 3. Check if the amount matches the expected amount
            
            # For withdrawals, we might check withdrawal history
            if "transaction_id" in transfer:
                # Simulate checking transaction status using the transaction ID
                # In a real implementation, we would check with the exchange API
                print(f"Confirming transaction: {transfer['transaction_id']}")
                
                # In production, this would be a call to check transaction status
                # Example: status = await self.client.get_transaction_status(transfer['transaction_id'])
                
            # Return the confirmed amount (in a real implementation, this would come from the blockchain/exchange)
            return float(transfer["transfer_amount"])
        except Exception as e:
            print(f"Error confirming transfer: {str(e)}")
            raise

    async def get_withdraw_address(self, arb_instrument):
        """
        Get the address to withdraw assets from.

        In most exchange APIs, the withdrawal address is typically managed and 
        retrieved from previously saved addresses in the user's account.
        
        Args:
            arb_instrument (str): Trading instrument (cryptocurrency symbol)

        Returns:
            str: Withdrawal address string
            
        Raises:
            ValueError: If the instrument is invalid or no withdrawal address is found
            Exception: For other API errors
        """
        if not arb_instrument or not isinstance(arb_instrument, str):
            raise ValueError(f"Invalid instrument: {arb_instrument}. Must be a valid string symbol.")
            
        try:
            # In a real implementation, you might have different logic here, such as:
            # 1. Retrieving saved withdrawal addresses from the exchange
            # 2. Getting the default/preferred withdrawal address
            # 3. Potentially accessing a database of predefined addresses
            
            # For now, we'll use the deposit address as a fallback
            # In a production environment, you'd want to verify this address is
            # actually valid for withdrawals
            address_info = await self.get_deposit_address(arb_instrument)
            
            if not address_info:
                raise ValueError(f"Could not retrieve valid withdrawal address for {arb_instrument}")
                
            # Return just the address string
            return address_info
        except Exception as e:
            print(f"Error retrieving withdrawal address for {arb_instrument}: {str(e)}")
            raise

    async def get_balance(self, arb_instrument):
        """
        Get the current balance of a specific asset.

        Args:
            arb_instrument (str): Trading instrument

        Returns:
            float: Balance of the asset
        """
        # Implement logic to get balance
        balances = await self.get_balances([arb_instrument])
        return float(balances[arb_instrument]["free"])

# --- Async test code in __main__ ---
async def main():
    config = Config()
    instrument = "ETHUSDC"
    asset_list = ["ETH", "USDC"]

    connector = Binance(config.binance_api_key, config.binance_api_secret, instrument)
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
