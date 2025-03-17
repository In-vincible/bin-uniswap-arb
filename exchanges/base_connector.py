import abc
from typing import Any, Dict, List

class BaseExchange(abc.ABC):
    """
    Abstract base class that defines the interface for all exchange connectors.
    
    This class provides a common interface that all specific exchange implementations
    must adhere to. It ensures that ExecutionEngine can interact with any exchange
    through a consistent API, regardless of underlying implementation details.
    """
        
    def __init__(self, *args, **kwargs):
        """
        Initialize the exchange connector with configuration.
        """
        pass

    async def init(self):
        """
        Initialize the exchange connector.
        """
        raise NotImplementedError()
        
    @abc.abstractmethod
    async def execute_trade(self, direction: str, size: float) -> Dict[str, Any]:
        """
        Execute a buy or sell trade on the exchange.
        
        Args:
            direction: Trade direction ('buy' or 'sell')
            size: Size of the trade in base currency units
            
        Returns:
            Dict containing trade execution details including trade ID, status, etc.
            
        Raises:
            Exception: If trade execution fails
        """
        raise NotImplementedError()
    
    @abc.abstractmethod
    async def confirm_trade(self, trade: Dict[str, Any]) -> float:
        """
        Confirm that a trade was executed and return the confirmed size.
        
        Args:
            trade: Trade details returned from execute_trade
            
        Returns:
            float: The actual executed size (0 if trade failed)
            
        Raises:
            Exception: If trade confirmation fails
        """
        raise NotImplementedError()
    
    @abc.abstractmethod
    async def get_deposit_address(self, instrument: str) -> str:
        """
        Get the deposit address for a specific instrument.
        
        Args:
            instrument: The trading instrument code (e.g., 'ETH')
            
        Returns:
            str: The deposit address
            
        Raises:
            Exception: If address retrieval fails
        """
        raise NotImplementedError()
    
    async def get_base_asset_deposit_address(self) -> str:
        """
        Get the deposit address for the base asset.
        """
        raise NotImplementedError()
    
    @abc.abstractmethod
    async def get_balance(self, instrument: str) -> float:
        """
        Get the current balance of a specific instrument.
        
        Args:
            instrument: The trading instrument code (e.g., 'ETH')
            
        Returns:
            float: Current balance of the instrument
            
        Raises:
            Exception: If balance retrieval fails
        """
        raise NotImplementedError()
    
    @abc.abstractmethod
    async def get_base_asset_balance(self) -> float:
        """
        Get the current balance of the base asset.
        """
        raise NotImplementedError()
    
    @abc.abstractmethod
    async def confirm_deposit(self, asset: str, amount: float) -> float:
        """
        Confirm that a deposit was completed and return the confirmed size.

        Args:
            asset: The asset code (e.g., 'ETH')
            amount: The amount of the deposit

        Returns:
        """
        raise NotImplementedError()
    
    @abc.abstractmethod
    async def confirm_withdrawal(self, withdrawal_info: Dict[str, Any]) -> float:
        """
        Confirm that a withdrawal was completed and return the confirmed size.

        Args:
            withdrawal_info: The withdrawal information

        Returns:
            float: The confirmed withdrawal amount
        """
        raise NotImplementedError()
    
    @abc.abstractmethod
    async def withdraw(self, asset: str, address: str, amount: float) -> Dict[str, Any]:
        """
        Withdraw assets from the exchange.

        Args:
            asset: The asset code (e.g., 'ETH')
            address: The address to withdraw to
            amount: The amount to withdraw

        Returns:
            Dict containing withdrawal details including withdrawal ID, status, etc.

        Raises:
            Exception: If withdrawal fails
        """
        raise NotImplementedError()
    
    @abc.abstractmethod
    async def get_current_price(self, asset: str) -> float:
        """
        Get the current price of an asset.

        Args:
            asset: The asset code (e.g., 'ETH')

        Returns:
            float: The current price of the asset
        """
        raise NotImplementedError()
    
    @abc.abstractmethod
    async def get_base_asset_price(self) -> float:
        """
        Get the current price of the base asset.
        """
        raise NotImplementedError()
    
    @abc.abstractmethod
    async def pre_validate_transfers(self, asset: str, amount: float, max_transfer_time_seconds: int = 10) -> bool:
        """
        Pre-validate transfers for a specific asset, verify if network is healthy and if the asset is supported by the pool.

        Args:
            asset: The asset code (e.g., 'ETH')
            max_transfer_time_seconds: The maximum transfer time in seconds

        Returns:
            bool: True if the transfer is valid, False otherwise
        """
        raise NotImplementedError()
    
    @abc.abstractmethod
    async def compute_buy_and_transfer_costs(self, asset: str, amount: float) -> float:
        """
        Compute the execution costs in BPS for a specific asset.

        Args:
            asset: The asset code (e.g., 'ETH')
            amount: The amount of the buy order
        Returns:
            float: The execution costs in BPS
        """
        raise NotImplementedError()
    
    @abc.abstractmethod
    async def compute_sell_costs(self, asset: str, amount: float) -> float:
        """
        Compute the execution costs in BPS for a sell order.

        Args:
            asset: The asset code (e.g., 'ETH')
            amount: The amount of the sell order

        Returns:
            float: The execution costs in BPS
        """
        raise NotImplementedError()
    
    @abc.abstractmethod
    async def get_max_executible_size(self, asset: str, direction: str) -> float:
        """
        Get the maximum size that can be executed for a specific asset and direction.

        Args:
            asset: The asset code (e.g., 'ETH')
            direction: The direction of the trade ('buy' or 'sell')

        Returns:
            float: The maximum size that can be executed
        """
        raise NotImplementedError()
    
    @abc.abstractmethod
    async def wrap_asset(self, asset: str, amount: float) -> float:
        """
        Perform a block wrap for a specific assets.

        Required for some exchanges to wrap the asset before sending it to the pool. for eg. ETH to WETH.
        """
        raise NotImplementedError()
    
    @abc.abstractmethod
    async def unwrap_asset(self, asset: str, amount: float) -> float:
        """
        Perform a block unwrap for a specific assets.

        Required for some exchanges to unwrap the asset for transfer. for eg. WETH to ETH.
        """
        raise NotImplementedError()
    