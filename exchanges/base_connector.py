import abc
from typing import Any, Dict

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