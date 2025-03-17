from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Union
from config import Config
import logging
import asyncio


class BaseStrategy(ABC):
    """
    Abstract base class for trading strategies.
    
    This class defines the interface that all trading strategy implementations
    must adhere to. It ensures a consistent API across different strategy types
    while allowing for strategy-specific implementations.
    """
    
    def __init__(self, config: Config):
        """
        Initialize the strategy with configuration parameters.
        
        Args:
            config: Dictionary containing strategy configuration parameters
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.is_running = False
        self.monitoring_task = None
        
    @abstractmethod
    async def initialize(self) -> None:
        """
        Perform any necessary initialization for the strategy.
        
        This method should handle setup tasks such as connecting to exchanges,
        loading historical data, or setting up data structures needed by the strategy.
        
        Returns:
            None
        """
        raise NotImplementedError("Strategy subclass must implement initialize method")
        
    @abstractmethod
    async def analyze(self) -> Dict[str, Any]:
        """
        Analyze market conditions and identify trading opportunities.
        
        This core method implements the strategy's analysis logic to determine
        whether conditions are favorable for executing trades.
        
        Returns:
            Dictionary containing analysis results and potential signals
        """
        raise NotImplementedError("Strategy subclass must implement analyze method")
    
    async def validate_opportunity(self, analysis_result: Dict[str, Any]) -> bool:
        """
        Validate a potential trading opportunity identified in the analysis step.
        
        This method allows for additional validation logic beyond the basic
        analysis, such as checking liquidity, estimating execution costs,
        or verifying external conditions.
        
        Args:
            analysis_result: The result from the analyze method
            
        Returns:
            Boolean indicating whether the opportunity is valid and should be executed
        """
        # Default implementation considers all opportunities valid
        return analysis_result.get("should_execute", False)
        
    @abstractmethod
    async def execute(self, analysis_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute trades based on the analysis results.
        
        This method is responsible for implementing the trading logic,
        including order placement and execution.
        
        Args:
            analysis_result: Output from the analyze method containing signals
                             and other information needed for execution
        
        Returns:
            Dictionary containing execution results (orders placed, etc.)
        """
        raise NotImplementedError("Strategy subclass must implement execute method")
        
    async def run_once(self) -> Dict[str, Any]:
        """
        Run a single iteration of the strategy (analyze and execute).
        
        This method orchestrates the strategy execution flow by calling
        analyze() followed by execute() if applicable.
        
        Returns:
            Dictionary containing the results of strategy execution
        """
        self.logger.debug("Running strategy iteration")
        analysis_result = await self.analyze()
        
        # Validate the opportunity before execution
        should_execute = await self.validate_opportunity(analysis_result)
        analysis_result["should_execute"] = should_execute
        
        # Only execute if validation passed
        if should_execute:
            self.logger.info("Valid trading opportunity found, executing")
            execution_result = await self.execute(analysis_result)
            return {
                "analysis": analysis_result,
                "execution": execution_result
            }
        
        return {"analysis": analysis_result, "execution": None}
    
    async def monitor(self, interval_seconds: float = 1.0) -> None:
        """
        Continuously monitor market conditions and execute the strategy.
        
        This method implements a continuous loop that repeatedly calls run_once()
        at the specified interval. This is the main entry point for running
        a strategy indefinitely.
        
        Args:
            interval_seconds: Time to wait between strategy iterations
            
        Returns:
            None
        """
        self.logger.info(f"Starting continuous monitoring with {interval_seconds}s interval")
        while self.is_running:
            try:
                await self.run_once()
            except Exception as e:
                self.logger.error(f"Error during strategy execution: {e}", exc_info=True)
            
            await asyncio.sleep(interval_seconds)
    
    async def start(self, interval_seconds: float = 1.0) -> None:
        """
        Start the strategy.
        
        This method handles the lifecycle of the strategy, including initialization
        and setting the running state. It starts the continuous monitoring loop
        in a separate task.
        
        Args:
            interval_seconds: Time to wait between strategy iterations
            
        Returns:
            None
        """
        if self.is_running:
            self.logger.warning("Strategy is already running")
            return
            
        await self.initialize()
        self.is_running = True
        self.logger.info("Strategy started")
        
        # Start the monitoring loop in a separate task
        self.monitoring_task = asyncio.create_task(self.monitor(interval_seconds))
    
    async def stop(self) -> None:
        """
        Stop the strategy gracefully.
        
        This method handles cleanup tasks and sets the running state.
        If the strategy is running a continuous monitoring loop, it will
        be cancelled.
        
        Returns:
            None
        """
        if not self.is_running:
            self.logger.warning("Strategy is not running")
            return
            
        self.is_running = False
        
        # Cancel the monitoring task if it exists
        if self.monitoring_task and not self.monitoring_task.done():
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
            
        self.logger.info("Strategy stopped")
    
    @abstractmethod
    async def get_state(self) -> Dict[str, Any]:
        """
        Get the current state of the strategy.
        
        This method should return a comprehensive view of the strategy's
        internal state, which can be used for monitoring, debugging,
        or state restoration.
        
        Returns:
            Dictionary containing the current state of the strategy
        """
        raise NotImplementedError("Strategy subclass must implement get_state method")
        
    async def cleanup(self) -> None:
        """
        Perform cleanup operations before stopping the strategy.
        
        This method should handle tasks such as closing connections,
        cancelling orders, or persisting state. It is called automatically
        by the stop() method.
        
        Returns:
            None
        """
        # Default implementation does nothing
        pass
