import asyncio
import signal
import sys
import argparse
from typing import Dict, List, Any, Optional
import logging

from config import Config
from utils.log_utils import setup_logger, get_logger
from strategies.arb_strategy import ArbitrageStrategy
from strategies.base_strategy import BaseStrategy

# Configure default parameters
TRADING_ENGINE_INTERVAL_SECS = 30
DEFAULT_LOGFILE = 'crypto-trading-engine.log'

class TradingEngine:
    """
    Crypto Trading Engine that manages multiple arbitrage strategies.
    
    This engine orchestrates the execution of various crypto arbitrage strategies,
    handling their lifecycle, monitoring their performance, and ensuring graceful
    shutdown when needed. It uses asyncio for concurrent strategy execution instead
    of threading, which is well-suited for I/O-bound operations like API calls.
    """
    
    def __init__(self, strategy_names: List[str], config_path: Optional[str] = None):
        """
        Initialize the trading engine with multiple strategies.
        
        Args:
            strategy_names: List of strategy names to run
            config_path: Optional path to configuration file
        """
        self.logger = logging.getLogger(__name__)
        self.strategy_names = strategy_names
        self.strat_contexts = {}
        self.running = False
        self.config = Config(config_path) if config_path else Config()
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, lambda sig, frame: asyncio.create_task(self.exit_gracefully()))
        signal.signal(signal.SIGTERM, lambda sig, frame: asyncio.create_task(self.exit_gracefully()))
        
        # Initialize strategy contexts
        self._initialize_strategies()
    
    def _initialize_strategies(self) -> None:
        """
        Initialize all strategy instances based on configuration.
        
        This method creates strategy objects for each requested strategy and
        stores them in the strategy context dictionary.
        """
        for strategy_name in self.strategy_names:
            # For now we only support arbitrage strategies
            if strategy_name == "arbitrage":
                strategy = ArbitrageStrategy(self.config)
                self.strat_contexts[strategy_name] = {
                    'strategy': strategy,
                    'active': True,
                    'last_run_time': 0,
                    'statistics': {
                        'opportunities_found': 0,
                        'trades_executed': 0,
                        'failed_executions': 0
                    }
                }
            else:
                self.logger.warning(f"Strategy {strategy_name} not supported")
    
    async def run_strategy(self, strategy_name: str, context: Dict[str, Any]) -> None:
        """
        Run a single strategy in a continuous loop.
        
        Args:
            strategy_name: Name of the strategy to run
            context: Dictionary containing strategy context
        """
        strategy = context['strategy']
        self.logger.info(f"Initializing {strategy_name} strategy")
        
        # Initialize the strategy
        await strategy.initialize()
        self.logger.info(f"{strategy_name} strategy initialized")
        
        # Start the strategy's internal loop if using BaseStrategy
        if isinstance(strategy, BaseStrategy):
            await strategy.start()
            
        # Run the strategy evaluation loop
        while self.running and context['active']:
            try:
                # Analyze market for opportunities
                analysis_result = await strategy.analyze()
                
                # Update context statistics
                if analysis_result.get('basic_opportunity', False):
                    context['statistics']['opportunities_found'] += 1
                    self.logger.info(f"Opportunity found: {analysis_result}")
                    
                    # Run pre-validations
                    if await strategy.run_pre_validations(analysis_result):
                        # Verify profitability
                        if await strategy.verify_profitability_against_costs(analysis_result):
                            analysis_result['should_execute'] = True
                
                # Execute if opportunity is valid
                if analysis_result.get('should_execute', False):
                    self.logger.info(f"Executing opportunity")
                    try:
                        execution_result = await strategy.execute(analysis_result)
                        if execution_result.get('success', False):
                            context['statistics']['trades_executed'] += 1
                            self.logger.info(f"Execution successful: {execution_result}")
                        else:
                            context['statistics']['failed_executions'] += 1
                            self.logger.warning(f"Execution failed: {execution_result}")
                    except Exception as e:
                        context['statistics']['failed_executions'] += 1
                        self.logger.error(f"Error executing {strategy_name}: {e}", exc_info=True)
                
                # Wait for next interval
                await asyncio.sleep(TRADING_ENGINE_INTERVAL_SECS)
                
            except Exception as e:
                self.logger.error(f"Error in strategy {strategy_name}: {e}", exc_info=True)
                await asyncio.sleep(TRADING_ENGINE_INTERVAL_SECS)
    
    async def run_strategies(self) -> None:
        """
        Run all strategies concurrently using asyncio tasks.
        """
        self.running = True
        self.logger.info(f"Starting Trading Engine with strategies: {', '.join(self.strategy_names)}")
        
        # Create tasks for each strategy
        tasks = []
        for strategy_name, context in self.strat_contexts.items():
            task = asyncio.create_task(self.run_strategy(strategy_name, context))
            tasks.append(task)
        
        # Wait for all tasks to complete (they should run indefinitely until shutdown)
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def cleanup(self) -> None:
        """
        Clean up resources before shutting down.
        
        This includes canceling open orders and stopping strategies gracefully.
        """
        self.logger.info("Cleaning up trading engine resources")
        cleanup_tasks = []
        
        for strategy_name, context in self.strat_contexts.items():
            strategy = context['strategy']
            self.logger.info(f"Stopping {strategy_name} strategy")
            
            # For BaseStrategy instances, call stop()
            if isinstance(strategy, BaseStrategy):
                cleanup_task = asyncio.create_task(strategy.stop())
                cleanup_tasks.append(cleanup_task)
        
        # Wait for all cleanup tasks to complete
        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)
        
        self.logger.info("All strategies stopped")
    
    async def exit_gracefully(self) -> None:
        """
        Handle graceful shutdown of the trading engine.
        """
        self.logger.info("Shutting down trading engine gracefully")
        self.running = False
        await self.cleanup()
        self.logger.info("Trading engine shutdown complete")
        # Exit the program
        sys.exit(0)
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics for all running strategies.
        
        Returns:
            Dictionary containing statistics for all strategies
        """
        stats = {}
        for strategy_name, context in self.strat_contexts.items():
            stats[strategy_name] = context['statistics']
        return stats
    
    async def toggle_strategy(self, strategy_name: str, active: bool) -> bool:
        """
        Toggle a strategy on or off.
        
        Args:
            strategy_name: Name of the strategy to toggle
            active: Whether to activate or deactivate the strategy
            
        Returns:
            Boolean indicating success or failure
        """
        if strategy_name in self.strat_contexts:
            context = self.strat_contexts[strategy_name]
            context['active'] = active
            self.logger.info(f"Strategy {strategy_name} {'activated' if active else 'deactivated'}")
            return True
        
        self.logger.warning(f"Strategy {strategy_name} not found")
        return False


async def main():
    """
    Entry point when running the trading engine as a standalone script.
    """
    parser = argparse.ArgumentParser(description='Crypto Trading Engine')
    parser.add_argument('strategies', type=str, nargs='+', help='Trading Strategy names')
    parser.add_argument('--config', type=str, help='Path to configuration file')
    parser.add_argument('--log', type=str, default=DEFAULT_LOGFILE, help='Log file path')
    args = parser.parse_args()
    
    # Set up logging
    setup_logger(args.log)
    logger = get_logger()
    logger.info(f"Starting Crypto Trading Engine with strategies: {', '.join(args.strategies)}")
    
    # Initialize and run the trading engine
    trading_engine = TradingEngine(args.strategies, args.config)
    try:
        await trading_engine.run_strategies()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        await trading_engine.exit_gracefully()
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        await trading_engine.exit_gracefully()


if __name__ == "__main__":
    asyncio.run(main()) 