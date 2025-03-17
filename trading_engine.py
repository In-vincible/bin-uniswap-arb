import asyncio
import signal
import sys
import argparse
import logging

from config import Config
from utils.log_utils import setup_logger, get_logger
from strategies.arb_strategy import ArbitrageStrategy

DEFAULT_LOGFILE = 'crypto-trading-engine.log'

class TradingEngine:
    """
    Crypto Trading Engine that manages the arbitrage strategy.
    
    This engine orchestrates the execution of the arbitrage strategy,
    handling its lifecycle, monitoring its performance, and ensuring graceful
    shutdown when needed. It uses asyncio for execution which is well-suited 
    for I/O-bound operations like API calls.
    """
    
    def __init__(self):
        """
        Initialize the trading engine with the arbitrage strategy.
        """
        self.logger = get_logger(__name__)
        self.running = False
        self.config = Config()
        
        # Initialize the arbitrage strategy
        self.strategy = ArbitrageStrategy(self.config)
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, lambda sig, frame: asyncio.create_task(self.exit_gracefully()))
        signal.signal(signal.SIGTERM, lambda sig, frame: asyncio.create_task(self.exit_gracefully()))
    
    async def run_strategy(self) -> None:
        """
        Run the arbitrage strategy.
        """
        self.logger.info("Starting arbitrage strategy")
        
        # Initialize and start the strategy
        await self.strategy.start()
        # Keep the event loop alive while strategy runs
        while self.running:
            await asyncio.sleep(1)
    
    async def run(self) -> None:
        """
        Run the trading engine.
        """
        self.running = True
        self.logger.info("Starting Trading Engine with arbitrage strategy")
        
        try:
            # Run the strategy until interrupted
            await self.run_strategy()
        except Exception as e:
            self.logger.error(f"Error running strategy: {e}", exc_info=True)
            await self.exit_gracefully()
    
    async def cleanup(self) -> None:
        """
        Clean up resources before shutting down.
        
        This includes stopping the strategy gracefully.
        """
        self.logger.info("Cleaning up trading engine resources")
        
        try:
            await self.strategy.stop()
        except Exception as e:
            self.logger.error(f"Error stopping strategy: {e}", exc_info=True)
        
        self.logger.info("Arbitrage strategy stopped")
    
    async def exit_gracefully(self) -> None:
        """
        Handle graceful shutdown of the trading engine.
        """
        if not self.running:  # Prevent multiple exit attempts
            return
            
        self.logger.info("Shutting down trading engine gracefully")
        self.running = False
        
        # Cleanup will stop the strategy
        await self.cleanup()
        
        self.logger.info("Trading engine shutdown complete")
        sys.exit(0)


async def main():
    """
    Entry point when running the trading engine as a standalone script.
    """
    parser = argparse.ArgumentParser(description='Crypto Trading Engine')
    parser.add_argument('--log', type=str, default=DEFAULT_LOGFILE, help='Log file path')
    args = parser.parse_args()
    
    # Set up logging
    setup_logger(args.log)
    logger = get_logger()
    logger.info("Starting Crypto Trading Engine with arbitrage strategy")
    
    # Initialize and run the trading engine
    trading_engine = TradingEngine()
    try:
        await trading_engine.run()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        await trading_engine.exit_gracefully()
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        await trading_engine.exit_gracefully()


if __name__ == "__main__":
    asyncio.run(main()) 