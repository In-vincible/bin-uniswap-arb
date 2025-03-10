import logging

logger = logging.getLogger(__name__)

class ExecutionEngine:
    @classmethod
    async def execute_arb(cls, buy_exchange, sell_exchange, arb_size, arb_instrument, min_rollback_size):
        """
        Execute the entire arbitrage process.

        Args:
            buy_exchange: Connector for the exchange to buy from
            sell_exchange: Connector for the exchange to sell to
            arb_size (float): Size of the arbitrage opportunity
            arb_instrument (str): Trading instrument (e.g., 'ETHUSDT')
            min_rollback_size (float): Minimum size to trigger a rollback sale
        """
        try:
            # Step 1: Execute buy
            buy_trade = await buy_exchange.execute_trade('buy', arb_size)
            logger.info(f"Buy executed: {buy_trade}")

            # Step 2: Confirm buy execution
            confirmed_size = await buy_exchange.confirm_trade(buy_trade)
            logger.info(f"Buy confirmed with size: {confirmed_size}")

            if confirmed_size <= 0:
                logger.error("Buy confirmation failed, initiating rollback.")
                await cls.rollback_trades_and_transfers(buy_exchange, sell_exchange, arb_instrument, min_rollback_size)
                return False

            # Step 3: Transfer bought assets to sell venue
            from_address = await buy_exchange.get_withdraw_address(arb_instrument)
            to_address = await sell_exchange.get_deposit_address(arb_instrument)
            transfer = await buy_exchange.initiate_transfer('withdraw', confirmed_size, from_address, to_address)
            logger.info(f"Transfer initiated: {transfer}")

            # Step 4: Confirm transfer
            confirmed_transfer_size = await sell_exchange.confirm_transfer(transfer)
            logger.info(f"Transfer confirmed with size: {confirmed_transfer_size}")

            if confirmed_transfer_size <= 0:
                logger.error("Transfer confirmation failed, initiating rollback.")
                await cls.rollback_trades_and_transfers(buy_exchange, sell_exchange, arb_instrument, min_rollback_size)
                return False

            # Step 5: Execute sell on sell venue
            sell_trade = await sell_exchange.execute_trade('sell', confirmed_transfer_size)
            logger.info(f"Sell executed: {sell_trade}")

            return True
        except Exception as e:
            logger.error(f"Arbitrage execution failed: {e}")
            await cls.rollback_trades_and_transfers(buy_exchange, sell_exchange, arb_instrument, min_rollback_size)
            return False
    

    @classmethod
    async def rollback_trades_and_transfers(cls, buy_exchange, sell_exchange, arb_instrument, min_rollback_size):
        """
        Rollback trades and transfers if execution fails.

        Args:
            buy_exchange: Connector for the exchange to buy from
            sell_exchange: Connector for the exchange to sell to
            arb_instrument (str): Trading instrument (e.g., 'ETHUSDT')
            min_rollback_size (float): Minimum size to trigger a rollback sale
        """
        logger.info("Unwinding arbitrage position...")
        try:
            # Check current balances
            buy_balance = await buy_exchange.get_balance(arb_instrument)
            sell_balance = await sell_exchange.get_balance(arb_instrument)
            logger.info(f"Current buy balance: {buy_balance}, sell balance: {sell_balance}")

            # Sell any holdings if above min_rollback_size
            if buy_balance > min_rollback_size:
                await buy_exchange.execute_trade('sell', buy_balance)
                logger.info(f"Sold {buy_balance} of {arb_instrument} on buy exchange.")

            if sell_balance > min_rollback_size:
                await sell_exchange.execute_trade('sell', sell_balance)
                logger.info(f"Sold {sell_balance} of {arb_instrument} on sell exchange.")

        except Exception as rollback_error:
            logger.error(f"Rollback failed: {rollback_error}")
