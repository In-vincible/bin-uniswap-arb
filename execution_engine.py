import logging
from exchanges.base_connector import BaseExchange

logger = logging.getLogger(__name__)

class ExecutionEngine:
    @classmethod
    async def execute_arb(cls, buy_exchange: BaseExchange, sell_exchange: BaseExchange, arb_size: float, arb_instrument: str, min_rollback_size: float):
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

            # Step 3: Unwrap base asset if necessary
            unwrapped_confirmed_size = await buy_exchange.unwrap_asset(arb_instrument, confirmed_size)
            logger.info(f"Unwrapped confirmed size: {unwrapped_confirmed_size}")

            # Step 4: Transfer bought assets to sell venue
            deposit_address = await sell_exchange.get_deposit_address(arb_instrument)
            withdrawal_info = await buy_exchange.withdraw(arb_instrument, deposit_address, unwrapped_confirmed_size)
            logger.info(f"Transfer initiated: {withdrawal_info}")

            # Step 5: Confirm Withdrawal
            withdrawal_confirmation = await buy_exchange.confirm_withdrawal(withdrawal_info)
            if withdrawal_confirmation <= 0:
                logger.error("Withdrawal confirmation failed, initiating rollback.")
                await cls.rollback_trades_and_transfers(buy_exchange, sell_exchange, arb_instrument, min_rollback_size)
                return False
            logger.info(f"Withdrawal confirmed with size: {withdrawal_confirmation}")

            # Step 6: Confirm Deposit
            deposit_confirmation = await sell_exchange.confirm_deposit(arb_instrument, withdrawal_confirmation)
            if deposit_confirmation <= 0:
                logger.error("Deposit confirmation failed, initiating rollback.")
                await cls.rollback_trades_and_transfers(buy_exchange, sell_exchange, arb_instrument, min_rollback_size)
                return False
            logger.info(f"Deposit confirmed with size: {deposit_confirmation}")

            # Step 7: Wrap base asset if necessary
            wrapped_deposit_confirmation = await sell_exchange.wrap_asset(arb_instrument, deposit_confirmation)

            # Step 8: Execute sell on sell venue
            sell_trade = await sell_exchange.execute_trade('sell', wrapped_deposit_confirmation)
            logger.info(f"Sell executed: {sell_trade}")

            # Step 9: Confirm Sell
            confirmed_sell_size = await sell_exchange.confirm_trade(sell_trade)
            if confirmed_sell_size <= 0:
                logger.error("Sell confirmation failed, initiating rollback.")
                await cls.rollback_trades_and_transfers(buy_exchange, sell_exchange, arb_instrument, min_rollback_size)
                return False
            logger.info(f"Sell confirmed with size: {confirmed_sell_size}")

            return True
        except Exception as e:
            logger.error(f"Arbitrage execution failed: {e}")
            await cls.rollback_trades_and_transfers(buy_exchange, sell_exchange, arb_instrument, min_rollback_size)
            return False
    

    @classmethod
    async def rollback_trades_and_transfers(cls, buy_exchange: BaseExchange, sell_exchange: BaseExchange, arb_instrument: str, min_rollback_size: float):
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
            buy_exchange_balance = await buy_exchange.get_base_asset_balance()
            sell_exchange_balance = await sell_exchange.get_base_asset_balance()
            logger.info(f"Current buy balance: {buy_exchange_balance}, sell balance: {sell_exchange_balance}")

            # Sell any holdings if above min_rollback_size
            # First wrap if necessary and then sell (In case there was some error during wrapping/unwrapping)
            if buy_exchange_balance > min_rollback_size:
                wrapped_buy_exchange_balance = await buy_exchange.wrap_asset(arb_instrument, buy_exchange_balance)
                await buy_exchange.execute_trade('sell', wrapped_buy_exchange_balance)
                logger.info(f"Sold {wrapped_buy_exchange_balance} of {arb_instrument} on buy exchange.")

            if sell_exchange_balance > min_rollback_size:
                wrapped_sell_exchange_balance = await sell_exchange.wrap_asset(arb_instrument, sell_exchange_balance)
                await sell_exchange.execute_trade('sell', wrapped_sell_exchange_balance)
                logger.info(f"Sold {wrapped_sell_exchange_balance} of {arb_instrument} on sell exchange.")

        except Exception as rollback_error:
            logger.error(f"Rollback failed: {rollback_error}")
