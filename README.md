# Arbitrage Trading Bot

## Basic Philosophy

This project focuses on validating potential arbitrage trades between Binance and Uniswap to avoid losses. The core philosophy is to thoroughly check trades against various scenarios that could go wrong, like network congestion, sudden price movements, suspended deposits/withdrawals or high gas costs. While the bot monitors price differences between platforms it prioritizes careful validation over speed - running extensive checks before executing any trade to protect against common pitfalls in arbitrage trading.

## Arbitrage Conditions and Validations

The arbitrage strategy is based on the following conditions and validations:

- **Price dislocation Check**: The bot price dislocations b/w binance and uniswap. 
- **Slippage Tolerance**: The bot accounts for slippage, ensuring that the price movement during the trade does not erode the expected profit.
- **Gas Fee and transaction costs limit**: Trades are validated against a maximum gas fee limit to ensure that transaction costs do not outweigh the benefits.
- **Order Size Limit**: The bot restricts the size of each order to a predefined limit to manage risk and liquidity.
- **Simulation**: The bot uses simulations to validate arbitrage opportunities. For more details, refer to the.

NOTE: Check `validate_arbitrage_opportunity` function in `arb_strategy.py`

## Issues with code and suggested enhancements

1. **Better Size Calculation**: Currently, an estimation is taken as TOB for Binance and 0.1% of reserve, ideal solution should be done using weighted orderbook price from binance, and [If 2nd is done] estimated best price from uniswap to maintain min profit rate defined in config.

2. **Blocknative Simulation Caching**: Simulations could be cached to run asynchronously for a series of sizes, e.g., 10 USDC, 50 USDC, 100 USDC, 200 USDC...10k USDC, both for buy/sell. This way, when running simulations while validating, we can simply take the expected price of the size above `arb_size` from the tier of sizes. Similarly, gas estimates in `token_monitoring` can be cached. The only problem is they need to be cached in size tiers to have any reasonable accuracy, just like Blocknative. [This and a few other caching strategies will make the detection and validation part a microsecond operation.]

3. **Baseline Risk**: This is the usual way to do it, but due to time constraints, it can't be done. We calculate baseline spot position on the basis of funding rate - simply speaking if funding is negative baseline will be 0 and if funding is positive we use that as propertional to baseline spot position, we maintain a delta-neutral position with short perp while getting paid funding, and we have a ratio of defined in config which we use to spread spot across binance/uniswap. This allows us to move really fast when arbitrage arises, as we don't have to wait for transfers.

4. [Pre-requisite 3] Passive sell order we put at very far from top of the book on binance [wicks] - very useful if a huge move happens on binance and we can instantly hedge that on uniswap. [wicks statiscally make money most of the times]

5. **Usage of Loans**: Very useful during arbitrage if available. If the latency of detection and validation is super low, we have a very good chance of getting a loan at reasonable prices if arbitrage is not due to some exogenous factor.

6. **Private Execution on Uniswap**: For MEV protection.

7. **Better Error Handling and Risk Management**: For example, to maintain baseline risks in each asset ideally at all times.

8. **Extensibility**: The code in its current form is not very extensible in terms of changing strategy or running strategy for multiple pairs. Although some modules were written with that in mind, others had to be written hastily due to lack of time.

9. **Multi-hop Trades on Uniswap**: Basically triangular arbitrage or more than 2-step arbitrages. It is possible with atomicity provided by Uniswap in terms of packing all Uniswap transactions together.

9. [Pre-requisite 2.] - If we've the lateny to evaluate arb in micro-seconds - we should ideally use "monitor_price" as callback in tick by tick data in binance/uniswap, in current state when everything isn't really cached it isn't possible and could lead to congestion.

## How to setup and run

```
pip install -r requirements.txt
Setup config.json in root directory of repo as instructed below. (Can disable simulation if block native API absent.)
python arb_strategy.py
```

## Configuration File (`config.json`)

The `config.json` file contains essential parameters and API keys required for the bot's operation. It includes:

- **API Keys**: Credentials for accessing Binance, Uniswap, infura, block-native.
- **Instrument Configuration**: Details about the trading instruments, including token addresses and pool information.
- **Arbitrage Configuration**: Parameters defining the arbitrage strategy, such as profit thresholds and gas limits.

### Sample `config.json`

```json
{
    "binance_api_key": "your_binance_api_key",
    "binance_api_secret": "your_binance_api_secret",
    "uniswap_private_key": "your_uniswap_private_key",
    "uniswap_address": "your_uniswap_address",
    "infura_url": "https://mainnet.infura.io/v3/your_infura_project_id",
    "infura_ws_url": "wss://mainnet.infura.io/ws/v3/your_infura_project_id",
    "blocknative_api_key": "your_blocknative_api_key",
    "instrument_config": [
        {
            "instrument": "ETHUSDC",
            "pool_address": "your_pool_address",
            "base_token": "ETH",
            "quote_token": "USDC",
            "base_token_address": "your_base_token_address",
            "quote_token_address": "your_quote_token_address"
        }
    ],
    "arb_config": {
        "min_price_dislocation_bps": 100,
        "min_profit_threshold": 0.5,
        "slippage_tolerance": 1.0,
        "gas_fee_limit": 100.0,
        "order_size_limit": 1.0,
        "execution_mode": "both",
        "max_transfer_time_seconds": 10,
        "max_transaction_cost_bps": 50,
        "disable_blocknative_simulation": false,
        "disable_network_level_validations": false,
        "min_rollback_order_size": 0.001
    }
}
```

execution_mode can be:
- "both": Execute arbitrage in both directions (Binance -> Uniswap and Uniswap -> Binance)
- "binance_to_uniswap": buy on binance and sell on uniswap
- "uniswap_to_binance": buy on uniswap and sell on binance

## Arbitrage Configuration Parameters

The `arb_config` section in the `config.json` file defines the parameters for the arbitrage strategy:

- **min_price_dislocation_bps**: Minimum basis points of price dislocation required to consider an arbitrage opportunity.
- **min_profit_threshold**: Minimum profit threshold in percentage required to execute an arbitrage trade.
- **slippage_tolerance**: Maximum acceptable slippage in percentage during trade execution.
- **gas_fee_limit**: Maximum gas fee allowed for executing trades, in Gwei.
- **order_size_limit**: Maximum size of each order to manage risk and liquidity.
- **execution_mode**: Determines the direction of arbitrage execution. Options are "both", "binance_to_uniswap", or "uniswap_to_binance".
- **max_transfer_time_seconds**: Maximum time allowed for asset transfers between exchanges, in seconds.
- **max_transaction_cost_bps**: Maximum transaction cost in basis points allowed for executing trades.
- **disable_blocknative_simulation**: Boolean flag to disable Blocknative simulation for transaction validation.
- **disable_network_level_validations**: Boolean flag to disable network-level validations such as gas costs and slippage.
- **min_rollback_order_size**: Minimum order size required to trigger a rollback in case of execution failure.

## Arbitrage Execution and Rollback

The arbitrage execution process is managed by the `ExecutionEngine` class, located in the `execution_engine.py` file. This class handles the entire lifecycle of an arbitrage trade, including:

1. **Execution**: The engine executes buy and sell trades on the specified exchanges, ensuring that each step is confirmed before proceeding.
2. **Verification**: After each trade and transfer, the engine verifies the operation's success. If any step fails, it initiates a rollback.
3. **Rollback**: In case of failure, the engine unwinds the arbitrage position by selling any acquired assets and canceling pending trades or transfers. This ensures that the system returns to a stable state.

The `ExecutionEngine` ensures atomicity in arbitrage operations, meaning that either all steps are completed successfully, or none are, thus protecting against partial execution risks.

## Module Descriptions

### Token Monitoring

The `