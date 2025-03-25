# Binance-uniswap ARB bot

## Basic Philosophy

This project focuses on validating potential arbitrage trades between Binance and Uniswap to avoid losses. The core philosophy is to thoroughly check trades against various scenarios that could go wrong, like network congestion, sudden price movements, suspended deposits/withdrawals or high gas costs. While the bot monitors price differences between platforms it prioritizes careful validation over speed - running extensive checks before executing any trade to protect against common pitfalls in arbitrage trading.

## Arbitrage Conditions and Validations

The arbitrage strategy is based on the following conditions and validations:

- **Price dislocation Check**: The bot price dislocations b/w binance and uniswap. 
- **Slippage Tolerance**: The bot accounts for slippage, ensuring that the price movement during the trade does not erode the expected profit.
- **Gas Fee and transaction costs limit**: Wrap, Unwrap, Transfer, Swap gas costs are factored in.
- **Order Size Limit**: The bot restricts the size of each order to a predefined limit to manage risk and liquidity. (tob in binance and available liquidity)
- **Network health checks**: Network status, congestion check, transfer time checks against config.

## [Detailed Info on pre-validations](https://github.com/AcheronTrading/acheron-tech-interview-invincible/blob/main/strategies/arb_strategy.py#L109)

1. Capital check (verify if buy exchange has sufficient quote tokens USDC, USDT etc to fund buy trade)
2. [Binance checks](https://github.com/AcheronTrading/acheron-tech-interview-invincible/blob/main/exchanges/binance_connector.py#L397): deposit/withdrawal open, quantity above min withdrawal amount
3. [Uniswap checks](https://github.com/AcheronTrading/acheron-tech-interview-invincible/blob/main/exchanges/uniswap_connector.py#L1218): Network is up, Congestion check: transactions are happening in realtime and aren't stuck, recent transaction on network is below min_tranfer_time in config

## [Detailed Info on profitability checks (as per cost)](https://github.com/AcheronTrading/acheron-tech-interview-invincible/blob/main/strategies/arb_strategy.py#L132)

Profitability is calculated after considering below costs
1. [Binance](https://github.com/AcheronTrading/acheron-tech-interview-invincible/blob/main/exchanges/binance_connector.py#L415): slippage (last price against bid(sell)/ask(buy)), withrawal fee, taker fee
2. [Uniswap](https://github.com/AcheronTrading/acheron-tech-interview-invincible/blob/main/exchanges/uniswap_connector.py#L1331): slippage (on the basis of current liquidity available in tick so that we don't cross tick in either direction), wrap/unwrap gas cost, transfer gas cost, pool fee (static fee of pool), swap gas cost


## [Execution Path](https://github.com/AcheronTrading/acheron-tech-interview-invincible/blob/main/execution_engine.py#L9)

1. Execute buy
2. Confirm buy execution
3. Unwrap base asset if necessary
4. Transfer bought assets to sell venue
5. Confirm Withdrawal
6. Confirm Deposit
7. Wrap base asset if necessary
8. Execute sell on sell venue
9. Confirm Sell

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
export PYTHONPATH=$PYTHONPATH:$(pwd)
python trading_engine.py
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
    "instrument_config": [
        {
            "binance_instrument": "ETHUSDC",
            "uniswap_instrument": "WETH",
            "pool_address": "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
            "binance_base_asset": "ETH",
            "binance_quote_asset": "USDC",
            "uniswap_base_asset": "WETH",
            "uniswap_quote_asset": "USDC",
            "enable_flash_bot": false
        }
    ],
    "arb_config": {
        "min_price_dislocation_bps": 100,
        "min_profit_bps": 5,
        "size_scale_factor": 0.1,
        "slippage_tolerance": 1.0,
        "order_size_limit": 1.0,
        "execution_mode": "both",
        "max_transfer_time_seconds": 10,
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
- **min_profit_bps**: Minimum profit threshold in bps required to execute an arbitrage trade after costs.
- **slippage_tolerance**: Maximum acceptable slippage in percentage during trade execution.
- **order_size_limit**: Maximum size of each order to manage risk and liquidity.
- **execution_mode**: Determines the direction of arbitrage execution. Options are "both", "binance_to_uniswap", or "uniswap_to_binance".
- **max_transfer_time_seconds**: Maximum time allowed for asset transfers between exchanges, in seconds.
- **min_rollback_order_size**: Minimum order size required to trigger a rollback in case of execution failure.
- **enable_flash_bot**: To enable/disable swaps via flashbot (default: V3 router)

## Arbitrage Execution and Rollback

The arbitrage execution process is managed by the `ExecutionEngine` class, located in the `execution_engine.py` file. This class handles the entire lifecycle of an arbitrage trade, including:

1. **Execution**: The engine executes buy and sell trades on the specified exchanges, ensuring that each step is confirmed before proceeding.
2. **Verification**: After each trade and transfer, the engine verifies the operation's success. If any step fails, it initiates a rollback.
3. **Rollback**: In case of failure, the engine unwinds the arbitrage position by selling any acquired assets and canceling pending trades or transfers. This ensures that the system returns to a stable state.

The `ExecutionEngine` ensures atomicity in arbitrage operations, meaning that either all steps are completed successfully, or none are, thus protecting against partial execution risks.

## Module Descriptions

### Token Monitoring

The `token_monitoring` module is responsible for tracking token prices and network conditions. It provides real-time data on token balances, network congestion, and gas prices, which are crucial for making informed trading decisions.

### Binance Connector

The `binance_connector` module interfaces with the Binance API to fetch market data and execute trades. It manages market data subscriptions and provides methods for placing orders, withdrawing funds, and checking account balances.

### Uniswap Connector

The `uniswap_connector` module interacts with the Uniswap protocol to monitor pool prices and execute swaps. It handles the initialization of pool and token information, price, liquidity, reserve calculations and balance updates. [All calculations as per uniswap V3 white paper]

### Arbitrage Strategy

The `arb_strategy` module implements the core logic for identifying and executing arbitrage opportunities. It integrates data from both Binance and Uniswap connectors and uses the `token_monitoring` module to validate trades against current network conditions.

### Blocknative Simulator

The Blocknative simulator is used to simulate Ethereum transactions and network conditions. It provides insights into potential transaction outcomes, helping to optimize gas fees and execution strategies. This tool is essential for testing and refining the bot's performance in a controlled environment.

### Execution Engine

To execute arb (buy => transfer => sell) and rollback in cases when required.

### Config

To load config.


