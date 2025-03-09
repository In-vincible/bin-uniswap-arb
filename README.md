# Arbitrage Trading Bot

## Basic Philosophy

This project focuses on validating potential arbitrage trades between Binance and Uniswap to avoid losses. The core philosophy is to thoroughly check trades against various scenarios that could go wrong, like network congestion sudden price movements, or high gas costs. While the bot monitors price differences between platforms it prioritizes careful validation over speed - running extensive checks before executing any trade to protect against common pitfalls in arbitrage trading.

## Arbitrage Conditions and Validations

The arbitrage strategy is based on the following conditions and validations:

NOTE: `validate_arbitrage_opportunity` function in `arb_strategy.py`

- **Profitability Check**: The bot calculates potential profit by comparing prices on Binance and Uniswap. A trade is executed only if the profit exceeds the `min_profit_threshold` defined in the configuration.
- **Slippage Tolerance**: The bot accounts for slippage, ensuring that the price movement during the trade does not erode the expected profit.
- **Gas Fee Limit**: Trades are validated against a maximum gas fee limit to ensure that transaction costs do not outweigh the benefits.
- **Order Size Limit**: The bot restricts the size of each order to a predefined limit to manage risk and liquidity.
- **Simulation**: The bot uses simulations to validate arbitrage opportunities. For more details, refer to the.

## Future Enhancements

1. **Better Size Calculation**: Currently, an estimation is taken as TOB for Binance and 0.1% of reserve.

2. **Blocknative Simulation Caching**: Simulations could be cached to run asynchronously for a series of sizes, e.g., 10 USDC, 50 USDC, 100 USDC, 200 USDC...10k USDC, both for buy/sell. This way, when running simulations while validating, we can simply take the expected price of the size above `arb_size` from the tier of sizes. Similarly, gas estimates in `token_monitoring` can be cached. The only problem is they need to be cached in size tiers to have any reasonable accuracy, just like Blocknative. This and a few other caching strategies will make the detection and validation part a microsecond operation.

3. **Baseline Risk**: This is the usual way to do it, but due to time constraints, it can't be done. Basically, let's say we are interested in arbitrage of a token with positive funding. We can have a delta-neutral position while getting paid funding, and we have a ratio of sizes spread across Uniswap and Binance. This allows us to move really fast when arbitrage arises, as we don't have to wait for transfers.

4. **Usage of Loans**: Very useful during arbitrage if available. If the latency of detection and validation is super low, we have a very good chance of getting a loan at reasonable prices if arbitrage is not due to some exogenous factor.

5. **Private Execution on Uniswap**: For MEV protection.

6. **Better Error Handling and Risk Management**: For example, to maintain baseline risks in each asset ideally at all times.

7. **Extensibility**: The code in its current form is not very extensible in terms of changing strategy or running strategy for multiple pairs. Although some modules were written with that in mind, others had to be written hastily due to lack of time.

8. **Multi-hop Trades on Uniswap**: Basically triangular arbitrage or more than 2-step arbitrages. It is possible with atomicity provided by Uniswap in terms of packing all Uniswap transactions together.

## Configuration File (`config.json`)

The `config.json` file contains essential parameters and API keys required for the bot's operation. It includes:

- **API Keys**: Credentials for accessing Binance and Uniswap services.
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
        "min_profit_threshold": 0.5,
        "slippage_tolerance": 1.0,
        "gas_fee_limit": 100.0,
        "order_size_limit": 1.0,
        "execution_mode": "both"
    }
}
```

## Module Descriptions

### Token Monitoring

The `token_monitoring` module is responsible for tracking token prices and network conditions. It provides real-time data on token balances, network congestion, and gas prices, which are crucial for making informed trading decisions.

### Binance Connector

The `binance_connector` module interfaces with the Binance API to fetch market data and execute trades. It manages market data subscriptions and provides methods for placing orders, withdrawing funds, and checking account balances.

### Uniswap Connector

The `uniswap_connector` module interacts with the Uniswap protocol to monitor pool prices and execute swaps. It handles the initialization of pool and token information, price calculations, and balance updates.

### Arbitrage Strategy

The `arb_strategy` module implements the core logic for identifying and executing arbitrage opportunities. It integrates data from both Binance and Uniswap connectors and uses the `token_monitoring` module to validate trades against current network conditions.

### Blocknative Simulator

The Blocknative simulator is used to simulate Ethereum transactions and network conditions. It provides insights into potential transaction outcomes, helping to optimize gas fees and execution strategies. This tool is essential for testing and refining the bot's performance in a controlled environment.

---

This README now includes the specified future enhancements and rearranged sections as requested. Let me know if you need any further modifications or additional details!
