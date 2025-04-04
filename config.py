import logging
from dataclasses import dataclass

@dataclass
class Config:
    """
    Holds configuration parameters for the arbitrage bot.
    
    WHY: Centralizing configuration helps maintain flexibility, security, and
         easier modifications for deployment or testing environments.
    """
    execution_mode: str = "both"          # Options: 'binance_to_uniswap', 'uniswap_to_binance', 'both'
    binance_api_key: str = ""
    binance_api_secret: str = ""
    infura_url: str = ""
    infura_ws_url: str = ""
    wallet_private_key: str = ""
    blocknative_api_key: str = ""
    uniswap_subgraph_api_key: str = ""

    def __post_init__(self):
        """Load API keys and URLs from config file after initialization"""
        import json
        try:
            with open('config.json') as f:
                config = json.load(f)
                self.binance_api_key = config['binance_api_key']
                self.binance_api_secret = config['binance_api_secret'] 
                self.infura_url = config['infura_url']
                self.wallet_private_key = config['uniswap_private_key']
                self.infura_ws_url = config['infura_ws_url']
                self.instrument_config = config['instrument_config']
                self.arb_config = config['arb_config']
                self.blocknative_api_key = config.get('blocknative_api_key')
                self.disable_network_level_validations = config.get('disable_network_level_validations', False)
                self.uniswap_subgraph_api_key = config.get('uniswap_subgraph_api_key')
        except Exception as e:
            logging.error(f"Error loading config.json: {e}")
            logging.error(f"Please add config.json to the root directory with appropriate API keys and configuration as described in the README")

# Example usage
if __name__ == "__main__":
    config = Config()
    print(config)
