import requests
import json

from config import Config

class BlocknativeSimulator:
    def __init__(self, api_key: str):
        self.api_key = api_key
        # Replace the endpoint below with the current simulation endpoint per Blocknative docs.
        self.simulation_endpoint = "https://api.blocknative.com/transaction-simulation/v1/simulate"
        self.headers = {
            "Authorization": self.api_key,
            "Content-Type": "application/json"
        }
    
    def simulate_transaction(self, tx_data: dict):
        """
        Estimate transaction costs using Blocknative's Gas Price API.
        
        Parameters:
          tx_data (dict): Transaction object containing fields like "from", "to", "value", "gas", "gasPrice", and optionally "data".
                        For example:
                        {
                          "from": "0xYourAddress",
                          "to": "0xRecipientAddress",
                          "value": "0xde0b6b3a7640000",  # 1 ETH in wei (hex)
                          "gas": "0x5208",               # 21000 (hex)
                          "gasPrice": "0x3B9ACA00"        # 1 Gwei in wei (hex)
                        }
        
        Returns:
          dict: The JSON response from Blocknative Gas Price API.
        """
        # New endpoint for gas price estimation
        gas_price_endpoint = "https://api.blocknative.com/gasprices/blockprices"
        
        try:
            response = requests.get(gas_price_endpoint, headers=self.headers)
            response.raise_for_status()  # Raise an error for bad responses
            response_data = response.json()
            
            # Extracting estimated prices with confidence levels
            block_prices = response_data.get('blockPrices', [])
            if block_prices:
                # Assuming we want the highest confidence level available
                highest_confidence_price = max(block_prices[0]['estimatedPrices'], key=lambda x: x['confidence'])
                # Calculate total estimated cost
                total_estimated_cost = highest_confidence_price['price'] + \
                                      tx_data['gasPrice'] * tx_data['gas']
                
                return {
                    'estimated_price': highest_confidence_price['price'],
                    'max_priority_fee_per_gas': highest_confidence_price['maxPriorityFeePerGas'],
                    'max_fee_per_gas': highest_confidence_price['maxFeePerGas'],
                    'total_estimated_cost': total_estimated_cost
                }
            else:
                raise Exception("No block prices available in the response.")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to fetch gas prices: {str(e)}")

# --- Simple test code in __main__ ---
if __name__ == "__main__":
    config = Config()
    # Instantiate the simulator
    simulator = BlocknativeSimulator(config.blocknative_api_key)
    
    # Example transaction data: a simple ETH transfer
    # Note: Values must be provided as hexadecimal strings.
    tx_data = {
        "from": "0xYourAddress",           # Replace with your sending address
        "to": "0xRecipientAddress",        # Replace with the recipient address
        "value": hex(10**18),              # 1 ETH in wei, e.g., 0xde0b6b3a7640000
        "gas": hex(21000),                 # 21000 gas limit
        "gasPrice": hex(10**9)             # 1 Gwei in wei
        # Optionally, include "data": "0x..." if needed.
    }
    
    try:
        result = simulator.simulate_transaction(tx_data)
        print("Simulation Result:")
        print(json.dumps(result, indent=2))
    except Exception as e:
        print("Error during simulation:", e)
