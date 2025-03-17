import requests
import json

from config import Config

class BlocknativeSimulator:
    def __init__(self, api_key: str):
        self.api_key = api_key
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

    def simulate_uniswap_swap(self, tx_data: dict, pool_address: str, amount_in: float, token_in: str, token_out: str):
        """
        Simulate a Uniswap swap transaction using Blocknative's API.
        
        Parameters:
            tx_data (dict): Base transaction object with from/to addresses and gas parameters
            pool_address (str): Address of the Uniswap pool
            amount_in (float): Input amount for the swap
            token_in (str): Address of input token
            token_out (str): Address of output token
            
        Returns:
            dict: Simulation results including estimated costs and output amount
        """
        # Endpoint for simulating contract interactions
        simulation_endpoint = "https://api.blocknative.com/simulate"

        # Encode the swap function call
        # This is a simplified example - actual encoding would depend on the specific Uniswap function
        swap_data = {
            "methodName": "swap",
            "params": {
                "amountIn": amount_in,
                "tokenIn": token_in,
                "tokenOut": token_out,
                "recipient": tx_data["from"]
            }
        }

        # Build simulation payload
        payload = {
            "system": "ethereum", 
            "network": "main",
            "transaction": {
                **tx_data,
                "to": pool_address,
                "data": swap_data
            }
        }

        try:
            response = requests.post(
                simulation_endpoint, 
                headers=self.headers,
                json=payload
            )
            response.raise_for_status()
            simulation_data = response.json()

            # Extract relevant simulation results
            result = {
                'success': simulation_data.get('success', False),
                'estimated_gas_used': simulation_data.get('gasUsed'),
                'estimated_output': simulation_data.get('outputAmount'),
                'total_cost': simulation_data.get('totalCost'),
                'simulation_status': simulation_data.get('status'),
                'error': simulation_data.get('error')
            }

            # Get current gas prices to estimate total transaction cost
            gas_price_data = self.simulate_transaction(tx_data)
            result['total_estimated_cost'] = (
                result['estimated_gas_used'] * gas_price_data['estimated_price']
            )

            return result

        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to simulate Uniswap swap: {str(e)}")

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
