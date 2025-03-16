import requests
import logging
import time
from typing import Dict, List, Optional, Union, Any, Generator
from requests.exceptions import RequestException, Timeout, ConnectionError

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("uniswap_subgraph")


class UniswapSubgraph:
    """
    A client for interacting with the Uniswap V3 subgraph API.
    
    This class provides methods to query Uniswap V3 data using The Graph protocol,
    with a focus on fetching liquidity positions for specific pools. It includes
    robust error handling, rate limiting, and pagination support.
    """
    
    # Current working Uniswap v3 subgraph endpoints as of March 2025
    # These endpoints are on The Graph's decentralized network
    MAINNET_ENDPOINT = "https://gateway-arbitrum.network.thegraph.com/api/fa9bb59172cc462fd71da21c6ef69e21/subgraphs/id/ELUcwgpm14LKPLrBRuVvPvNKHQ9HvwmtKgKSH6123cr7"
    OPTIMISM_ENDPOINT = "https://gateway-arbitrum.network.thegraph.com/api/fa9bb59172cc462fd71da21c6ef69e21/subgraphs/id/HUZDsRpEVP9yssM8TRmmxHeuVUKtMrG9Dz8HJfYs8UF9"
    ARBITRUM_ENDPOINT = "https://gateway-arbitrum.network.thegraph.com/api/fa9bb59172cc462fd71da21c6ef69e21/subgraphs/id/3oF1d3rKSz2jCKL2uKVLnurPyDknp6sMLhgssUmvfaEJ"
    POLYGON_ENDPOINT = "https://gateway-arbitrum.network.thegraph.com/api/fa9bb59172cc462fd71da21c6ef69e21/subgraphs/id/F9qpRzj2hZXUvmYiKJCjnErV4MUkhBQo3F2fZypHF3k"
    
    # Common GraphQL queries
    POSITION_QUERY = """
    query get_positions($num_skip: Int, $pool_id: ID!, $min_liquidity: BigInt!) {
      positions(
        skip: $num_skip, 
        where: {pool: $pool_id, liquidity_gt: $min_liquidity}, 
        orderBy: liquidity, 
        orderDirection: desc
      ) {
        id
        tickLower { tickIdx }
        tickUpper { tickIdx }
        liquidity
        owner
        depositedToken0
        depositedToken1
        withdrawnToken0
        withdrawnToken1
        feeGrowthInside0LastX128
        feeGrowthInside1LastX128
        createdAtTimestamp
      }
    }
    """
    
    # Simple query to test connectivity
    TEST_QUERY = """
    query {
      pools(first: 5, orderBy: totalValueLockedUSD, orderDirection: desc) {
        id
        token0 {
          symbol
        }
        token1 {
          symbol
        }
        feeTier
        liquidity
        totalValueLockedUSD
      }
    }
    """
    
    def __init__(
        self, 
        network: str = "mainnet", 
        custom_endpoint: Optional[str] = None,
        max_retries: int = 3, 
        retry_delay: int = 2,
        timeout: int = 10,
        page_size: int = 100
    ):
        """
        Initialize the Uniswap subgraph client.
        
        Args:
            network: The blockchain network to query ("mainnet", "arbitrum", "optimism", "polygon")
            custom_endpoint: Optional custom subgraph endpoint URL
            max_retries: Maximum number of retry attempts for failed requests
            retry_delay: Base delay between retries in seconds (uses exponential backoff)
            timeout: Request timeout in seconds
            page_size: Number of records to fetch per page when paginating
        """
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = timeout
        self.page_size = page_size
        
        # Set endpoint based on input parameters
        if custom_endpoint:
            self.endpoint = custom_endpoint
        else:
            self.endpoint = self._get_network_endpoint(network)
            
        logger.info(f"Initialized Uniswap subgraph client for {network} at {self.endpoint}")
    
    def _get_network_endpoint(self, network: str) -> str:
        """
        Get the appropriate subgraph endpoint URL for the specified network.
        
        Args:
            network: The blockchain network name
            
        Returns:
            The subgraph endpoint URL
            
        Raises:
            ValueError: If an unsupported network is specified
        """
        network = network.lower()
        
        if network == "mainnet":
            return self.MAINNET_ENDPOINT
        elif network == "arbitrum":
            return self.ARBITRUM_ENDPOINT
        elif network == "optimism":
            return self.OPTIMISM_ENDPOINT
        elif network == "polygon":
            return self.POLYGON_ENDPOINT
        else:
            raise ValueError(
                f"Unsupported network: {network}. "
                f"Supported networks: mainnet, arbitrum, optimism, polygon"
            )
    
    def _execute_query(
        self, 
        query: str, 
        variables: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Execute a GraphQL query against the Uniswap subgraph.
        
        Args:
            query: The GraphQL query string
            variables: Variables to include with the GraphQL query
            
        Returns:
            The query response data
            
        Raises:
            RequestException: If the request fails after all retries
        """
        if variables is None:
            variables = {}
            
        payload = {
            "query": query,
            "variables": variables
        }
        
        headers = {
            "Content-Type": "application/json"
        }
        
        # Implement retry logic with exponential backoff
        for attempt in range(self.max_retries):
            try:
                response = requests.post(
                    self.endpoint,
                    json=payload,
                    headers=headers,
                    timeout=self.timeout
                )
                
                # Check for HTTP errors
                response.raise_for_status()
                
                # Parse response
                result = response.json()
                
                # Check for GraphQL errors
                if "errors" in result:
                    error_msg = "; ".join([err.get("message", "Unknown error") for err in result["errors"]])
                    logger.warning(f"GraphQL query returned errors: {error_msg}")
                    
                    # Check for rate limiting errors and back off if needed
                    if any("rate limit" in str(err).lower() for err in result["errors"]):
                        retry_delay = self.retry_delay * (2 ** attempt)
                        logger.info(f"Rate limited. Retrying in {retry_delay}s (attempt {attempt + 1}/{self.max_retries})")
                        time.sleep(retry_delay)
                        continue
                
                # If we have data, return it even if there were some errors
                if "data" in result:
                    return result["data"]
                    
                # If we got here, we have errors but no data
                raise RequestException(f"GraphQL query failed: {error_msg}")
                
            except (ConnectionError, Timeout) as e:
                # Network-related errors
                retry_delay = self.retry_delay * (2 ** attempt)
                logger.warning(f"Request failed: {str(e)}. Retrying in {retry_delay}s (attempt {attempt + 1}/{self.max_retries})")
                time.sleep(retry_delay)
                
            except RequestException as e:
                # Define error message 
                error_msg = str(e)
                
                # Rate limiting errors
                if "429" in error_msg or "Too Many Requests" in error_msg:
                    retry_delay = self.retry_delay * (2 ** attempt)
                    logger.warning(f"Rate limited. Retrying in {retry_delay}s (attempt {attempt + 1}/{self.max_retries})")
                    time.sleep(retry_delay)
                else:
                    # Other request errors
                    logger.error(f"Request failed: {error_msg}")
                    if attempt < self.max_retries - 1:
                        retry_delay = self.retry_delay * (2 ** attempt)
                        logger.info(f"Retrying in {retry_delay}s (attempt {attempt + 1}/{self.max_retries})")
                        time.sleep(retry_delay)
                    else:
                        raise RequestException(f"Failed after {self.max_retries} attempts: {error_msg}")
        
        # If we've exhausted all retries
        raise RequestException(f"Failed to execute query after {self.max_retries} retries")
    
    def get_pool_positions(
        self, 
        pool_id: str, 
        min_liquidity: int = 0, 
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all liquidity positions for a specific Uniswap V3 pool.
        
        Args:
            pool_id: The Uniswap pool ID (contract address)
            min_liquidity: Minimum liquidity threshold for positions
            limit: Maximum number of positions to return (None for all)
            
        Returns:
            List of position data dictionaries
        """
        all_positions = []
        current_skip = 0
        
        # If pool_id doesn't start with '0x', add it
        if not pool_id.startswith('0x'):
            pool_id = f'0x{pool_id}'
            
        # Normalize pool_id to lowercase
        pool_id = pool_id.lower()
        
        logger.info(f"Fetching liquidity positions for pool {pool_id}")
        
        # Keep fetching until we get all positions or hit the limit
        while True:
            variables = {
                "pool_id": pool_id,
                "num_skip": current_skip,
                "min_liquidity": str(min_liquidity)  # Ensure min_liquidity is a string
            }
            
            try:
                result = self._execute_query(self.POSITION_QUERY, variables)
                
                positions = result.get("positions", [])
                
                # Break if no more positions
                if not positions:
                    break
                    
                # Add positions to result list
                all_positions.extend(positions)
                logger.debug(f"Fetched {len(positions)} positions (total: {len(all_positions)})")
                
                # Check if we've hit the limit
                if limit and len(all_positions) >= limit:
                    all_positions = all_positions[:limit]
                    break
                
                # Update skip value for next page
                current_skip += self.page_size
                
                # If we got fewer positions than page_size, we've reached the end
                if len(positions) < self.page_size:
                    break
                    
            except Exception as e:
                logger.error(f"Error fetching positions: {str(e)}")
                break
        
        logger.info(f"Fetched a total of {len(all_positions)} positions for pool {pool_id}")
        return all_positions
    
    def get_pool_positions_generator(
        self, 
        pool_id: str, 
        min_liquidity: int = 0, 
        batch_size: int = 100
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Get liquidity positions for a pool as a generator to conserve memory.
        
        This is useful when dealing with pools that have a large number of positions
        and you want to process them in batches without loading everything into memory.
        
        Args:
            pool_id: The Uniswap pool ID (contract address)
            min_liquidity: Minimum liquidity threshold for positions
            batch_size: Number of positions to fetch in each batch
            
        Yields:
            Position data dictionaries, one at a time
        """
        current_skip = 0
        
        # If pool_id doesn't start with '0x', add it
        if not pool_id.startswith('0x'):
            pool_id = f'0x{pool_id}'
            
        # Normalize pool_id to lowercase
        pool_id = pool_id.lower()
        
        logger.info(f"Streaming liquidity positions for pool {pool_id}")
        
        # Keep fetching until we run out of positions
        while True:
            variables = {
                "pool_id": pool_id,
                "num_skip": current_skip,
                "min_liquidity": str(min_liquidity)  # Ensure min_liquidity is a string
            }
            
            try:
                result = self._execute_query(self.POSITION_QUERY, variables)
                
                positions = result.get("positions", [])
                
                # Break if no more positions
                if not positions:
                    break
                    
                # Yield positions one by one
                for position in positions:
                    yield position
                
                logger.debug(f"Fetched {len(positions)} positions (batch starting at {current_skip})")
                
                # Update skip value for next page
                current_skip += batch_size
                
                # If we got fewer positions than batch_size, we've reached the end
                if len(positions) < batch_size:
                    break
                    
            except Exception as e:
                logger.error(f"Error fetching positions: {str(e)}")
                break
    
    def run_custom_query(
        self, 
        query: str, 
        variables: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Run a custom GraphQL query against the Uniswap subgraph.
        
        This method allows for flexibility beyond the predefined queries.
        
        Args:
            query: The GraphQL query string
            variables: Variables to include with the GraphQL query
            
        Returns:
            The query response data
        """
        logger.info("Running custom query against Uniswap subgraph")
        return self._execute_query(query, variables)
    
    def test_connection(self) -> bool:
        """
        Test the connection to the Uniswap subgraph.
        
        This method sends a simple query to verify that the API is accessible
        and responding correctly.
        
        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            result = self._execute_query(self.TEST_QUERY)
            pools = result.get("pools", [])
            if pools:
                logger.info(f"Connection test successful. Found {len(pools)} pools.")
                return True
            else:
                logger.warning("Connection test successful but no pools were returned.")
                return True
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False


# Example usage demonstration
if __name__ == "__main__":
    # Create a client for the Uniswap V3 subgraph
    client = UniswapSubgraph(network="mainnet")
    
    # Test the connection to make sure it works
    print("\nTesting connection to Uniswap subgraph...")
    if client.test_connection():
        print("✅ Connection successful!")
        
        # Example pool ID (WETH/USDC 0.3% fee tier)
        pool_id = "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8"
        
        print(f"\nFetching positions for pool {pool_id}...")
        # Fetch positions with liquidity > 0
        positions = client.get_pool_positions(pool_id, min_liquidity=1, limit=5)
        
        # Print the results
        print(f"Found {len(positions)} positions")
        for position in positions:
            print(f"Position ID: {position['id']}")
            print(f"  Tick Range: {position['tickLower']['tickIdx']} to {position['tickUpper']['tickIdx']}")
            print(f"  Liquidity: {position['liquidity']}")
    else:
        print("❌ Connection failed.")
        print("\nAlternative endpoints to try:")
        print("1. Uniswap on The Graph Network: https://thegraph.com/explorer/subgraphs/ELUcwgpm14LKPLrBRuVvPvNKHQ9HvwmtKgKSH6123cr7")
        print("2. Uniswap Labs API: https://docs.uniswap.org/api/reference/overview")
        print("3. Request an API key from The Graph: https://thegraph.com/studio/apikeys/")
    