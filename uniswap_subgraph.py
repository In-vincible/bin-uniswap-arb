import argparse
import logging
import time
import asyncio
import aiohttp
from typing import Dict, List, Optional, Any, Generator, AsyncGenerator
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("uniswap_subgraph")


class UniswapSubgraph:
    """
    A client for fetching liquidity positions from the Uniswap V3 subgraph API.
    
    This class provides methods to query liquidity positions for specific pools 
    with pagination support, robust error handling, and rate limiting protection.
    Uses async/await for improved performance.
    """
    
    # GraphQL query for fetching positions
    POSITION_QUERY = """
    query get_positions($num_skip: Int, $pool_id: ID!, $min_liquidity: BigInt!, $page_size: Int!) {
      positions(
        skip: $num_skip, 
        where: {pool: $pool_id, liquidity_gt: $min_liquidity}, 
        orderBy: liquidity, 
        orderDirection: desc,
        first: $page_size
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
      }
      # Include block information for this query
      _meta {
        block {
          number
          hash
          timestamp
        }
      }
    }
    """
    
    # GraphQL query for fetching the latest indexed block
    LATEST_BLOCK_QUERY = """
    query get_latest_block {
      _meta {
        block {
          number
          hash
          timestamp
        }
      }
    }
    """
    
    # GraphQL query for getting pool details
    POOL_DETAILS_QUERY = """
    query get_pool_details($pool_id: ID!) {
      pool(id: $pool_id) {
        id
        token0 {
          id
          symbol
          name
          decimals
        }
        token1 {
          id
          symbol
          name
          decimals
        }
        feeTier
        liquidity
        volumeUSD
        txCount
        totalValueLockedUSD
      }
      # Include block information
      _meta {
        block {
          number
          hash
          timestamp
        }
      }
    }
    """
    
    def __init__(
        self, 
        api_key: str = "232256ca200587119cbca5c3583dc5fb",
        subgraph_id: str = "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",
        max_retries: int = 3, 
        retry_delay: int = 2,
        timeout: int = 10,
        page_size: int = 100,
        debug: bool = False
    ):
        """
        Initialize the Uniswap subgraph client.
        
        Args:
            api_key: The Graph API key
            subgraph_id: The ID of the Uniswap subgraph
            max_retries: Maximum number of retry attempts for failed requests
            retry_delay: Base delay between retries in seconds (uses exponential backoff)
            timeout: Request timeout in seconds
            page_size: Number of records to fetch per page when paginating
            debug: Whether to print debug information
        """
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = timeout
        self.page_size = page_size
        self.debug = debug
        self._session = None
        
        # Construct the endpoint URL with the provided API key
        self.endpoint = f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{subgraph_id}"
        
        logger.info(f"Initialized Uniswap subgraph client for subgraph {subgraph_id}")
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """
        Get or create an aiohttp client session.
        
        Returns:
            An aiohttp ClientSession
        """
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout)
            )
        return self._session
    
    async def close(self):
        """Close the aiohttp session if it exists."""
        if self._session and not self._session.closed:
            await self._session.close()
    
    async def _execute_query(
        self, 
        query: str, 
        variables: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Execute a GraphQL query against the Uniswap subgraph.
        
        Args:
            query: The GraphQL query string
            variables: Variables to include with the GraphQL query
            
        Returns:
            The query response data
            
        Raises:
            Exception: If the request fails after all retries
        """
        payload = {
            "query": query,
            "variables": variables
        }
        
        headers = {
            "Content-Type": "application/json"
        }
        
        session = await self._get_session()
        
        # Implement retry logic with exponential backoff
        for attempt in range(self.max_retries):
            try:
                async with session.post(
                    self.endpoint,
                    json=payload,
                    headers=headers
                ) as response:
                    # Check for HTTP errors
                    response.raise_for_status()
                    
                    # Parse response
                    result = await response.json()
                    
                    # Debug mode: log the raw response
                    if self.debug:
                        import json
                        logger.debug(f"Raw GraphQL response: {json.dumps(result, indent=2)}")
                    
                    # Check for GraphQL errors
                    if "errors" in result:
                        error_msg = "; ".join([err.get("message", "Unknown error") for err in result["errors"]])
                        logger.warning(f"GraphQL query returned errors: {error_msg}")
                        
                        # Check for rate limiting errors and back off if needed
                        if any("rate limit" in str(err).lower() for err in result["errors"]):
                            retry_delay = self.retry_delay * (2 ** attempt)
                            logger.info(f"Rate limited. Retrying in {retry_delay}s (attempt {attempt + 1}/{self.max_retries})")
                            await asyncio.sleep(retry_delay)
                            continue
                        
                        # If there are errors that aren't rate limiting issues, we might still have partial data
                        # If not, return empty dict to avoid NoneType errors
                        if "data" not in result or result["data"] is None:
                            return {"positions": []}
                    
                    # If we have data, return it even if there were some errors
                    if "data" in result and result["data"] is not None:
                        return result["data"]
                        
                    # If we got here, we have errors but no data
                    raise Exception(f"GraphQL query failed: {error_msg}")
                    
            except asyncio.TimeoutError:
                # Timeout errors
                retry_delay = self.retry_delay * (2 ** attempt)
                logger.warning(f"Request timed out. Retrying in {retry_delay}s (attempt {attempt + 1}/{self.max_retries})")
                await asyncio.sleep(retry_delay)
                
            except aiohttp.ClientError as e:
                # Other request errors
                error_msg = str(e)
                
                # Rate limiting errors
                if "429" in error_msg or "Too Many Requests" in error_msg:
                    retry_delay = self.retry_delay * (2 ** attempt)
                    logger.warning(f"Rate limited. Retrying in {retry_delay}s (attempt {attempt + 1}/{self.max_retries})")
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f"Request failed: {error_msg}")
                    if attempt < self.max_retries - 1:
                        retry_delay = self.retry_delay * (2 ** attempt)
                        logger.info(f"Retrying in {retry_delay}s (attempt {attempt + 1}/{self.max_retries})")
                        await asyncio.sleep(retry_delay)
                    else:
                        raise Exception(f"Failed after {self.max_retries} attempts: {error_msg}")
        
        # If we've exhausted all retries
        raise Exception(f"Failed to execute query after {self.max_retries} retries")
    
    async def get_pool_positions(
        self, 
        pool_id: str, 
        min_liquidity: int = 0, 
        limit: Optional[int] = None,
        pagination_delay: float = 0.01  # 10ms delay between paginated requests
    ) -> Dict[str, Any]:
        """
        Get all liquidity positions for a specific Uniswap V3 pool.
        
        Args:
            pool_id: The Uniswap pool ID (contract address)
            min_liquidity: Minimum liquidity threshold for positions
            limit: Maximum number of positions to return (None for all)
            pagination_delay: Delay in seconds between paginated requests to avoid rate limiting
            
        Returns:
            Dictionary containing:
            {
                "positions": List of position data dictionaries,
                "block": Information about the block from which positions were fetched
            }
        """
        all_positions = []
        current_skip = 0
        latest_block = {}
        
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
                "min_liquidity": str(min_liquidity),  # Ensure min_liquidity is a string
                "page_size": self.page_size
            }
            
            try:
                result = await self._execute_query(self.POSITION_QUERY, variables)
                
                # Get block information
                if "_meta" in result and "block" in result["_meta"]:
                    latest_block = result["_meta"]["block"]
                
                positions = result.get("positions", [])
                
                # Break if no more positions
                if not positions:
                    break
                    
                # Add positions to result list
                all_positions.extend(positions)
                logger.debug(f"Fetched {len(positions)} positions (total: {len(all_positions)}) from {self.format_block_time(latest_block)}")
                
                # Check if we've hit the limit
                if limit and len(all_positions) >= limit:
                    logger.info(f"Reached position limit of {limit}")
                    all_positions = all_positions[:limit]
                    break
                
                # Update skip value for next page
                current_skip += self.page_size
                
                # If we got fewer positions than page_size, we've reached the end
                if len(positions) < self.page_size:
                    logger.debug(f"Received fewer positions than page size, reached end of data")
                    break
                
                # Small delay between paginated requests to be nice to the API
                if current_skip < 10000:  # Only log for the first 10 pages to avoid spam
                    logger.debug(f"Sleeping for {pagination_delay}s before fetching next page")
                await asyncio.sleep(pagination_delay)
                    
            except Exception as e:
                logger.error(f"Error fetching positions: {str(e)}")
                break
        
        logger.info(f"Fetched a total of {len(all_positions)} positions for pool {pool_id} from {self.format_block_time(latest_block)}")
        
        # Return both positions and block info
        return {
            "positions": all_positions,
            "block": latest_block
        }
    
    async def get_pool_positions_generator(
        self, 
        pool_id: str, 
        min_liquidity: int = 0,
        pagination_delay: float = 0.01  # 10ms delay between paginated requests
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Get liquidity positions for a pool as an async generator to conserve memory.
        
        This is useful when dealing with pools that have a large number of positions
        and you want to process them in batches without loading everything into memory.
        
        Args:
            pool_id: The Uniswap pool ID (contract address)
            min_liquidity: Minimum liquidity threshold for positions
            pagination_delay: Delay in seconds between paginated requests to avoid rate limiting
            
        Yields:
            Position data dictionaries, one at a time
            The last item yielded will have a special key "_block_info" containing block data
        """
        current_skip = 0
        position_count = 0
        latest_block = {}
        
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
                "min_liquidity": str(min_liquidity),  # Ensure min_liquidity is a string
                "page_size": self.page_size
            }
            
            try:
                result = await self._execute_query(self.POSITION_QUERY, variables)
                
                # Get block information
                if "_meta" in result and "block" in result["_meta"]:
                    latest_block = result["_meta"]["block"]
                
                positions = result.get("positions", [])
                
                # Break if no more positions
                if not positions:
                    logger.debug("No more positions found")
                    break
                
                # Update total count
                position_count += len(positions)
                
                # Yield positions one by one
                for position in positions:
                    yield position
                
                logger.debug(f"Fetched {len(positions)} positions (batch starting at {current_skip}) from {self.format_block_time(latest_block)}")
                
                # Update skip value for next page
                current_skip += self.page_size
                
                # If we got fewer positions than page_size, we've reached the end
                if len(positions) < self.page_size:
                    logger.debug("Reached end of positions (received fewer than page size)")
                    break
                    
                # Small delay between paginated requests to be nice to the API
                if current_skip < 10000:  # Only log for the first 10 pages to avoid spam
                    logger.debug(f"Sleeping for {pagination_delay}s before fetching next page")
                await asyncio.sleep(pagination_delay)
                    
            except Exception as e:
                logger.error(f"Error fetching positions: {str(e)}")
                break
        
        logger.info(f"Streamed a total of {position_count} positions for pool {pool_id} from {self.format_block_time(latest_block)}")
        
        # Yield block info as the final item
        yield {"_block_info": latest_block}

    async def explore_schema(self) -> Dict[str, Any]:
        """
        Explore the GraphQL schema to understand available types and fields.
        
        This is useful for debugging field errors and understanding what data
        is available in the subgraph.
        
        Returns:
            Schema information from the introspection query
        """
        introspection_query = """
        {
          __schema {
            types {
              name
              kind
              fields {
                name
              }
            }
          }
        }
        """
        
        logger.info("Exploring GraphQL schema")
        try:
            result = await self._execute_query(introspection_query, {})
            return result
        except Exception as e:
            logger.error(f"Error exploring schema: {str(e)}")
            return {}

    async def check_pool_exists(self, pool_id: str) -> bool:
        """
        Check if a pool with the given ID exists in the Uniswap subgraph.
        
        Args:
            pool_id: The Uniswap pool ID (contract address)
            
        Returns:
            True if the pool exists, False otherwise
        """
        # If pool_id doesn't start with '0x', add it
        if not pool_id.startswith('0x'):
            pool_id = f'0x{pool_id}'
            
        # Normalize pool_id to lowercase
        pool_id = pool_id.lower()
        
        query = """
        query check_pool($pool_id: ID!) {
          pool(id: $pool_id) {
            id
            token0 {
              symbol
            }
            token1 {
              symbol
            }
          }
        }
        """
        
        variables = {
            "pool_id": pool_id
        }
        
        try:
            result = await self._execute_query(query, variables)
            pool = result.get("pool")
            return pool is not None
        except Exception as e:
            logger.error(f"Error checking if pool exists: {str(e)}")
            return False

    @staticmethod
    def format_block_time(block_data: Dict[str, Any]) -> str:
        """
        Format block timestamp into a human readable string.
        
        Args:
            block_data: Dictionary containing block information with 'number' and 'timestamp'
            
        Returns:
            Formatted string with block number and time
        """
        if not block_data or 'number' not in block_data:
            return "unknown block"
            
        block_num = block_data.get('number', 'unknown')
        
        if 'timestamp' not in block_data:
            return f"block #{block_num}"
            
        try:
            timestamp = int(block_data['timestamp'])
            block_time = datetime.fromtimestamp(timestamp)
            # Format: "block #123456 (2023-04-15 14:30:45)"
            return f"block #{block_num} ({block_time.strftime('%Y-%m-%d %H:%M:%S')})"
        except (ValueError, TypeError):
            return f"block #{block_num} (invalid timestamp)"
            
    async def get_latest_block(self) -> Dict[str, Any]:
        """
        Get information about the latest block indexed by the subgraph.
        
        This method allows checking how up-to-date the subgraph data is compared
        to the actual blockchain state.
        
        Returns:
            Dictionary containing block information:
            {
                "number": Block number (string),
                "hash": Block hash (string),
                "timestamp": Block timestamp (string, Unix timestamp)
            }
            Or empty dict if block information couldn't be retrieved
        """
        logger.info("Fetching latest indexed block information from subgraph")
        
        try:
            result = await self._execute_query(self.LATEST_BLOCK_QUERY, {})
            
            if "_meta" in result and "block" in result["_meta"]:
                block_info = result["_meta"]["block"]
                
                # Use the formatter helper
                logger.info(f"Latest indexed block: {self.format_block_time(block_info)}")
                
                return block_info
            else:
                logger.warning("No block information found in the response")
                return {}
                
        except Exception as e:
            logger.error(f"Error fetching latest block information: {str(e)}")
            return {}

    async def get_pool_details(self, pool_id: str) -> Dict[str, Any]:
        """
        Get detailed information about a Uniswap V3 pool.
        
        Args:
            pool_id: The Uniswap pool ID (contract address)
            
        Returns:
            Dictionary containing:
            {
                "pool": Pool data with tokens, fees, liquidity, etc.,
                "block": Information about the block from which data was fetched
            }
            Or empty dict if pool does not exist or there was an error
        """
        # If pool_id doesn't start with '0x', add it
        if not pool_id.startswith('0x'):
            pool_id = f'0x{pool_id}'
            
        # Normalize pool_id to lowercase
        pool_id = pool_id.lower()
        
        logger.info(f"Fetching details for pool {pool_id}")
        
        try:
            variables = {"pool_id": pool_id}
            result = await self._execute_query(self.POOL_DETAILS_QUERY, variables)
            
            # Extract block information
            latest_block = {}
            if "_meta" in result and "block" in result["_meta"]:
                latest_block = result["_meta"]["block"]
            
            pool_data = result.get("pool")
            
            if not pool_data:
                logger.warning(f"No data found for pool {pool_id}")
                return {"pool": {}, "block": latest_block}
            
            logger.info(f"Found pool {pool_id}: {pool_data['token0']['symbol']}/{pool_data['token1']['symbol']} (Fee tier: {pool_data['feeTier']})")
            
            # Calculate some additional metrics
            if "liquidity" in pool_data and pool_data['liquidity']:
                logger.info(f"Pool liquidity: {pool_data['liquidity']}")
                
            if "totalValueLockedUSD" in pool_data and pool_data['totalValueLockedUSD']:
                logger.info(f"Total value locked: ${float(pool_data['totalValueLockedUSD']):.2f} USD")
                
            if "volumeUSD" in pool_data and pool_data['volumeUSD']:
                logger.info(f"Total volume: ${float(pool_data['volumeUSD']):.2f} USD")
                
            if "txCount" in pool_data and pool_data['txCount']:
                logger.info(f"Transaction count: {pool_data['txCount']}")
            
            return {
                "pool": pool_data,
                "block": latest_block
            }
            
        except Exception as e:
            logger.error(f"Error fetching pool details: {str(e)}")
            return {"pool": {}, "block": {}}


# Example usage demonstration
async def main():
    """Main function to run when this module is executed directly."""
    parser = argparse.ArgumentParser(description="Uniswap Pool Position Fetcher")
    parser.add_argument("pool_id", help="Uniswap Pool ID to query")
    parser.add_argument("--api-key", help="The Graph API key for the gateway", default="232256ca200587119cbca5c3583dc5fb")
    parser.add_argument("--min-liquidity", type=int, default=10, help="Minimum liquidity threshold (default: 0)")
    parser.add_argument("--limit", type=int, help="Maximum number of positions to fetch (default: all)")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode with detailed GraphQL responses")
    parser.add_argument("--delay", type=float, default=0.01, help="Delay in seconds between pagination requests (default: 0.01s)")
    parser.add_argument("--show-details", action="store_true", help="Show detailed information for each position")
    parser.add_argument("--pool-info-only", action="store_true", help="Only show pool information, don't fetch positions")
    
    args = parser.parse_args()
    
    # Configure logging level based on debug flag
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    pool_id = args.pool_id
    
    # Create the Uniswap subgraph client
    client = UniswapSubgraph(api_key=args.api_key, debug=args.debug)
    
    try:
        # First, test the connection to the subgraph
        logger.info("Testing connection to Uniswap subgraph...")
        
        # Fetch latest block information
        latest_block = await client.get_latest_block()
        
        if not latest_block:
            logger.error("Failed to connect to the Uniswap subgraph. Please check your API key and internet connection.")
            sys.exit(1)
        
        logger.info(f"Successfully connected to Uniswap subgraph")
        
        # Get pool details
        pool_result = await client.get_pool_details(pool_id)
        pool_data = pool_result["pool"]
        
        if not pool_data:
            logger.error(f"Pool ID {pool_id} does not exist in the subgraph")
            logger.error("Please check the pool ID and try again")
            sys.exit(1)
        
        # Print summary of pool information
        logger.info("\n===== POOL INFORMATION =====")
        logger.info(f"Pool: {pool_id}")
        logger.info(f"Pair: {pool_data['token0']['symbol']}/{pool_data['token1']['symbol']}")
        logger.info(f"Fee Tier: {int(pool_data['feeTier'])/10000:.4f}%")
        logger.info(f"TVL: ${float(pool_data['totalValueLockedUSD']):.2f}")
        logger.info(f"Total Volume: ${float(pool_data['volumeUSD']):.2f}")
        logger.info(f"Transaction Count: {pool_data['txCount']}")
        logger.info(f"Block: {client.format_block_time(pool_result['block'])}")
        logger.info("============================\n")
        
        # If pool-info-only flag is set, exit after showing pool information
        if args.pool_info_only:
            logger.info("Only showing pool information as requested")
            return
        
        logger.info(f"Fetching positions for pool {pool_id} with {args.delay}s delay between pages...")
        
        # Fetch positions with liquidity > 0
        result = await client.get_pool_positions(
            pool_id, 
            min_liquidity=args.min_liquidity, 
            limit=args.limit,
            pagination_delay=args.delay
        )
        
        positions = result["positions"]
        position_block = result["block"]
        
        # Print block information comparison
        logger.info("\n===== BLOCK INFORMATION =====")
        logger.info(f"Latest indexed block: {client.format_block_time(latest_block)}")
        logger.info(f"Positions data from: {client.format_block_time(position_block)}")
        
        # Compare position block with latest block
        if position_block and latest_block and "number" in position_block and "number" in latest_block:
            position_block_num = int(position_block["number"])
            latest_block_num = int(latest_block["number"])
            if position_block_num < latest_block_num:
                block_diff = latest_block_num - position_block_num
                logger.warning(f"Positions data is {block_diff} blocks behind the latest indexed block")
                
                # If we have timestamps, calculate time difference
                if "timestamp" in position_block and "timestamp" in latest_block:
                    position_time = int(position_block["timestamp"])
                    latest_time = int(latest_block["timestamp"])
                    time_diff = latest_time - position_time
                    logger.warning(f"Data is approximately {time_diff} seconds behind latest block")
            else:
                logger.info("Positions data is from the latest indexed block")
        logger.info("==============================\n")
        
        # Process the results
        logger.info(f"Found {len(positions)} positions (from {client.format_block_time(position_block)})")
        
        if positions:
            # Show detailed information if requested
            if args.show_details or args.debug:
                for i, position in enumerate(positions):
                    logger.info(f"Position {i+1}/{len(positions)}: ID {position['id']}")
                    logger.info(f"  Tick Range: {position['tickLower']['tickIdx']} to {position['tickUpper']['tickIdx']}")
                    logger.info(f"  Liquidity: {position['liquidity']}")
                    logger.info(f"  Owner: {position['owner']}")
                    logger.info(f"  Deposited: {position['depositedToken0']} token0, {position['depositedToken1']} token1")
                    logger.info(f"  Withdrawn: {position['withdrawnToken0']} token0, {position['withdrawnToken1']} token1")
            else:
                logger.info(f"First position ID: {positions[0]['id']} (use --show-details to see all)")
        else:
            logger.warning("No positions found. This could be due to:")
            logger.warning("  - The pool has no positions with the specified minimum liquidity")
            logger.warning("  - There was an error in the query")
        
        # Log total positions fetched with block information
        logger.info(f"Total positions fetched: {len(positions)} (from {client.format_block_time(position_block)})")
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        if args.debug:
            import traceback
            logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        # Close the client session
        await client.close()

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
    