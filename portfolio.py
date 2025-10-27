import json
import pandas as pd
import numpy as np
import concurrent.futures
import copy
import time
from functools import partial
import os

try:
    from metrics import compute_drawdown
except ImportError:
    print("Warning: Could not import compute_drawdown from metrics.py. Make sure the file is in the same directory.")
    #fallback
    def compute_drawdown(price_series):
        if price_series.empty: return 0.0
        cumulative_max = price_series.cummax()
        drawdowns = (price_series - cumulative_max) / cumulative_max
        max_drawdown = drawdowns.min()
        return max_drawdown if pd.notna(max_drawdown) else 0.0


def compute_position_metrics(position_data, latest_data_dict, symbol_price_history):
    """
    Computes metrics for a single position.
    This is the "work unit" for parallel processing.

    It must be a top-level function to be 'picklable' by multiprocessing.

    Args:
        position_data (dict): A dict with 'symbol' and 'quantity'.
        latest_data_dict (dict): A lookup dict for {symbol -> latest_price}.
        symbol_price_history (dict): A lookup dict for {symbol -> pd.Series(prices)}.

    Returns:
        dict: The position_data dict updated with 'value', 'volatility', 'drawdown'.
    """
    try:
        symbol = position_data['symbol']
        quantity = position_data['quantity']

        # Get latest data
        latest = latest_data_dict.get(symbol)
        if not latest:
            print(f"Warning: No market data for symbol {symbol}. Skipping.")
            return {**position_data, "value": 0, "volatility": 0, "drawdown": 0}

        # Compute Value = quantity * latest price
        latest_price = latest['price']
        value = quantity * latest_price

        # Get price history for vol and drawdown
        history = symbol_price_history.get(symbol)
        if history is None or history.empty:
            volatility = 0.0
            drawdown = 0.0
        else:
            # Compute Volatility (using last 20-period std dev of returns)
            returns = history.pct_change()
            volatility = returns.rolling(20).std().iloc[-1]
            if pd.isna(volatility):
                volatility = 0.0  # Handle case where window is not full

            # Compute Drawdown
            drawdown = compute_drawdown(history)

        return {
            "symbol": symbol,
            "quantity": quantity,
            "value": round(value, 2),
            "volatility": round(volatility, 4),
            "drawdown": round(drawdown, 4)
        }
    except Exception as e:
        print(f"Error processing {position_data.get('symbol')}: {e}")
        return {**position_data, "value": 0, "volatility": 0, "drawdown": 0}


def get_all_positions(portfolio_node):
    """
    Recursively finds all position dicts in the nested structure
    and returns them as a flat list.
    """
    all_positions = []
    if "positions" in portfolio_node:
        all_positions.extend(portfolio_node["positions"])

    if "sub_portfolios" in portfolio_node:
        for sub in portfolio_node["sub_portfolios"]:
            all_positions.extend(get_all_positions(sub))
    return all_positions


def map_positions_back(portfolio_node, position_map):
    """
    Recursively traverses the tree and replaces the simple
    position dicts with the fully computed ones from the position_map.
    This modifies the tree in-place.
    """
    if "positions" in portfolio_node:
        new_positions = []
        for pos in portfolio_node["positions"]:
            # Use (symbol, quantity) as a unique key
            key = (pos['symbol'], pos['quantity'])
            if key in position_map:
                new_positions.append(position_map[key])
        portfolio_node["positions"] = new_positions

    if "sub_portfolios" in portfolio_node:
        for sub in portfolio_node["sub_portfolios"]:
            map_positions_back(sub, position_map)


def aggregate_portfolio(portfolio_node):
    """
    Recursively computes and aggregates metrics for a portfolio tree.
    This function MODIFIES the node in-place and assumes that all
    'positions' have already been computed by compute_position_metrics.
    """
    total_value = 0.0
    weighted_vol = 0.0
    max_drawdown = 0.0  # Drawdowns are negative, so 0 is the 'best'

    #Aggregate positions in this node
    if "positions" in portfolio_node:
        for pos in portfolio_node["positions"]:
            # 'pos' is now the fully computed dict
            total_value += pos.get("value", 0)
            weighted_vol += pos.get("value", 0) * pos.get("volatility", 0)
            max_drawdown = min(max_drawdown, pos.get("drawdown", 0))

    #Recurse into sub-portfolios
    if "sub_portfolios" in portfolio_node:
        for sub_portfolio in portfolio_node["sub_portfolios"]:
            # Recursively call, which aggregates from the bottom up
            aggregate_portfolio(sub_portfolio)

            # Aggregate results from the child
            total_value += sub_portfolio.get("total_value", 0)
            weighted_vol += sub_portfolio.get("total_value", 0) * sub_portfolio.get("aggregate_volatility", 0)
            max_drawdown = min(max_drawdown, sub_portfolio.get("max_drawdown", 0))

    #Set metrics for the current node
    portfolio_node["total_value"] = round(total_value, 2)
    if total_value > 0:
        # Weighted average volatility
        portfolio_node["aggregate_volatility"] = round(weighted_vol / total_value, 4)
    else:
        portfolio_node["aggregate_volatility"] = 0.0
    # Worst drawdown
    portfolio_node["max_drawdown"] = round(max_drawdown, 4)

    return portfolio_node


def process_portfolio_sequentially(portfolio_json, latest_data_dict, symbol_price_history):
    """
    Orchestrator for the sequential (single-process) run.
    """
    # Use deepcopy to avoid modifying the original data
    portfolio_struct = copy.deepcopy(portfolio_json)

    # Get flat list of all positions
    all_positions = get_all_positions(portfolio_struct)

    # Compute metrics for all positions sequentially
    computed_positions = [
        compute_position_metrics(pos, latest_data_dict, symbol_price_history)
        for pos in all_positions
    ]

    # Map computed positions back into the tree
    position_map = {
        (pos['symbol'], pos['quantity']): pos
        for pos in computed_positions
    }
    map_positions_back(portfolio_struct, position_map)

    # Recursively aggregate
    return aggregate_portfolio(portfolio_struct)


def process_portfolio_parallel(portfolio_json, latest_data_dict, symbol_price_history):
    """
    Orchestrator for the parallel (multiprocessing) run.
    """
    portfolio_struct = copy.deepcopy(portfolio_json)
    all_positions = get_all_positions(portfolio_struct)
    worker_func = partial(
        compute_position_metrics,
        latest_data_dict=latest_data_dict,
        symbol_price_history=symbol_price_history
    )

    computed_positions = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        computed_positions = list(executor.map(worker_func, all_positions))
    position_map = {
        (pos['symbol'], pos['quantity']): pos
        for pos in computed_positions
    }
    map_positions_back(portfolio_struct, position_map)

    # Recursively aggregate
    return aggregate_portfolio(portfolio_struct)


def profile_portfolio_aggregation(df_pd, portfolio_filepath="portfolio_structure-1.json"):
    """
    Main profiling function for Task 4.
    Prepares data, runs sequential and parallel versions, and prints results.

    Args:
        df_pd (pd.DataFrame): The main pandas DataFrame with all market data.
        portfolio_filepath (str): Path to the portfolio JSON file.

    Returns:
        dict: A dictionary of timing results.
    """
    print("Portfolio Aggregation Profiling...")

    # Load the portfolio structure
    try:
        with open(portfolio_filepath, "r") as f:
            portfolio_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: {portfolio_filepath} not found.")
        return {"portfolio_seq_time": 0, "portfolio_par_time": 0}


    # Get latest price for all symbols
    latest_data = df_pd.reset_index().groupby('symbol').last()
    latest_data_dict = latest_data.to_dict('index')

    # Get price history for all symbols
    symbol_price_history = {
        symbol: group['price']
        for symbol, group in df_pd.groupby('symbol')
    }

    # Run Sequential (Baseline)
    start_time_seq = time.time()
    result_seq = process_portfolio_sequentially(
        portfolio_data, latest_data_dict, symbol_price_history
    )
    time_seq = time.time() - start_time_seq
    print(f"Sequential Aggregation Time:    {time_seq:.4f} seconds")

    # Run Parallel (Multiprocessing)
    start_time_par = time.time()
    result_par = process_portfolio_parallel(
        portfolio_data, latest_data_dict, symbol_price_history
    )
    time_par = time.time() - start_time_par
    print(f"Parallel Aggregation Time:      {time_par:.4f} seconds")
    print("\nPerformance Discussion:")
    if time_seq < time_par:
        print("Sequential was faster. For a small number of positions, the")
        print("overhead of spawning processes (pickling data) is greater")
        print("than the computational benefit of parallelism.")
    else:
        speedup = time_seq / time_par
        print(f"Parallel was {speedup:.2f}x faster. The task was complex")
        print("enough (drawdown, volatility) to benefit from parallelization.")

    print("\nNote: This task is CPU-bound (drawdown calc) and involves pure-Python")
    print("logic, making it a good candidate for Multiprocessing (unlike Task 3).")

    # Print a sample of the final aggregated structure
    print("\n--- Final Aggregated Portfolio (Parallel) ---")
    print(json.dumps(result_par, indent=2))
    print("-" * 40 + "\n")

    return {
        "portfolio_seq_time": time_seq,
        "portfolio_par_time": time_par
    }


if __name__ == "__main__":
    # Import the loader from Task 1
    import data_loader

    print("Loading data for portfolio.py test...")
    df_pd = data_loader.load_pandas()

    if df_pd is not None:
        profile_portfolio_aggregation(df_pd, "portfolio_structure-1.json")
    else:
        print("Data loading failed. Please check data_loader.py.")