import pandas as pd
import polars as pl
import numpy as np
import time
import plotly.express as px
import plotly.io as pio

pio.templates.default = "plotly_white"


def compute_rolling_pandas(df, window=20):
    """
    Computes 20-period rolling metrics (SMA, Vol, Sharpe) using pandas.

    Args:
        df (pd.DataFrame): Input DataFrame with a 'timestamp' index and
                           columns 'symbol' and 'price'.
        window (int): The rolling window period.

    Returns:
        pd.DataFrame: The original DataFrame with new columns for
                      'sma_20', 'vol_20', and 'sharpe_20'.
    """
    df = df.reset_index()
    df = df.sort_values(by=['symbol', 'timestamp'])
    grouped = df.groupby('symbol')
    df['sma_20'] = grouped['price'].rolling(window=window).mean().reset_index(level=0, drop=True)
    df['vol_20'] = grouped['price'].rolling(window=window).std().reset_index(level=0, drop=True)
    returns = grouped['price'].pct_change()
    rolling_mean_ret = returns.rolling(window=window).mean()
    rolling_std_ret = returns.rolling(window=window).std()
    df['sharpe_20'] = (rolling_mean_ret / rolling_std_ret)
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df = df.set_index('timestamp')

    return df

def compute_drawdown(price_series):
    if price_series.empty: return 0.0
    cumulative_max = price_series.cummax()
    drawdowns = (price_series - cumulative_max) / cumulative_max
    max_drawdown = drawdowns.min()
    return max_drawdown if pd.notna(max_drawdown) else 0.0

def compute_rolling_polars(df, window=20):
    """
    Computes 20-period rolling metrics (SMA, Vol, Sharpe) using polars.

    Args:
        df (pl.DataFrame): Input Polars DataFrame with columns
                           'timestamp', 'symbol', and 'price'.
        window (int): The rolling window period.

    Returns:
        pl.DataFrame: The original DataFrame with new columns for
                      'sma_20', 'vol_20', and 'sharpe_20'.
    """

    df = df.sort("symbol", "timestamp")
    df = df.with_columns([
        pl.col("price").rolling_mean(window_size=window).over("symbol").alias("sma_20"),
        pl.col("price").rolling_std(window_size=window).over("symbol").alias("vol_20"),
        pl.col("price").pct_change().over("symbol").alias("returns")
    ])

    df = df.with_columns([
        pl.col("returns").rolling_mean(window_size=window).over("symbol").alias("mean_ret"),
        pl.col("returns").rolling_std(window_size=window).over("symbol").alias("std_ret")
    ])

    df = df.with_columns(
        (pl.col("mean_ret") / pl.col("std_ret")).alias("sharpe_20")
    )
    df = df.drop(["returns", "mean_ret", "std_ret"])

    return df


def visualize_symbol_metrics(df_with_metrics, symbol="AAPL"):

    print(f"Visualizing metrics for {symbol}...")

    # Filter for the chosen symbol and drop NaNs from the rolling window
    df_symbol = df_with_metrics[df_with_metrics['symbol'] == symbol].dropna()

    if df_symbol.empty:
        print(f"No data found for symbol '{symbol}' after dropping NaNs. Skipping plot.")
        return

    df_symbol = df_symbol.reset_index()

    # Plot Price and SMA
    # We plot only the last 1000 points for clarity
    fig_price = px.line(
        df_symbol.tail(1000),
        x='timestamp',
        y=['price', 'sma_20'],
        title=f'{symbol} Price vs. 20-Period SMA'
    )
    fig_price.show()

    # Plot Volatility and Sharpe Ratio
    fig_metrics = px.line(
        df_symbol.tail(1000),
        x='timestamp',
        y=['vol_20', 'sharpe_20'],
        title=f'{symbol} 20-Period Rolling Volatility and Sharpe Ratio'
    )
    fig_metrics.show()


def profile_rolling_analytics(df_pd, df_pl):
    """
    Times and compares the performance of pandas vs polars
    for the rolling analytics task.

    Args:
        df_pd (pd.DataFrame): The loaded pandas DataFrame.
        df_pl (pl.DataFrame): The loaded polars DataFrame.

    Returns:
        dict: A dictionary containing the execution times.
    """
    print("--- Task 2: Rolling Analytics Profiling ---")

    start_time_pd = time.time()
    df_pd_rolling = compute_rolling_pandas(df_pd.copy())
    time_pd = time.time() - start_time_pd
    print(f"Pandas Rolling Time: {time_pd:.4f} seconds")
    #polars
    start_time_pl = time.time()
    df_pl_rolling = compute_rolling_polars(df_pl.clone())
    time_pl = time.time() - start_time_pl
    print(f"Polars Rolling Time: {time_pl:.4f} seconds")

    print("\nPerformance Discussion:")
    if time_pl < time_pd:
        speedup = time_pd / time_pl
        print(f"Polars was {speedup:.2f}x faster than Pandas.")
    else:
        speedup = time_pl / time_pd
        print(f"Pandas was {speedup:.2f}x faster than Polars.")

    print("Syntax Discussion:")
    print(
        "- Pandas uses a .groupby().rolling() chain, which is familiar but complex to align back to the original index.")
    print(
        "- Polars uses an expression-based API with .over('symbol'), which is highly parallelizable and often more readable for complex transforms.")
    print("-" * 40 + "\n")

    return {
        "pandas_rolling_time": time_pd,
        "polars_rolling_time": time_pl,
        "df_pd_rolling": df_pd_rolling,
        "df_pl_rolling": df_pl_rolling
    }

if __name__ == "__main__":
    import data_loader

    print("Loading data for metrics.py test...")
    df_pd = data_loader.load_pandas()
    df_pl = data_loader.load_polars()

    if df_pd is not None and df_pl is not None:
        profile_results = profile_rolling_analytics(df_pd, df_pl)
        df_pd_with_metrics = profile_results["df_pd_rolling"]
        visualize_symbol_metrics(df_pd_with_metrics, symbol="AAPL")

    else:
        print("Data loading failed. Please check data_loader.py and your file path.")