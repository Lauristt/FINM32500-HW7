import pandas as pd
import polars as pl
import timeit
import os
from memory_profiler import memory_usage

CFG_PATH ='/Users/laurisli/Desktop/32500hw7/market_data-1.csv'


def load_pandas(filepath=CFG_PATH):
    if not os.path.exists(filepath):
        print(f"Error: File not found at {filepath}")
        print("Please check the CFG_PATH variable in data_loader.py")
        return None

    try:
        df_pd = pd.read_csv(
            filepath,
            parse_dates=['timestamp']
        )
        df_pd = df_pd.set_index('timestamp')
        return df_pd
    except Exception as e:
        print(f"Error loading data with pandas: {e}")
        return None

def load_polars(filepath=CFG_PATH):
    if not os.path.exists(filepath):
        print(f"Error: File not found at {filepath}")
        print("Please check the CFG_PATH variable in data_loader.py")
        return None

    try:
        df_pl = pl.read_csv(
            filepath,
            try_parse_dates=True
        )
        df_pl = df_pl.sort("timestamp")
        return df_pl
    except Exception as e:
        print(f"Error loading data with polars: {e}")
        return None


def profile_ingestion():
    """
    Profiles and compares the ingestion time and memory usage for
    both pandas and polars loaders, as required by Task 1.
    """
    print("Data Ingestion Profiling...")
    print(f"Loading data from: {CFG_PATH}\n")

    if not os.path.exists(CFG_PATH):
        print(f"Fatal Error: Data file not found. Profiling aborted.")
        print("Please check the CFG_PATH variable.")
        return None

    print("Profiling Pandas...")
    pandas_time = timeit.timeit(load_pandas, number=3) / 3
    mem_usage_pd = memory_usage((load_pandas,), interval=0.1, max_usage=True)

    print(f"Pandas Average Load Time: {pandas_time:.4f} seconds")
    print(f"Pandas Peak Memory Usage: {mem_usage_pd:.2f} MiB")

    print("\nProfiling Polars...")
    polars_time = timeit.timeit(load_polars, number=3) / 3
    mem_usage_pl = memory_usage((load_polars,), interval=0.1, max_usage=True)

    print(f"Polars Average Load Time: {polars_time:.4f} seconds")
    print(f"Polars Peak Memory Usage: {mem_usage_pl:.2f} MiB")

    return {
        "pandas_time": pandas_time,
        "pandas_mem": mem_usage_pd,
        "polars_time": polars_time,
        "polars_mem": mem_usage_pl
    }


if __name__ == "__main__":
    profile_ingestion()
    print("Loading data for inspection...")
    df_pd = load_pandas()
    if df_pd is not None:
        print("\nPandas DataFrame Head:")
        print(df_pd.head())
        print("\nPandas DataFrame Info:")
        df_pd.info()

    df_pl = load_polars()
    if df_pl is not None:
        print("\nPolars DataFrame Head:")
        print(df_pl.head())
        print(f"\nPolars DataFrame Schema: {df_pl.schema}")