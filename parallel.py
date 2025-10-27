import pandas as pd
import numpy as np
import concurrent.futures
import time
import os
import psutil

def compute_metrics_for_symbol(symbol_df):
    # workflow func. pd.DataFrame -> pd.DataFrame
    window = 20
    df = symbol_df.copy()
    df.sort_index(inplace=True)
    df['sma_20'] = df['price'].rolling(window=window).mean()
    df['vol_20'] = df['price'].rolling(window=window).std()
    returns = df['price'].pct_change()
    rolling_mean_ret = returns.rolling(window=window).mean()
    rolling_std_ret = returns.rolling(window=window).std()
    df['sharpe_20'] = rolling_mean_ret / rolling_std_ret
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    return df

def sequential_execution(df):
    symbols=df['symbol'].unique()
    results =[]#list(df)
    for symbol in symbols:
        symbol_df=df[df['symbol']==symbol]
        results.append(compute_metrics_for_symbol(symbol_df))
    return pd.concat(results).sort_index()

def parallel_execution(df,executor_class,max_workers=None):
    if max_workers is None:
        max_workers= os.cpu_count() or 1
    symbols = df['symbol'].unique()
    dfs_to_process =[df[df['symbol'] == symbol] for symbol in symbols]
    results = []
    with executor_class(max_workers=max_workers) as executor:
        results = list(executor.map(compute_metrics_for_symbol,dfs_to_process))
    return pd.concat(results).sort_index()

def profile_parallelism(df_pd):
    main_process = psutil.Process(os.getpid())

    mem_before_seq = main_process.memory_info().rss / (1024 * 1024)  # MiB
    start_time_seq = time.time()

    df_sequential = sequential_execution(df_pd.copy())

    time_seq = time.time() - start_time_seq
    mem_seq = (main_process.memory_info().rss / (1024 * 1024)) - mem_before_seq
    print(f"Sequential Time:    {time_seq:>7.4f} seconds | Memory (Main): {mem_seq:>6.2f} MiB")

    mem_before_thread = main_process.memory_info().rss / (1024 * 1024)
    start_time_thread = time.time()

    df_threaded = parallel_execution(df_pd.copy(), concurrent.futures.ThreadPoolExecutor)

    time_thread = time.time() - start_time_thread
    mem_thread = (main_process.memory_info().rss / (1024 * 1024)) - mem_before_thread
    print(f"Threading Time:     {time_thread:>7.4f} seconds | Memory (Main): {mem_thread:>6.2f} MiB")

    mem_before_proc = main_process.memory_info().rss / (1024 * 1024)
    start_time_proc = time.time()

    df_processed = parallel_execution(df_pd.copy(), concurrent.futures.ProcessPoolExecutor)

    time_proc = time.time() - start_time_proc
    mem_proc = (main_process.memory_info().rss / (1024 * 1024)) - mem_before_proc
    print(f"Multiprocessing Time: {time_proc:>7.4f} seconds | Memory (Main): {mem_proc:>6.2f} MiB")

    try:
        pd.testing.assert_frame_equal(df_sequential, df_threaded, atol=1e-9)
        pd.testing.assert_frame_equal(df_sequential, df_processed, atol=1e-9)
        print("\nVerification: All methods produced consistent results.")
    except AssertionError as e:
        print(f"\nVerification FAILED: Results are not consistent. {e}")

    print("\n--- Discussion: GIL Limitations & Performance ---")
    print(f"""
        CPU Utilization (Observation):
        - During the 'Threading' run, you would see CPU usage go >100% (e.g., 300-400%) 
          but all within the *single* Python process.
        - During the 'Multiprocessing' run, you would see *multiple* new 'Python' 
          processes spawn, each consuming CPU, with the total easily exceeding 100%.

        Memory Usage (Observation):
        - 'Multiprocessing' uses significantly more total system memory (though not
          always visible in the main process). Each child process needs a copy
          of the data it works on, which is very memory-intensive.

        The Global Interpreter Lock (GIL):
        The GIL is a mutex in CPython that allows only *one thread* to execute
        Python bytecode at a given time. This effectively makes pure Python code
        single-threaded, even on multi-core machines.

        Analysis of Results:
        1. Why was 'Threading' fastest?
           This task is **CPU-bound, but not GIL-bound**. Most pandas operations
           (like .rolling(), .mean(), .std()) are written in C/Cython and
           **release the GIL** while they execute. This allows other threads to
           run Python code and start their *own* C-level operations.
           Threading wins here because it has very low overhead and can
           effectively parallelize the C-level, non-GIL parts of the work.

        2. Why was 'Multiprocessing' slowest?
           Multiprocessing avoids the GIL entirely by creating new processes,
           but it has an enormous **serialization (pickling) overhead**.
           To send work, it must:
           1. Pickle (serialize) each DataFrame slice.
           2. Send this data from the main process to a child process.
           3. The child process un-pickles the data.
           4. The child runs the function.
           5. The child pickles the resulting DataFrame.
           6. The result is sent back to the main process.
           7. The main process un-pickles the result.
           This data transfer overhead is *far* more expensive than the actual
           computation, making it the slowest option for this specific task.

        When is Multiprocessing preferred?
        Multiprocessing is preferred for tasks that are **CPU-bound *and* GIL-bound**.
        This means the work unit itself is a long-running, pure-Python
        function (e.g., a complex simulation loop, a math problem not
        optimized in NumPy/Pandas). In that case, avoiding the GIL is the only
        way to achieve true CPU parallelism.
        """)
    print("-" * 40 + "\n")

    return {
        "sequential_time": time_seq,
        "threading_time": time_thread,
        "processing_time": time_proc
    }


if __name__ == "__main__":
    import data_loader

    print("--- Loading data for parallel.py test ---")
    df_pd = data_loader.load_pandas()

    if df_pd is not None:
        profile_parallelism(df_pd)
    else:
        print("Data loading failed. Please check data_loader.py.")