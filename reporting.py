import pandas as pd
import plotly.express as px
import plotly.io as pio

# Set a clean default theme for plots
pio.templates.default = "plotly_white"


def plot_performance_charts(df_performance):
    """
    Generates and displays interactive bar charts for performance comparison.

    Args:
        df_performance (pd.DataFrame): The summary DataFrame of all metrics.
    """
    print("Generating Performance Visualizations...")

    try:
        # Ingestion Time
        df_ingest_time = df_performance[
            df_performance['Task'] == '1. Ingestion Time (s)'
            ].melt(id_vars='Task', var_name='Library', value_name='Time (s)')

        fig1 = px.bar(df_ingest_time, x='Library', y='Time (s)', color='Library',
                      title='Data Ingestion Time (Lower is Better)')
        fig1.show()

        df_ingest_mem = df_performance[
            df_performance['Task'] == '1. Ingestion Peak Memory (MiB)'
            ].melt(id_vars='Task', var_name='Library', value_name='Memory (MiB)')

        fig2 = px.bar(df_ingest_mem, x='Library', y='Memory (MiB)', color='Library',
                      title='Data Ingestion Peak Memory (Lower is Better)')
        fig2.show()

        df_rolling_time = df_performance[
            df_performance['Task'] == '2. Rolling Analytics Time (s)'
            ].melt(id_vars='Task', var_name='Library', value_name='Time (s)')

        fig3 = px.bar(df_rolling_time, x='Library', y='Time (s)', color='Library',
                      title='Rolling Analytics Time (Lower is Better)')
        fig3.show()

        # Parallelism Comparison (Pandas)
        parallel_tasks = [
            "3. Parallelism - Sequential (s)",
            "3. Parallelism - Threading (s)",
            "3. Parallelism - Multiprocessing (s)"
        ]
        df_parallel_perf = df_performance[df_performance['Task'].isin(parallel_tasks)]

        df_parallel_melted = df_parallel_perf.melt(
            id_vars='Task', value_vars=['Pandas'], value_name='Time (s)'
        )

        fig4 = px.bar(df_parallel_melted, x='Task', y='Time (s)', color='Task',
                      title='Pandas Parallelism Strategies (Lower is Better)')
        fig4.update_xaxes(categoryorder='array', categoryarray=parallel_tasks)
        fig4.show()

        print("Visualization complete...")

    except Exception as e:
        print(f"Error generating plots: {e}")


def generate_performance_report(df_performance):
    """
    Generates the final performance_report.md content.

    Args:
        df_performance (pd.DataFrame): The summary DataFrame of all metrics.

    Returns:
        str: A string containing the full markdown report.
    """

    # Create the Summary Table...
    try:
        # Use .to_markdown() for a clean table format
        summary_table = df_performance.to_markdown(index=False)
    except Exception as e:
        print(f"Error generating markdown table: {e}")
        summary_table = "Error creating performance table."

    # Create the Discussion Text...
    discussion = """
##  tradeoffs in Syntax, Ecosystem, and Scalability

### 1. Pandas vs. Polars (Tasks 1 & 2)

* **Performance (Time & Memory):** `Polars` is demonstrably faster and more memory-efficient in both data ingestion and rolling analytics. This is due to its Rust backend, multi-threaded query engine, and column-based storage.
* **Syntax:** `Pandas` is more established, and its `groupby().rolling()` syntax is familiar, though it can be complex (as seen in the "duplicate label" error). `Polars` uses a more functional, expression-based API (`.over()`, `.with_columns()`) which is highly consistent and easier to parallelize.
* **Scalability:** `Polars` is built for scalability. Its lazy API allows it to optimize entire query chains and handle datasets larger than available RAM, which `Pandas` (in its default eager mode) cannot do.

### 2. Threading vs. Multiprocessing (Task 3)

* **Performance:** `Threading` was significantly faster than both `Sequential` and `Multiprocessing` for this task.
* **The GIL (Global Interpreter Lock):** The GIL prevents multiple threads from executing Python code at the same time. However, many `pandas` operations (like `.rolling().mean()`) are C-extensions that **release the GIL**.
* **Why Threading Won:** Because the GIL was released, other threads could start their own C-level computations. It provided true parallelism with very low overhead.
* **Why Multiprocessing Lost:** `Multiprocessing` avoids the GIL but has massive **serialization (pickling) overhead**. The cost of pickling, sending, and un-pickling large DataFrame slices to other processes was far greater than the computational gain.

### 3. Portfolio Aggregation (Task 4)

* **Performance:** For a small portfolio, `Sequential` is often faster because the overhead of starting new processes (as `Multiprocessing` does) is higher than the task's computation time.
* **When to use Multiprocessing:** As the number of positions grows from 3 to 3,000, `Multiprocessing` would win. The task (`compute_drawdown`, `rolling().std()`) is pure-Python and CPU-bound, making it a perfect case for avoiding the GIL, *assuming* the number of tasks is large enough to overcome the initial setup cost.
"""

    report_content = f"""# ⚙️ Performance Comparison Report

## 1. Performance Summary Table

{summary_table}

## 2. Discussion of Tradeoffs

{discussion}
"""
    return report_content