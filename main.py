import pandas as pd
import sys


try:
    import data_loader
    import metrics
    import parallel
    import portfolio
    import reporting
except ImportError:
    print("Fatal! Source Broken. Please Install Source Packages")


def run_analysis():
    """
    Main orchestration function.
    """
    print("--- Starting Parallel Computing Analysis ---")

    ingestion_metrics = data_loader.profile_ingestion()
    if ingestion_metrics is None:
        print("Fatal Error: Ingestion profiling failed. Exiting.")
        sys.exit(1)

    # Load data for use in subsequent tasks
    print("Loading data for analysis...")
    df_pd = data_loader.load_pandas()
    df_pl = data_loader.load_polars()

    if df_pd is None or df_pl is None:
        print("Fatal Error: Data loading failed. Exiting.")
        sys.exit(1)

    print("Data loaded successfully.")

    # Profile rolling analytics and get metrics
    rolling_metrics = metrics.profile_rolling_analytics(df_pd, df_pl)

    # Get the pandas df with metrics for visualization (Task 2)
    df_pd_with_metrics = rolling_metrics["df_pd_rolling"]
    metrics.visualize_symbol_metrics(df_pd_with_metrics, symbol="AAPL")  # You can change 'AAPL'

    # Profile the parallel strategies
    parallel_metrics = parallel.profile_parallelism(df_pd)

    # Profile the portfolio aggregation
    # Make sure 'portfolio_structure-1.json' is in the same directory
    portfolio_metrics = portfolio.profile_portfolio_aggregation(
        df_pd,
        "portfolio_structure-1.json"
    )

    print("\nAssembling Final Performance Report...")

    # Assemble all metrics into a single DataFrame
    try:
        performance_data = {
            "Task": [
                "1. Ingestion Time (s)",
                "1. Ingestion Peak Memory (MiB)",
                "2. Rolling Analytics Time (s)",
                "3. Parallelism - Sequential (s)",
                "3. Parallelism - Threading (s)",
                "3. Parallelism - Multiprocessing (s)",
                "4. Portfolio Aggregation - Sequential (s)",
                "4. Portfolio Aggregation - Parallel (s)"
            ],
            "Pandas": [
                ingestion_metrics["pandas_time"],
                ingestion_metrics["pandas_mem"],
                rolling_metrics["pandas_rolling_time"],
                parallel_metrics["sequential_time"],
                parallel_metrics["threading_time"],
                parallel_metrics["processing_time"],
                portfolio_metrics["portfolio_seq_time"],
                portfolio_metrics["portfolio_par_time"]
            ],
            "Polars": [
                ingestion_metrics["polars_time"],
                ingestion_metrics["polars_mem"],
                rolling_metrics["polars_rolling_time"],
                None,  # N/A for this task
                None,  # N/A for this task
                None,  # N/A for this task
                None,  # N/A for this task
                None  # N/A for this task
            ]
        }
        df_performance = pd.DataFrame(performance_data)

        print("Performance Summary DataFrame:")
        print(df_performance.to_markdown(index=False))

    except KeyError as e:
        print(f"Fatal Error: A metric key is missing: {e}. Cannot build report.")
        sys.exit(1)
    except Exception as e:
        print(f"Fatal Error: Failed to build performance dataframe: {e}")
        sys.exit(1)

    # Generate the .md report file
    report_content = reporting.generate_performance_report(df_performance)
    try:
        with open("performance_report.md", "w", encoding="utf-8") as f:
            f.write(report_content)
        print("\nSuccessfully generated 'performance_report.md'")
    except Exception as e:
        print(f"Error writing report file: {e}")
    reporting.plot_performance_charts(df_performance)

    print("Analysis Complete/")


if __name__ == "__main__":
    run_analysis()