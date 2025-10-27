# Assignment 7: Parallel Computing for Financial Data Processing

This project is designed to analyze and compare the performance of different data processing libraries (Pandas and Polars) and parallelization strategies when applied to financial market data. It calculates various performance metrics, including rolling averages, volatility, Sharpe ratio, and drawdown, and provides interactive visualizations and reports to summarize the findings. It aims to identify the most efficient methods for handling large datasets and complex calculations in a financial context.

**Authors:** Yuting Li, Xiangchen Liu, Simon Guo, Rajdeep Choudhury

![Python Version](https://img.shields.io/badge/python-3.9+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green)
![Built with uv](https://img.shields.io/badge/built%20with-uv-40c2a8?style=flat&logo=astral)


##  Key Features

- **Data Loading**: Efficiently loads market data from CSV files into both Pandas and Polars DataFrames.
- **Metric Calculation**: Calculates key financial metrics such as Simple Moving Average (SMA), volatility, Sharpe ratio, and drawdown.
- **Parallel Processing**: Implements parallel processing techniques using both threading and process-based execution to accelerate computations.
- **Performance Profiling**: Profiles the execution time and memory usage of different data processing libraries and parallelization strategies.
- **Interactive Visualizations**: Generates interactive plots using Plotly to visualize performance comparisons and financial metrics.
- **Comprehensive Reporting**: Creates detailed markdown reports summarizing the performance analysis results.
- **Portfolio Aggregation**: Aggregates individual stock positions to determine overall portfolio value, volatility, and drawdown.

##  Tech Stack

- **Programming Languages**: Python
- **Data Analysis Libraries**:
    - Pandas
    - Polars
    - NumPy
- **Visualization**: Plotly
- **Parallel Processing**: `concurrent.futures` (ThreadPoolExecutor, ProcessPoolExecutor)
- **Build Tool**: TOML
- **Other**:
    - `timeit` (for performance measurement)
    - `memory_profiler` (for memory usage analysis)
    - `os` (for OS interactions)
    - `psutil` (for process monitoring)
    - `json` (for portfolio structure)
    - `copy` (for deep copies)
    - `functools.partial` (for parallel processing)

##  Getting Started

### Prerequisites

- Python (>=3.10)
- `uv` (Python package installer, or `pip`)

### Installation

1.  Clone the repository:

    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```

2.  Create a virtual environment (recommended):

    ```bash
    python -m venv venv
    source venv/bin/activate  # On Linux/macOS
    venv\Scripts\activate  # On Windows
    ```

3.  Install the dependencies:

    ```bash
    pip install --upgrade pip
    pip install -r requirements.txt
    ```
    Alternatively, you can use `pyproject.toml`

    ```bash
    uv add .
    ```

### Running Locally

1.  Ensure you have a CSV file containing market data. By default, the script looks for data at `CFG_PATH` defined in `data_loader.py`.

2.  Run the main script:

    ```bash
    python main.py
    ```

3.  The script will execute the performance analysis, generate visualizations, and create a markdown report.

## 📂 Project Structure

```
├── main.py
├── data_loader.py
├── metrics.py
├── parallel.py
├── portfolio.py
├── reporting.py
├── pyproject.toml
├── README.md
└── requirements.txt
```



## 🤝 Contribution Guidelines

We welcome contributions to the `FINM32500-HW7` project! Please follow these guidelines to ensure a smooth collaboration process.

### Code Style

*   Adhere to [PEP 8](https://www.python.org/dev/peps/pep-0008/) for Python code style.
*   Use a linter (e.g., `flake8` or `ruff`) to check your code before submitting.

### Branch Naming Conventions

Please use descriptive branch names for new features and bug fixes:

*   **Features:** `feature/your-feature-name` (e.g., `feature/add-mean-reversion-strategy`)
*   **Bug Fixes:** `bugfix/issue-description` (e.g., `bugfix/fix-data-loading-error`)
*   **Documentation:** `docs/update-readme`

### Pull Request Process

1.  **Fork** the repository.
2.  **Create** a new branch from `main` (or the appropriate base branch).
3.  **Implement** your changes, ensuring they align with the project's goals.
4.  **Write Tests:** For new features or bug fixes, please add or update relevant tests in the `tests/` directory.
5.  **Run Tests:** Ensure all existing tests pass and your new tests cover your changes adequately.
6.  **Commit** your changes with clear, concise commit messages.
7.  **Push** your branch to your forked repository.
8.  **Open a Pull Request** against the `main` branch of the original repository.
9.  **Provide a clear description** of your changes, why they are needed, and any relevant context.

### Testing Requirements

All contributions must include appropriate tests. New features require new tests, and bug fixes should include a test that demonstrates the bug and its fix. Ensure your changes do not break existing functionality by running the full test suite.

### License

This project is protected under the MIT LICENSE. For more details, refer to the LICENSE file.

##  Acknowledgments

This project was created as part of the FINM 32500 course at The University of Chicago. Inspiration from various open-source backtesting frameworks.

**Copyright © 2025 Lauristt**
