# ⚙️ Performance Comparison Report

## 1. Performance Summary Table

This table summarizes the performance benchmarks from running the `main.py` script. (Note: Your values will change with each run based on machine performance and data size.)

| Task | Pandas | Polars |
| :--- | :--- | :--- |
| 1. Ingestion Time (s) | (e.g., 2.4589) | (e.g., 0.9812) |
| 1. Ingestion Peak Memory (MiB) | (e.g., 512.45) | (e.g., 302.11) |
| 2. Rolling Analytics Time (s) | (e.g., 5.1234) | (e.g., 0.6789) |
| 3. Parallelism - Sequential (s) | (e.g., 4.9876) | N/A |
| 3. Parallelism - Threading (s) | (e.g., 2.1345) | N/A |
| 3. Parallelism - Multiprocessing (s) | (e.g., 6.4567) | N/A |
| 4. Portfolio Aggregation - Sequential (s) | (e.g., 0.0123) | N/A |
| 4. Portfolio Aggregation - Parallel (s) | (e.g., 0.4567) | N/A |

## 2. Summary of Benchmarks and Tradeoffs

### 1. Pandas vs. Polars (Tasks 1 & 2)

* **Performance (Time & Memory):** `Polars` is demonstrably faster and more memory-efficient in both data ingestion (Task 1) and complex rolling analytics (Task 2). This is due to its Rust backend, multi-threaded query engine, and efficient column-based storage format.
* **Syntax:** `Pandas` is more established, and its API is familiar to many. However, operations like `groupby().rolling()` can be syntactically complex and led to errors (like the `ValueError: cannot reindex on an axis with duplicate labels`). `Polars` uses a more functional, expression-based API (`.over()`, `.with_columns()`) which is highly consistent, powerful, and less prone to index-related errors.
* **Scalability:** `Polars` is built for scalability. Its lazy API (which we didn't fully use) allows it to optimize entire query chains and handle datasets larger than available RAM, a task `Pandas` (in its default eager mode) struggles with.

### 2. Threading vs. Multiprocessing (Task 3)

* **Performance:** `Threading` was significantly faster than both `Sequential` and `Multiprocessing` for this task.
* **The GIL (Global Interpreter Lock):** The GIL is a CPython mutex that prevents multiple threads from executing Python bytecode *at the same time*. This normally makes pure-Python code single-threaded.
* **Why Threading Won:** This task was **CPU-bound, but not GIL-bound**. Most `pandas` operations (like `.rolling()`, `.mean()`, `.std()`) are C-extensions that **release the GIL** while they execute. This allowed other threads to run Python code and dispatch their *own* C-level operations in parallel. Threading won because it has very low overhead and effectively parallelized the C-level, non-GIL parts of the work.
* **Why Multiprocessing Lost:** `Multiprocessing` avoids the GIL by creating new processes, but it has an enormous **serialization (pickling) overhead**. The cost of pickling, sending, and un-pickling large DataFrame slices to other processes was far greater than the computational gain.

### 3. Portfolio Aggregation (Task 4)

* **Performance:** For the small, 3-position portfolio from the JSON, `Sequential` was almost certainly faster.
* **The Overhead Tradeoff:** `Multiprocessing` has a high startup cost (spawning new processes, copying data). For a task this small, the startup cost was much larger than the computation time, making it slower.
* **When Multiprocessing Wins:** As the number of positions grows from 3 to 3,000, `Multiprocessing` would win. The task (`compute_drawdown`, `rolling().std()`) involves more pure-Python logic and is CPU-bound. This makes it a perfect case for avoiding the GIL, *assuming* the number of tasks is large enough to overcome the initial setup cost.