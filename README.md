# ChronosMatch
### Zero-Copy High-Frequency Trading Engine

ChronosMatch is a high-performance trading engine prototype developed in Python and Cython. It demonstrates order matching, shared-memory inter-process communication (IPC), asynchronous market simulation, latency monitoring, and persistent trade storage.

The project focuses on reducing Python-level overhead in order matching and efficiently transferring large volumes of market orders between processes.

---

## Key Features

- **Cython Matching Engine:** Uses C structures, pointers, and typed data for efficient order-book operations.
- **Price-Time Priority:** Matches orders using price priority followed by arrival time.
- **Shared-Memory IPC:** Uses a custom ring buffer backed by `multiprocessing.shared_memory` to transfer orders between processes without pickling individual orders.
- **Async Market Simulator:** Generates simulated market orders using Python's `asyncio`.
- **Latency Monitoring:** Provides a live terminal dashboard using Python `curses`.
- **SQLite Trade Persistence:** Stores executed trades in a SQLite database through a separate background process.
- **Stress Testing:** Includes a one-million-order IPC stress test.
- **Correctness Testing:** Includes Python and Cython engine tests and ring-buffer tests.

---

## System Architecture

```text
             Market Simulator
                  |
             Async Producer
                  |
                  v
       Shared Memory Ring Buffer
                  |
                  v
             Consumer
                  |
                  v
         Cython Matching Engine
                  |
          +-------+-------+
          |               |
          v               v
       Order Book     Executed Trades
                          |
                          v
                    Trade Queue
                          |
                          v
                   SQLite Writer
                          |
                          v
                    SQLite Database

       Latency Dashboard
       (Shared Statistics)
```

### Processing Flow

1. The market simulator generates mock buy and sell orders.
2. The producer places orders into the shared-memory ring buffer.
3. The consumer retrieves orders from the buffer.
4. The Cython engine validates and processes orders.
5. The matching engine executes trades according to Price-Time Priority.
6. Executed trades are sent to a background SQLite writer.
7. The dashboard displays order, trade, order-book, throughput, and IPC latency statistics.

---

## Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.10 | Core application and process management |
| Cython 3.x | Optimized matching engine |
| C | Low-level structures and pointer-based operations |
| asyncio | Asynchronous market simulation |
| multiprocessing | Producer-consumer process architecture |
| shared_memory | Inter-process shared-memory communication |
| struct | Binary order serialization |
| SQLite | Persistent trade storage |
| curses | Live terminal monitoring |
| unittest | Automated testing |

---

## Project Structure

```text
ChronosMatch/
│
├── main.py
├── market_simulator.py
├── matching_engine.py
├── cython_engine.pyx
├── order_struct.pyx
├── setup.py
│
├── latency_dashboard.py
│
├── benchmark_match.py
├── benchmark_profiling.py
├── benchmark_streaming.py
│
├── profile_cython.py
├── profile_engine.py
│
├── test_cython_engine.py
├── test_matching_engine.py
├── test_ring_buffer.py
├── test_engine_comparison.py
├── test_ipc_stress.py
│
├── .gitignore
└── README.md
```

The SQLite database is generated locally when the application runs and is excluded from Git tracking.

---

## Requirements

- Python 3.10
- Cython 3.x
- setuptools
- A C compiler:
  - Microsoft Visual C++ Build Tools on Windows
  - GCC or another supported C compiler on Linux

For Windows dashboard support, install `windows-curses`.

---

## Installation and Setup

### 1. Clone the Repository

```bash
git clone https://github.com/Raj0854/ChronosMatch.git
cd ChronosMatch
```

### 2. Install Dependencies

```bash
python -m pip install Cython setuptools
```

For Windows:

```bash
python -m pip install windows-curses
```

### 3. Build Cython Extensions

```bash
python setup.py build_ext --inplace --force
```

This compiles the Cython source files into Python extension modules.

### 4. Verify the Extensions

```bash
python -c "from cython_engine import CythonMatchingEngine; print('Cython Engine: PASS')"
```

```bash
python -c "import order_struct; print('Order Struct: PASS')"
```

---

## Running the Project

### Run the Main Benchmark

```bash
python main.py
```

Runs the configured workloads and validates processing correctness, trade persistence, and output consistency.

### Run the Live Latency Dashboard

```bash
python main.py --dashboard
```

Displays live system statistics, including:

- Orders received and processed
- Trades executed
- Total traded quantity
- Best bid and best ask
- Processing throughput
- Average and maximum IPC latency

### Run the One-Million-Order Stress Test

```bash
python test_ipc_stress.py
```

This test sends one million simulated orders through the producer-consumer IPC pipeline and validates order delivery and SQLite trade persistence.

### Run Unit Tests

```bash
python -m unittest -v
```

---

## Verified Test Results

The project has passed the following automated tests:

| Test Category | Result |
|---|---:|
| Cython Matching Engine | 13 tests passed |
| Python Matching Engine | 9 tests passed |
| Ring Buffer | 5 tests passed |
| **Total** | **27/27 passed** |

The tests cover:

- Price-Time Priority
- Partial and full order matching
- Duplicate and invalid order rejection
- Order-book consistency
- Ring-buffer FIFO behavior
- Circular wraparound
- Buffer capacity limits
- Randomized Python/Cython equivalence
- Trade storage and memory growth

---

## One Million Order Stress Test

The following results were recorded during a one-million-order IPC stress test.

| Metric | Measured Result |
|---|---:|
| Orders sent | 1,000,000 |
| Orders received | 1,000,000 |
| Orders accepted | 1,000,000 |
| Orders rejected | 0 |
| Order loss | 0 |
| Trades executed | 710,514 |
| Trades saved to SQLite | 710,514 |
| Total traded quantity | 2,158,437 |
| Execution time | 35.2703 seconds |
| Throughput | 28,352.43 orders/sec |
| Average IPC latency | 24,134.91 µs |
| Maximum IPC latency | 416,808.90 µs |

**Stress Test Result: PASS**

All one million orders were received and accepted, with zero order loss. All executed trades were successfully saved to SQLite.

### Understanding the Latency Metric

IPC latency is measured from order insertion into the shared-memory ring buffer to order retrieval by the consumer. It is not the complete exchange execution latency.

---

## Performance Notes and Limitations

- Throughput depends on the hardware, workload, process scheduling, and database activity.
- The measured one-million-order throughput was approximately 28.35K orders/sec.
- The original 100K orders/sec target has not yet been achieved.
- The measured IPC latency does not establish a sub-millisecond end-to-end execution guarantee.
- The market simulator generates mock orders; it is not connected to a live exchange.
- The IPC implementation uses Python's `multiprocessing.shared_memory` with a custom binary ring buffer.

These results represent measurements from the tested development environment and should not be treated as universal performance guarantees.

---

## Database

ChronosMatch stores executed trades in a local SQLite database named:

```text
chronosmatch.db
```

The database is created and maintained by the application. It is intentionally excluded from the Git repository because it contains runtime data and can become large.

---

## Future Improvements

- Optimize producer-consumer throughput toward the 100K orders/sec target.
- Reduce IPC and end-to-end processing latency.
- Improve matching-loop profiling and memory-management overhead.
- Explore more efficient trade batching and persistence.
- Evaluate alternative database backends for larger workloads.

---

## Author

**Raj Gupta**

GitHub: [@Raj0854](https://github.com/Raj0854)

Project Repository: [ChronosMatch](https://github.com/Raj0854/ChronosMatch)

---

*ChronosMatch — A Python and Cython based high-performance trading engine prototype.*