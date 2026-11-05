
import random
import time
import statistics

from matching_engine import MatchingEngine
from cython_engine import CythonMatchingEngine


TOTAL_ORDERS = 50000
WARMUP_RUNS = 2
MEASURED_RUNS = 10


def generate_test_orders(total_orders=TOTAL_ORDERS):
    rng = random.Random(42)
    orders = []

    for order_id in range(1, total_orders + 1):
        side = rng.choice(["BUY", "SELL"])
        price = rng.randint(9900, 10100)
        quantity = rng.randint(1, 10)

        orders.append((order_id, side, price, quantity))

    return orders


def run_python_engine(orders):
    engine = MatchingEngine()

    for order_id, side, price, quantity in orders:
        if side == "BUY":
            engine.add_buy_order(order_id, price, quantity)
        else:
            engine.add_sell_order(order_id, price, quantity)

        engine.match_orders()

    return engine.trades, (
        engine.buy_orders,
        engine.sell_orders
    )


def run_cython_engine(orders):
    engine = CythonMatchingEngine()

    for order_id, side, price, quantity in orders:
        if side == "BUY":
            engine.add_buy_order(order_id, price, quantity)
        else:
            engine.add_sell_order(order_id, price, quantity)

        engine.match_orders()

    return engine.get_trades(), engine.get_order_books()


def percentile(values, percentile_value):
    sorted_values = sorted(values)
    index = round((len(sorted_values) - 1) * percentile_value / 100)
    return sorted_values[index]


def main():
    orders = generate_test_orders()

    print("Generated orders:", len(orders))
    print("Warm-up runs:", WARMUP_RUNS)
    print("Measured runs:", MEASURED_RUNS)

    python_times = []
    cython_times = []

    python_result = None
    cython_result = None

    # Warm-up runs
    print("\nRunning warm-up...")

    for _ in range(WARMUP_RUNS):
        run_python_engine(orders)
        run_cython_engine(orders)

    # Measured runs
    print("\nRunning benchmark...")

    for run_number in range(1, MEASURED_RUNS + 1):

        start = time.perf_counter()
        current_python_result = run_python_engine(orders)
        python_time = time.perf_counter() - start

        start = time.perf_counter()
        current_cython_result = run_cython_engine(orders)
        cython_time = time.perf_counter() - start

        python_times.append(python_time)
        cython_times.append(cython_time)

        if python_result is None:
            python_result = current_python_result
            cython_result = current_cython_result

        print(
            f"Run {run_number:02d}/{MEASURED_RUNS} | "
            f"Python: {python_time:.4f}s | "
            f"Cython: {cython_time:.4f}s"
        )

    # Correctness comparison
    trades_match = python_result[0] == cython_result[0]
    books_match = python_result[1] == cython_result[1]

    # Performance statistics
    python_median = statistics.median(python_times)
    cython_median = statistics.median(cython_times)

    python_p95 = percentile(python_times, 95)
    cython_p95 = percentile(cython_times, 95)

    print("\n========== PERFORMANCE RESULTS ==========")

    print(f"Python Median: {python_median:.4f} seconds")
    print(f"Cython Median: {cython_median:.4f} seconds")

    print(f"Python P95: {python_p95:.4f} seconds")
    print(f"Cython P95: {cython_p95:.4f} seconds")

    print(f"Median Speedup: {python_median / cython_median:.2f}x")

    print("\n========== CORRECTNESS COMPARISON ==========")

    print("Python trades:", len(python_result[0]))
    print("Cython trades:", len(cython_result[0]))

    print("Trades identical:", trades_match)
    print("Order books identical:", books_match)

    if trades_match and books_match:
        print("\nRESULT: PASS")
        print("Python and Cython engines produced identical results.")
    else:
        print("\nRESULT: FAIL")

        if not trades_match:
            print("Trade results differ.")

        if not books_match:
            print("Remaining order books differ.")

    print("============================================")


if __name__ == "__main__":
    main()
