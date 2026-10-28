
import random
import statistics
import time
import io
from contextlib import redirect_stdout
from matching_engine import MatchingEngine
from cython_engine import CythonMatchingEngine


TOTAL_ORDERS = 5000
WARMUP_RUNS = 2
MEASURED_RUNS = 15


def generate_orders(total_orders):
    rng = random.Random(42)
    orders = []

    for order_id in range(1, total_orders + 1):
        side = "BUY" if order_id % 2 == 0 else "SELL"
        price = rng.randint(9900, 10100)
        quantity = rng.randint(1, 10)
        orders.append((order_id, side, price, quantity))

    return orders


def run_python(orders):
    engine = MatchingEngine()

    for order_id, side, price, quantity in orders:
        if side == "BUY":
            accepted = engine.add_buy_order(order_id, price, quantity)
        else:
            accepted = engine.add_sell_order(order_id, price, quantity)

        if accepted is False:
            raise RuntimeError(f"Python engine rejected order {order_id}")

    engine.match_orders()
    return (engine.buy_orders, engine.sell_orders), engine.trades


def run_cython(orders):
    engine = CythonMatchingEngine(capacity=len(orders))

    for order_id, side, price, quantity in orders:
        if side == "BUY":
            accepted = engine.add_buy_order(order_id, price, quantity)
        else:
            accepted = engine.add_sell_order(order_id, price, quantity)

        if accepted is False:
            raise RuntimeError(f"Cython engine rejected order {order_id}")

    engine.match_orders()
    return engine.get_order_books(), engine.get_trades()


def percentile(values, percent):
    sorted_values = sorted(values)
    index = round((percent / 100) * (len(sorted_values) - 1))
    return sorted_values[index]


def measure_engine(name, run_engine, orders):
    # Warm-up runs are excluded from the measurements.
    for _ in range(WARMUP_RUNS):
        with redirect_stdout(io.StringIO()):
            run_engine(orders)

    elapsed_ms = []
    reference_result = None

    for run_number in range(MEASURED_RUNS):
        start = time.perf_counter_ns()

        with redirect_stdout(io.StringIO()):
            result = run_engine(orders)

        elapsed_ns = time.perf_counter_ns() - start

        elapsed_ms.append(elapsed_ns / 1_000_000)

        # Verify output consistency between repeated runs.
        if reference_result is None:
            reference_result = result
        elif result != reference_result:
            raise AssertionError(
                f"{name} produced inconsistent results between runs"
            )

    print(f"\n{name}")
    print(f"  Median: {statistics.median(elapsed_ms):.3f} ms")
    print(f"  P95:    {percentile(elapsed_ms, 95):.3f} ms")
    print(f"  P99:    {percentile(elapsed_ms, 99):.3f} ms")
    print(f"  Min:    {min(elapsed_ms):.3f} ms")
    print(f"  Max:    {max(elapsed_ms):.3f} ms")

    return reference_result, statistics.median(elapsed_ms)


if __name__ == "__main__":
    orders = generate_orders(TOTAL_ORDERS)

    print(f"Orders per run: {TOTAL_ORDERS}")
    print(f"Warm-up runs: {WARMUP_RUNS}")
    print(f"Measured runs: {MEASURED_RUNS}")

    python_result, python_median = measure_engine(
        "Python Engine", run_python, orders
    )

    cython_result, cython_median = measure_engine(
        "Cython Engine", run_cython, orders
    )

    # Both engines must produce the same order book and trade history.
    if python_result != cython_result:
        raise AssertionError(
            "Mismatch: Python and Cython engine results differ!"
        )

    print("\nCorrectness: PASS — both engines produced identical results.")

    if cython_median > 0:
        speedup = python_median / cython_median
        print(f"Median speedup: {speedup:.2f}x")
