
import io
import statistics
import time
from contextlib import redirect_stdout

from matching_engine import MatchingEngine
from cython_engine import CythonMatchingEngine
from benchmark_match import generate_orders

TOTAL_ORDERS = 5000
WARMUP_RUNS = 2
MEASURED_RUNS = 15


def run_engine(engine_type, orders):
    if engine_type == "Python":
        engine = MatchingEngine()
    else:
        engine = CythonMatchingEngine(capacity=len(orders))

    for order_id, side, price, quantity in orders:
        if side == "BUY":
            accepted = engine.add_buy_order(order_id, price, quantity)
        else:
            accepted = engine.add_sell_order(order_id, price, quantity)

        if accepted is False:
            raise RuntimeError(f"Order rejected: {order_id}")

        # Match immediately after every incoming order.
        engine.match_orders()

    if engine_type == "Python":
        books = (engine.buy_orders, engine.sell_orders)
        trades = engine.trades
    else:
        books = engine.get_order_books()
        trades = engine.get_trades()

    return books, trades


def measure(name, engine_type, orders):
    timings = []
    reference_result = None

    for run in range(WARMUP_RUNS + MEASURED_RUNS):
        start = time.perf_counter_ns()

        with redirect_stdout(io.StringIO()):
            result = run_engine(engine_type, orders)

        elapsed_ms = (time.perf_counter_ns() - start) / 1_000_000

        if run >= WARMUP_RUNS:
            timings.append(elapsed_ms)

            if reference_result is None:
                reference_result = result
            elif result != reference_result:
                raise AssertionError(
                    f"{name} results changed between runs"
                )

    median_ms = statistics.median(timings)

    print(f"\n{name} — streaming workload")
    print(f"Median: {median_ms:.3f} ms")
    print(f"Min:    {min(timings):.3f} ms")
    print(f"Max:    {max(timings):.3f} ms")

    return reference_result, median_ms


if __name__ == "__main__":
    orders = generate_orders(TOTAL_ORDERS)

    print(f"Orders: {TOTAL_ORDERS}")
    print(f"Warm-up runs: {WARMUP_RUNS}")
    print(f"Measured runs: {MEASURED_RUNS}")

    python_result, python_ms = measure(
        "Python Engine", "Python", orders
    )
    cython_result, cython_ms = measure(
        "Cython Engine", "Cython", orders
    )

    if python_result != cython_result:
        raise AssertionError("Python and Cython results differ!")

    print("\nCorrectness: PASS — identical final results.")
    print(f"Median speedup: {python_ms / cython_ms:.2f}x")
