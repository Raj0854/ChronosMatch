
import statistics
import time

from cython_engine import CythonMatchingEngine
from benchmark_match import generate_orders

TOTAL_ORDERS = 5000
WARMUP_RUNS = 2
MEASURED_RUNS = 15


def measure_stages(orders):
    buy_sort_times = []
    sell_sort_times = []
    matching_times = []

    for run in range(WARMUP_RUNS + MEASURED_RUNS):
        engine = CythonMatchingEngine(capacity=len(orders))

        for order_id, side, price, quantity in orders:
            if side == "BUY":
                accepted = engine.add_buy_order(
                    order_id, price, quantity
                )
            else:
                accepted = engine.add_sell_order(
                    order_id, price, quantity
                )

            if accepted is False:
                raise RuntimeError(f"Order rejected: {order_id}")

        buy_ns, sell_ns, matching_ns = engine.profile_stages()

        if run >= WARMUP_RUNS:
            buy_sort_times.append(buy_ns / 1_000_000)
            sell_sort_times.append(sell_ns / 1_000_000)
            matching_times.append(matching_ns / 1_000_000)

    print(f"\nCython stage profiling — {len(orders)} orders")
    print(
        f"Buy sorting median:  "
        f"{statistics.median(buy_sort_times):.3f} ms"
    )
    print(
        f"Sell sorting median: "
        f"{statistics.median(sell_sort_times):.3f} ms"
    )
    print(
        f"Trade matching median: "
        f"{statistics.median(matching_times):.3f} ms"
    )


if __name__ == "__main__":
    measure_stages(generate_orders(TOTAL_ORDERS))
