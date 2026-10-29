
import statistics
import time

from benchmark_match import generate_orders, TOTAL_ORDERS
from cython_engine import CythonMatchingEngine


WARMUP_RUNS = 3
MEASURED_RUNS = 20


# Generate the workload once so both modes use identical orders.
ORDERS = generate_orders(TOTAL_ORDERS)


def run_once(profile_enabled):
    engine = CythonMatchingEngine(capacity=len(ORDERS))

    for order_id, side, price, quantity in ORDERS:
        if side == "BUY":
            engine.add_buy_order(order_id, price, quantity)
        else:
            engine.add_sell_order(order_id, price, quantity)

    start = time.perf_counter_ns()

    if profile_enabled:
        engine.profile_stages()
    else:
        engine.match_orders()

    elapsed_ns = time.perf_counter_ns() - start

    return elapsed_ns / 1_000_000, engine.get_trades(), engine.get_order_books()


def percentile(values, p):
    ordered = sorted(values)
    index = round((len(ordered) - 1) * p / 100)
    return ordered[index]


# Warm up both modes.
for _ in range(WARMUP_RUNS):
    run_once(False)
    run_once(True)


normal_times = []
profiled_times = []
correctness_pass = True

# Alternate execution order to reduce order-related timing bias.
for run_index in range(MEASURED_RUNS):
    if run_index % 2 == 0:
        normal_result = run_once(False)
        profiled_result = run_once(True)
    else:
        profiled_result = run_once(True)
        normal_result = run_once(False)

    normal_times.append(normal_result[0])
    profiled_times.append(profiled_result[0])

    if normal_result[1:] != profiled_result[1:]:
        correctness_pass = False


normal_median = statistics.median(normal_times)
profiled_median = statistics.median(profiled_times)

print(f"Orders per run: {TOTAL_ORDERS}")
print(f"Warm-up runs per mode: {WARMUP_RUNS}")
print(f"Measured runs per mode: {MEASURED_RUNS}")

print("\nNormal matching")
print(f"Median: {normal_median:.3f} ms")
print(f"P95:    {percentile(normal_times, 95):.3f} ms")

print("\nProfiling-enabled matching")
print(f"Median: {profiled_median:.3f} ms")
print(f"P95:    {percentile(profiled_times, 95):.3f} ms")

if normal_median > 0:
    overhead_pct = (
        (profiled_median - normal_median) / normal_median
    ) * 100
    print(f"\nMeasured difference: {overhead_pct:+.2f}%")

print(
    "\nCorrectness:",
    "PASS — identical trades and remaining order books."
    if correctness_pass
    else "FAIL — outputs differ!"
)
