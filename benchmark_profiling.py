import statistics
import time

from benchmark_match import generate_orders, TOTAL_ORDERS
from cython_engine import CythonMatchingEngine

WARMUP_RUNS = 3
MEASURED_RUNS = 20

# Generate identical workload for every run.
ORDERS = generate_orders(TOTAL_ORDERS)


def run_once(profile_enabled):

    engine = CythonMatchingEngine(capacity=len(ORDERS))

    start_ns = time.perf_counter_ns()
    internal_ns = 0

    for order_id, side, price, quantity in ORDERS:

        if side == "BUY":

            accepted = engine.add_buy_order(order_id, price, quantity)

        else:

            accepted = engine.add_sell_order(order_id, price, quantity)

        if accepted is False:
            continue

        if profile_enabled:

            engine.profile_stages()

            internal_ns += engine.get_last_profile()[0]

        else:

            engine.match_orders()

    elapsed_ns = time.perf_counter_ns() - start_ns

    trades = engine.get_trades()
    order_books = engine.get_order_books()

    return elapsed_ns, internal_ns, trades, order_books


def percentile(values, p):
    ordered = sorted(values)
    index = round((len(ordered) - 1) * p / 100)
    return ordered[index]


# Warm-up
print("Running warm-up...")
for _ in range(WARMUP_RUNS):
    run_once(False)
    run_once(True)


normal_times = []
profiled_times = []
internal_times = []
trade_counts = []
correctness_pass = True

print("Running measured benchmarks...")

for run_index in range(MEASURED_RUNS):
    if run_index % 2 == 0:
        normal = run_once(False)
        profiled = run_once(True)
    else:
        profiled = run_once(True)
        normal = run_once(False)

    normal_times.append(normal[0])
    profiled_times.append(profiled[0])
    internal_times.append(profiled[1])
    trade_counts.append(len(profiled[2]))

    if normal[2:] != profiled[2:]:
        correctness_pass = False

# Statistics in nanoseconds
normal_median_ns = statistics.median(normal_times)
profiled_median_ns = statistics.median(profiled_times)
internal_median_ns = statistics.median(internal_times)

normal_p95_ns = percentile(normal_times, 95)
profiled_p95_ns = percentile(profiled_times, 95)
internal_p95_ns = percentile(internal_times, 95)

median_trade_count = statistics.median(trade_counts)

print("\n========== NANOSECOND LATENCY REPORT ==========")

print(f"Orders per run: {TOTAL_ORDERS:,}")
print(f"Trades per run (median): {median_trade_count:,.0f}")
print(f"Warm-up runs: {WARMUP_RUNS}")
print(f"Measured runs: {MEASURED_RUNS}")

print("\nNORMAL MATCHING")
print(f"Median: {normal_median_ns:,.0f} ns")
print(f"P95:    {normal_p95_ns:,.0f} ns")

print("\nPROFILING-ENABLED MATCHING")
print(f"Median: {profiled_median_ns:,.0f} ns")
print(f"P95:    {profiled_p95_ns:,.0f} ns")

print("\nINTERNAL CYTHON MATCHING LOOP")
print(f"Median: {internal_median_ns:,.0f} ns")
print(f"P95:    {internal_p95_ns:,.0f} ns")

if median_trade_count > 0:
    average_ns = internal_median_ns / median_trade_count
    print(f"Average matching time per trade: {average_ns:,.2f} ns")
    print(f"Average matching time per trade: {average_ns / 1000:,.3f} µs")

if normal_median_ns > 0:
    overhead = ((profiled_median_ns - normal_median_ns) / normal_median_ns) * 100
    print(f"\nProfiling measurement difference: {overhead:+.2f}%")

print("\nCORRECTNESS:", "PASS" if correctness_pass else "FAIL")

print("===============================================")
