
from time import perf_counter_ns
from benchmark_match import generate_orders, TOTAL_ORDERS
from cython_engine import CythonMatchingEngine

orders = generate_orders(TOTAL_ORDERS)

engine = CythonMatchingEngine(capacity=len(orders))

for order_id, side, price, quantity in orders:
    if side == "BUY":
        engine.add_buy_order(order_id, price, quantity)
    else:
        engine.add_sell_order(order_id, price, quantity)

start = perf_counter_ns()
buy_sort_ns, sell_sort_ns, matching_ns = engine.profile_stages()
total_ns = perf_counter_ns() - start

print(f"Buy sort:  {buy_sort_ns / 1_000_000:.3f} ms")
print(f"Sell sort: {sell_sort_ns / 1_000_000:.3f} ms")
print(f"Matching:  {matching_ns / 1_000_000:.3f} ms")
print(f"Total:     {total_ns / 1_000_000:.3f} ms")

loop_ns, shift_ns, buy_full, sell_full = engine.get_last_profile()

print(f"Matching loop time: {loop_ns / 1_000_000:.3f} ms")
print(f"Memory shift time:  {shift_ns / 1_000_000:.3f} ms")
print(f"Buy full fills:     {buy_full}")
print(f"Sell full fills:    {sell_full}")
