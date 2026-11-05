import asyncio
import time
import struct
import multiprocessing as mp
import random
import statistics
import math
import hashlib
from multiprocessing import shared_memory
from market_simulator import generate_orders
from cython_engine import CythonMatchingEngine

# --------------------------------------------------
# STEP 1: Binary memory layouts
# --------------------------------------------------

# order_id, side_code, price_ticks, quantity
ORDER = struct.Struct("<IIII")

# write_pos, read_pos, count, capacity
HEADER = struct.Struct("<IIII")


# --------------------------------------------------
# STEP 2: Shared Memory Ring Buffer
# --------------------------------------------------


class RingBuffer:
    def __init__(self, shm, lock):
        self.shm = shm
        self.lock = lock

    def push(self, order_id, side_code, price_ticks, quantity):
        with self.lock:
            write_pos, read_pos, count, capacity = HEADER.unpack_from(self.shm.buf, 0)

            if count >= capacity:
                return False

            offset = HEADER.size + write_pos * ORDER.size

            ORDER.pack_into(
                self.shm.buf, offset, order_id, side_code, price_ticks, quantity
            )

            write_pos = (write_pos + 1) % capacity
            count += 1

            HEADER.pack_into(self.shm.buf, 0, write_pos, read_pos, count, capacity)

            return True

    def pop(self):
        with self.lock:
            write_pos, read_pos, count, capacity = HEADER.unpack_from(self.shm.buf, 0)

            if count == 0:
                return None

            offset = HEADER.size + read_pos * ORDER.size

            order = ORDER.unpack_from(self.shm.buf, offset)

            read_pos = (read_pos + 1) % capacity
            count -= 1

            HEADER.pack_into(self.shm.buf, 0, write_pos, read_pos, count, capacity)

            return order

    def is_empty(self):
        with self.lock:
            _, _, count, _ = HEADER.unpack_from(self.shm.buf, 0)
            return count == 0


# --------------------------------------------------
# STEP 3: Producer Process
# --------------------------------------------------


async def producer(buffer, total_orders=10, delay=0.1):
    sent = 0

    async for order in generate_orders(total_orders=total_orders, delay=delay):
        order_id, side, price_ticks, quantity = order
        side_code = 0 if side == "BUY" else 1

        while not buffer.push(order_id, side_code, price_ticks, quantity):
            await asyncio.sleep(0.001)

        sent += 1

        if sent % 1000 == 0:
            print(f"[PRODUCER] Sent: {sent}")

    print(f"[PRODUCER] Finished. Total sent: {sent}")


def producer_process(shm_name, lock, producer_done, total_orders, delay):
    shm = shared_memory.SharedMemory(name=shm_name)

    try:
        buffer = RingBuffer(shm, lock)
        random.seed(42)
        asyncio.run(producer(buffer, total_orders, delay))
    finally:
        producer_done.set()
        shm.close()


# --------------------------------------------------
# STEP 4: Consumer Process
# --------------------------------------------------


def consumer_process(
    shm_name,
    lock,
    producer_done,
    consumed_count,
    trade_count,
    traded_quantity,
    buy_remaining,
    sell_remaining,
    output_digest,
):

    shm = shared_memory.SharedMemory(name=shm_name)

    try:
        buffer = RingBuffer(shm, lock)
        engine = CythonMatchingEngine()

        consumed = 0

        while not producer_done.is_set() or not buffer.is_empty():

            order = buffer.pop()

            if order is None:
                time.sleep(0.001)
                continue

            order_id, side_code, price_ticks, quantity = order

            if side_code == 0:
                engine.add_buy_order(order_id, price_ticks, quantity)
            else:
                engine.add_sell_order(order_id, price_ticks, quantity)

            engine.match_orders()
            consumed += 1

            if consumed % 1000 == 0:
                print(f"[CONSUMER] Received: {consumed}")

        print("\nConsumer finished.")
        print("Total orders consumed:", consumed)
        buy_orders, sell_orders = engine.get_order_books()

        print("Remaining buy orders:", len(buy_orders))
        print("Remaining sell orders:", len(sell_orders))
        trades = engine.get_trades()
        # Generate SHA-256 digest of complete matching results
        final_output = repr((trades, buy_orders, sell_orders)).encode("utf-8")

        digest = hashlib.sha256(final_output).digest()

        # Store 32-byte digest in shared memory
        output_digest[:] = digest
        print("Total trades executed:", len(trades))
        print("Total traded quantity:", sum(t[3] for t in trades))
        consumed_count.value = consumed
        trade_count.value = len(trades)
        traded_quantity.value = sum(t[3] for t in trades)
        buy_remaining.value = len(buy_orders)
        sell_remaining.value = len(sell_orders)

    finally:
        shm.close()


# --------------------------------------------------
# STEP 5: Main Process
# --------------------------------------------------


def run_pipeline(run_number, total_orders=50000):
    generation_delay = 0
    capacity = 1024

    memory_size = HEADER.size + capacity * ORDER.size

    ctx = mp.get_context("spawn")

    lock = ctx.Lock()
    producer_done = ctx.Event()

    consumed_count = ctx.Value("i", 0)
    trade_count = ctx.Value("i", 0)
    traded_quantity = ctx.Value("q", 0)
    buy_remaining = ctx.Value("i", 0)
    sell_remaining = ctx.Value("i", 0)
    output_digest = ctx.Array("B", 32)
    shm = shared_memory.SharedMemory(create=True, size=memory_size)

    try:
        HEADER.pack_into(shm.buf, 0, 0, 0, 0, capacity)

        consumer = ctx.Process(
            target=consumer_process,
            args=(
                shm.name,
                lock,
                producer_done,
                consumed_count,
                trade_count,
                traded_quantity,
                buy_remaining,
                sell_remaining,
                output_digest,
            ),
        )

        producer = ctx.Process(
            target=producer_process,
            args=(shm.name, lock, producer_done, total_orders, generation_delay),
        )

        start_time = time.perf_counter()

        consumer.start()
        producer.start()

        producer.join()
        consumer.join()

        elapsed_time = time.perf_counter() - start_time

        if producer.exitcode != 0:
            raise RuntimeError("Producer process failed")

        if consumer.exitcode != 0:
            raise RuntimeError("Consumer process failed")

        received = consumed_count.value

        return {
            "run": run_number,
            "time": elapsed_time,
            "received": received,
            "loss": total_orders - received,
            "throughput": received / elapsed_time,
            "trades": trade_count.value,
            "quantity": traded_quantity.value,
            "buy_remaining": buy_remaining.value,
            "sell_remaining": sell_remaining.value,
            "output_hash": bytes(output_digest[:]).hex(),
        }

    finally:
        shm.close()
        shm.unlink()


def main():
    total_orders = 50000
    warmup_runs = 1
    measured_runs = 10

    print("========== IPC BENCHMARK ==========")
    print("Orders per run:", total_orders)
    print("Warm-up runs:", warmup_runs)
    print("Measured runs:", measured_runs)

    print("\nRunning warm-up...")

    for i in range(warmup_runs):
        run_pipeline(f"WARMUP-{i + 1}", total_orders)

    results = []

    print("\nRunning measured benchmarks...")

    for i in range(1, measured_runs + 1):
        result = run_pipeline(i, total_orders)
        results.append(result)

        print(
            f"Run {i:02d}/{measured_runs} | "
            f"Time: {result['time']:.4f}s | "
            f"Throughput: {result['throughput']:.2f} orders/sec"
        )

    times = [r["time"] for r in results]
    throughputs = [r["throughput"] for r in results]

    sorted_times = sorted(times)
    p95_index = math.ceil(0.95 * len(sorted_times)) - 1
    p95 = sorted_times[p95_index]

    print("\n========== FINAL IPC RESULTS ==========")

    print("Orders per run:", total_orders)
    print("Median execution time:", f"{statistics.median(times):.4f} sec")
    print("P95 execution time:", f"{p95:.4f} sec")
    print("Median throughput:", f"{statistics.median(throughputs):.2f} orders/sec")

    print("\n========== CORRECTNESS ==========")

    print("Orders received:", results[-1]["received"])
    print("Order loss:", results[-1]["loss"])
    print("Trades executed:", results[-1]["trades"])
    print("Total traded quantity:", results[-1]["quantity"])
    print("Remaining buy orders:", results[-1]["buy_remaining"])
    print("Remaining sell orders:", results[-1]["sell_remaining"])

    all_received = all(r["received"] == total_orders for r in results)
    no_loss = all(r["loss"] == 0 for r in results)

    same_trades = len(set(r["trades"] for r in results)) == 1
    same_quantity = len(set(r["quantity"] for r in results)) == 1
    same_output = len(set(r["output_hash"] for r in results)) == 1

    print("Trade count consistency:", "PASS" if same_trades else "FAIL")
    print("Traded quantity consistency:", "PASS" if same_quantity else "FAIL")
    print("Complete output hash consistency:", "PASS" if same_output else "FAIL")
    if all_received and no_loss and same_trades and same_quantity and same_output:
        print("\nRESULT: PASS")
        print("All IPC runs completed with consistent results.")
    else:
        print("\nRESULT: FAIL")
        print("IPC correctness or consistency check failed.")

    print("======================================")


if __name__ == "__main__":
    mp.freeze_support()
    main()
