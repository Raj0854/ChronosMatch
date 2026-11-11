import asyncio
import hashlib
import multiprocessing as mp
import os
import random
import sqlite3
import statistics
import struct
import sys
import time

from multiprocessing import shared_memory

from market_simulator import generate_orders
from cython_engine import CythonMatchingEngine
from latency_dashboard import run_dashboard

# ============================================================
# CONFIGURATION
# ============================================================

# Includes producer timestamp.
ORDER = struct.Struct("<IIIIQ")
HEADER = struct.Struct("<IIII")

RING_BUFFER_CAPACITY = 1024

WARMUP_RUNS = 1
MEASURED_RUNS = 5

WORKLOADS = [50_000, 100_000, 250_000]

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "chronosmatch.db")


# Dashboard shared metrics:
# 0  = orders received
# 1  = orders processed
# 2  = trades executed
# 3  = total traded quantity
# 4  = remaining buy orders
# 5  = remaining sell orders
# 6  = best bid price
# 7  = best bid quantity
# 8  = best ask price
# 9  = best ask quantity
# 10 = total IPC latency ns
# 11 = maximum IPC latency ns
# 12 = pipeline start timestamp ns


# ============================================================
# SHARED MEMORY RING BUFFER
# ============================================================


class RingBuffer:

    def __init__(self, shm, lock):

        self.shm = shm
        self.lock = lock

        self.capacity = HEADER.unpack(self.shm.buf[: HEADER.size])[3]

    def push(self, order):

        order_id, side, price, quantity = order

        if side in (0, "BUY"):
            side_code = 0
        elif side in (1, "SELL"):
            side_code = 1
        else:
            raise ValueError(f"Invalid order side: {side}")

        with self.lock:

            write_pos, read_pos, count, capacity = HEADER.unpack(
                self.shm.buf[: HEADER.size]
            )

            if count >= capacity:
                return False

            # Timestamp is captured when the order enters the buffer.
            timestamp_ns = time.perf_counter_ns()

            offset = HEADER.size + write_pos * ORDER.size

            ORDER.pack_into(
                self.shm.buf, offset, order_id, side_code, price, quantity, timestamp_ns
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

            order_id, side_code, price, quantity, timestamp_ns = ORDER.unpack_from(
                self.shm.buf, offset
            )

            read_pos = (read_pos + 1) % capacity
            count -= 1

            HEADER.pack_into(self.shm.buf, 0, write_pos, read_pos, count, capacity)

            return (order_id, side_code, price, quantity, timestamp_ns)

    def is_empty(self):

        with self.lock:

            _, _, count, _ = HEADER.unpack(self.shm.buf[: HEADER.size])

            return count == 0


# ============================================================
# ASYNC PRODUCER
# ============================================================


async def producer(buffer, total_orders=10, delay=0):

    async for order in generate_orders(total_orders=total_orders, delay=delay):

        while not buffer.push(order):
            await asyncio.sleep(0.0001)


def producer_process(shm_name, lock, producer_done, total_orders, generation_delay):

    shm = shared_memory.SharedMemory(name=shm_name)

    try:

        buffer = RingBuffer(shm, lock)

        random.seed(42)

        asyncio.run(producer(buffer, total_orders, generation_delay))

    finally:

        producer_done.value = True
        shm.close()


# ============================================================
# SQLITE DATABASE WRITER
# ============================================================


def database_writer_process(db_path, trade_queue, saved_count):

    connection = sqlite3.connect(db_path)

    try:

        cursor = connection.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trades (

                trade_id INTEGER PRIMARY KEY AUTOINCREMENT,

                run_id TEXT NOT NULL,

                buy_order_id INTEGER NOT NULL,

                sell_order_id INTEGER NOT NULL,

                price INTEGER NOT NULL,

                quantity INTEGER NOT NULL,

                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_trades_run_id
            ON trades(run_id)
        """)

        connection.commit()

        total_saved = 0

        while True:

            item = trade_queue.get()

            if item is None:
                break

            run_id, batch = item

            if not batch:
                continue

            rows = [
                (run_id, buy_order_id, sell_order_id, price, quantity)
                for (buy_order_id, sell_order_id, price, quantity) in batch
            ]

            cursor.executemany(
                """
                INSERT INTO trades (
                    run_id,
                    buy_order_id,
                    sell_order_id,
                    price,
                    quantity
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                rows,
            )

            connection.commit()

            total_saved += len(rows)

        saved_count.value = total_saved

    finally:

        connection.close()


# ============================================================
# UPDATE DASHBOARD METRICS
# ============================================================


def update_dashboard_stats(
    stats,
    received,
    trades,
    traded_quantity,
    order_books,
    total_latency_ns,
    max_latency_ns,
):

    buy_book, sell_book = order_books

    with stats.get_lock():

        stats[0] = received
        stats[1] = received

        stats[2] = trades
        stats[3] = traded_quantity

        stats[4] = len(buy_book)
        stats[5] = len(sell_book)

        if buy_book:

            stats[6] = buy_book[0][1]
            stats[7] = buy_book[0][2]

        else:

            stats[6] = 0
            stats[7] = 0

        if sell_book:

            stats[8] = sell_book[0][1]
            stats[9] = sell_book[0][2]

        else:

            stats[8] = 0
            stats[9] = 0

        stats[10] = total_latency_ns
        stats[11] = max_latency_ns


# ============================================================
# CONSUMER PROCESS
# ============================================================


def consumer_process(
    shm_name,
    lock,
    producer_done,
    total_orders,
    engine_capacity,
    trade_queue,
    run_id,
    result_queue,
    dashboard_stats,
):

    shm = shared_memory.SharedMemory(name=shm_name)

    try:

        buffer = RingBuffer(shm, lock)

        engine = CythonMatchingEngine(capacity=engine_capacity)

        received = 0
        accepted = 0
        rejected = 0

        queued_trade_count = 0

        total_latency_ns = 0
        max_latency_ns = 0

        last_dashboard_update = 0

        while not producer_done.value or not buffer.is_empty():

            order = buffer.pop()

            if order is None:

                time.sleep(0.00001)
                continue

            order_id, side, price, quantity, timestamp_ns = order

            received += 1

            # Actual IPC latency measurement.
            latency_ns = max(0, time.perf_counter_ns() - timestamp_ns)

            total_latency_ns += latency_ns

            max_latency_ns = max(max_latency_ns, latency_ns)

            try:
                if side == 0:
                    engine.add_buy_order(order_id, price, quantity)
                elif side == 1:
                    engine.add_sell_order(order_id, price, quantity)
                else:
                    raise ValueError(f"Invalid side code: {side}")

                accepted += 1

                engine.match_orders()

            except Exception as error:

                rejected += 1

                print(f"[CONSUMER] Order rejected: {order_id} | {error}")

            # Send newly generated trades every 1000 orders.
            if received % 25000 == 0:

                current_trades = engine.get_trades()

                new_trades = current_trades[queued_trade_count:]

                if new_trades:

                    trade_queue.put((run_id, new_trades))

                    queued_trade_count = len(current_trades)

                # Update live dashboard.
                order_books = engine.get_order_books()

                total_quantity = sum(trade[3] for trade in current_trades)

                update_dashboard_stats(
                    dashboard_stats,
                    received,
                    len(current_trades),
                    total_quantity,
                    order_books,
                    total_latency_ns,
                    max_latency_ns,
                )

                last_dashboard_update = received

        # Final matching results.
        trades = engine.get_trades()

        order_books = engine.get_order_books()

        # Send remaining trades to SQLite.
        remaining_trades = trades[queued_trade_count:]

        if remaining_trades:

            trade_queue.put((run_id, remaining_trades))

        total_traded_quantity = sum(trade[3] for trade in trades)

        remaining_buy_orders = len(order_books[0])
        remaining_sell_orders = len(order_books[1])

        # Final dashboard update.
        update_dashboard_stats(
            dashboard_stats,
            received,
            len(trades),
            total_traded_quantity,
            order_books,
            total_latency_ns,
            max_latency_ns,
        )

        # Hash complete matching output.
        output_data = repr((trades, order_books)).encode("utf-8")

        output_hash = hashlib.sha256(output_data).hexdigest()

        # Send summary only to avoid large queue transfers.
        result = {
            "received": received,
            "accepted": accepted,
            "rejected": rejected,
            "trade_count": len(trades),
            "total_traded_quantity": total_traded_quantity,
            "remaining_buy_orders": remaining_buy_orders,
            "remaining_sell_orders": remaining_sell_orders,
            "output_hash": output_hash,
        }

        result_queue.put(result)

        print("\n========== CONSUMER RESULTS ==========")

        print("Orders received:", received)
        print("Orders accepted:", accepted)
        print("Orders rejected:", rejected)

        print("Trades executed:", len(trades))

        print("Total traded quantity:", total_traded_quantity)

        print("Remaining buy orders:", remaining_buy_orders)

        print("Remaining sell orders:", remaining_sell_orders)

        print("======================================\n")

    finally:

        shm.close()


# ============================================================
# SINGLE PIPELINE EXECUTION
# ============================================================


def run_pipeline(run_number, total_orders=50000, show_dashboard=False):

    print("\n========================================")
    print(f"Starting Run: {run_number}")
    print(f"Total Orders: {total_orders:,}")
    print("========================================")

    ctx = mp.get_context("spawn")

    capacity = RING_BUFFER_CAPACITY

    engine_capacity = total_orders

    generation_delay = 0

    run_id = f"{run_number}-{time.time_ns()}"

    db_path = DB_PATH

    shm_size = HEADER.size + capacity * ORDER.size

    shm = shared_memory.SharedMemory(create=True, size=shm_size)

    HEADER.pack_into(shm.buf, 0, 0, 0, 0, capacity)

    lock = ctx.Lock()

    producer_done = ctx.Value("b", False)

    trade_queue = ctx.Queue()

    result_queue = ctx.Queue()

    saved_count = ctx.Value("i", 0)

    dashboard_stats = ctx.Array("q", 13, lock=True)

    dashboard_stop = ctx.Event()

    dashboard_stats[12] = time.perf_counter_ns()

    writer = ctx.Process(
        target=database_writer_process, args=(db_path, trade_queue, saved_count)
    )

    consumer = ctx.Process(
        target=consumer_process,
        args=(
            shm.name,
            lock,
            producer_done,
            total_orders,
            engine_capacity,
            trade_queue,
            run_id,
            result_queue,
            dashboard_stats,
        ),
    )

    producer_instance = ctx.Process(
        target=producer_process,
        args=(shm.name, lock, producer_done, total_orders, generation_delay),
    )

    dashboard_process = None

    start_time = time.perf_counter()

    try:

        writer.start()

        consumer.start()

        producer_instance.start()

        if show_dashboard:

            dashboard_process = ctx.Process(
                target=run_dashboard, args=(dashboard_stats, dashboard_stop)
            )

            dashboard_process.start()

        producer_instance.join()

        consumer.join()

        if producer_instance.exitcode != 0:

            raise RuntimeError("Producer process failed.")

        if consumer.exitcode != 0:

            raise RuntimeError("Consumer process failed.")

        # Stop dashboard after processing is complete.
        if dashboard_process:

            dashboard_stop.set()

            dashboard_process.join(timeout=3)

            if dashboard_process.is_alive():

                dashboard_process.terminate()
                dashboard_process.join()

        # Stop SQLite writer after all trade batches are queued.
        trade_queue.put(None)

        writer.join()

        if writer.exitcode != 0:

            raise RuntimeError("SQLite writer process failed.")

        elapsed = time.perf_counter() - start_time

        result = result_queue.get(timeout=10)

        # Verify SQLite records for this run.
        connection = sqlite3.connect(db_path)

        try:

            cursor = connection.cursor()

            cursor.execute(
                """
                SELECT
                    COUNT(*),
                    COALESCE(SUM(quantity), 0)
                FROM trades
                WHERE run_id = ?
                """,
                (run_id,),
            )

            database_count, database_quantity = cursor.fetchone()

        finally:

            connection.close()

        throughput = total_orders / elapsed if elapsed > 0 else 0

        order_loss = total_orders - result["received"]

        result.update(
            {
                "run_id": run_id,
                "execution_time": elapsed,
                "throughput": throughput,
                "order_loss": order_loss,
                "saved_trades": database_count,
                "database_quantity": database_quantity,
            }
        )

        # Actual latency summary.
        with dashboard_stats.get_lock():

            total_latency = dashboard_stats[10]
            maximum_latency = dashboard_stats[11]

        average_latency_us = (
            total_latency / result["received"] / 1000 if result["received"] else 0
        )

        maximum_latency_us = maximum_latency / 1000

        result["average_latency_us"] = average_latency_us
        result["maximum_latency_us"] = maximum_latency_us

        print("\n========== PIPELINE RESULT ==========")

        print("Orders sent:", total_orders)
        print("Orders received:", result["received"])
        print("Order loss:", order_loss)

        print("Trades executed:", result["trade_count"])

        print("Trades saved to SQLite:", database_count)

        print("Total traded quantity:", result["total_traded_quantity"])

        print(f"Execution time: {elapsed:.4f} seconds")

        print(f"Throughput: {throughput:,.2f} orders/sec")

        print(f"Average IPC latency: {average_latency_us:.2f} us")

        print(f"Maximum IPC latency: {maximum_latency_us:.2f} us")

        print("=====================================\n")

        return result

    finally:

        dashboard_stop.set()

        # Cleanup remaining processes.
        for process in (producer_instance, consumer, writer, dashboard_process):

            if process is not None and process.is_alive():

                process.terminate()
                process.join()

        shm.close()
        shm.unlink()

        trade_queue.close()
        result_queue.close()

        trade_queue.join_thread()
        result_queue.join_thread()


# ============================================================
# CORRECTNESS VALIDATION
# ============================================================


def validate_results(results, total_orders):

    result = results[-1]

    same_trade_count = all(
        item["trade_count"] == result["trade_count"] for item in results
    )

    same_quantity = all(
        item["total_traded_quantity"] == result["total_traded_quantity"]
        for item in results
    )

    same_order_books = all(
        item["output_hash"] == result["output_hash"] for item in results
    )

    same_hash = all(item["output_hash"] == result["output_hash"] for item in results)

    all_received = all(item["received"] == total_orders for item in results)

    zero_loss = all(item["order_loss"] == 0 for item in results)

    all_accepted = all(item["accepted"] == total_orders for item in results)

    zero_rejection = all(item["rejected"] == 0 for item in results)

    trade_consistency = all(
        item["trade_count"] == item["saved_trades"] for item in results
    )

    quantity_consistency = all(
        item["total_traded_quantity"] == item["database_quantity"] for item in results
    )

    print("\n========== CORRECTNESS RESULTS ==========")

    print("Orders received:", result["received"])
    print("Order loss:", result["order_loss"])

    print("Trades executed:", result["trade_count"])

    print("Trades saved to SQLite:", result["saved_trades"])

    print("Total traded quantity:", result["total_traded_quantity"])

    print("Remaining buy orders:", result["remaining_buy_orders"])

    print("Remaining sell orders:", result["remaining_sell_orders"])

    checks = {
        "All orders received": all_received,
        "Zero order loss": zero_loss,
        "All orders accepted": all_accepted,
        "Zero order rejection": zero_rejection,
        "Trade count consistency": trade_consistency,
        "SQLite persistence": trade_consistency,
        "Traded quantity consistency": quantity_consistency,
        "Order book consistency": same_order_books,
        "Complete output hash consistency": same_hash,
        "Trade consistency across runs": same_trade_count,
        "Quantity consistency across runs": same_quantity,
    }

    for name, passed in checks.items():

        print(f"{name}: {'PASS' if passed else 'FAIL'}")

    workload_passed = all(checks.values())

    print("\nWORKLOAD RESULT:", "PASS" if workload_passed else "FAIL")

    print("========================================")

    return workload_passed


# ============================================================
# BENCHMARK
# ============================================================


def benchmark_workload(total_orders):

    print("\n\n")
    print("#" * 55)

    print(f"BENCHMARK: {total_orders:,} ORDERS")

    print("#" * 55)

    print(f"Warm-up runs: {WARMUP_RUNS}")
    print(f"Measured runs: {MEASURED_RUNS}")

    print("\nRunning warm-up...")

    for index in range(WARMUP_RUNS):

        run_pipeline(f"{total_orders}-WARMUP-{index + 1}", total_orders)

    results = []

    print("\nRunning measured benchmarks...")

    for index in range(MEASURED_RUNS):

        result = run_pipeline(f"{total_orders}-RUN-{index + 1}", total_orders)

        results.append(result)

    execution_times = [item["execution_time"] for item in results]

    throughput_values = [item["throughput"] for item in results]

    print("\n========== BENCHMARK SUMMARY ==========")

    print("Orders:", f"{total_orders:,}")

    print(
        f"Median execution time: " f"{statistics.median(execution_times):.4f} seconds"
    )

    print(f"Average execution time: " f"{statistics.mean(execution_times):.4f} seconds")

    print(
        f"Median throughput: " f"{statistics.median(throughput_values):,.2f} orders/sec"
    )

    print("=======================================\n")

    return validate_results(results, total_orders)


# ============================================================
# MAIN
# ============================================================


def main():

    print("\n==========================================")

    print("CHRONOSMATCH - HIGH FREQUENCY TRADING ENGINE")

    print("==========================================")

    print("\nPermanent SQLite database:")
    print(DB_PATH)

    # Dashboard mode.
    if "--dashboard" in sys.argv:

        print("\nStarting live dashboard mode...")

        result = run_pipeline(
            "LIVE-DASHBOARD", total_orders=50_000, show_dashboard=True
        )

        if (
            result["received"] != 50_000
            or result["order_loss"] != 0
            or result["trade_count"] != result["saved_trades"]
        ):

            raise SystemExit("Dashboard pipeline validation failed.")

        print("\nLIVE DASHBOARD RUN: PASS")

        return

    # Normal benchmark mode.
    overall_passed = True

    for total_orders in WORKLOADS:

        passed = benchmark_workload(total_orders)

        if not passed:

            overall_passed = False

            print(f"\nBenchmark failed for {total_orders:,} orders.")

            break

    print("\n========== FINAL RESULT ==========")

    if overall_passed:

        print("ALL BENCHMARKS PASSED")

        print("SQLite database retained successfully.")

    else:

        print("BENCHMARK FAILED")

    print("Database:", DB_PATH)

    print("==================================")

    if not overall_passed:

        raise SystemExit(1)


if __name__ == "__main__":

    mp.freeze_support()

    main()
