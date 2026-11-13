import asyncio
import gc
import hashlib
import mmap
import multiprocessing as mp
import os
import random
import sqlite3
import statistics
import sys
import tempfile
import time

from market_simulator import generate_orders
from cython_engine import CythonMatchingEngine
from native_ring import NativeRingBuffer
from latency_dashboard import run_dashboard

# ============================================================
# CONFIGURATION
# ============================================================

RING_BUFFER_CAPACITY = 4096
BATCH_SIZE = 128

NATIVE_HEADER_SIZE = 64
NATIVE_ORDER_SIZE = 24

WARMUP_RUNS = 1
MEASURED_RUNS = 5

WORKLOADS = [50_000, 100_000, 250_000]

DB_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "chronosmatch.db",
)


# ============================================================
# NATIVE RING BUFFER INITIALIZATION
# ============================================================


def initialize_native_ring(mapping, capacity):

    buffer = NativeRingBuffer(mapping, capacity)

    try:
        buffer.initialize()

    finally:
        del buffer
        gc.collect()


# ============================================================
# ASYNC PRODUCER
# ============================================================


async def producer(buffer, total_orders=10, delay=0):

    batch = []

    total_push_ns = 0
    total_wait_ns = 0
    full_retries = 0
    pushed_orders = 0

    async def flush_batch(items):

        nonlocal total_push_ns
        nonlocal total_wait_ns
        nonlocal full_retries
        nonlocal pushed_orders

        position = 0

        while position < len(items):

            push_start = time.perf_counter_ns()

            pushed = buffer.push_many(items[position:])

            total_push_ns += time.perf_counter_ns() - push_start

            if pushed == 0:

                full_retries += 1

                wait_start = time.perf_counter_ns()

                await asyncio.sleep(0.0001)

                total_wait_ns += time.perf_counter_ns() - wait_start

            else:

                position += pushed
                pushed_orders += pushed

    async for order in generate_orders(
        total_orders=total_orders,
        delay=delay,
    ):

        batch.append(order)

        if len(batch) >= BATCH_SIZE:

            await flush_batch(batch)
            batch.clear()

    if batch:
        await flush_batch(batch)

    print("\n========== PRODUCER PROFILING ==========")
    print("Orders pushed:", pushed_orders)

    print(f"Push calls time: " f"{total_push_ns / 1_000_000:.3f} ms")

    print(f"Producer wait time: " f"{total_wait_ns / 1_000_000:.3f} ms")

    print("Full buffer retries:", full_retries)
    print("========================================")


# ============================================================
# PRODUCER PROCESS
# ============================================================


def producer_process(
    mapping_path,
    mapping_size,
    producer_done,
    total_orders,
    generation_delay,
):

    buffer = None

    try:

        with open(mapping_path, "r+b") as mapping_file:

            mapping = mmap.mmap(
                mapping_file.fileno(),
                mapping_size,
                access=mmap.ACCESS_WRITE,
            )

            try:

                buffer = NativeRingBuffer(
                    mapping,
                    RING_BUFFER_CAPACITY,
                )

                random.seed(42)

                asyncio.run(
                    producer(
                        buffer,
                        total_orders,
                        generation_delay,
                    )
                )

            finally:

                if buffer is not None:
                    del buffer
                    buffer = None
                    gc.collect()

                mapping.close()

    finally:

        producer_done.value = True


# ============================================================
# SQLITE DATABASE WRITER
# ============================================================


def database_writer_process(
    db_path,
    trade_queue,
    saved_count,
):

    connection = sqlite3.connect(db_path)

    total_saved = 0
    total_insert_ns = 0
    total_commit_ns = 0
    total_wait_ns = 0
    batch_count = 0

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

        while True:

            wait_start = time.perf_counter_ns()
            item = trade_queue.get()

            total_wait_ns += time.perf_counter_ns() - wait_start

            if item is None:
                break

            run_id, batch = item

            if not batch:
                continue

            rows = [
                (
                    run_id,
                    buy_order_id,
                    sell_order_id,
                    price,
                    quantity,
                )
                for (
                    buy_order_id,
                    sell_order_id,
                    price,
                    quantity,
                ) in batch
            ]

            insert_start = time.perf_counter_ns()

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

            total_insert_ns += time.perf_counter_ns() - insert_start

            commit_start = time.perf_counter_ns()
            connection.commit()

            total_commit_ns += time.perf_counter_ns() - commit_start

            total_saved += len(rows)
            batch_count += 1

        saved_count.value = total_saved

        print("\n========== SQLITE PROFILING ==========")
        print("Trades saved:", total_saved)
        print("Batches processed:", batch_count)

        print(f"Total insert time: " f"{total_insert_ns / 1_000_000:.3f} ms")

        print(f"Total commit time: " f"{total_commit_ns / 1_000_000:.3f} ms")

        print(f"Total queue wait time: " f"{total_wait_ns / 1_000_000:.3f} ms")

        print("======================================")

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
    mapping_path,
    mapping_size,
    producer_done,
    total_orders,
    engine_capacity,
    trade_queue,
    run_id,
    result_queue,
    dashboard_stats,
):

    buffer = None

    with open(mapping_path, "r+b") as mapping_file:

        mapping = mmap.mmap(
            mapping_file.fileno(),
            mapping_size,
            access=mmap.ACCESS_WRITE,
        )

        try:

            buffer = NativeRingBuffer(
                mapping,
                RING_BUFFER_CAPACITY,
            )

            engine = CythonMatchingEngine(
                capacity=engine_capacity,
            )

            received = 0
            accepted = 0
            rejected = 0

            queued_trade_count = 0

            total_latency_ns = 0
            max_latency_ns = 0

            total_pop_ns = 0
            total_insertion_ns = 0
            total_matching_ns = 0
            total_trade_queue_ns = 0

            pop_count = 0
            insertion_count = 0
            matching_count = 0
            trade_queue_count = 0

            latency_samples = []

            while True:

                pop_start = time.perf_counter_ns()
                orders = buffer.pop_many(BATCH_SIZE)

                total_pop_ns += time.perf_counter_ns() - pop_start

                pop_count += 1

                if not orders:

                    if producer_done.value:
                        break

                    time.sleep(0.00001)
                    continue

                for order in orders:

                    (
                        order_id,
                        side,
                        price,
                        quantity,
                        timestamp_ns,
                    ) = order

                    received += 1

                    latency_ns = max(
                        0,
                        time.perf_counter_ns() - timestamp_ns,
                    )

                    total_latency_ns += latency_ns

                    max_latency_ns = max(
                        max_latency_ns,
                        latency_ns,
                    )

                    if received % 100 == 0:
                        latency_samples.append(latency_ns)

                    try:

                        insertion_start = time.perf_counter_ns()

                        if side == 0:

                            insertion_result = engine.add_buy_order(
                                order_id,
                                price,
                                quantity,
                            )

                        elif side == 1:

                            insertion_result = engine.add_sell_order(
                                order_id,
                                price,
                                quantity,
                            )

                        else:

                            raise ValueError(f"Invalid side code: {side}")

                        total_insertion_ns += time.perf_counter_ns() - insertion_start

                        insertion_count += 1

                        if insertion_result is False:

                            rejected += 1

                        else:

                            accepted += 1

                            matching_start = time.perf_counter_ns()

                            engine.match_orders()

                            total_matching_ns += time.perf_counter_ns() - matching_start

                            matching_count += 1

                    except Exception as error:

                        rejected += 1

                        print(f"[CONSUMER] Order rejected: " f"{order_id} | {error}")

                    # OPTIMIZATION:
                    # Extract only newly generated trades.
                    # Do not convert the complete trade history
                    # at every 25,000-order checkpoint.

                    if received % 25000 == 0:

                        new_trades = engine.get_trades_since(queued_trade_count)

                        if new_trades:

                            queue_start = time.perf_counter_ns()

                            trade_queue.put((run_id, new_trades))

                            total_trade_queue_ns += time.perf_counter_ns() - queue_start

                            trade_queue_count += 1

                            queued_trade_count += len(new_trades)

                        # Get trade count and quantity directly
                        # from native Cython trade storage.
                        current_trade_count, total_quantity = engine.get_trade_summary()

                        order_books = engine.get_order_books()

                        update_dashboard_stats(
                            dashboard_stats,
                            received,
                            current_trade_count,
                            total_quantity,
                            order_books,
                            total_latency_ns,
                            max_latency_ns,
                        )
            # ====================================================
            # NATIVE RING BUFFER OCCUPANCY
            # ====================================================

            ring_buffer_stats = buffer.get_stats()
            # ====================================================
            # FINAL MATCHING RESULTS
            # ====================================================

            # Convert complete history only once at the end.
            trades = engine.get_trades()

            trade_count, total_traded_quantity = engine.get_trade_summary()

            order_books = engine.get_order_books()

            # Send only trades not already submitted to SQLite.
            remaining_trades = trades[queued_trade_count:]

            if remaining_trades:

                queue_start = time.perf_counter_ns()

                trade_queue.put((run_id, remaining_trades))

                total_trade_queue_ns += time.perf_counter_ns() - queue_start

                trade_queue_count += 1

            remaining_buy_orders = len(order_books[0])
            remaining_sell_orders = len(order_books[1])

            update_dashboard_stats(
                dashboard_stats,
                received,
                trade_count,
                total_traded_quantity,
                order_books,
                total_latency_ns,
                max_latency_ns,
            )

            # ====================================================
            # IPC LATENCY DISTRIBUTION
            # ====================================================

            if latency_samples:

                latency_samples.sort()

                def percentile(values, p):

                    index = int((len(values) - 1) * p)

                    return values[index] / 1000

                print("\n========== IPC LATENCY DISTRIBUTION ==========")

                print(f"P50: " f"{percentile(latency_samples, 0.50):.2f} us")

                print(f"P95: " f"{percentile(latency_samples, 0.95):.2f} us")

                print(f"P99: " f"{percentile(latency_samples, 0.99):.2f} us")

                print("==============================================")

            # ====================================================
            # CONSUMER PROFILING
            # ====================================================

            print("\n========== CONSUMER PROFILING ==========")

            print(
                f"Native ring buffer pop time: "
                f"{total_pop_ns / 1_000_000:.3f} ms "
                f"({pop_count:,} batch reads)"
            )

            print(
                f"Cython insertion time: "
                f"{total_insertion_ns / 1_000_000:.3f} ms "
                f"({insertion_count:,} calls)"
            )

            print(
                f"Cython matching time: "
                f"{total_matching_ns / 1_000_000:.3f} ms "
                f"({matching_count:,} calls)"
            )

            print(
                f"Trade queue submission time: "
                f"{total_trade_queue_ns / 1_000_000:.3f} ms "
                f"({trade_queue_count} submissions)"
            )

            print("\n========== RING BUFFER OCCUPANCY ==========")

            print(f"Capacity: " f"{ring_buffer_stats['capacity']:,}")

            print(f"Current occupancy: " f"{ring_buffer_stats['current_occupancy']:,}")

            print(f"Maximum occupancy: " f"{ring_buffer_stats['max_occupancy']:,}")

            print(
                f"Average sampled occupancy: "
                f"{ring_buffer_stats['average_sampled_occupancy']:.2f}"
            )

            print(f"Occupancy samples: " f"{ring_buffer_stats['occupancy_samples']:,}")

            print(
                f"Full buffer events: " f"{ring_buffer_stats['full_buffer_events']:,}"
            )

            print("===========================================")

            # ====================================================
            # OUTPUT HASH
            # ====================================================

            output_data = repr((trades, order_books)).encode("utf-8")

            output_hash = hashlib.sha256(output_data).hexdigest()

            # ====================================================
            # RESULT SUMMARY
            # ====================================================

            result = {
                "received": received,
                "accepted": accepted,
                "rejected": rejected,
                "trade_count": trade_count,
                "total_traded_quantity": total_traded_quantity,
                "remaining_buy_orders": remaining_buy_orders,
                "remaining_sell_orders": remaining_sell_orders,
                "output_hash": output_hash,
                "ring_buffer_stats": ring_buffer_stats,
            }

            result_queue.put(result)

            print("\n========== CONSUMER RESULTS ==========")
            print("Orders received:", received)
            print("Orders accepted:", accepted)
            print("Orders rejected:", rejected)
            print("Trades executed:", trade_count)

            print(
                "Total traded quantity:",
                total_traded_quantity,
            )

            print(
                "Remaining buy orders:",
                remaining_buy_orders,
            )

            print(
                "Remaining sell orders:",
                remaining_sell_orders,
            )

            print("======================================\n")

        finally:

            if buffer is not None:
                del buffer
                buffer = None
                gc.collect()

            mapping.close()


# ============================================================
# SINGLE PIPELINE EXECUTION
# ============================================================


def run_pipeline(
    run_number,
    total_orders=50000,
    show_dashboard=False,
):

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

    mapping_size = NATIVE_HEADER_SIZE + capacity * NATIVE_ORDER_SIZE

    fd, mapping_path = tempfile.mkstemp(
        prefix="chronosmatch_",
        suffix=".mmap",
    )

    try:

        with os.fdopen(fd, "r+b") as mapping_file:

            mapping_file.truncate(mapping_size)

            with mmap.mmap(
                mapping_file.fileno(),
                mapping_size,
                access=mmap.ACCESS_WRITE,
            ) as initial_mapping:

                initialize_native_ring(
                    initial_mapping,
                    capacity,
                )

        producer_done = ctx.Value("b", False)

        trade_queue = ctx.Queue()
        result_queue = ctx.Queue()

        saved_count = ctx.Value("i", 0)

        dashboard_stats = ctx.Array(
            "q",
            13,
            lock=True,
        )

        dashboard_stop = ctx.Event()

        dashboard_stats[12] = time.perf_counter_ns()

        writer = ctx.Process(
            target=database_writer_process,
            args=(
                db_path,
                trade_queue,
                saved_count,
            ),
        )

        consumer = ctx.Process(
            target=consumer_process,
            args=(
                mapping_path,
                mapping_size,
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
            args=(
                mapping_path,
                mapping_size,
                producer_done,
                total_orders,
                generation_delay,
            ),
        )

        dashboard_process = None

        start_time = time.perf_counter()

        try:

            writer.start()
            consumer.start()
            producer_instance.start()

            if show_dashboard:

                dashboard_process = ctx.Process(
                    target=run_dashboard,
                    args=(
                        dashboard_stats,
                        dashboard_stop,
                    ),
                )

                dashboard_process.start()

            producer_instance.join()
            consumer.join()

            if producer_instance.exitcode != 0:
                raise RuntimeError("Producer process failed.")

            if consumer.exitcode != 0:
                raise RuntimeError("Consumer process failed.")

            if dashboard_process:

                dashboard_stop.set()
                dashboard_process.join(timeout=3)

                if dashboard_process.is_alive():
                    dashboard_process.terminate()
                    dashboard_process.join()

            # All trade batches have been submitted before sentinel.
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

            print(
                "Total traded quantity:",
                result["total_traded_quantity"],
            )

            print(f"Execution time: {elapsed:.4f} seconds")

            print(f"Throughput: {throughput:,.2f} orders/sec")

            print(f"Average IPC latency: " f"{average_latency_us:.2f} us")

            print(f"Maximum IPC latency: " f"{maximum_latency_us:.2f} us")

            print("=====================================\n")

            return result

        finally:

            dashboard_stop.set()

            for process in (
                producer_instance,
                consumer,
                writer,
                dashboard_process,
            ):

                if process is not None and process.is_alive():

                    process.terminate()
                    process.join()

            trade_queue.close()
            result_queue.close()

            trade_queue.join_thread()
            result_queue.join_thread()

    finally:

        if os.path.exists(mapping_path):
            os.remove(mapping_path)


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

    print(
        "Trades saved to SQLite:",
        result["saved_trades"],
    )

    print(
        "Total traded quantity:",
        result["total_traded_quantity"],
    )

    print(
        "Remaining buy orders:",
        result["remaining_buy_orders"],
    )

    print(
        "Remaining sell orders:",
        result["remaining_sell_orders"],
    )

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

    print(
        "\nWORKLOAD RESULT:",
        "PASS" if workload_passed else "FAIL",
    )

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

    print("Warm-up runs:", WARMUP_RUNS)
    print("Measured runs:", MEASURED_RUNS)
    print("\nRunning warm-up...")

    for index in range(WARMUP_RUNS):

        run_pipeline(
            f"{total_orders}-WARMUP-{index + 1}",
            total_orders,
        )

    results = []

    print("\nRunning measured benchmarks...")

    for index in range(MEASURED_RUNS):

        result = run_pipeline(
            f"{total_orders}-RUN-{index + 1}",
            total_orders,
        )

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

    return validate_results(
        results,
        total_orders,
    )


# ============================================================
# MAIN
# ============================================================


def main():

    print("\n==========================================")
    print("CHRONOSMATCH - HIGH FREQUENCY TRADING ENGINE")
    print("==========================================")

    print("\nPermanent SQLite database:")
    print(DB_PATH)

    if "--dashboard" in sys.argv:

        print("\nStarting live dashboard mode...")

        result = run_pipeline(
            "LIVE-DASHBOARD",
            total_orders=50_000,
            show_dashboard=True,
        )

        if (
            result["received"] != 50_000
            or result["order_loss"] != 0
            or result["trade_count"] != result["saved_trades"]
        ):

            raise SystemExit("Dashboard pipeline validation failed.")

        print("\nLIVE DASHBOARD RUN: PASS")
        return

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
