import mmap
import gc
import time

from native_ring import NativeRingBuffer

CAPACITY = 4096
TOTAL_ORDERS = 1_000_000
BATCH_SIZE = 128

MAPPING_SIZE = 64 + (CAPACITY * 24)


def main():
    mapping = mmap.mmap(-1, MAPPING_SIZE)
    ring = NativeRingBuffer(mapping, CAPACITY)

    ring.initialize()

    start = time.perf_counter()
    processed = 0

    try:
        while processed < TOTAL_ORDERS:
            count = min(BATCH_SIZE, TOTAL_ORDERS - processed)

            orders = [
                (
                    processed + i + 1,
                    "BUY" if (processed + i) % 2 == 0 else "SELL",
                    100 + (processed + i) % 100,
                    10,
                )
                for i in range(count)
            ]

            pushed = ring.push_many(orders)

            if pushed != count:
                raise RuntimeError(
                    f"Push failed: expected {count}, got {pushed}"
                )

            received = ring.pop_many(count)

            if len(received) != count:
                raise RuntimeError(
                    f"Pop failed: expected {count}, got {len(received)}"
                )

            for i, order in enumerate(received):
                expected_id = processed + i + 1

                if order[0] != expected_id:
                    raise RuntimeError(
                        f"FIFO mismatch: expected {expected_id}, got {order[0]}"
                    )

                if order[4] <= 0:
                    raise RuntimeError("Invalid timestamp")

            processed += count

        elapsed = time.perf_counter() - start

        print("\n========== NATIVE RING TEST ==========")
        print(f"Orders processed : {processed:,}")
        print(f"Capacity         : {CAPACITY}")
        print(f"Batch size       : {BATCH_SIZE}")
        print(f"Execution time   : {elapsed:.4f} seconds")
        print(f"Throughput       : {processed / elapsed:,.2f} orders/sec")
        print("FIFO correctness : PASS")
        print("======================================")

    finally:
        del ring
        gc.collect()
        mapping.close()


if __name__ == "__main__":
    main()