import asyncio
import time
import struct
import multiprocessing as mp

from multiprocessing import shared_memory
from market_simulator import generate_orders
from matching_engine import MatchingEngine


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
            write_pos, read_pos, count, capacity = HEADER.unpack_from(
                self.shm.buf, 0
            )

            if count >= capacity:
                return False

            offset = HEADER.size + write_pos * ORDER.size

            ORDER.pack_into(
                self.shm.buf,
                offset,
                order_id,
                side_code,
                price_ticks,
                quantity
            )

            write_pos = (write_pos + 1) % capacity
            count += 1

            HEADER.pack_into(
                self.shm.buf,
                0,
                write_pos,
                read_pos,
                count,
                capacity
            )

            return True

    def pop(self):
        with self.lock:
            write_pos, read_pos, count, capacity = HEADER.unpack_from(
                self.shm.buf, 0
            )

            if count == 0:
                return None

            offset = HEADER.size + read_pos * ORDER.size

            order = ORDER.unpack_from(
                self.shm.buf,
                offset
            )

            read_pos = (read_pos + 1) % capacity
            count -= 1

            HEADER.pack_into(
                self.shm.buf,
                0,
                write_pos,
                read_pos,
                count,
                capacity
            )

            return order

    def is_empty(self):
        with self.lock:
            _, _, count, _ = HEADER.unpack_from(
                self.shm.buf, 0
            )
            return count == 0


# --------------------------------------------------
# STEP 3: Producer Process
# --------------------------------------------------

async def producer(buffer, total_orders=10, delay=0.1):
    sent = 0

    async for order in generate_orders(
        total_orders=total_orders,
        delay=delay
    ):
        order_id, side, price_ticks, quantity = order
        side_code = 0 if side == "BUY" else 1

        while not buffer.push(
            order_id,
            side_code,
            price_ticks,
            quantity
        ):
            await asyncio.sleep(0.001)

        sent += 1

        if sent % 1000 == 0:
            print(f"[PRODUCER] Sent: {sent}")

    print(f"[PRODUCER] Finished. Total sent: {sent}")
def producer_process(
    shm_name,
    lock,
    producer_done,
    total_orders,
    delay
):
    shm = shared_memory.SharedMemory(name=shm_name)

    try:
        buffer = RingBuffer(shm, lock)
        asyncio.run(
            producer(buffer, total_orders, delay)
        )
    finally:
        producer_done.set()
        shm.close()

# --------------------------------------------------
# STEP 4: Consumer Process
# --------------------------------------------------

def consumer_process(shm_name, lock, producer_done):

    shm = shared_memory.SharedMemory(name=shm_name)

    try:
        buffer = RingBuffer(shm, lock)
        engine = MatchingEngine()

        consumed = 0

        while not producer_done.is_set() or not buffer.is_empty():

            order = buffer.pop()

            if order is None:
                time.sleep(0.001)
                continue

            order_id, side_code, price_ticks, quantity = order

            if side_code == 0:
                engine.add_buy_order(
                    order_id,
                    price_ticks,
                    quantity
                )
            else:
                engine.add_sell_order(
                    order_id,
                    price_ticks,
                    quantity
                )

            engine.match_orders()
            consumed += 1

            if consumed % 1000 == 0:
                print(f"[CONSUMER] Received: {consumed}")
                
            

        print("\nConsumer finished.")
        print("Total orders consumed:", consumed)
        print("Remaining buy orders:", len(engine.buy_orders))
        print("Remaining sell orders:", len(engine.sell_orders))

    finally:
        shm.close()


# --------------------------------------------------
# STEP 5: Main Process
# --------------------------------------------------

def main():
    total_orders = 10000
    generation_delay = 0
    capacity = 1024

    memory_size = (
        HEADER.size + capacity * ORDER.size
    )

    ctx = mp.get_context("spawn")

    lock = ctx.Lock()
    producer_done = ctx.Event()

    shm = shared_memory.SharedMemory(
        create=True,
        size=memory_size
    )

    try:
        # Initialize shared-memory header
        HEADER.pack_into(
            shm.buf,
            0,
            0,          # write_pos
            0,          # read_pos
            0,          # count
            capacity
        )

        consumer = ctx.Process(
            target=consumer_process,
            args=(shm.name, lock, producer_done)
        )

        producer = ctx.Process(
            target=producer_process,
            args=(shm.name, lock, producer_done,total_orders,generation_delay)
        )

        consumer.start()
        producer.start()

        producer.join()
        consumer.join()

        if producer.exitcode != 0:
            raise RuntimeError("Producer process failed")

        if consumer.exitcode != 0:
            raise RuntimeError("Consumer process failed")

        print("\nIPC pipeline completed successfully.")

    finally:
        shm.close()
        shm.unlink()


if __name__ == "__main__":
    mp.freeze_support()
    main()