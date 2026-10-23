import mmap
import struct
import tempfile
import asyncio
from market_simulator import generate_orders
from matching_enigne import MatchingEngine

# --------------------------------------------------
# STEP 1: Define one order's binary layout with struct
# --------------------------------------------------
# <  = little-endian byte order
# I  = unsigned 32-bit integer (4 bytes)
# III = three integers: order ID, price ticks, quantity
# Total record size = 3 * 4 = 12 bytes
ORDER = struct.Struct("<IIII")


# --------------------------------------------------
# STEP 2: Create a ring buffer
# --------------------------------------------------
class RingBuffer:
    def __init__(self, memory, capacity):
        self.memory = memory  # mmap #memory: jahan order bytes store hongi
        self.capacity = capacity  # maximum number of orders
        self.write_pos = 0  # next slot to write into
        self.read_pos = 0  # next slot to read from
        self.count = 0  # number of orders currently stored

    def push(self, order_id, side_code, price_ticks, quantity):
        if self.count == self.capacity:
            return False
        offset = self.write_pos * ORDER.size
        ORDER.pack_into(self.memory, offset, order_id, side_code, price_ticks, quantity)
        self.write_pos = (self.write_pos + 1) % self.capacity
        self.count += 1
        return True

    def pop(self):
        if self.count == 0:  # If count is zero, there is nothing to read.
            return None
        offset = (
            self.read_pos * ORDER.size
        )  # Calculate the byte offset of the oldest unread order.
        order = ORDER.unpack_from(
            self.memory, offset
        )  # Unpack the three numbers from mmap memory
        self.read_pos = (
            self.read_pos + 1
        ) % self.capacity  # Move to the next slot; wrap to zero at the end.
        self.count -= 1  # One order has been removed from the buffer.
        return order  # Give the order details back to the caller.


# --------------------------------------------------
# STEP 3: Create process order functions
# --------------------------------------------------
async def producer(buffer):
    async for order_id, side, price_ticks, quantity in generate_orders():
        side_code = 0 if side == "BUY" else 1
        order = (order_id, side_code, price_ticks, quantity)
        while not buffer.push(*order):
            await asyncio.sleep(0.01)

        print("Produced:", (order_id, side, price_ticks, quantity))


async def consumer(buffer, producer_task):
    engine = MatchingEngine()
    while not producer_task.done() or buffer.count > 0:
        order = buffer.pop()
        if order is not None:
            order_id, side_code, price_ticks, quantity = order
            side = "BUY" if side_code == 0 else "SELL"
            print("Consumed:", (order_id, side, price_ticks, quantity))
            if side_code == 0:
                engine.add_buy_order(order_id, price_ticks, quantity)
            else:
                engine.add_sell_order(order_id, price_ticks, quantity)
            engine.match_orders()
        else:
            await asyncio.sleep(0.01)

    print("Remaining buy orders:", engine.buy_orders)
    print("Remaining sell orders:", engine.sell_orders)


# --------------------------------------------------
# STEP 4: Run a small demonstration
# --------------------------------------------------
def main():
    capacity = 3
    memory_size = (
        capacity * ORDER.size
    )  # Reserve enough bytes for 'capacity' order records.
    with tempfile.TemporaryFile() as file:  # Temporary file backs the memory map for this demo.
        file.truncate(memory_size)

        # Map the file into memory so struct can read/write its bytes.
        with mmap.mmap(
            file.fileno(),
            length=memory_size,
            access=mmap.ACCESS_WRITE,
        ) as memory:

            buffer = RingBuffer(memory, capacity)

            async def run_pipeline():
                producer_task = asyncio.create_task(producer(buffer))
                await consumer(buffer, producer_task)
                await producer_task

            asyncio.run(run_pipeline())


if __name__ == "__main__":
    main()
