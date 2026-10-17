import mmap
import struct
import tempfile
from market_simulator import generate_order

# --------------------------------------------------
# STEP 1: Define one order's binary layout with struct
# --------------------------------------------------
# <  = little-endian byte order
# I  = unsigned 32-bit integer (4 bytes)
# III = three integers: order ID, price ticks, quantity
# Total record size = 3 * 4 = 12 bytes
ORDER = struct.Struct("<III")


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

    def push(self, order_id, price_ticks, quantity):
        # If count equals capacity, the buffer is full.
        if self.count == self.capacity:
            return False
        offset = (
            self.write_pos * ORDER.size
        )  # Calculate the byte offset of the next slot.
        ORDER.pack_into(  # Pack three numbers into mmap memory at that offset.
            self.memory,
            offset,
            order_id,
            price_ticks,
            quantity,
        )
        self.write_pos = (
            self.write_pos + 1
        ) % self.capacity  # Move to the next slot; wrap to zero at the end.
        self.count += 1  # One more order is now stored.
        return True  # Tell the caller the order was added.

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
# STEP 3: Run a small demonstration
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

            for order_id in range(101, 106):
                order = generate_order(order_id)
                added = buffer.push(*order)
                print(f"Order : {order} | Added : {added}. ")
            print("\nReadings order from buffer:")
            while True:
                order=buffer.pop()
                if order is None:
                    break
                print(f"Consumed order : {order}")


if __name__ == "__main__":
    main()
