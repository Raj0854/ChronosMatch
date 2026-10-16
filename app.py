import mmap
import struct
import tempfile
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
        self.memory = memory       # mmap memory where order bytes are stored
        self.capacity = capacity   # maximum number of orders
        self.write_pos = 0         # next slot to write into
        self.read_pos = 0          # next slot to read from
        self.count = 0              # number of orders currently stored

    def push(self, order_id, price_ticks, quantity):
        # If count equals capacity, the buffer is full.
        if self.count == self.capacity:
            return False
        offset = self.write_pos * ORDER.size # Calculate the byte offset of the next slot.
        ORDER.pack_into(# Pack three numbers into mmap memory at that offset.
            self.memory,
            offset,
            order_id,
            price_ticks,
            quantity,
        )
        self.write_pos = (self.write_pos + 1) % self.capacity # Move to the next slot; wrap to zero at the end.
        self.count += 1 # One more order is now stored.
        return True # Tell the caller the order was added.

    def pop(self):
        if self.count == 0:# If count is zero, there is nothing to read.
            return None
        offset = self.read_pos * ORDER.size # Calculate the byte offset of the oldest unread order.
        order = ORDER.unpack_from(self.memory, offset) # Unpack the three numbers from mmap memory
        self.read_pos = (self.read_pos + 1) % self.capacity  # Move to the next slot; wrap to zero at the end. 
        self.count -= 1  # One order has been removed from the buffer.
        return order # Give the order details back to the caller.


# --------------------------------------------------
# STEP 3: Run a small demonstration
# --------------------------------------------------
def main():
    capacity = 3

    # Reserve enough bytes for 'capacity' order records.
    memory_size = capacity * ORDER.size
     # Temporary file backs the memory map for this demo.
    with tempfile.TemporaryFile() as file:
        file.truncate(memory_size)

        # Map the file into memory so struct can read/write its bytes.
        with mmap.mmap(
            file.fileno(),
            length=memory_size,
            access=mmap.ACCESS_WRITE,
        ) as memory:

            buffer = RingBuffer(memory, capacity)

            print("=== ChronosMatch Learning Demo ===")
            print("Order record size:", ORDER.size, "bytes")
            print("Buffer capacity:", capacity, "orders")
            print()

            print("Adding three orders:")
            print("Order 101 added:", buffer.push(101, 10000, 5))
            print("Order 102 added:", buffer.push(102, 10025, 2))
            print("Order 103 added:", buffer.push(103, 9990, 7))
            print()

            print("Trying to add a fourth order to a full buffer:")
            print("Order 104 added:", buffer.push(104, 10010, 1))
            print()

            print("Reading orders back from the buffer:")
            print("Read:", buffer.pop())
            print("Read:", buffer.pop())
            print("Read:", buffer.pop())
            print()

            print("Trying to read from an empty buffer:")
            print("Read:", buffer.pop())


if __name__ == "__main__":
    main()