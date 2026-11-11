
import unittest
import struct
from multiprocessing import shared_memory, Lock

from main import RingBuffer, ORDER, HEADER


class TestRingBuffer(unittest.TestCase):

    def setUp(self):
        self.capacity = 4
        self.shm = shared_memory.SharedMemory(
            create=True,
            size=HEADER.size + self.capacity * ORDER.size
        )
        HEADER.pack_into(self.shm.buf, 0, 0, 0, 0, self.capacity)
        self.buffer = RingBuffer(self.shm, Lock())

    def tearDown(self):
        self.shm.close()
        self.shm.unlink()

    def test_push_and_pop(self):
        self.assertTrue(self.buffer.push((101, 0, 10000, 5)))
        result = self.buffer.pop()
        self.assertIsNotNone(result)
        self.assertEqual(result[:4], (101, 0, 10000, 5))

    def test_empty_buffer(self):
        self.assertIsNone(self.buffer.pop())

    def test_full_buffer(self):
        for i in range(self.capacity):
            self.assertTrue(self.buffer.push((101 + i, 0, 10000, 5)))

        self.assertFalse(self.buffer.push((200, 0, 10000, 5)))

    def test_fifo_order(self):
        orders = [
            (101, 0, 10000, 5),
            (102, 1, 10100, 3),
            (103, 0, 9900, 2)
        ]

        for order in orders:
            self.buffer.push(order)

        for expected in orders:
            result = self.buffer.pop()
            self.assertEqual(result[:4], expected)

    def test_circular_wraparound(self):
        for i in range(self.capacity):
            self.buffer.push((101 + i, 0, 10000, 5))

        for _ in range(2):
            self.buffer.pop()

        self.assertTrue(self.buffer.push((200, 0, 10000, 5)))
        self.assertTrue(self.buffer.push((201, 0, 10000, 5)))

        self.assertEqual(self.buffer.pop()[:4], (103, 0, 10000, 5))
        self.assertEqual(self.buffer.pop()[:4], (104, 0, 10000, 5))
        self.assertEqual(self.buffer.pop()[:4], (200, 0, 10000, 5))
        self.assertEqual(self.buffer.pop()[:4], (201, 0, 10000, 5))


if __name__ == "__main__":
    unittest.main()
