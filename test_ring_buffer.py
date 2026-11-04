import unittest
import multiprocessing as mp

from multiprocessing import shared_memory

from main import RingBuffer, ORDER, HEADER


class TestRingBuffer(unittest.TestCase):

    def setUp(self):
        self.capacity = 3
        self.ctx = mp.get_context("spawn")
        self.lock = self.ctx.Lock()

        memory_size = HEADER.size + self.capacity * ORDER.size

        self.shm = shared_memory.SharedMemory(
            create=True,
            size=memory_size
        )

        HEADER.pack_into(
            self.shm.buf,
            0,
            0,
            0,
            0,
            self.capacity
        )

        self.buffer = RingBuffer(self.shm, self.lock)

    def tearDown(self):
        self.shm.close()
        self.shm.unlink()

    def test_push_and_pop(self):
        self.assertTrue(
            self.buffer.push(101, 0, 10000, 5)
        )

        self.assertEqual(
            self.buffer.pop(),
            (101, 0, 10000, 5)
        )

    def test_full_buffer(self):
        self.assertTrue(self.buffer.push(101, 0, 10000, 5))
        self.assertTrue(self.buffer.push(102, 1, 10025, 2))
        self.assertTrue(self.buffer.push(103, 0, 9990, 7))

        self.assertFalse(
            self.buffer.push(104, 1, 10010, 1)
        )

    def test_empty_buffer(self):
        self.assertIsNone(self.buffer.pop())

    def test_fifo_order(self):
        self.buffer.push(101, 0, 10000, 5)
        self.buffer.push(102, 1, 10025, 2)
        self.buffer.push(103, 0, 9990, 7)

        self.assertEqual(self.buffer.pop()[0], 101)
        self.assertEqual(self.buffer.pop()[0], 102)
        self.assertEqual(self.buffer.pop()[0], 103)

    def test_circular_wraparound(self):
        self.buffer.push(101, 0, 10000, 5)
        self.buffer.push(102, 1, 10025, 2)
        self.buffer.push(103, 0, 9990, 7)

        self.buffer.pop()
        self.buffer.pop()

        self.assertTrue(
            self.buffer.push(104, 1, 10010, 1)
        )

        self.assertTrue(
            self.buffer.push(105, 0, 10050, 3)
        )

        self.assertEqual(self.buffer.pop()[0], 103)
        self.assertEqual(self.buffer.pop()[0], 104)
        self.assertEqual(self.buffer.pop()[0], 105)


if __name__ == "__main__":
    unittest.main()