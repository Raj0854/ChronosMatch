
import unittest #automatically code test karne ke liye.
import mmap
import tempfile

from main import RingBuffer, ORDER


class TestRingBuffer(unittest.TestCase):
    def setUp(self):
        self.capacity = 3
        self.file = tempfile.TemporaryFile()
        self.file.truncate(self.capacity * ORDER.size)

        self.memory = mmap.mmap(
            self.file.fileno(),
            length=self.capacity * ORDER.size,
            access=mmap.ACCESS_WRITE,
        )

        self.buffer = RingBuffer(self.memory, self.capacity)

    def tearDown(self):
        self.memory.close()
        self.file.close()

    def test_push_and_pop(self):
        self.assertTrue(self.buffer.push(101, 10000, 5))
        self.assertEqual(self.buffer.pop(), (101, 10000, 5))

    def test_full_buffer(self):
        self.buffer.push(101, 10000, 5)
        self.buffer.push(102, 10025, 2)
        self.buffer.push(103, 9990, 7)

        self.assertFalse(self.buffer.push(104, 10010, 1))

    def test_empty_buffer(self):
        self.assertIsNone(self.buffer.pop())


if __name__ == "__main__":
    unittest.main()
