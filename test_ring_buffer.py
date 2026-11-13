
# test_ring_buffer.py

import mmap
import tempfile
import unittest
import os
import gc

from native_ring import NativeRingBuffer


class TestNativeRingBuffer(unittest.TestCase):

    def setUp(self):

        self.capacity = 4
        self.header_size = 64
        self.order_size = 24

        self.mapping_size = (
            self.header_size
            + self.capacity * self.order_size
        )

        self.temp_file = tempfile.NamedTemporaryFile(
            delete=False
        )

        self.temp_path = self.temp_file.name

        self.temp_file.truncate(self.mapping_size)
        self.temp_file.close()

        self.file = open(self.temp_path, "r+b")

        self.mapping = mmap.mmap(
            self.file.fileno(),
            self.mapping_size
        )

        self.buffer = NativeRingBuffer(
            self.mapping,
            self.capacity
        )

        self.buffer.initialize()

    def tearDown(self):

        # Release NativeRingBuffer's exported memoryview.
        self.buffer = None
        gc.collect()

        self.mapping.close()
        self.file.close()

        if os.path.exists(self.temp_path):
            os.remove(self.temp_path)

    def test_push_and_pop(self):

        pushed = self.buffer.push_many([
            (101, 0, 10000, 5)
        ])

        self.assertEqual(pushed, 1)

        result = self.buffer.pop_many(1)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][:4], (101, 0, 10000, 5))
        self.assertGreater(result[0][4], 0)

    def test_empty_buffer(self):

        self.assertEqual(self.buffer.pop_many(1), [])
        self.assertEqual(len(self.buffer), 0)

    def test_full_buffer(self):

        orders = [
            (101 + i, 0, 10000, 5)
            for i in range(self.capacity)
        ]

        self.assertEqual(
            self.buffer.push_many(orders),
            self.capacity
        )

        self.assertEqual(len(self.buffer), self.capacity)

        self.assertEqual(
            self.buffer.push_many([(200, 0, 10000, 5)]),
            0
        )

    def test_fifo_order(self):

        orders = [
            (101, 0, 10000, 5),
            (102, 1, 10100, 3),
            (103, 0, 9900, 2)
        ]

        self.assertEqual(
            self.buffer.push_many(orders),
            len(orders)
        )

        results = self.buffer.pop_many(10)

        self.assertEqual(len(results), len(orders))

        for result, expected in zip(results, orders):
            self.assertEqual(result[:4], expected)

        self.assertEqual(len(self.buffer), 0)

    def test_circular_wraparound(self):

        orders = [
            (101, 0, 10000, 5),
            (102, 0, 10000, 5),
            (103, 0, 10000, 5),
            (104, 0, 10000, 5)
        ]

        self.assertEqual(self.buffer.push_many(orders), 4)

        first_results = self.buffer.pop_many(2)

        self.assertEqual(
            [item[:4] for item in first_results],
            orders[:2]
        )

        new_orders = [
            (200, 0, 10000, 5),
            (201, 0, 10000, 5)
        ]

        self.assertEqual(
            self.buffer.push_many(new_orders),
            2
        )

        results = self.buffer.pop_many(4)

        expected = [
            (103, 0, 10000, 5),
            (104, 0, 10000, 5),
            (200, 0, 10000, 5),
            (201, 0, 10000, 5)
        ]

        self.assertEqual(
            [item[:4] for item in results],
            expected
        )

    def test_timestamp_preservation(self):

        self.buffer.push_many([
            (301, 1, 10500, 7)
        ])

        result = self.buffer.pop_many(1)[0]

        self.assertEqual(
            result[:4],
            (301, 1, 10500, 7)
        )

        self.assertIsInstance(result[4], int)
        self.assertGreater(result[4], 0)

    def test_partial_push(self):

        orders = [
            (401 + i, 0, 10000, i + 1)
            for i in range(6)
        ]

        pushed = self.buffer.push_many(orders)

        self.assertEqual(pushed, self.capacity)

        results = self.buffer.pop_many(10)

        self.assertEqual(len(results), self.capacity)

        self.assertEqual(
            [item[0] for item in results],
            [401, 402, 403, 404]
        )

    def test_invalid_capacity(self):

        with self.assertRaises(ValueError):
            NativeRingBuffer(self.mapping, 3)

    def test_invalid_order_side(self):

        with self.assertRaises(ValueError):
            self.buffer.push_many([
                (501, 5, 10000, 1)
            ])


if __name__ == "__main__":
    unittest.main()
