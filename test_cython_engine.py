import unittest
from cython_engine import CythonMatchingEngine


class TestCythonMatchingEngine(unittest.TestCase):

    def test_partial_fill(self):
        engine = CythonMatchingEngine()
        engine.add_buy_order(101, 10000, 10)
        engine.add_sell_order(201, 9900, 4)
        engine.match_orders()

        self.assertEqual(engine.get_trades(), [(101, 201, 9900, 4)])
        self.assertEqual(engine.get_order_books(), ([(101, 10000, 6, 0)], []))

    def test_price_time_priority(self):
        engine = CythonMatchingEngine()
        engine.add_buy_order(101, 10000, 2)
        engine.add_buy_order(102, 10000, 5)
        engine.add_sell_order(201, 9900, 4)
        engine.match_orders()

        self.assertEqual(
            engine.get_trades(), [(101, 201, 9900, 2), (102, 201, 9900, 2)]
        )

    def test_no_match_when_buy_price_is_lower(self):
        engine = CythonMatchingEngine()
        engine.add_buy_order(101, 9000, 5)
        engine.add_sell_order(201, 10000, 5)
        engine.match_orders()

        self.assertEqual(engine.get_trades(), [])
        self.assertEqual(len(engine.get_order_books()[0]), 1)
        self.assertEqual(len(engine.get_order_books()[1]), 1)

    def test_full_fill_removes_both_orders(self):
        engine = CythonMatchingEngine()
        engine.add_buy_order(101, 10000, 5)
        engine.add_sell_order(201, 9900, 5)
        engine.match_orders()

        self.assertEqual(engine.get_order_books(), ([], []))
        self.assertEqual(engine.get_trades(), [(101, 201, 9900, 5)])

    def test_capacity_limit(self):
        engine = CythonMatchingEngine(1)

        self.assertTrue(engine.add_buy_order(101, 10000, 5))
        self.assertFalse(engine.add_buy_order(102, 9900, 2))

        self.assertTrue(engine.add_sell_order(201, 9900, 3))
        self.assertFalse(engine.add_sell_order(202, 9800, 1))

    def test_equal_price_uses_arrival_order(self):
        engine = CythonMatchingEngine()
        engine.add_sell_order(201, 9900, 2)
        engine.add_sell_order(202, 9900, 2)
        engine.add_buy_order(101, 10000, 3)
        engine.match_orders()

        self.assertEqual(
            engine.get_trades(), [(101, 201, 9900, 2), (101, 202, 9900, 1)]
        )

    def test_invalid_and_duplicate_orders(self):
        engine = CythonMatchingEngine()

        self.assertTrue(engine.add_buy_order(101, 10000, 5))
        self.assertFalse(engine.add_sell_order(101, 9900, 3))
        self.assertFalse(engine.add_buy_order(102, 0, 5))
        self.assertFalse(engine.add_sell_order(103, 9900, 0))

        self.assertEqual(engine.get_order_books(), ([(101, 10000, 5, 0)], []))


if __name__ == "__main__":
    unittest.main()
