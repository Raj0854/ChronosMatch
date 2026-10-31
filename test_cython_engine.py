import unittest
from cython_engine import CythonMatchingEngine
import random
from matching_engine import MatchingEngine


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

    def test_repeated_matching_with_new_orders(self):
        engine = CythonMatchingEngine()

        # First matching cycle
        engine.add_buy_order(101, 10000, 5)
        engine.add_sell_order(201, 9900, 3)
        engine.match_orders()

        self.assertEqual(engine.get_trades(), [(101, 201, 9900, 3)])
        self.assertEqual(engine.get_order_books(), ([(101, 10000, 2, 0)], []))

        # Add new orders after the first matching cycle
        engine.add_buy_order(102, 10100, 4)
        engine.add_sell_order(202, 10000, 3)
        engine.match_orders()

        self.assertEqual(
            engine.get_trades(),
            [
                (101, 201, 9900, 3),
                (102, 202, 10000, 3),
            ],
        )
        self.assertEqual(
            engine.get_order_books(), ([(102, 10100, 1, 2), (101, 10000, 2, 0)], [])
        )

    def test_stress_many_orders(self):
        engine = CythonMatchingEngine(capacity=1000)

        # Add 500 buy orders at different prices.
        for i in range(1, 501):
            self.assertTrue(engine.add_buy_order(i, 10000 + i, 2))

        # Add 500 sell orders at prices that cross the buy orders.
        for i in range(501, 1001):
            self.assertTrue(engine.add_sell_order(i, 9000 + i - 500, 2))

        engine.match_orders()

        buy_book, sell_book = engine.get_order_books()

        # Every sell order should match a buy order.
        self.assertEqual(len(engine.get_trades()), 500)
        self.assertEqual(buy_book, [])
        self.assertEqual(sell_book, [])

    def test_stress_randomized_orders(self):
        rng = random.Random(42)
        engine = CythonMatchingEngine(capacity=1000)

        for order_id in range(1, 501):
            price = rng.randint(9500, 10500)
            quantity = rng.randint(1, 20)
            self.assertTrue(engine.add_buy_order(order_id, price, quantity))

        for order_id in range(501, 1001):
            price = rng.randint(9500, 10500)
            quantity = rng.randint(1, 20)
            self.assertTrue(engine.add_sell_order(order_id, price, quantity))

        engine.match_orders()

        buy_book, sell_book = engine.get_order_books()
        trades = engine.get_trades()

        # Every executed trade must have a positive quantity.
        self.assertTrue(all(trade[3] > 0 for trade in trades))

        # Remaining orders must be sorted by priority.
        self.assertEqual(
            buy_book, sorted(buy_book, key=lambda order: (-order[1], order[3]))
        )
        self.assertEqual(
            sell_book, sorted(sell_book, key=lambda order: (order[1], order[3]))
        )

        # No remaining best buy should cross the best sell.
        if buy_book and sell_book:
            self.assertLess(buy_book[0][1], sell_book[0][1])

    def test_randomized_python_cython_equivalence(self):
        rng = random.Random(42)

        python_engine = MatchingEngine()
        cython_engine = CythonMatchingEngine(capacity=1000)

        orders = []

        for order_id in range(1, 501):
            side = "buy" if order_id % 2 == 1 else "sell"
            price = rng.randint(9500, 10500)
            quantity = rng.randint(1, 20)
            orders.append((side, order_id, price, quantity))

        for side, order_id, price, quantity in orders:
            if side == "buy":
                self.assertTrue(python_engine.add_buy_order(order_id, price, quantity))
                self.assertTrue(cython_engine.add_buy_order(order_id, price, quantity))
            else:
                self.assertTrue(python_engine.add_sell_order(order_id, price, quantity))
                self.assertTrue(cython_engine.add_sell_order(order_id, price, quantity))

        python_engine.match_orders()
        cython_engine.match_orders()

        self.assertEqual(python_engine.trades, cython_engine.get_trades())
        self.assertEqual(python_engine.buy_orders, cython_engine.get_order_books()[0])
        self.assertEqual(python_engine.sell_orders, cython_engine.get_order_books()[1])

    def test_c_level_trade_storage(self):
        engine = CythonMatchingEngine()
        engine.add_buy_order(101, 100, 10)
        engine.add_sell_order(201, 90, 5)
        engine.match_orders()
        self.assertEqual(engine.get_trades(), [(101, 201, 90, 5)])

    def test_trade_memory_growth(self):
        engine = CythonMatchingEngine()

        total_orders = 1025

        # Add 1025 BUY orders
        for i in range(total_orders):
            engine.add_buy_order(i + 1, 100, 1)

        # Add 1025 SELL orders
        for i in range(total_orders):
            engine.add_sell_order(i + 10001, 90, 1)

        engine.match_orders()

        trades = engine.get_trades()

        self.assertEqual(len(trades), 1025)
        self.assertEqual(trades[0], (1, 10001, 90, 1))
        self.assertEqual(trades[-1], (1025, 11025, 90, 1))
if __name__ == "__main__":
    unittest.main()
