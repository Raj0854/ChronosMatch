import unittest
from matching_enigne import MatchingEngine


class TestMatchingEngine(unittest.TestCase):

    def test_partial_fill(self):
        engine = MatchingEngine()

        engine.add_buy_order(101, 100, 10)
        engine.add_sell_order(201, 100, 4)
        engine.match_orders()

        self.assertEqual(engine.buy_orders, [(101, 100, 6, 0)])
        self.assertEqual(engine.sell_orders, [])

    def test_trade_history(self):
        engine = MatchingEngine()
        engine.add_buy_order(101, 100, 10)
        engine.add_sell_order(201, 100, 4)
        engine.match_orders()
        self.assertEqual(engine.trades, [(101, 201, 100, 4)])

    def test_multiple_trades_recorded(self):
        engine = MatchingEngine()
        engine.add_buy_order(101, 100, 5)
        engine.add_sell_order(201, 100, 2)
        engine.add_sell_order(202, 100, 3)
        engine.match_orders()       
        self.assertEqual(
            engine.trades,
            [
                (101, 201, 100, 2),
                (101, 202, 100, 3),
            ],
        )

    def test_reject_duplicate_order_id(self):
        engine = MatchingEngine()

        self.assertTrue(engine.add_buy_order(101, 100, 5))

        result = engine.add_sell_order(101, 100, 3)
        self.assertFalse(result)
        self.assertEqual(len(engine.buy_orders), 1)
        self.assertEqual(engine.sell_orders, [])

    def test_reject_invalid_buy_order(self):
        engine = MatchingEngine()

        result = engine.add_buy_order(101, 0, 5)

        self.assertFalse(result)
        self.assertEqual(engine.buy_orders, [])

    def test_reject_invalid_sell_order(self):
        engine = MatchingEngine()

        result = engine.add_sell_order(201, 100, -5)

        self.assertFalse(result)
        self.assertEqual(engine.sell_orders, [])

    def test_price_time_priority(self):
        engine = MatchingEngine()

        engine.add_buy_order(101, 100, 5)
        engine.add_buy_order(102, 100, 5)
        engine.add_sell_order(201, 100, 7)

        engine.match_orders()

        self.assertEqual(engine.buy_orders, [(102, 100, 3, 1)])
        self.assertEqual(engine.sell_orders, [])
    def test_order_book_after_full_match(self):
        engine = MatchingEngine()

        engine.add_buy_order(101, 100, 5)
        engine.add_sell_order(201, 100, 5)

        engine.match_orders()

        self.assertEqual(engine.buy_orders, [])
        self.assertEqual(engine.sell_orders, [])
        self.assertEqual(len(engine.trades), 1)
    def test_reject_reused_id_after_full_fill(self):
        engine = MatchingEngine()

        engine.add_buy_order(101, 100, 5)
        engine.add_sell_order(201, 100, 5)
        engine.match_orders()

        # Order 101 fully execute ho chuka hai.
        result = engine.add_sell_order(101, 100, 2)

        self.assertFalse(result)
        self.assertEqual(engine.sell_orders, [])
if __name__ == "__main__":
    unittest.main()
