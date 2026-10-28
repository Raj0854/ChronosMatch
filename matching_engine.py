class MatchingEngine:
    def __init__(self):
        self.buy_orders = []
        self.sell_orders = []
        self.trades = []
        self.next_sequence = 0
        self.used_order_ids = set()
        self.verbose = True

    def validate_order(self, order_id, price, quantity):
        if order_id <= 0 or price <= 0 or quantity <= 0:
            return False
        if order_id in self.used_order_ids:
            return False
        return True

    def add_buy_order(self, order_id, price, quantity):
        if not self.validate_order(order_id, price, quantity):
            print("Rejected invalid BUY order:", order_id)
            return False
        self.used_order_ids.add(order_id)
        order = (order_id, price, quantity, self.next_sequence)
        self.next_sequence += 1
        self.buy_orders.append(order)
        return True

    def add_sell_order(self, order_id, price, quantity):
        if not self.validate_order(order_id, price, quantity):
            print("Rejected invalid SELL order:", order_id)
            return False
        self.used_order_ids.add(order_id)
        order = (order_id, price, quantity, self.next_sequence)
        self.next_sequence += 1
        self.sell_orders.append(order)
        return True

    def sort_buy_orders(self):
        self.buy_orders.sort(key=lambda order: (-order[1], order[3]))

    def sort_sell_orders(self):
        self.sell_orders.sort(key=lambda order: (order[1], order[3]))

    def match_orders(self):
        self.sort_buy_orders()
        self.sort_sell_orders()

        while self.buy_orders and self.sell_orders:
            buy = self.buy_orders[0]
            sell = self.sell_orders[0]

            if buy[1] < sell[1]:
                break
            trade_quantity = min(buy[2], sell[2])
            self.trades.append((buy[0], sell[0], sell[1], trade_quantity))
            if self.verbose:
                print(
                    f"Trade: Buy Order {buy[0]} + Sell Order {sell[0]} | "
                    f"Price: {sell[1]} | Quantity: {trade_quantity}"
                )

            buy_remaining = buy[2] - trade_quantity
            sell_remaining = sell[2] - trade_quantity

            if buy_remaining == 0:
                self.buy_orders.pop(0)
            else:
                self.buy_orders[0] = (buy[0], buy[1], buy_remaining, buy[3])

            if sell_remaining == 0:
                self.sell_orders.pop(0)
            else:
                self.sell_orders[0] = (sell[0], sell[1], sell_remaining, sell[3])

  
if __name__ == "__main__":
    engine = MatchingEngine()

    engine.add_buy_order(101, 100, 5)
    engine.add_buy_order(102, 100, 5)
    engine.add_sell_order(201, 100, 7)

    engine.match_orders()

    print("Remaining buy orders:", engine.buy_orders)
    print("Remaining sell orders:", engine.sell_orders)
