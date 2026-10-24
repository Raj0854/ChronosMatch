
# cython_engine.pyx

cdef struct Order:
    int order_id
    int price
    int quantity
    int sequence


cdef class CythonMatchingEngine:
    cdef list buy_orders
    cdef list sell_orders
    cdef list trades
    cdef int next_sequence

    def __cinit__(self):
        self.buy_orders = []
        self.sell_orders = []
        self.trades = []
        self.next_sequence = 0
    
    cdef set used_order_ids

    def __cinit__(self):
        self.buy_orders = []
        self.sell_orders = []
        self.trades = []
        self.next_sequence = 0
        self.used_order_ids = set()

    cdef bint _valid_order(self, int order_id, int price, int quantity):
        if order_id <= 0 or price <= 0 or quantity <= 0:
            return False
        if order_id in self.used_order_ids:
            return False
        return True


    
    def add_buy_order(self, int order_id, int price, int quantity):
        if not self._valid_order(order_id, price, quantity):
            return False

        self.used_order_ids.add(order_id)
        self.buy_orders.append(
            (order_id, price, quantity, self.next_sequence)
        )
        self.next_sequence += 1
        return True

    def add_sell_order(self, int order_id, int price, int quantity):
        if not self._valid_order(order_id, price, quantity):
            return False

        self.used_order_ids.add(order_id)
        self.sell_orders.append(
            (order_id, price, quantity, self.next_sequence)
        )
        self.next_sequence += 1
        return True

    def get_order_books(self):
        return self.buy_orders.copy(), self.sell_orders.copy()

    def get_trades(self):
        return self.trades.copy()
    
    def match_orders(self):
        self.buy_orders.sort(key=lambda order: (-order[1], order[3]))
        self.sell_orders.sort(key=lambda order: (order[1], order[3]))

        while self.buy_orders and self.sell_orders:
            buy = self.buy_orders[0]
            sell = self.sell_orders[0]

            # No match if the highest buy price is below the lowest sell price.
            if buy[1] < sell[1]:
                break

            trade_quantity = min(buy[2], sell[2])

            # Record: buy ID, sell ID, execution price, quantity.
            self.trades.append(
                (buy[0], sell[0], sell[1], trade_quantity)
            )

            buy_remaining = buy[2] - trade_quantity
            sell_remaining = sell[2] - trade_quantity

            if buy_remaining == 0:
                self.buy_orders.pop(0)
            else:
                self.buy_orders[0] = (
                    buy[0], buy[1], buy_remaining, buy[3]
                )

            if sell_remaining == 0:
                self.sell_orders.pop(0)
            else:
                self.sell_orders[0] = (
                    sell[0], sell[1], sell_remaining, sell[3]
                )
