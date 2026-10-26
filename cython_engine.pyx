
# cython_engine.pyx

from libc.stdlib cimport malloc, free
from libc.stddef cimport size_t


cdef struct Order:
    int order_id
    int price
    int quantity
    int sequence


cdef class CythonMatchingEngine:
    cdef Order* buy_orders
    cdef Order* sell_orders
    cdef int buy_count
    cdef int sell_count
    cdef int capacity
    cdef int next_sequence
    cdef set used_order_ids
    cdef list trades

    def __cinit__(self, int capacity=10000):
        self.buy_orders = NULL
        self.sell_orders = NULL
        self.buy_count = 0
        self.sell_count = 0
        self.next_sequence = 0
        self.capacity = 0
        self.used_order_ids = set()
        self.trades = []

        if capacity <= 0:
            raise ValueError("Capacity must be positive")

        self.buy_orders = <Order*>malloc(capacity * sizeof(Order))
        if self.buy_orders == NULL:
            raise MemoryError()

        self.sell_orders = <Order*>malloc(capacity * sizeof(Order))
        if self.sell_orders == NULL:
            free(self.buy_orders)
            self.buy_orders = NULL
            raise MemoryError()

        self.capacity = capacity

    def __dealloc__(self):
        if self.buy_orders != NULL:
            free(self.buy_orders)
        if self.sell_orders != NULL:
            free(self.sell_orders)

    cdef bint _valid_order(self, int order_id, int price, int quantity):
        if order_id <= 0 or price <= 0 or quantity <= 0:
            return False
        if order_id in self.used_order_ids:
            return False
        return True

    def add_buy_order(self, int order_id, int price, int quantity):
        cdef Order order

        if not self._valid_order(order_id, price, quantity):
            return False
        if self.buy_count >= self.capacity:
            return False

        order.order_id = order_id
        order.price = price
        order.quantity = quantity
        order.sequence = self.next_sequence

        self.buy_orders[self.buy_count] = order
        self.buy_count += 1
        self.next_sequence += 1
        self.used_order_ids.add(order_id)
        return True

    def add_sell_order(self, int order_id, int price, int quantity):
        cdef Order order

        if not self._valid_order(order_id, price, quantity):
            return False
        if self.sell_count >= self.capacity:
            return False

        order.order_id = order_id
        order.price = price
        order.quantity = quantity
        order.sequence = self.next_sequence

        self.sell_orders[self.sell_count] = order
        self.sell_count += 1
        self.next_sequence += 1
        self.used_order_ids.add(order_id)
        return True

    def get_order_books(self):
        cdef int i
        buy_result = []
        sell_result = []

        for i in range(self.buy_count):
            order = self.buy_orders[i]
            buy_result.append(
                (order.order_id, order.price, order.quantity, order.sequence)
            )

        for i in range(self.sell_count):
            order = self.sell_orders[i]
            sell_result.append(
                (order.order_id, order.price, order.quantity, order.sequence)
            )

        return buy_result, sell_result

    def get_trades(self):
        return self.trades.copy()
    
    cdef void _sort_buy_orders(self):
        cdef int i, j
        cdef Order temp

        for i in range(1, self.buy_count):
            temp = self.buy_orders[i]
            j = i - 1

            while j >= 0 and (
                self.buy_orders[j].price < temp.price or
                (self.buy_orders[j].price == temp.price and
                 self.buy_orders[j].sequence > temp.sequence)
            ):
                self.buy_orders[j + 1] = self.buy_orders[j]
                j -= 1

            self.buy_orders[j + 1] = temp

    cdef void _sort_sell_orders(self):
        cdef int i, j
        cdef Order temp

        for i in range(1, self.sell_count):
            temp = self.sell_orders[i]
            j = i - 1

            while j >= 0 and (
                self.sell_orders[j].price > temp.price or
                (self.sell_orders[j].price == temp.price and
                 self.sell_orders[j].sequence > temp.sequence)
            ):
                self.sell_orders[j + 1] = self.sell_orders[j]
                j -= 1

            self.sell_orders[j + 1] = temp

    
    cdef void _execute_matching(self):
        cdef int i
        cdef Order buy
        cdef Order sell
        cdef int trade_quantity
        cdef int buy_remaining
        cdef int sell_remaining

        while self.buy_count > 0 and self.sell_count > 0:
            buy = self.buy_orders[0]
            sell = self.sell_orders[0]

            if buy.price < sell.price:
                break

            trade_quantity = min(buy.quantity, sell.quantity)

            self.trades.append(
                (buy.order_id, sell.order_id, sell.price, trade_quantity)
            )

            buy_remaining = buy.quantity - trade_quantity
            sell_remaining = sell.quantity - trade_quantity

            if buy_remaining == 0:
                for i in range(1, self.buy_count):
                    self.buy_orders[i - 1] = self.buy_orders[i]
                self.buy_count -= 1
            else:
                self.buy_orders[0].quantity = buy_remaining

            if sell_remaining == 0:
                for i in range(1, self.sell_count):
                    self.sell_orders[i - 1] = self.sell_orders[i]
                self.sell_count -= 1
            else:
                self.sell_orders[0].quantity = sell_remaining

    
    def match_orders(self):
        self._sort_buy_orders()
        self._sort_sell_orders()
        self._execute_matching()
    
    def profile_stages(self):
        import time

        start = time.perf_counter_ns()
        self._sort_buy_orders()
        buy_sort_ns = time.perf_counter_ns() - start

        start = time.perf_counter_ns()
        self._sort_sell_orders()
        sell_sort_ns = time.perf_counter_ns() - start

        start = time.perf_counter_ns()
        self._execute_matching()
        matching_ns = time.perf_counter_ns() - start

        return buy_sort_ns, sell_sort_ns, matching_ns
