
# cython_engine.pyx

from libc.stdlib cimport malloc, free
from libc.stddef cimport size_t
from libc.string cimport memmove


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
    cdef tuple _last_profile

    def __cinit__(self, int capacity=10000):
        self.buy_orders = NULL
        self.sell_orders = NULL
        self.buy_count = 0
        self.sell_count = 0
        self.next_sequence = 0
        self.capacity = 0
        self.used_order_ids = set()
        self.trades = []
        self._last_profile = (0, 0, 0, 0)

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

        self._sift_up_buy(self.buy_count - 1)

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

        # BUY: highest price first; earlier sequence first on ties
        buy_result.sort(key=lambda order: (-order[1], order[3]))

        # SELL: lowest price first; earlier sequence first on ties
        sell_result.sort(key=lambda order: (order[1], order[3]))

        return buy_result, sell_result


    def get_trades(self):
        return self.trades.copy()
    
    cdef bint _higher_priority(self, Order a, Order b, bint buy_side):
        if a.price != b.price:
            if buy_side:
                return a.price > b.price
            return a.price < b.price

        return a.sequence < b.sequence

    cdef void _sift_up(self, Order* orders, int index, bint buy_side):
        cdef int parent
        cdef Order temp

        while index > 0:
            parent = (index - 1) // 2

            if not self._higher_priority(
                orders[index], orders[parent], buy_side
            ):
                break

            temp = orders[parent]
            orders[parent] = orders[index]
            orders[index] = temp

            index = parent

    cdef void _pop_buy_root(self):
        if self.buy_count <= 0:
            return

        self.buy_count -= 1

        if self.buy_count > 0:
            self.buy_orders[0] = self.buy_orders[self.buy_count]
            self._sift_down_buy(0)
    cdef void _sift_down(self, Order* orders, int start, int end, bint buy_side):
        cdef int root = start
        cdef int child
        cdef int worst
        cdef Order temp

        while root * 2 + 1 <= end:
            child = root * 2 + 1
            worst = root

            # Select the lower-priority child for the heap root.
            if buy_side:
                if (
                    orders[child].price < orders[worst].price or
                    (
                        orders[child].price == orders[worst].price and
                        orders[child].sequence > orders[worst].sequence
                    )
                ):
                    worst = child

                if child + 1 <= end and (
                    orders[child + 1].price < orders[worst].price or
                    (
                        orders[child + 1].price == orders[worst].price and
                        orders[child + 1].sequence > orders[worst].sequence
                    )
                ):
                    worst = child + 1

            else:
                if (
                    orders[child].price > orders[worst].price or
                    (
                        orders[child].price == orders[worst].price and
                        orders[child].sequence > orders[worst].sequence
                    )
                ):
                    worst = child

                if child + 1 <= end and (
                    orders[child + 1].price > orders[worst].price or
                    (
                        orders[child + 1].price == orders[worst].price and
                        orders[child + 1].sequence > orders[worst].sequence
                    )
                ):
                    worst = child + 1

            if worst == root:
                return

            temp = orders[root]
            orders[root] = orders[worst]
            orders[worst] = temp
            root = worst
    cdef void _sift_up_buy(self, int index):
        cdef int parent
        cdef Order temp

        while index > 0:
            parent = (index - 1) // 2

            if (
                self.buy_orders[parent].price >
                self.buy_orders[index].price
            ):
                break

            if (
                self.buy_orders[parent].price ==
                self.buy_orders[index].price and
                self.buy_orders[parent].sequence <
                self.buy_orders[index].sequence
            ):
                break

            temp = self.buy_orders[parent]
            self.buy_orders[parent] = self.buy_orders[index]
            self.buy_orders[index] = temp

            index = parent
    cdef void _sift_down_buy(self, int index):
        cdef int left
        cdef int right
        cdef int best
        cdef Order temp

        while True:
            left = 2 * index + 1
            right = left + 1
            best = index

            if left < self.buy_count and self._higher_priority(
                self.buy_orders[left], self.buy_orders[best], True
            ):
                best = left

            if right < self.buy_count and self._higher_priority(
                self.buy_orders[right], self.buy_orders[best], True
            ):
                best = right

            if best == index:
                break

            temp = self.buy_orders[index]
            self.buy_orders[index] = self.buy_orders[best]
            self.buy_orders[best] = temp

            index = best

    cdef void _sort_buy_orders(self):
        cdef int start, end
        cdef Order temp

        if self.buy_count < 2:
            return

        start = self.buy_count // 2 - 1

        while start >= 0:
            self._sift_down(
                self.buy_orders, start, self.buy_count - 1, True
            )
            start -= 1

        end = self.buy_count - 1

        while end > 0:
            temp = self.buy_orders[0]
            self.buy_orders[0] = self.buy_orders[end]
            self.buy_orders[end] = temp

            end -= 1
            self._sift_down(self.buy_orders, 0, end, True)


    cdef void _sort_sell_orders(self):
        cdef int start, end
        cdef Order temp

        if self.sell_count < 2:
            return

        start = self.sell_count // 2 - 1

        while start >= 0:
            self._sift_down(
                self.sell_orders, start, self.sell_count - 1, False
            )
            start -= 1

        end = self.sell_count - 1

        while end > 0:
            temp = self.sell_orders[0]
            self.sell_orders[0] = self.sell_orders[end]
            self.sell_orders[end] = temp

            end -= 1
            self._sift_down(self.sell_orders, 0, end, False)



    
    
    cdef void _execute_matching(self):
        import time

        cdef Order buy
        cdef Order sell
        cdef int trade_quantity
        cdef int buy_remaining
        cdef int sell_remaining

        cdef long long loop_ns = 0
        cdef long long shift_ns = 0
        cdef long long start_ns
        cdef long long shift_start_ns

        cdef int buy_full_fills = 0
        cdef int sell_full_fills = 0

        while self.buy_count > 0 and self.sell_count > 0:
            start_ns = time.perf_counter_ns()

            buy = self.buy_orders[0]
            sell = self.sell_orders[0]

            if buy.price < sell.price:
                loop_ns += time.perf_counter_ns() - start_ns
                break

            trade_quantity = min(buy.quantity, sell.quantity)

            self.trades.append(
                (buy.order_id, sell.order_id, sell.price, trade_quantity)
            )

            buy_remaining = buy.quantity - trade_quantity
            sell_remaining = sell.quantity - trade_quantity

            loop_ns += time.perf_counter_ns() - start_ns

            if buy_remaining == 0:
                buy_full_fills += 1
                self._pop_buy_root()
            else:
                self.buy_orders[0].quantity = buy_remaining

            if sell_remaining == 0:
                sell_full_fills += 1

                if self.sell_count > 1:
                    shift_start_ns = time.perf_counter_ns()

                    memmove(
                        self.sell_orders,
                        self.sell_orders + 1,
                        (self.sell_count - 1) * sizeof(Order)
                    )

                    shift_ns += time.perf_counter_ns() - shift_start_ns

                self.sell_count -= 1
            else:
                self.sell_orders[0].quantity = sell_remaining

        self._last_profile = (
            loop_ns, shift_ns, buy_full_fills, sell_full_fills
        )

    
    def match_orders(self):
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
    
    def get_last_profile(self):
        return self._last_profile
