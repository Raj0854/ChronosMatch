
# cython_engine.pyx

import time

from libc.stdlib cimport malloc, free, realloc
from libc.stddef cimport size_t
from libc.stdint cimport SIZE_MAX, int64_t
from libc.limits cimport INT_MAX


cdef struct Order:
    int order_id
    int price
    int quantity
    int64_t sequence


cdef struct Trade:
    int buy_order_id
    int sell_order_id
    int price
    int quantity


cdef inline bint _higher_priority_nogil(
    Order* a,
    Order* b,
    bint buy_side
) noexcept nogil:

    if a.price != b.price:
        if buy_side:
            return a.price > b.price
        return a.price < b.price

    return a.sequence < b.sequence


cdef void _sift_down_buy_nogil(
    Order* orders,
    int count,
    int index
) noexcept nogil:

    cdef int left
    cdef int right
    cdef int best
    cdef Order temp

    while True:
        left = 2 * index + 1
        right = left + 1
        best = index

        if left < count and _higher_priority_nogil(
            &orders[left], &orders[best], True
        ):
            best = left

        if right < count and _higher_priority_nogil(
            &orders[right], &orders[best], True
        ):
            best = right

        if best == index:
            break

        temp = orders[index]
        orders[index] = orders[best]
        orders[best] = temp
        index = best


cdef void _sift_down_sell_nogil(
    Order* orders,
    int count,
    int index
) noexcept nogil:

    cdef int left
    cdef int right
    cdef int best
    cdef Order temp

    while True:
        left = 2 * index + 1
        right = left + 1
        best = index

        if left < count and _higher_priority_nogil(
            &orders[left], &orders[best], False
        ):
            best = left

        if right < count and _higher_priority_nogil(
            &orders[right], &orders[best], False
        ):
            best = right

        if best == index:
            break

        temp = orders[index]
        orders[index] = orders[best]
        orders[best] = temp
        index = best


cdef void _execute_matching_nogil(
    Order* buy_orders,
    int* buy_count,
    Order* sell_orders,
    int* sell_count,
    Trade* trade_records,
    int* trade_count
) noexcept nogil:

    cdef Order buy
    cdef Order sell
    cdef int quantity
    cdef int buy_remaining
    cdef int sell_remaining

    while buy_count[0] > 0 and sell_count[0] > 0:

        buy = buy_orders[0]
        sell = sell_orders[0]

        if buy.price < sell.price:
            break

        if buy.quantity < sell.quantity:
            quantity = buy.quantity
        else:
            quantity = sell.quantity

        trade_records[trade_count[0]].buy_order_id = buy.order_id
        trade_records[trade_count[0]].sell_order_id = sell.order_id
        trade_records[trade_count[0]].price = sell.price
        trade_records[trade_count[0]].quantity = quantity

        trade_count[0] += 1

        buy_remaining = buy.quantity - quantity
        sell_remaining = sell.quantity - quantity

        if buy_remaining == 0:
            buy_count[0] -= 1

            if buy_count[0] > 0:
                buy_orders[0] = buy_orders[buy_count[0]]
                _sift_down_buy_nogil(
                    buy_orders, buy_count[0], 0
                )
        else:
            buy_orders[0].quantity = buy_remaining

        if sell_remaining == 0:
            sell_count[0] -= 1

            if sell_count[0] > 0:
                sell_orders[0] = sell_orders[sell_count[0]]
                _sift_down_sell_nogil(
                    sell_orders, sell_count[0], 0
                )
        else:
            sell_orders[0].quantity = sell_remaining


cdef class CythonMatchingEngine:

    cdef Order* buy_orders
    cdef Order* sell_orders
    cdef int buy_count
    cdef int sell_count
    cdef int capacity
    cdef int64_t next_sequence
    cdef set used_order_ids
    cdef tuple _last_profile

    cdef Trade* trade_records
    cdef int trade_count
    cdef int trade_capacity

    # Incremental total traded quantity
    cdef long long total_traded_quantity

    def __cinit__(self, int capacity=10000):

        self.buy_orders = NULL
        self.sell_orders = NULL
        self.buy_count = 0
        self.sell_count = 0
        self.next_sequence = 0
        self.capacity = 0

        self.used_order_ids = set()
        self._last_profile = (0, 0, 0, 0)

        self.trade_records = NULL
        self.trade_count = 0
        self.trade_capacity = 0

        self.total_traded_quantity = 0

        if capacity <= 0:
            raise ValueError("Capacity must be positive")

        if capacity > SIZE_MAX // sizeof(Order):
            raise OverflowError("Order capacity is too large")

        self.buy_orders = <Order*>malloc(
            capacity * sizeof(Order)
        )

        if self.buy_orders == NULL:
            raise MemoryError()

        self.sell_orders = <Order*>malloc(
            capacity * sizeof(Order)
        )

        if self.sell_orders == NULL:
            free(self.buy_orders)
            self.buy_orders = NULL
            raise MemoryError()

        self.capacity = capacity

    cdef bint _reserve_trade_capacity(self, int required):

        cdef size_t new_capacity
        cdef size_t max_capacity
        cdef Trade* new_records

        if required <= self.trade_capacity:
            return True

        max_capacity = SIZE_MAX // sizeof(Trade)

        if max_capacity > INT_MAX:
            max_capacity = INT_MAX

        if required > max_capacity:
            return False

        new_capacity = self.trade_capacity

        if new_capacity < 1024:
            new_capacity = 1024

        while new_capacity < required:

            if new_capacity > max_capacity // 2:
                new_capacity = max_capacity
                break

            new_capacity *= 2

        if new_capacity < required:
            return False

        new_records = <Trade*>realloc(
            self.trade_records,
            new_capacity * sizeof(Trade)
        )

        if new_records == NULL:
            return False

        self.trade_records = new_records
        self.trade_capacity = <int>new_capacity

        return True

    def __dealloc__(self):

        if self.buy_orders != NULL:
            free(self.buy_orders)

        if self.sell_orders != NULL:
            free(self.sell_orders)

        if self.trade_records != NULL:
            free(self.trade_records)

    cdef bint _valid_order(
        self,
        int order_id,
        int price,
        int quantity
    ):

        if order_id <= 0 or price <= 0 or quantity <= 0:
            return False

        if order_id in self.used_order_ids:
            return False

        return True

    def add_buy_order(
        self,
        int order_id,
        int price,
        int quantity
    ):

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

    def add_sell_order(
        self,
        int order_id,
        int price,
        int quantity
    ):

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

        self._sift_up_sell(self.sell_count - 1)

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
                (
                    order.order_id,
                    order.price,
                    order.quantity,
                    order.sequence
                )
            )

        for i in range(self.sell_count):

            order = self.sell_orders[i]

            sell_result.append(
                (
                    order.order_id,
                    order.price,
                    order.quantity,
                    order.sequence
                )
            )

        buy_result.sort(
            key=lambda order: (-order[1], order[3])
        )

        sell_result.sort(
            key=lambda order: (order[1], order[3])
        )

        return buy_result, sell_result

    def get_trades(self):

        cdef int i
        cdef list result = []

        for i in range(self.trade_count):

            result.append(
                (
                    self.trade_records[i].buy_order_id,
                    self.trade_records[i].sell_order_id,
                    self.trade_records[i].price,
                    self.trade_records[i].quantity
                )
            )

        return result

    # Return only trades created after start_index.
    def get_trades_since(self, int start_index):

        cdef int i
        cdef list result = []

        if start_index < 0:
            raise ValueError("start_index cannot be negative")

        if start_index > self.trade_count:
            raise ValueError("start_index exceeds trade count")

        for i in range(start_index, self.trade_count):

            result.append(
                (
                    self.trade_records[i].buy_order_id,
                    self.trade_records[i].sell_order_id,
                    self.trade_records[i].price,
                    self.trade_records[i].quantity
                )
            )

        return result

    # O(1): No iteration over the complete trade history.
    def get_trade_summary(self):

        return (
            self.trade_count,
            self.total_traded_quantity
        )

    cdef bint _higher_priority(
        self,
        Order a,
        Order b,
        bint buy_side
    ):

        if a.price != b.price:

            if buy_side:
                return a.price > b.price

            return a.price < b.price

        return a.sequence < b.sequence

    cdef void _pop_buy_root(self):

        if self.buy_count <= 0:
            return

        self.buy_count -= 1

        if self.buy_count > 0:

            self.buy_orders[0] = self.buy_orders[self.buy_count]
            self._sift_down_buy(0)

    cdef void _sift_up_sell(self, int index):

        cdef int parent
        cdef Order temp

        while index > 0:

            parent = (index - 1) // 2

            if (
                self.sell_orders[parent].price <
                self.sell_orders[index].price
            ):
                break

            if (
                self.sell_orders[parent].price ==
                self.sell_orders[index].price and
                self.sell_orders[parent].sequence <
                self.sell_orders[index].sequence
            ):
                break

            temp = self.sell_orders[parent]
            self.sell_orders[parent] = self.sell_orders[index]
            self.sell_orders[index] = temp

            index = parent

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
                self.buy_orders[left],
                self.buy_orders[best],
                True
            ):
                best = left

            if right < self.buy_count and self._higher_priority(
                self.buy_orders[right],
                self.buy_orders[best],
                True
            ):
                best = right

            if best == index:
                break

            temp = self.buy_orders[index]
            self.buy_orders[index] = self.buy_orders[best]
            self.buy_orders[best] = temp

            index = best

    cdef void _sift_down_sell(self, int index):

        cdef int left
        cdef int right
        cdef int best
        cdef Order temp

        while True:

            left = 2 * index + 1
            right = left + 1
            best = index

            if left < self.sell_count and self._higher_priority(
                self.sell_orders[left],
                self.sell_orders[best],
                False
            ):
                best = left

            if right < self.sell_count and self._higher_priority(
                self.sell_orders[right],
                self.sell_orders[best],
                False
            ):
                best = right

            if best == index:
                break

            temp = self.sell_orders[index]
            self.sell_orders[index] = self.sell_orders[best]
            self.sell_orders[best] = temp

            index = best

    cdef void _pop_sell_root(self):

        if self.sell_count <= 0:
            return

        self.sell_count -= 1

        if self.sell_count > 0:

            self.sell_orders[0] = self.sell_orders[self.sell_count]
            self._sift_down_sell(0)

    cdef void _execute_matching(self, bint profile=False):

        cdef int buy_count
        cdef int sell_count
        cdef int trade_count
        cdef int required

        cdef int previous_trade_count
        cdef int i

        cdef Order* buy_orders
        cdef Order* sell_orders
        cdef Trade* trade_records

        cdef long long start_ns = 0
        cdef long long loop_ns = 0

        if self.buy_count > INT_MAX - self.sell_count:
            raise OverflowError("Too many orders")

        required = (
            self.trade_count +
            self.buy_count +
            self.sell_count
        )

        if required < self.trade_count:
            raise OverflowError("Trade capacity overflow")

        if not self._reserve_trade_capacity(required):
            raise MemoryError("Unable to reserve trade memory")

        buy_count = self.buy_count
        sell_count = self.sell_count
        trade_count = self.trade_count

        previous_trade_count = self.trade_count

        buy_orders = self.buy_orders
        sell_orders = self.sell_orders
        trade_records = self.trade_records

        if profile:
            start_ns = time.perf_counter_ns()

        with nogil:

            _execute_matching_nogil(
                buy_orders,
                &buy_count,
                sell_orders,
                &sell_count,
                trade_records,
                &trade_count
            )

        self.buy_count = buy_count
        self.sell_count = sell_count
        self.trade_count = trade_count

        # Update total quantity only for newly created trades.
        for i in range(previous_trade_count, self.trade_count):
            self.total_traded_quantity += (
                self.trade_records[i].quantity
            )

        if profile:

            loop_ns = time.perf_counter_ns() - start_ns

            self._last_profile = (
                loop_ns,
                0,
                0,
                0
            )

    def match_orders(self):

        self._execute_matching(False)

    def profile_stages(self):

        self._execute_matching(True)

        matching_ns = self._last_profile[0]

        return 0, 0, matching_ns

    def get_last_profile(self):

        return self._last_profile
