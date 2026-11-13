# native_ring.pyx

from libc.stdint cimport uint32_t, uint64_t
import time

cdef object _perf_counter_ns = time.perf_counter_ns


# ============================================================
# WINDOWS NATIVE ATOMIC OPERATIONS
# ============================================================

cdef extern from *:
    """
    #include <windows.h>

    static __inline long long cm_load64(long long* address) {
        return (long long)InterlockedCompareExchange64(
            (volatile LONG64*)address, 0, 0
        );
    }

    static __inline void cm_store64(
        long long* address,
        long long value
    ) {
        InterlockedExchange64(
            (volatile LONG64*)address,
            (LONG64)value
        );
    }

    static __inline long long cm_add64(
        long long* address,
        long long value
    ) {
        return InterlockedExchangeAdd64(
            (volatile LONG64*)address,
            (LONG64)value
        );
    }

    static __inline void cm_update_max64(
        long long* address,
        long long value
    ) {
        long long current;

        do {
            current = cm_load64(address);

            if (current >= value) {
                return;
            }

        } while (
            InterlockedCompareExchange64(
                (volatile LONG64*)address,
                (LONG64)value,
                (LONG64)current
            ) != current
        );
    }
    """

    long long cm_load64(long long* address) noexcept nogil
    void cm_store64(long long* address, long long value) noexcept nogil
    long long cm_add64(long long* address, long long value) noexcept nogil
    void cm_update_max64(long long* address, long long value) noexcept nogil


# ============================================================
# SHARED ORDER STRUCTURE
# ============================================================

cdef struct NativeOrder:
    uint32_t order_id
    uint32_t side
    uint32_t price
    uint32_t quantity
    uint64_t timestamp_ns


# ============================================================
# SHARED MEMORY HEADER OFFSETS
# ============================================================

cdef int HEADER_WRITE = 0
cdef int HEADER_READ = 8

# Profiling counters
cdef int HEADER_MAX_OCCUPANCY = 16
cdef int HEADER_OCCUPANCY_SUM = 24
cdef int HEADER_OCCUPANCY_SAMPLES = 32
cdef int HEADER_FULL_EVENTS = 40

cdef int DATA_OFFSET = 64


# ============================================================
# NATIVE RING BUFFER
# ============================================================

cdef class NativeRingBuffer:

    cdef unsigned char[:] view
    cdef unsigned char* base

    cdef int capacity
    cdef int mask

    def __cinit__(self, mapping, int capacity):

        if capacity <= 0 or (capacity & (capacity - 1)) != 0:
            raise ValueError(
                "Capacity must be a positive power of two"
            )

        required_size = (
            DATA_OFFSET
            + capacity * sizeof(NativeOrder)
        )

        self.view = mapping

        if self.view.shape[0] < required_size:
            raise ValueError(
                "Mapped memory is too small"
            )

        self.base = &self.view[0]

        self.capacity = capacity
        self.mask = capacity - 1

    # ========================================================
    # INITIALIZATION
    # ========================================================

    def initialize(self):
        """
        Call exactly once from the parent process,
        before starting producer and consumer.
        """

        cm_store64(
            <long long*>(self.base + HEADER_WRITE),
            0
        )

        cm_store64(
            <long long*>(self.base + HEADER_READ),
            0
        )

        cm_store64(
            <long long*>(self.base + HEADER_MAX_OCCUPANCY),
            0
        )

        cm_store64(
            <long long*>(self.base + HEADER_OCCUPANCY_SUM),
            0
        )

        cm_store64(
            <long long*>(self.base + HEADER_OCCUPANCY_SAMPLES),
            0
        )

        cm_store64(
            <long long*>(self.base + HEADER_FULL_EVENTS),
            0
        )

    # ========================================================
    # PRODUCER
    # ========================================================

    def push_many(self, orders):

        cdef long long write_pos
        cdef long long read_pos
        cdef long long available
        cdef long long occupancy

        cdef int pushed = 0
        cdef int index = 0

        cdef NativeOrder* slot

        cdef object order
        cdef uint32_t side_code

        write_pos = cm_load64(
            <long long*>(self.base + HEADER_WRITE)
        )

        read_pos = cm_load64(
            <long long*>(self.base + HEADER_READ)
        )

        available = (
            self.capacity - (write_pos - read_pos)
        )

        if available <= 0:

            cm_add64(
                <long long*>(
                    self.base + HEADER_FULL_EVENTS
                ),
                1
            )

            return 0

        for order in orders:

            if pushed >= available:
                break

            order_id, side, price, quantity = order

            if side in (0, "BUY"):
                side_code = 0

            elif side in (1, "SELL"):
                side_code = 1

            else:
                raise ValueError(
                    "Invalid order side"
                )

            index = <int>(write_pos & self.mask)

            slot = <NativeOrder*>(
                self.base
                + DATA_OFFSET
                + index * sizeof(NativeOrder)
            )

            slot.order_id = order_id
            slot.side = side_code
            slot.price = price
            slot.quantity = quantity

            slot.timestamp_ns = _perf_counter_ns()

            write_pos += 1
            pushed += 1

        if pushed:

            # Publish written orders.
            cm_store64(
                <long long*>(
                    self.base + HEADER_WRITE
                ),
                write_pos
            )

            # Update maximum occupancy.
            # Reload read position to avoid using an old value.
            read_pos = cm_load64(
                <long long*>(
                    self.base + HEADER_READ
                )
            )

            occupancy = write_pos - read_pos

            cm_update_max64(
                <long long*>(
                    self.base + HEADER_MAX_OCCUPANCY
                ),
                occupancy
            )

        return pushed

    # ========================================================
    # CONSUMER
    # ========================================================

    def pop_many(self, int max_items=128):

        cdef long long write_pos
        cdef long long read_pos
        cdef long long occupancy

        cdef int amount
        cdef int index

        cdef NativeOrder* slot

        cdef list orders = []

        if max_items <= 0:
            return orders

        read_pos = cm_load64(
            <long long*>(
                self.base + HEADER_READ
            )
        )

        write_pos = cm_load64(
            <long long*>(
                self.base + HEADER_WRITE
            )
        )

        occupancy = write_pos - read_pos

        if occupancy <= 0:
            return orders

        # Record occupancy before consuming this batch.
        cm_add64(
            <long long*>(
                self.base + HEADER_OCCUPANCY_SUM
            ),
            occupancy
        )

        cm_add64(
            <long long*>(
                self.base + HEADER_OCCUPANCY_SAMPLES
            ),
            1
        )

        amount = <int>min(
            occupancy,
            max_items
        )

        for _ in range(amount):

            index = <int>(read_pos & self.mask)

            slot = <NativeOrder*>(
                self.base
                + DATA_OFFSET
                + index * sizeof(NativeOrder)
            )

            orders.append((
                slot.order_id,
                slot.side,
                slot.price,
                slot.quantity,
                slot.timestamp_ns
            ))

            read_pos += 1

        # Publish consumed slots.
        cm_store64(
            <long long*>(
                self.base + HEADER_READ
            ),
            read_pos
        )

        return orders

    # ========================================================
    # OCCUPANCY PROFILING
    # ========================================================

    def get_stats(self):

        cdef long long write_pos
        cdef long long read_pos

        cdef long long current_occupancy
        cdef long long max_occupancy
        cdef long long occupancy_sum
        cdef long long occupancy_samples
        cdef long long full_events

        write_pos = cm_load64(
            <long long*>(
                self.base + HEADER_WRITE
            )
        )

        read_pos = cm_load64(
            <long long*>(
                self.base + HEADER_READ
            )
        )

        current_occupancy = write_pos - read_pos

        max_occupancy = cm_load64(
            <long long*>(
                self.base + HEADER_MAX_OCCUPANCY
            )
        )

        occupancy_sum = cm_load64(
            <long long*>(
                self.base + HEADER_OCCUPANCY_SUM
            )
        )

        occupancy_samples = cm_load64(
            <long long*>(
                self.base + HEADER_OCCUPANCY_SAMPLES
            )
        )

        full_events = cm_load64(
            <long long*>(
                self.base + HEADER_FULL_EVENTS
            )
        )

        average_occupancy = (
            occupancy_sum / occupancy_samples
            if occupancy_samples > 0
            else 0.0
        )

        return {
            "current_occupancy": current_occupancy,
            "max_occupancy": max_occupancy,
            "average_sampled_occupancy": average_occupancy,
            "occupancy_samples": occupancy_samples,
            "full_buffer_events": full_events,
            "capacity": self.capacity,
        }

    # ========================================================
    # BUFFER LENGTH
    # ========================================================

    def __len__(self):

        cdef long long write_pos
        cdef long long read_pos

        write_pos = cm_load64(
            <long long*>(
                self.base + HEADER_WRITE
            )
        )

        read_pos = cm_load64(
            <long long*>(
                self.base + HEADER_READ
            )
        )

        return write_pos - read_pos