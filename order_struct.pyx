
# matching_engine.pyx

cdef struct Order:
    int order_id
    int price
    int quantity
    int sequence


def test_order_struct(int order_id, int price, int quantity, int sequence):
    cdef Order order

    order.order_id = order_id
    order.price = price
    order.quantity = quantity
    order.sequence = sequence

    return (
        order.order_id,
        order.price,
        order.quantity,
        order.sequence
    )
