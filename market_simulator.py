import random


def generate_order(order_id):
    price_ticks = random.randint(9900, 10100)
    quantity = random.randint(1, 10)
    return order_id, price_ticks, quantity


