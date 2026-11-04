import asyncio
import random


async def generate_orders(
    total_orders=10,
    delay=0.1,
    start_id=101
):
    for order_id in range(start_id, start_id + total_orders):
        side = random.choice(["BUY", "SELL"])
        price_ticks = random.randint(9900, 10100)
        quantity = random.randint(1, 10)

        order = (order_id, side, price_ticks, quantity)
        yield order

        if delay > 0:
            await asyncio.sleep(delay)


if __name__ == "__main__":

    async def test():
        async for order in generate_orders():
            print("Generated:", order)

    asyncio.run(test())