
import asyncio
import random


async def generate_orders():
    for order_id in range(101, 111):
        side = random.choice(["BUY", "SELL"])
        price_ticks = random.randint(9900, 10100)
        quantity = random.randint(1, 10)

        order = (order_id, side, price_ticks, quantity)
        yield order

        await asyncio.sleep(0.1)


if __name__ == "__main__":
    async def test():
        async for order in generate_orders():
            print("Generated:", order)

    asyncio.run(test())
