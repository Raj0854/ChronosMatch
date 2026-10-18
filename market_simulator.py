import random
import asyncio


async def generate_orders():
    for order_id in range(101, 111):
        price_ticks = random.randint(9900, 10100)
        quantity = random.randint(1, 10)

        order = (order_id, price_ticks, quantity)
        yield order
        await asyncio.sleep(0.1)


# Is file ko import karne par simulator automatically run nahi hoga.
if __name__ == "__main__":
    async def test():
        async for order in generate_orders():
            print(f"Generated : {order}")
            
    asyncio.run(test())
