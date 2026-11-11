
import multiprocessing as mp

from main import run_pipeline


if __name__ == "__main__":

    mp.freeze_support()

    total_orders = 1_000_000

    print("=" * 50)
    print("1 MILLION ORDER IPC STRESS TEST")
    print("=" * 50)

    result = run_pipeline("1M-STRESS", total_orders)

    print("\n========== FINAL RESULTS ==========")

    print(f"Orders sent/received: {result['received']:,}")
    print(f"Orders accepted:      {result['accepted']:,}")
    print(f"Orders rejected:      {result['rejected']:,}")
    print(f"Order loss:           {result['order_loss']:,}")
    print(f"Trades executed:      {result['trade_count']:,}")
    print(f"Trades saved:         {result['saved_trades']:,}")
    print(f"Total traded quantity:{result['total_traded_quantity']:,}")

    print(f"Execution time:       {result['execution_time']:.4f} seconds")
    print(f"Throughput:           {result['throughput']:,.2f} orders/sec")

    passed = (
        result["received"] == total_orders
        and result["accepted"] == total_orders
        and result["rejected"] == 0
        and result["order_loss"] == 0
        and result["trade_count"] == result["saved_trades"]
    )

    print("\nSTRESS TEST:", "PASS" if passed else "FAIL")
    print("=" * 50)
