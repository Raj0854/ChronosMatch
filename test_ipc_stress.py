
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
    print(f"Order loss:           {result['loss']:,}")
    print(f"Trades:               {result['trades']:,}")
    print(f"Execution time:       {result['time']:.4f} sec")
    print(f"Throughput:           {result['throughput']:.2f} orders/sec")

    passed = (
        result["received"] == total_orders
        and result["accepted"] == total_orders
        and result["rejected"] == 0
        and result["loss"] == 0
    )

    print("\nSTRESS TEST:", "PASS" if passed else "FAIL")
    print("=" * 50)
