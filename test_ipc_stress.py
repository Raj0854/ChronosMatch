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

    print(f"Orders sent:          {total_orders:,}")
    print(f"Orders received:      {result['received']:,}")
    print(f"Orders accepted:      {result['accepted']:,}")
    print(f"Orders rejected:      {result['rejected']:,}")
    print(f"Order loss:           {result['order_loss']:,}")

    print(f"Trades executed:      {result['trade_count']:,}")
    print(f"Trades saved:         {result['saved_trades']:,}")

    print(
        f"Total traded quantity: "
        f"{result['total_traded_quantity']:,}"
    )

    print(
        f"Database quantity:   "
        f"{result['database_quantity']:,}"
    )

    print(
        f"Execution time:       "
        f"{result['execution_time']:.4f} seconds"
    )

    print(
        f"Throughput:           "
        f"{result['throughput']:,.2f} orders/sec"
    )

    print(
        f"Average IPC latency:  "
        f"{result['average_latency_us']:.2f} us"
    )

    print(
        f"Maximum IPC latency:  "
        f"{result['maximum_latency_us']:.2f} us"
    )

    checks = {
        "All orders received":
            result["received"] == total_orders,

        "All orders accepted":
            result["accepted"] == total_orders,

        "Zero rejected orders":
            result["rejected"] == 0,

        "Zero order loss":
            result["order_loss"] == 0,

        "Trade count consistency":
            result["trade_count"] == result["saved_trades"],

        "Traded quantity consistency":
            result["total_traded_quantity"]
            == result["database_quantity"],
    }

    print("\n========== VALIDATION ==========")

    for name, passed in checks.items():

        print(
            f"{name}: {'PASS' if passed else 'FAIL'}"
        )

    passed = all(checks.values())

    print("\nSTRESS TEST:", "PASS" if passed else "FAIL")
    print("=" * 50)

    if not passed:
        raise SystemExit(1)