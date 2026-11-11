
import curses
import time


def draw_dashboard(stdscr, stats, stop_event):

    curses.curs_set(0)
    stdscr.nodelay(True)

    while not stop_event.is_set():

        with stats.get_lock():
            values = list(stats[:])

        (
            received,
            processed,
            trades,
            traded_quantity,
            buy_count,
            sell_count,
            best_bid,
            best_bid_qty,
            best_ask,
            best_ask_qty,
            total_latency_ns,
            max_latency_ns,
            start_ns
        ) = values

        now_ns = time.perf_counter_ns()

        elapsed = max((now_ns - start_ns) / 1_000_000_000, 0.000001)

        avg_latency_us = (
            total_latency_ns / processed / 1000
            if processed else 0
        )

        max_latency_us = max_latency_ns / 1000

        throughput = processed / elapsed

        stdscr.erase()

        height, width = stdscr.getmaxyx()

        def write(y, x, message, attr=0):

            if y < height and x < width:

                try:
                    stdscr.addnstr(
                        y, x, str(message),
                        max(0, width - x - 1), attr
                    )
                except curses.error:
                    pass

        title = "CHRONOSMATCH - LIVE LATENCY MONITOR"

        write(1, max(0, (width - len(title)) // 2),
              title, curses.A_BOLD)

        write(3, 2, "=" * max(0, width - 4))

        write(5, 4, "SYSTEM STATUS", curses.A_BOLD)
        write(6, 4, "Status: RUNNING")
        write(7, 4, f"Runtime: {elapsed:.2f} seconds")

        write(9, 4, "ORDER STATISTICS", curses.A_BOLD)
        write(10, 4, f"Orders received: {received:,}")
        write(11, 4, f"Orders processed: {processed:,}")
        write(12, 4, f"Throughput: {throughput:,.2f} orders/sec")

        write(14, 4, "MATCHING ENGINE", curses.A_BOLD)
        write(15, 4, f"Trades executed: {trades:,}")
        write(16, 4, f"Traded quantity: {traded_quantity:,}")
        write(17, 4, f"Remaining buy orders: {buy_count:,}")
        write(18, 4, f"Remaining sell orders: {sell_count:,}")

        write(20, 4, "TOP OF BOOK", curses.A_BOLD)
        write(21, 4, f"Best Bid: {best_bid} | Quantity: {best_bid_qty}")
        write(22, 4, f"Best Ask: {best_ask} | Quantity: {best_ask_qty}")

        write(24, 4, "IPC LATENCY", curses.A_BOLD)
        write(25, 4, f"Average: {avg_latency_us:.2f} us")
        write(26, 4, f"Maximum observed: {max_latency_us:.2f} us")

        write(28, 4, "Press Q to close dashboard")

        stdscr.refresh()

        key = stdscr.getch()

        if key in (ord("q"), ord("Q")):
            break

        time.sleep(0.1)


def run_dashboard(stats, stop_event):

    curses.wrapper(
        draw_dashboard,
        stats,
        stop_event
    )
