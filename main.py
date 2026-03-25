import asyncio
import threading
from cruzebot import (
    run_telegram_bot, run_stats_pusher, 
    load_total_signals, load_active_targets, load_managed_trades
)

def main():
    print("""
  ╔══════════════════════════════════════════╗
  ║   🤖  CruzeBot  —  Main Entry Point     ║
  ║   Thread 1 › Telegram listener + trades  ║
  ║   Thread 2 › Vercel stats pusher         ║
  ╚══════════════════════════════════════════╝
""")
    # 1. Load persistent state from local JSON files
    load_total_signals()
    load_active_targets()
    load_managed_trades()

    # 2. Start Thread 2: Stats Pusher & Trade Manager Logic
    # This thread periodically syncs data to Vercel KV and runs automated management.
    threading.Thread(target=run_stats_pusher, name="StatsPusher", daemon=True).start()

    # 3. Start Thread 1: Telegram Signal Listener
    # This runs on the main thread as Telethon requires a stable event loop.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run_telegram_bot())
    except KeyboardInterrupt:
        print("\n👋  Shutting down …")
    finally:
        loop.close()

if __name__ == "__main__":
    main()
