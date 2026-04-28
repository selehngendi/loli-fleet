"""
Molty Royale AI Agent Fleet — Entry Point v3.0
Runs N agents concurrently in one process.

Config via env vars:
  AGENT_COUNT=10        → number of concurrent agents (default: 1)

Per-agent credentials (i = 1..N):
  AGENT_{i}_NAME        = "MoltyAgent1"
  AGENT_{i}_API_KEY     = ""   (auto-generated on first run)
  AGENT_{i}_PRIVATE_KEY = ""   (auto-generated)
  AGENT_{i}_WALLET_ADDRESS = ""
  AGENT_{i}_OWNER_EOA   = ""
  AGENT_{i}_OWNER_KEY   = ""

Run: python -m bot.main
"""
import asyncio
import os
import sys
from bot.agent_runner import AgentRunner
from bot.dashboard.server import start_dashboard
from bot.utils.logger import get_logger

log = get_logger(__name__)

DASHBOARD_PORT = int(os.getenv("PORT", os.getenv("DASHBOARD_PORT", "8080")))
AGENT_COUNT = int(os.getenv("AGENT_COUNT", "1"))


async def run_all():
    log.info("╔══════════════════════════════════════════════╗")
    log.info("║  MOLTY ROYALE AI FLEET  v3.0                ║")
    log.info("║  Agents: %-3d                                 ║", AGENT_COUNT)
    log.info("╚══════════════════════════════════════════════╝")

    # Dashboard server (shared for all agents)
    await start_dashboard(port=DASHBOARD_PORT)

    # Spawn agents with STAGGERED startup to avoid rate limiting
    # Each agent starts 10s after the previous one
    runners = [AgentRunner(i) for i in range(1, AGENT_COUNT + 1)]
    tasks = []
    for i, runner in enumerate(runners):
        task = asyncio.create_task(
            _delayed_start(runner, delay=i * 10),
            name=f"agent-{runner.index}"
        )
        tasks.append(task)

    log.info("🚀 Launching %d agent(s) with 10s stagger delay", AGENT_COUNT)

    # Background task to sync variables to Railway ONCE after all agents are setup
    if os.getenv("RAILWAY_API_TOKEN") and os.getenv("SETUP_COMPLETE", "").lower() != "true":
        asyncio.create_task(_wait_and_sync(runners))

    # Wait forever — tasks run indefinitely
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        log.info("Shutdown signal received, stopping all agents...")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _wait_and_sync(runners):
    """Wait for all agents to get an API key, then sync to Railway."""
    log.info("Waiting for all agents to finish setup before syncing to Railway...")
    while True:
        all_ready = True
        for r in runners:
            if not r._api_key:
                all_ready = False
                break
        if all_ready:
            break
        await asyncio.sleep(5)
    
    log.info("All agents setup! Syncing fleet variables to Railway...")
    from bot.utils.railway_sync import sync_fleet_to_railway
    await sync_fleet_to_railway(runners)


async def _delayed_start(runner: AgentRunner, delay: int):
    """Start an agent after a delay to avoid rate limiting."""
    if delay > 0:
        runner.log.info("⏳ Waiting %ds before starting (stagger)...", delay)
        await asyncio.sleep(delay)
    await runner.run()


def main():
    """Entry point."""
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(run_all())
    except KeyboardInterrupt:
        log.info("Shutdown complete.")


if __name__ == "__main__":
    main()
