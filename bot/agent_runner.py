"""
AgentRunner — per-agent isolated coroutine.
Each runner has its own: API key, wallet path, logger prefix, memory.

Environment variables per agent (i = 1..N):
  AGENT_{i}_NAME          = "MoltyAgent1"    (or auto-generated)
  AGENT_{i}_API_KEY       = ""               (auto-generated on first run)
  AGENT_{i}_PRIVATE_KEY   = ""               (auto-generated)
  AGENT_{i}_WALLET_ADDRESS= ""               (auto-generated)
  AGENT_{i}_OWNER_EOA     = ""               (auto-generated)
  AGENT_{i}_OWNER_KEY     = ""               (auto-generated)
"""
import asyncio
import os
import json
import pathlib
from bot.api_client import MoltyAPI, APIError
from bot.web3.wallet_manager import generate_agent_wallet, generate_owner_wallet
from bot.game.websocket_engine import WebSocketEngine
from bot.game.free_join import join_free_game
from bot.game.paid_join import join_paid_game
from bot.game.room_selector import select_room
from bot.game.settlement import settle_game
from bot.memory.agent_memory import AgentMemory
from bot.state_router import determine_state, NO_ACCOUNT, IN_GAME, READY_PAID, READY_FREE
from bot.strategy.brain import reset_game_state
from bot.config import ROOM_MODE, ENABLE_MEMORY, AUTO_IDENTITY
from bot.utils.logger import get_logger


class AgentRunner:
    """Isolated agent runner. Each instance = 1 independent bot."""

    def __init__(self, index: int):
        self.index = index
        self.prefix = f"AGENT_{index}"
        self.log = get_logger(f"agent.{index}")
        self.data_dir = pathlib.Path(f"dev-agent-{index}")
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.memory = AgentMemory() if ENABLE_MEMORY else None
        self.running = True
        self._creds: dict = {}
        self._api_key: str = ""
        self._agent_name: str = ""

    # ── Env helpers ──────────────────────────────────────────────────

    def _env(self, key: str, default: str = "") -> str:
        """Read AGENT_{index}_{key} or fall back to global {key}."""
        val = os.getenv(f"{self.prefix}_{key}", "")
        if val:
            return val
        return os.getenv(key, default)

    def _set_env(self, key: str, value: str):
        """Set AGENT_{index}_{key} in process env (so Railway sync works)."""
        os.environ[f"{self.prefix}_{key}"] = value

    # ── Credential management ────────────────────────────────────────

    def _creds_path(self) -> pathlib.Path:
        return self.data_dir / "credentials.json"

    def _load_creds(self) -> dict:
        p = self._creds_path()
        if p.exists():
            try:
                return json.loads(p.read_text())
            except Exception:
                pass
        return {}

    def _save_creds(self, creds: dict):
        self._creds_path().write_text(json.dumps(creds, indent=2))

    def _wallet_path(self, kind: str) -> pathlib.Path:
        return self.data_dir / f"{kind}-wallet.json"

    def _save_wallet(self, kind: str, address: str, pk: str):
        self._wallet_path(kind).write_text(json.dumps({
            "address": address, "privateKey": pk
        }, indent=2))

    # ── Setup ────────────────────────────────────────────────────────

    async def _setup(self) -> bool:
        """Ensure account exists. Returns True on success."""
        # Check existing env credentials (Railway restart)
        api_key   = self._env("API_KEY")
        agent_pk  = self._env("PRIVATE_KEY")
        agent_addr= self._env("WALLET_ADDRESS")
        owner_pk  = self._env("OWNER_KEY")
        owner_addr= self._env("OWNER_EOA")
        name      = self._env("NAME", f"lololpoi-{self.index:02d}")

        if api_key and agent_pk:
            self.log.info("♻️  Restoring from env vars (agent=%s)", name)
            self._api_key = api_key
            self._agent_name = name
            self._creds = {"api_key": api_key, "agent_name": name,
                           "agent_wallet_address": agent_addr, "owner_eoa": owner_addr}
            if agent_pk and agent_addr:
                self._save_wallet("agent", agent_addr, agent_pk)
            if owner_pk and owner_addr:
                self._save_wallet("owner", owner_addr, owner_pk)
            return True

        # Check cached credentials file
        creds = self._load_creds()
        if creds.get("api_key"):
            self.log.info("♻️  Restoring from credentials file (agent=%s)", creds.get("agent_name"))
            self._api_key = creds["api_key"]
            self._agent_name = creds.get("agent_name", name)
            self._creds = creds
            return True

        # First run — generate wallets
        self.log.info("🆕 First run — generating wallets for agent %d", self.index)

        # Agent wallet: ALWAYS unique per agent
        agent_addr, agent_pk = generate_agent_wallet()
        self._save_wallet("agent", agent_addr, agent_pk)
        self._set_env("WALLET_ADDRESS", agent_addr)
        self._set_env("PRIVATE_KEY", agent_pk)

        # Owner wallet: SHARE across all agents if global OWNER_KEY exists
        global_owner_key  = os.getenv("OWNER_KEY", "")
        global_owner_addr = os.getenv("OWNER_EOA", "")
        if global_owner_key and global_owner_addr:
            # Reuse global owner for all agents (1 owner → 10 agents)
            owner_pk = global_owner_key
            owner_addr = global_owner_addr
            self.log.info("  🔗 Using shared Owner wallet: %s", owner_addr[:16] + "...")
        else:
            # No global owner → generate one (only agent #1 creates, others reuse)
            owner_file = pathlib.Path("shared-owner-wallet.json")
            if owner_file.exists():
                try:
                    shared = json.loads(owner_file.read_text())
                    owner_addr = shared["address"]
                    owner_pk = shared["privateKey"]
                    self.log.info("  🔗 Reusing shared Owner from agent #1: %s",
                                 owner_addr[:16] + "...")
                except Exception:
                    owner_addr, owner_pk = generate_owner_wallet()
            else:
                owner_addr, owner_pk = generate_owner_wallet()
                # Save for other agents to reuse
                owner_file.write_text(json.dumps({
                    "address": owner_addr, "privateKey": owner_pk
                }, indent=2))
                self.log.info("  🆕 Generated shared Owner wallet: %s",
                              owner_addr[:16] + "...")

        self._save_wallet("owner", owner_addr, owner_pk)
        self._set_env("OWNER_EOA", owner_addr)
        self._set_env("OWNER_KEY", owner_pk)

        self.log.info("  Agent  wallet: %s", agent_addr[:16] + "...")
        self.log.info("  Owner  wallet: %s", owner_addr[:16] + "...")

        # POST /accounts (with staggered retry for rate limiting)
        api = MoltyAPI()
        max_retries = 10
        result = None
        for attempt in range(1, max_retries + 1):
            try:
                result = await api.create_account(name, agent_addr)
                break
            except APIError as e:
                if e.code == "CONFLICT":
                    self.log.warning("Wallet already registered, reloading creds.")
                    creds = self._load_creds()
                    if creds.get("api_key"):
                        self._api_key = creds["api_key"]
                        self._agent_name = creds.get("agent_name", name)
                        self._creds = creds
                        await api.close()
                        return True
                    break
                if e.code in ("FORBIDDEN", "SERVER_ERROR", "RATE_LIMITED") and attempt < max_retries:
                    wait = 15 * attempt + (self.index * 5)  # Stagger by agent index
                    self.log.warning("Account creation failed (%s), retry %d/%d in %ds",
                                     e.code, attempt, max_retries, wait)
                    await asyncio.sleep(wait)
                    continue
                self.log.error("Account creation failed permanently: %s", e)
                await api.close()
                return False
        await api.close()

        if not result:
            return False

        api_key = result.get("apiKey", "")
        if not api_key:
            self.log.error("No apiKey in response!")
            return False

        self._api_key = api_key
        self._agent_name = name
        self._creds = {
            "api_key": api_key, "agent_name": name,
            "agent_wallet_address": agent_addr, "owner_eoa": owner_addr,
        }
        self._save_creds(self._creds)
        self._set_env("API_KEY", api_key)
        self._set_env("NAME", name)

        self.log.info("✅ Account created! agent=%s", name)
        return True

    # ── Main loop ────────────────────────────────────────────────────

    async def run(self):
        """Run this agent's full lifecycle loop forever."""
        self.log.info("━━━ Agent #%d starting ━━━", self.index)

        # Setup with retry
        while self.running:
            try:
                ok = await self._setup()
                if ok:
                    break
            except Exception as e:
                self.log.error("Setup error: %s — retry in 60s", e)
            await asyncio.sleep(60)

        if not self._api_key:
            self.log.error("No API key — agent #%d exiting", self.index)
            return

        # Game loop
        consecutive_errors = 0
        while self.running:
            try:
                await self._game_cycle()
                consecutive_errors = 0
            except asyncio.CancelledError:
                break
            except Exception as e:
                consecutive_errors += 1
                wait = min(60 * consecutive_errors, 300)
                self.log.error("Game cycle error #%d: %s — retry in %ds",
                               consecutive_errors, e, wait)
                await asyncio.sleep(wait)

    async def _game_cycle(self):
        """One full game cycle: setup → join → play → settle → repeat."""
        api = MoltyAPI(self._api_key)
        try:
            # Check state — determine_state returns (state_str, context_dict)
            me = await api.get_accounts_me()
            state, ctx = determine_state(me)
            balance = me.get("smoltzBalance", me.get("balance", "?"))
            self.log.info("[#%d] State: %s | Balance: %s sMoltz",
                          self.index, state, balance)

            # ── NO_ACCOUNT: wait for setup ──
            if state == NO_ACCOUNT:
                self.log.warning("[#%d] Account not ready, waiting 30s", self.index)
                await asyncio.sleep(30)
                return

            # ── NO_IDENTITY: register ERC-8004 identity ──
            if state == NO_IDENTITY:
                self.log.info("[#%d] Registering ERC-8004 identity...", self.index)
                from bot.setup.identity import ensure_identity
                from bot.setup.whitelist import ensure_whitelist
                from bot.setup.wallet_setup import ensure_molty_wallet

                # Run setup steps in order
                try:
                    await ensure_molty_wallet(api)
                except Exception as e:
                    self.log.warning("[#%d] Wallet setup: %s", self.index, e)

                try:
                    await ensure_whitelist(api)
                except Exception as e:
                    self.log.warning("[#%d] Whitelist: %s", self.index, e)

                ok = await ensure_identity(api)
                if ok:
                    self.log.info("[#%d] ✅ Identity registered!", self.index)
                else:
                    self.log.warning("[#%d] Identity not ready, retry in 30s", self.index)
                    await asyncio.sleep(30)
                return

            # ── IN_GAME: reconnect to active game ──
            if state == IN_GAME:
                game_id = ctx.get("game_id", "")
                self.log.info("[#%d] Already in game %s — connecting WS",
                              self.index, game_id[:12] if game_id else "?")
                if game_id:
                    engine = WebSocketEngine(self._api_key, game_id,
                                             agent_name=self._agent_name,
                                             memory=self.memory)
                    await engine.run()
                    await settle_game(api, game_id)
                    reset_game_state()
                return

            # ── READY: join a room ──
            if state in (READY_FREE, READY_PAID):
                room_mode = select_room(state, ROOM_MODE)
                if room_mode == "paid":
                    self.log.info("[#%d] Joining PAID room", self.index)
                    join_result = await join_paid_game(api)
                else:
                    self.log.info("[#%d] Joining FREE room", self.index)
                    join_result = await join_free_game(api)

                if not join_result:
                    self.log.warning("[#%d] Join failed, retry in 30s", self.index)
                    await asyncio.sleep(30)
                    return

                game_id = join_result.get("gameId", "")
                if not game_id:
                    await asyncio.sleep(15)
                    return

                self.log.info("[#%d] Joined game %s", self.index, game_id[:12])
                engine = WebSocketEngine(self._api_key, game_id,
                                         agent_name=self._agent_name,
                                         memory=self.memory)
                await engine.run()
                await settle_game(api, game_id)
                reset_game_state()
                return

        finally:
            await api.close()

        await asyncio.sleep(10)
