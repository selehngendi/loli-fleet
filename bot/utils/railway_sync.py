"""
Railway Variables auto-sync.
After account creation, saves API_KEY + private keys back to Railway Variables
so credentials survive container restarts.

Uses variableCollectionUpsert to set ALL variables in ONE API call = ONE redeploy.
Only syncs ONCE (checks SETUP_COMPLETE flag to prevent infinite redeploy loop).

Requires: RAILWAY_API_TOKEN (create at https://railway.com/account/tokens)
Railway auto-provides: RAILWAY_PROJECT_ID, RAILWAY_ENVIRONMENT_ID, RAILWAY_SERVICE_ID
"""
import os
import httpx
from bot.utils.logger import get_logger

log = get_logger(__name__)

RAILWAY_API_URL = "https://backboard.railway.com/graphql/v2"


def is_railway() -> bool:
    """Check if running on Railway."""
    return bool(os.getenv("RAILWAY_PROJECT_ID"))


def is_setup_complete() -> bool:
    """Check if first-run sync was already done (prevents redeploy loop)."""
    return os.getenv("SETUP_COMPLETE", "").lower() == "true"


def _get_railway_config() -> dict | None:
    """Get Railway config from env vars. Returns None if not on Railway or missing token."""
    token = os.getenv("RAILWAY_API_TOKEN", "")
    project_id = os.getenv("RAILWAY_PROJECT_ID", "")
    env_id = os.getenv("RAILWAY_ENVIRONMENT_ID", "")
    service_id = os.getenv("RAILWAY_SERVICE_ID", "")

    if not all([token, project_id, env_id, service_id]):
        if is_railway() and not token:
            log.warning(
                "⚠️ RAILWAY_API_TOKEN not set. Cannot auto-save credentials. "
                "Create one at: https://railway.com/account/tokens → "
                "then add RAILWAY_API_TOKEN to Railway Variables."
            )
        return None

    return {
        "token": token,
        "project_id": project_id,
        "environment_id": env_id,
        "service_id": service_id,
    }


async def _collection_upsert(variables_dict: dict) -> bool:
    """
    Save ALL variables in ONE API call using variableCollectionUpsert.
    This triggers only ONE redeploy (not one per variable).
    """
    config = _get_railway_config()
    if not config:
        return False

    # variableCollectionUpsert sets all variables in a single mutation
    mutation = """
    mutation variableCollectionUpsert($input: VariableCollectionUpsertInput!) {
        variableCollectionUpsert(input: $input)
    }
    """

    # Filter out empty values
    clean_vars = {k: v for k, v in variables_dict.items() if v}

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                RAILWAY_API_URL,
                json={
                    "query": mutation,
                    "variables": {
                        "input": {
                            "projectId": config["project_id"],
                            "environmentId": config["environment_id"],
                            "serviceId": config["service_id"],
                            "variables": clean_vars,
                        }
                    },
                },
                headers={
                    "Authorization": f"Bearer {config['token']}",
                    "Content-Type": "application/json",
                },
                timeout=15.0,
            )
            data = resp.json()
            if "errors" in data:
                log.warning("Railway collection upsert error: %s", data["errors"])
                return False
            log.info("Railway sync: %d variables saved in 1 API call", len(clean_vars))
            return True
    except Exception as e:
        log.warning("Railway collection upsert error: %s", e)
        return False


async def sync_all_to_railway(creds: dict, agent_pk: str, owner_pk: str = ""):
    """
    ONE-TIME sync of ALL variables to Railway after first-run.
    Combines config + credentials + private keys into a SINGLE API call.
    Uses variableCollectionUpsert = only 1 redeploy for all variables.
    Sets SETUP_COMPLETE=true in the same call to prevent redeploy loop.
    """
    if not is_railway():
        return

    # Skip if already synced (prevents infinite redeploy loop)
    if is_setup_complete():
        log.info("Railway sync already done (SETUP_COMPLETE=true). Skipping.")
        return

    config = _get_railway_config()
    if not config:
        return

    log.info("First-time Railway sync — saving ALL variables in one API call...")

    from bot.config import (
        ROOM_MODE, ADVANCED_MODE, AUTO_WHITELIST,
        AUTO_SC_WALLET, ENABLE_MEMORY, ENABLE_AGENT_TOKEN,
        AUTO_IDENTITY, LOG_LEVEL,
    )

    # Build complete variables map — ALL in one call = ONE redeploy
    all_vars = {
        # Config
        "ROOM_MODE": ROOM_MODE,
        "ADVANCED_MODE": str(ADVANCED_MODE).lower(),
        "AUTO_WHITELIST": str(AUTO_WHITELIST).lower(),
        "AUTO_SC_WALLET": str(AUTO_SC_WALLET).lower(),
        "ENABLE_MEMORY": str(ENABLE_MEMORY).lower(),
        "ENABLE_AGENT_TOKEN": str(ENABLE_AGENT_TOKEN).lower(),
        "AUTO_IDENTITY": str(AUTO_IDENTITY).lower(),
        "LOG_LEVEL": LOG_LEVEL,
        # Credentials
        "API_KEY": creds.get("api_key", ""),
        "AGENT_NAME": creds.get("agent_name", ""),
        "AGENT_WALLET_ADDRESS": creds.get("agent_wallet_address", ""),
        "OWNER_EOA": creds.get("owner_eoa", ""),
        # Private keys
        "AGENT_PRIVATE_KEY": agent_pk,
        "OWNER_PRIVATE_KEY": owner_pk,
        # Flag to prevent redeploy loop
        "SETUP_COMPLETE": "true",
    }

    ok = await _collection_upsert(all_vars)
    if ok:
        log.info("✅ All variables synced to Railway (1 API call = 1 redeploy). Credentials saved!")
    else:
        log.warning("Railway collection upsert failed — check RAILWAY_API_TOKEN permissions")


async def sync_fleet_to_railway(runners: list):
    """
    Fleet version: waits until all agents are setup, then syncs ALL variables
    (global config + global owner + per-agent keys) to Railway in ONE API call.
    """
    if not is_railway() or is_setup_complete():
        return

    config = _get_railway_config()
    if not config:
        return

    log.info("Fleet Railway sync — saving ALL variables in one API call...")

    # Start with global config and owner
    all_vars = {
        "SETUP_COMPLETE": "true",
        "OWNER_KEY": os.getenv("OWNER_KEY", ""),
        "OWNER_EOA": os.getenv("OWNER_EOA", ""),
    }

    # Add per-agent variables
    for r in runners:
        prefix = r.prefix  # e.g. "AGENT_1"
        all_vars[f"{prefix}_NAME"] = r._agent_name
        all_vars[f"{prefix}_API_KEY"] = r._api_key
        all_vars[f"{prefix}_PRIVATE_KEY"] = os.getenv(f"{prefix}_PRIVATE_KEY", "")

    ok = await _collection_upsert(all_vars)
    if ok:
        log.info("✅ Fleet variables synced to Railway. Keys are now permanent!")
    else:
        log.warning("Fleet Railway upsert failed.")
