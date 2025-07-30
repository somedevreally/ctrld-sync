#!/usr/bin/env python3
"""
Rename Control D folders to add HA- prefix
---------------------------------------
Renames existing folders in Control D profiles by adding HA- prefix.
"""

import os
import logging
import httpx
from dotenv import load_dotenv
from typing import Dict, Optional

# --------------------------------------------------------------------------- #
# 0. Bootstrap – load secrets and configure logging
# --------------------------------------------------------------------------- #
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("control-d-rename")

# --------------------------------------------------------------------------- #
# 1. Constants
# --------------------------------------------------------------------------- #
API_BASE = "https://api.controld.com/profiles"
TOKEN = os.getenv("TOKEN")
PROFILE_IDS = [p.strip() for p in os.getenv("PROFILE", "").split(",") if p.strip()]
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds

# --------------------------------------------------------------------------- #
# 2. Client
# --------------------------------------------------------------------------- #
_api = httpx.Client(
    headers={
        "Accept": "application/json",
        "Authorization": f"Bearer {TOKEN}",
    },
    timeout=30,
)

# --------------------------------------------------------------------------- #
# 3. Helpers
# --------------------------------------------------------------------------- #
def _api_get(url: str) -> httpx.Response:
    """GET helper for Control-D API with retries."""
    for attempt in range(MAX_RETRIES):
        try:
            response = _api.get(url)
            response.raise_for_status()
            return response
        except (httpx.HTTPError, httpx.TimeoutException) as e:
            if attempt == MAX_RETRIES - 1:
                log.error(f"Response content: {e.response.text if hasattr(e, 'response') else 'No response'}")
                raise
            wait_time = RETRY_DELAY * (2 ** attempt)
            log.warning(f"Request failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}. Retrying in {wait_time}s...")
            time.sleep(wait_time)

def _api_put(url: str, data: Dict) -> httpx.Response:
    """PUT helper for Control-D API with retries."""
    for attempt in range(MAX_RETRIES):
        try:
            response = _api.put(url, data=data)
            response.raise_for_status()
            return response
        except (httpx.HTTPError, httpx.TimeoutException) as e:
            if attempt == MAX_RETRIES - 1:
                log.error(f"Response content: {e.response.text if hasattr(e, 'response') else 'No response'}")
                raise
            wait_time = RETRY_DELAY * (2 ** attempt)
            log.warning(f"Request failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}. Retrying in {wait_time}s...")
            time.sleep(wait_time)

def list_existing_folders(profile_id: str) -> Dict[str, str]:
    """Return folder-name -> folder-id mapping."""
    try:
        data = _api_get(f"{API_BASE}/{profile_id}/groups").json()
        folders = data.get("body", {}).get("groups", [])
        return {f["group"].strip(): f["PK"] for f in folders if f.get("group") and f.get("PK")}
    except (httpx.HTTPError, KeyError) as e:
        log.error(f"Failed to list existing folders: {e}")
        return {}

def rename_folder(profile_id: str, folder_id: str, current_name: str) -> bool:
    """Rename a folder to add HA- prefix. Returns True if successful."""
    new_name = f"HA-{current_name}"
    try:
        _api_put(
            f"{API_BASE}/{profile_id}/groups/{folder_id}",
            data={"name": new_name}
        )
        log.info(f"Renamed folder '{current_name}' to '{new_name}' (ID {folder_id})")
        return True
    except httpx.HTTPError as e:
        log.error(f"Failed to rename folder '{current_name}' to '{new_name}': {e}")
        return False

# --------------------------------------------------------------------------- #
# 4. Main workflow
# --------------------------------------------------------------------------- #
def rename_folders_for_profile(profile_id: str) -> bool:
    """Rename all folders in a profile to add HA- prefix. Returns True if all successful."""
    try:
        existing_folders = list_existing_folders(profile_id)
        if not existing_folders:
            log.info(f"No folders found for profile {profile_id}")
            return True

        success_count = 0
        for name, folder_id in existing_folders.items():
            if not name.startswith("HA-"):  # Skip already prefixed folders
                if rename_folder(profile_id, folder_id, name):
                    success_count += 1

        log.info(f"Profile {profile_id}: {success_count}/{len(existing_folders)} folders renamed successfully")
        return success_count == len([n for n in existing_folders if not n.startswith("HA-")])
    except Exception as e:
        log.error(f"Unexpected error during rename for profile {profile_id}: {e}")
        return False

# --------------------------------------------------------------------------- #
# 5. Entry-point
# --------------------------------------------------------------------------- #
def main():
    if not TOKEN or not PROFILE_IDS:
        log.error("TOKEN and/or PROFILE missing - check your .env file")
        exit(1)

    success_count = 0
    for profile_id in PROFILE_IDS:
        log.info(f"Starting rename for profile {profile_id}")
        if rename_folders_for_profile(profile_id):
            success_count += 1

    log.info(f"All profiles processed: {success_count}/{len(PROFILE_IDS)} successful")
    exit(0 if success_count == len(PROFILE_IDS) else 1)

if __name__ == "__main__":
    main()