"""Apply Ollama via ngrok to Cursor local storage.

IMPORTANT: Quit Cursor completely before running, then reopen after.
While Cursor is running it overwrites this file from in-memory settings.
"""
from __future__ import annotations

import json
import os
import sqlite3

STORAGE_KEY = (
    "src.vs.platform.reactivestorage.browser.reactiveStorageServiceImpl."
    "persistentStorage.applicationUser"
)
OPENAI_KEY = "ollama"
MODEL = "qwen3:latest"
BASE_URL = "https://unbundle-existing-property.ngrok-free.dev/v1"


def _set_model_slot(slot: dict) -> None:
    slot["modelName"] = MODEL
    slot["maxMode"] = False
    slot["selectedModels"] = [{"modelId": MODEL, "parameters": []}]


def main() -> None:
    db = os.path.expandvars(r"%APPDATA%\Cursor\User\globalStorage\state.vscdb")
    conn = sqlite3.connect(db)
    cur = conn.cursor()

    cur.execute("SELECT value FROM ItemTable WHERE key=?", (STORAGE_KEY,))
    row = cur.fetchone()
    if not row:
        raise SystemExit(f"Missing storage key: {STORAGE_KEY}")

    raw = row[0]
    data = json.loads(raw.decode() if isinstance(raw, bytes) else raw)

    data["openAIBaseUrl"] = BASE_URL
    data["useOpenAIKey"] = True

    ai = data.setdefault("aiSettings", {})
    added = set(ai.get("userAddedModels") or [])
    added.add(MODEL)
    ai["userAddedModels"] = sorted(added)

    enabled = set(ai.get("modelOverrideEnabled") or [])
    enabled.add(MODEL)
    ai["modelOverrideEnabled"] = sorted(enabled)

    disabled = set(ai.get("modelOverrideDisabled") or [])
    disabled.discard(MODEL)
    ai["modelOverrideDisabled"] = sorted(disabled)

    model_cfg = ai.setdefault("modelConfig", {})
    for name in ("cmd-k", "composer", "background-composer", "quick-agent"):
        if name in model_cfg:
            _set_model_slot(model_cfg[name])

    cur.execute(
        "UPDATE ItemTable SET value=? WHERE key=?",
        (json.dumps(data).encode("utf-8"), STORAGE_KEY),
    )
    cur.execute(
        "UPDATE ItemTable SET value=? WHERE key=?",
        (OPENAI_KEY.encode("utf-8"), "cursorAuth/openAIKey"),
    )
    conn.commit()
    conn.close()

    print("Updated Cursor model settings:")
    print(f"  openAIBaseUrl = {BASE_URL}")
    print(f"  useOpenAIKey  = true")
    print(f"  openAIKey     = {OPENAI_KEY}")
    print(f"  model         = {MODEL}")
    print("Reload Cursor (Ctrl+Shift+P -> Developer: Reload Window) to apply.")


if __name__ == "__main__":
    main()
