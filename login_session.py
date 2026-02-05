import os
import argparse
import re
from typing import Optional
from telethon import TelegramClient
from dotenv import load_dotenv

load_dotenv("/opt/tg_leads/.env")

def env_key(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", name).upper()

def resolve_session_file(account: Optional[str], session_override: Optional[str]) -> str:
    if session_override:
        return session_override
    if account:
        key = env_key(account)
        env_name = f"ACCOUNT_{key}_SESSION_FILE"
        if env_name in os.environ:
            return os.environ[env_name]
    return os.environ["SESSION_FILE"]

parser = argparse.ArgumentParser(description="Login Telegram session for tg_leads")
parser.add_argument("--session", help="Path to .session file (overrides account/env)")
parser.add_argument("--account", help="Account name from ACCOUNTS to pick ACCOUNT_<NAME>_SESSION_FILE")
args = parser.parse_args()

api_id = int(os.environ["API_ID"])
api_hash = os.environ["API_HASH"]
session_file = resolve_session_file(args.account, args.session)

client = TelegramClient(session_file, api_id, api_hash)

async def main():
    await client.start()  # попросит номер и код
    me = await client.get_me()
    print("✅ Logged in as:", me.username or me.first_name, me.id)

with client:
    client.loop.run_until_complete(main())
