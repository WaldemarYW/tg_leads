import os
from telethon import TelegramClient
from dotenv import load_dotenv

load_dotenv("/opt/tg_leads/.env")

api_id = int(os.environ["API_ID"])
api_hash = os.environ["API_HASH"]
session_file = os.environ["SESSION_FILE"]

client = TelegramClient(session_file, api_id, api_hash)

async def main():
    await client.start()  # попросит номер и код
    me = await client.get_me()
    print("✅ Logged in as:", me.username or me.first_name, me.id)

with client:
    client.loop.run_until_complete(main())
