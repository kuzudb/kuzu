import discord
import os
import json
import sys

TOKEN = os.getenv("DISCORD_BOT_TOKEN")
CHANNEL_ID = os.getenv("DISCORD_CHANNEL_ID")
GITHUB_URL = os.getenv("GITHUB_URL")

message = ""


if __name__ == "__main__":
    if not len(sys.argv) == 2:
        print("Usage: python send-dicord-notification.py <result.json>")
        sys.exit(1)
    if not os.path.isfile(sys.argv[1]):
        print(f"Error: {sys.argv[1]} is not a file")
        sys.exit(1)
    if not TOKEN:
        print("Error: DISCORD_BOT_TOKEN is not set")
        sys.exit(1)

    if not CHANNEL_ID:
        print("Error: DISCORD_CHANNEL_ID is not set")
        sys.exit(1)
    client = discord.Client(intents=discord.Intents.default())

    @client.event
    async def on_ready():
        channel = client.get_channel(int(CHANNEL_ID))
        await channel.send(message)
        await client.close()

    message += "## Multiplatform test result:\n"
    with open(sys.argv[1], "r") as f:
        result = json.load(f)
        for platform in result:
            message += f"- **{platform}**:\n"
            for r in result[platform]:
                message += f"  - {r['stage']}: {r['status']}\n"
    if GITHUB_URL:
        message += f"  [Github]({GITHUB_URL})"
    client.run(TOKEN)
