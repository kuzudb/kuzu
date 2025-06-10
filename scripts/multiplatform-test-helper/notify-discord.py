import discord
import os
import json
import sys

TOKEN = os.getenv("DISCORD_BOT_TOKEN")
CHANNEL_ID = os.getenv("DISCORD_CHANNEL_ID")
GITHUB_URL = os.getenv("GITHUB_URL")

messages = []


if __name__ == "__main__":
    if not len(sys.argv) == 2:
        print("Usage: python send-discord-notification.py <result.json>")
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
        for message in messages:
            try:
                await channel.send(message)
            except Exception as e:
                print(f"Error: {e}")
                sys.exit(1)
        await client.close()

    message = "## Multiplatform test result:\n"
    with open(sys.argv[1], "r") as f:
        result = json.load(f)
        failures = {}
        stages = []
        for platform in sorted(result.keys()):
            for r in result[platform]:
                if r['stage'] not in stages:
                    stages.append(r['stage'])
                if r['status'] != "✅":
                    failures.setdefault(platform, list()).append(r)
        message += f"- Platforms:\n  - {', '.join(sorted(result.keys()))}\n"
        message += f"- Stages:\n  - {', '.join(stages)}\n"
        # Add only the failures.
        if len(failures) > 0:
            message += "### Failures:\n"
            for platform in sorted(failures.keys()):
                if len(message) >= 1500:
                    messages.append(message)
                    message = ""
                message += f"- **{platform}**:\n"
                for r in failures[platform]:
                    message += f"  - {r['stage']}: {r['status']}\n"
    if GITHUB_URL:
        message += "\n"
        message += f"  [Github]({GITHUB_URL})"
    if message:
        messages.append(message)
    client.run(TOKEN)
