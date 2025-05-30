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

    message = ""
    message += "## Multiplatform test result:\n"
    with open(sys.argv[1], "r") as f:
        result = json.load(f)
        failures = {}
        message += "### Success:\n"
        for platform in sorted(result.keys()):
            if len(message) >= 1500:
                messages.append(message)
                message = ""
            message += f"- **{platform}**:\n"
            for r in result[platform]:
                # Only show success status first.
                if r['status'] == "✅":
                    message += f"  - {r['stage']}: {r['status']}\n"
                else:
                    failures.setdefault(platform, list()).append(r)
        if len(failures) > 0:
            # Show all failures at the end.
            message += "### Failure:\n"
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
