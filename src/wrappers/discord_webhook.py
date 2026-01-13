import logging
import requests

from wrappers import track_info

logger = logging.getLogger("discord_fm").getChild(__name__)


class DiscordWebhook:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        self.last_track = None
        self.last_message_id = None
        self.connected = False

    def connect(self):
        if not self.webhook_url:
            raise ValueError("Webhook URL is not configured")
        self.connected = True
        logger.info("Webhook mode enabled")

    def clear_presence(self):
        if self.last_message_id and self.webhook_url:
            try:
                requests.delete(f"{self.webhook_url}/messages/{self.last_message_id}")
                self.last_message_id = None
            except Exception as e:
                logger.debug(f"Failed to delete webhook message: {e}")

    def clear_last_track(self):
        self.last_track = None

    def exit_rp(self):
        self.clear_presence()
        self.connected = False
        logger.info("Closed Discord Webhook connection")

    def update_status(self, track: track_info.TrackInfo):
        if self.last_track == track:
            logger.debug(
                f"Track {track.name} is the same as last track, not updating"
            )
            return

        logger.info("Now playing: " + track.name)
        self.last_track = track

        embed = {
            "title": track.name,
            "description": f"by **{track.artist}**",
            "url": track.url,
            "color": 0xD51007,  # Last.fm red
            "thumbnail": {"url": track.cover} if track.cover else None,
            "footer": {
                "text": "Powered by Discord.fm",
                "icon_url": "https://www.last.fm/static/images/lastfm_avatar_twitter.52a5d69a85ac.png"
            }
        }

        # Remove None values
        embed = {k: v for k, v in embed.items() if v is not None}

        payload = {
            "username": "Discord.fm",
            "embeds": [embed]
        }

        try:
            # Delete previous message if exists
            if self.last_message_id:
                try:
                    requests.delete(f"{self.webhook_url}/messages/{self.last_message_id}")
                except Exception:
                    pass

            # Send new message
            response = requests.post(
                f"{self.webhook_url}?wait=true",
                json=payload
            )
            response.raise_for_status()

            data = response.json()
            self.last_message_id = data.get("id")
            logger.debug(f"Webhook message sent, id: {self.last_message_id}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to send webhook: {e}")
            raise
