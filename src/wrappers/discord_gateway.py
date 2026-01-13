import json
import logging
import threading
import time
import websocket
import zlib

from wrappers import track_info

logger = logging.getLogger("discord_fm").getChild(__name__)

DISCORD_GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json"


class DiscordGateway:
    """
    Discord Gateway client for setting user presence directly via user token.
    Note: Using user tokens is against Discord's ToS.
    """

    def __init__(self, user_token: str, client_id: str = "881950079240536135"):
        self.token = user_token
        self.client_id = client_id
        self.ws = None
        self.heartbeat_interval = None
        self.heartbeat_thread = None
        self.last_sequence = None
        self.connected = False
        self.last_track = None
        self._stop_heartbeat = threading.Event()

    def connect(self):
        if not self.token:
            raise ValueError("User token is not configured")

        logger.info("Connecting to Discord Gateway...")
        self.ws = websocket.create_connection(
            DISCORD_GATEWAY_URL,
            header={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        )

        # Receive Hello
        hello = json.loads(self.ws.recv())
        if hello.get("op") == 10:
            self.heartbeat_interval = hello["d"]["heartbeat_interval"] / 1000
            logger.debug(f"Received Hello, heartbeat interval: {self.heartbeat_interval}s")
        else:
            raise ConnectionError("Did not receive Hello from Discord Gateway")

        # Send Identify
        identify_payload = {
            "op": 2,
            "d": {
                "token": self.token,
                "properties": {
                    "os": "linux",
                    "browser": "Discord.fm",
                    "device": "Discord.fm"
                },
                "presence": {
                    "status": "online",
                    "since": 0,
                    "activities": [],
                    "afk": False
                }
            }
        }
        self.ws.send(json.dumps(identify_payload))

        # Receive Ready
        ready = json.loads(self.ws.recv())
        if ready.get("op") == 0 and ready.get("t") == "READY":
            self.last_sequence = ready.get("s")
            logger.info("Successfully connected to Discord Gateway")
            self.connected = True
        else:
            raise ConnectionError(f"Did not receive Ready from Discord Gateway: {ready}")

        # Start heartbeat
        self._stop_heartbeat.clear()
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()

    def _heartbeat_loop(self):
        while not self._stop_heartbeat.is_set():
            try:
                heartbeat = {"op": 1, "d": self.last_sequence}
                self.ws.send(json.dumps(heartbeat))
                logger.debug("Sent heartbeat")
            except Exception as e:
                logger.error(f"Heartbeat failed: {e}")
                self.connected = False
                break
            self._stop_heartbeat.wait(self.heartbeat_interval)

    def clear_presence(self):
        if not self.connected or not self.ws:
            return

        try:
            presence_update = {
                "op": 3,
                "d": {
                    "since": None,
                    "activities": [],
                    "status": "online",
                    "afk": False
                }
            }
            self.ws.send(json.dumps(presence_update))
            logger.debug("Cleared presence")
        except Exception as e:
            logger.error(f"Failed to clear presence: {e}")

    def clear_last_track(self):
        self.last_track = None

    def exit_rp(self):
        self._stop_heartbeat.set()
        self.clear_presence()
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
        self.connected = False
        logger.info("Disconnected from Discord Gateway")

    def update_status(self, track: track_info.TrackInfo):
        if not self.connected or not self.ws:
            logger.warning("Not connected to Discord Gateway")
            return

        if self.last_track == track:
            logger.debug(f"Track {track.name} is the same as last track, not updating")
            return

        logger.info("Now playing: " + track.name)
        self.last_track = track

        start_time = int(time.time() * 1000)
        end_time = start_time + track.duration if track.duration else None

        # Build Listening activity - shows "Listening to [name]"
        # Timestamps must be in milliseconds for Listening activities
        activity = {
            "name": "Last.fm",
            "type": 2,  # 2 = Listening
            "details": track.name,
            "state": f"by {track.artist}",
            "timestamps": {
                "start": start_time,
            },
        }

        # Add end timestamp for progress bar if duration is available
        if end_time and track.duration > 0:
            activity["timestamps"]["end"] = end_time

        presence_update = {
            "op": 3,
            "d": {
                "since": None,
                "activities": [activity],
                "status": "online",
                "afk": False
            }
        }

        try:
            self.ws.send(json.dumps(presence_update))
            logger.debug(f"Presence updated (Listening to Last.fm)")
        except Exception as e:
            logger.error(f"Failed to update presence: {e}")
            self.connected = False
