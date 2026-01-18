import json
import logging
import threading
import time
import websocket
import zlib

from wrappers import track_info

logger = logging.getLogger("discord_fm").getChild(__name__)

# Try to import DBus for sleep/wake detection
try:
    import gi
    gi.require_version('Gio', '2.0')
    from gi.repository import Gio, GLib
    DBUS_AVAILABLE = True
except Exception:
    DBUS_AVAILABLE = False
    logger.debug("DBus not available for sleep/wake detection")

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
        self.monitor_thread = None
        self.sleep_listener_thread = None
        self.last_sequence = None
        self.connected = False
        self.last_track = None
        self._stop_heartbeat = threading.Event()
        self._stop_monitor = threading.Event()
        self._reconnect_lock = threading.Lock()
        self._reconnect_delay = 10  # seconds between attempts
        self._monitor_interval = 30  # seconds between connection checks
        self._dbus_loop = None

        # Start sleep/wake listener
        self._start_sleep_listener()

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

        # Start connection monitor (only once)
        if self.monitor_thread is None or not self.monitor_thread.is_alive():
            self._stop_monitor.clear()
            self.monitor_thread = threading.Thread(target=self._connection_monitor, daemon=True)
            self.monitor_thread.start()

    def _heartbeat_loop(self):
        while not self._stop_heartbeat.is_set():
            try:
                heartbeat = {"op": 1, "d": self.last_sequence}
                self.ws.send(json.dumps(heartbeat))
                logger.debug("Sent heartbeat")
            except Exception as e:
                logger.error(f"Heartbeat failed: {e}")
                self.connected = False
                self._attempt_reconnect()
                break
            self._stop_heartbeat.wait(self.heartbeat_interval)

    def _connection_monitor(self):
        """Background thread that monitors connection and reconnects indefinitely."""
        logger.info("Connection monitor started")
        while not self._stop_monitor.is_set():
            self._stop_monitor.wait(self._monitor_interval)

            if self._stop_monitor.is_set():
                break

            if not self.connected:
                logger.info("Connection monitor: Not connected, attempting reconnect...")
                self._attempt_reconnect()

    def _attempt_reconnect(self):
        """Attempt to reconnect to Discord Gateway."""
        with self._reconnect_lock:
            if self.connected:
                return True

            logger.info(f"Attempting to reconnect...")

            try:
                # Clean up old connection
                if self.ws:
                    try:
                        self.ws.close()
                    except Exception:
                        pass
                self._stop_heartbeat.set()

                time.sleep(self._reconnect_delay)

                # Reconnect
                self.connect()

                # Resend last track if we had one
                if self.last_track:
                    old_track = self.last_track
                    self.last_track = None  # Force resend
                    self.update_status(old_track)

                logger.info("Reconnected successfully")
                return True
            except Exception as e:
                logger.error(f"Reconnection failed: {e}")
                return False

    def _start_sleep_listener(self):
        """Start listening for system sleep/wake events via DBus."""
        if not DBUS_AVAILABLE:
            logger.info("DBus not available, sleep/wake detection disabled")
            return

        def on_prepare_for_sleep(connection, sender, path, interface, signal, params):
            """Called when system is about to sleep or has just woken up."""
            going_to_sleep = params.unpack()[0]
            if going_to_sleep:
                logger.info("System going to sleep, marking as disconnected")
                self.connected = False
            else:
                logger.info("System woke up, triggering immediate reconnect")
                # Give network a moment to come back up
                time.sleep(2)
                self._attempt_reconnect()

        def run_dbus_listener():
            try:
                bus = Gio.bus_get_sync(Gio.BusType.SYSTEM, None)
                bus.signal_subscribe(
                    "org.freedesktop.login1",
                    "org.freedesktop.login1.Manager",
                    "PrepareForSleep",
                    "/org/freedesktop/login1",
                    None,
                    Gio.DBusSignalFlags.NONE,
                    on_prepare_for_sleep,
                    None
                )
                logger.info("Sleep/wake listener started")
                self._dbus_loop = GLib.MainLoop()
                self._dbus_loop.run()
            except Exception as e:
                logger.error(f"Failed to start sleep listener: {e}")

        self.sleep_listener_thread = threading.Thread(target=run_dbus_listener, daemon=True)
        self.sleep_listener_thread.start()

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
        self._stop_monitor.set()
        if self._dbus_loop:
            self._dbus_loop.quit()
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
            logger.warning("Not connected to Discord Gateway, attempting reconnect...")
            if not self._attempt_reconnect():
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
            # Try to reconnect and resend
            if self._attempt_reconnect():
                try:
                    self.ws.send(json.dumps(presence_update))
                    logger.debug("Presence updated after reconnect")
                except Exception:
                    pass
