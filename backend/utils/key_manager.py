import os
import threading
from datetime import datetime, timedelta

class ApiKeyManager:
    """
    Manages a pool of API keys for services like Google Gemini.

    This class loads API keys from environment variables prefixed with 'GEMINI_API_KEY_'.
    It provides a mechanism to get a working key, and if a key is reported as rate-limited (429),
    it will be temporarily disabled and the manager will provide the next available key.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(ApiKeyManager, cls).__new__(cls)
        return cls._instance

    def __init__(self, cooldown_minutes=10):
        # The __init__ will be called every time, but we use a flag to run setup only once.
        if not hasattr(self, '_initialized'):
            with self._lock:
                if not hasattr(self, '_initialized'):
                    self.keys = self._load_keys()
                    self.current_key_index = 0
                    self.key_states = {key: {"active": True, "last_failure": None} for key in self.keys}
                    self.cooldown_period = timedelta(minutes=cooldown_minutes)
                    self._initialized = True

    def _load_keys(self):
        """Loads keys from environment variables like GEMINI_API_KEY_1, GEMINI_API_KEY_2, etc."""
        keys = []
        i = 1
        while True:
            key = os.environ.get(f"GEMINI_API_KEY_{i}")
            if key:
                keys.append(key)
                i += 1
            else:
                break
        if not keys:
            # Fallback to a single key if numbered ones aren't found
            single_key = os.environ.get("GEMINI_API_KEY")
            if single_key:
                keys.append(single_key)

        if not keys:
            print("WARNING: No GEMINI_API_KEY environment variables found.")

        return keys

    def get_key(self):
        """
        Gets the next available, non-rate-limited API key.

        Cycles through the key list. If a key is on cooldown, it's skipped.
        Returns None if all keys are on cooldown.
        """
        with self._lock:
            if not self.keys:
                return None

            start_index = self.current_key_index
            while True:
                key_to_check = self.keys[self.current_key_index]
                state = self.key_states[key_to_check]

                # Check if the key is on cooldown
                if not state["active"]:
                    if datetime.now() - state["last_failure"] > self.cooldown_period:
                        # Cooldown has passed, reactivate the key
                        state["active"] = True
                        state["last_failure"] = None
                        print(f"Key ending in ...{key_to_check[-4:]} has been reactivated.")
                    else:
                        # Still on cooldown, move to the next key
                        self._rotate_key()
                        if self.current_key_index == start_index:
                            # We've cycled through all keys and all are on cooldown
                            print("ERROR: All API keys are currently rate-limited.")
                            return None
                        continue

                # If we are here, the key is active
                key_to_return = key_to_check
                self._rotate_key() # Rotate for next call to distribute load
                return key_to_return

    def report_failure(self, key):
        """
        Reports that a key has failed, likely due to a rate limit (429).
        The key is put on a cooldown.
        """
        with self._lock:
            if key in self.key_states:
                self.key_states[key]["active"] = False
                self.key_states[key]["last_failure"] = datetime.now()
                print(f"Key ending in ...{key[-4:]} reported as rate-limited. Placing on cooldown.")

    def _rotate_key(self):
        """Moves the index to the next key in the list."""
        self.current_key_index = (self.current_key_index + 1) % len(self.keys)

    def get_key_states(self):
        """Returns a serializable dictionary of the current state of all keys."""
        with self._lock:
            # Create a copy to avoid issues with ongoing updates
            return {f"...{key[-4:]}": state for key, state in self.key_states.items()}

# Singleton instance
key_manager = ApiKeyManager()
