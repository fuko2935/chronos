import os
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

# It's a common practice to adjust the python path to import modules from the app
import sys
# We assume the tests are run from the root of the project
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.key_manager import ApiKeyManager

class TestApiKeyManager(unittest.TestCase):

    def setUp(self):
        """Clean up any singleton instance before each test."""
        if hasattr(ApiKeyManager, '_instance'):
            ApiKeyManager._instance = None

    @patch.dict(os.environ, {
        "GEMINI_API_KEY_1": "key1",
        "GEMINI_API_KEY_2": "key2",
        "GEMINI_API_KEY_3": "key3"
    })
    def test_load_keys_from_multiple_env_vars(self):
        """Tests that keys are loaded correctly from multiple environment variables."""
        manager = ApiKeyManager()
        self.assertEqual(len(manager.keys), 3)
        self.assertIn("key1", manager.keys)
        self.assertIn("key2", manager.keys)
        self.assertIn("key3", manager.keys)

    @patch.dict(os.environ, {"GEMINI_API_KEY": "single_key"})
    def test_load_single_key(self):
        """Tests fallback to a single GEMINI_API_KEY."""
        manager = ApiKeyManager()
        self.assertEqual(len(manager.keys), 1)
        self.assertEqual(manager.keys[0], "single_key")

    @patch.dict(os.environ, {})
    def test_no_keys_found(self):
        """Tests that the manager handles having no keys."""
        manager = ApiKeyManager()
        self.assertEqual(len(manager.keys), 0)
        self.assertIsNone(manager.get_key())

    @patch.dict(os.environ, {"GEMINI_API_KEY_1": "key1", "GEMINI_API_KEY_2": "key2"})
    def test_get_key_rotation(self):
        """Tests that get_key rotates through the available keys."""
        manager = ApiKeyManager()
        key1 = manager.get_key()
        key2 = manager.get_key()
        key3 = manager.get_key()
        self.assertEqual(key1, "key1")
        self.assertEqual(key2, "key2")
        self.assertEqual(key3, "key1") # Should wrap around

    @patch.dict(os.environ, {"GEMINI_API_KEY_1": "key1", "GEMINI_API_KEY_2": "key2"})
    def test_rate_limit_and_cooldown(self):
        """Tests that reporting a failure deactivates a key and it gets reactivated later."""
        # Use a very short cooldown for testing purposes
        manager = ApiKeyManager(cooldown_minutes=0.001)

        # Get the first key and report it as failed
        key1 = manager.get_key()
        self.assertEqual(key1, "key1")
        manager.report_failure(key1)

        # The next call should give us the second key
        key2 = manager.get_key()
        self.assertEqual(key2, "key2")

        # The next call should also give us the second key, as key1 is on cooldown
        key3 = manager.get_key()
        self.assertEqual(key3, "key2")

        # Now, we simulate time passing so the cooldown expires
        # We can do this by manually manipulating the 'last_failure' time
        with manager._lock:
            manager.key_states[key1]["last_failure"] = datetime.now() - timedelta(minutes=1)

        # Now the first key should be available again
        reactivated_key = manager.get_key()
        self.assertEqual(reactivated_key, "key1")

    @patch.dict(os.environ, {"GEMINI_API_KEY_1": "key1"})
    def test_all_keys_rate_limited(self):
        """Tests that get_key returns None if all keys are on cooldown."""
        manager = ApiKeyManager(cooldown_minutes=10)
        key1 = manager.get_key()
        manager.report_failure(key1)

        # Since it's the only key, the next call should return None
        self.assertIsNone(manager.get_key())

if __name__ == '__main__':
    unittest.main()
