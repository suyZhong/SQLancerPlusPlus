import importlib.util
import os
import pathlib
import tempfile
import unittest
from unittest import mock


CHAT_PATH = pathlib.Path(__file__).parents[1] / "src" / "chat.py"
MODULE_SPEC = importlib.util.spec_from_file_location("sqlancer_chat", CHAT_PATH)
CHAT = importlib.util.module_from_spec(MODULE_SPEC)
MODULE_SPEC.loader.exec_module(CHAT)


class TestChatConfiguration(unittest.TestCase):

    def write_url_config(self, contents):
        config = tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False)
        self.addCleanup(pathlib.Path(config.name).unlink, missing_ok=True)
        config.write(contents)
        config.close()
        return config.name

    def testUserAgentIsConfigured(self):
        self.assertTrue(os.environ["USER_AGENT"])

    def testConfiguredOverviewDoesNotNeedGoogleCredentials(self):
        config = self.write_url_config("DuckDB:\n  function:\n    overview:\n      - https://duckdb.org/docs/\n")

        with mock.patch.object(CHAT, "configure_google_search") as configure_search:
            urls = CHAT.get_urls_from_yaml("DuckDB", "function", config, "overview")

        self.assertEqual(["https://duckdb.org/docs/"], urls)
        configure_search.assert_not_called()

    def testMissingTopicFallsBackToOverviewWithoutGoogle(self):
        config = self.write_url_config("DuckDB:\n  datatype:\n    overview:\n      - https://duckdb.org/docs/\n")

        with mock.patch.object(CHAT, "configure_google_search", return_value=False):
            urls = CHAT.get_urls_from_yaml("DuckDB", "datatype", config, "INTEGER")

        self.assertEqual(["https://duckdb.org/docs/"], urls)


if __name__ == "__main__":
    unittest.main()
