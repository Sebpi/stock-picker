from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[2]
BACKEND = ROOT / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))


class VersionMetadataTests(unittest.TestCase):
    def setUp(self):
        import main

        self.main = main
        self.client = TestClient(main.app, headers={"host": "localhost"})
        self.package_version = json.loads((ROOT / "package.json").read_text(encoding="utf-8"))["version"]

    def test_api_version_uses_package_version(self):
        response = self.client.get("/api/version")

        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual(body["app"], "stock-picker")
        self.assertEqual(body["version"], self.package_version)
        self.assertIn("git_sha", body)
        self.assertIn("build_time", body)

    def test_frontend_assets_are_version_stamped(self):
        index = self.client.get("/")
        script = self.client.get("/static/react-app.js")

        self.assertEqual(index.status_code, 200, index.text)
        self.assertEqual(script.status_code, 200, script.text)
        self.assertIn(f"StockLens v{self.package_version}", index.text)
        self.assertIn(f"/static/react-app.js?v={self.package_version}", index.text)
        self.assertIn(f'const APP_VERSION = "{self.package_version}"', script.text)
        self.assertNotIn("__APP_VERSION__", index.text)
        self.assertNotIn("__APP_VERSION__", script.text)

