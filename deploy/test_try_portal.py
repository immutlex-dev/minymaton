from __future__ import annotations

import http.client
import subprocess
import tempfile
import threading
import unittest
from html.parser import HTMLParser
from http.server import ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

from try_portal import HELPER_FILES, INDEX_HTML, TryPortalHandler


class DownloadLinks(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        href = dict(attrs).get("href", "")
        if tag == "a" and href and "/helpers/" in href:
            self.links.append(href)


class HelperDownloadsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.helpers = Path(self.temp.name)
        for name in HELPER_FILES:
            (self.helpers / name).write_bytes(f"download:{name}\n".encode())
        cfg = SimpleNamespace(
            helpers_dir=self.helpers,
            mcp_public_url="https://example.test/try/mcp",
            portal_public_url="https://example.test/try",
        )
        handler = type("TestHandler", (TryPortalHandler,), {"cfg": cfg})
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.thread.join()
        self.server.server_close()
        self.temp.cleanup()

    def request(self, method: str, path: str) -> tuple[int, dict[str, str], bytes]:
        conn = http.client.HTTPConnection(*self.server.server_address)
        try:
            conn.request(method, path)
            response = conn.getresponse()
            return response.status, dict(response.getheaders()), response.read()
        finally:
            conn.close()

    def test_every_advertised_download_serves_its_file(self) -> None:
        status, _, page = self.request("GET", "/")
        self.assertEqual(status, 200)
        links = DownloadLinks()
        links.feed(page.decode())
        self.assertEqual(len(links.links), 10)
        for link in links.links:
            path = link.removeprefix("https://example.test/try")
            name = path.removeprefix("/helpers/")
            with self.subTest(name=name):
                status, headers, body = self.request("GET", path)
                self.assertEqual(status, 200)
                self.assertEqual(body, (self.helpers / name).read_bytes())
                self.assertEqual(int(headers["Content-Length"]), len(body))
                self.assertEqual(
                    headers["Content-Disposition"], f'attachment; filename="{name}"'
                )
                status, headers, body = self.request("HEAD", path)
                self.assertEqual(status, 200)
                self.assertEqual(body, b"")
                self.assertEqual(
                    int(headers["Content-Length"]), (self.helpers / name).stat().st_size
                )

    def test_missing_and_non_download_paths_are_not_served(self) -> None:
        (self.helpers / "tokens.conf").write_text("private", encoding="utf-8")
        (self.helpers / "minymaton-wait.py").unlink()
        for path in (
            "/helpers/tokens.conf",
            "/helpers/../tokens.conf",
            "/helpers/%2e%2e/tokens.conf",
            "/helpers/minymaton-wait.py",
        ):
            with self.subTest(path=path):
                status, _, body = self.request("GET", path)
                self.assertEqual(status, 404)
                self.assertNotIn(b"private", body)
                status, _, body = self.request("HEAD", path)
                self.assertEqual(status, 404)
                self.assertEqual(body, b"")

    def test_minted_handshake_uses_helpers_without_copying_tokens(self) -> None:
        script = INDEX_HTML.split("<script>", 1)[1].split("</script>", 1)[0]
        script = script.replace("__MCP_URL__", "https://example.test/try/mcp")
        script = script.replace("__PORTAL_URL__", "https://example.test/try")
        subprocess.run(
            [
                "node", "-e", """
const assert = require("node:assert/strict");
const vm = require("node:vm");
const fs = require("node:fs");
const elements = new Map();
const document = {
  getElementById(id) {
    if (!elements.has(id)) elements.set(id, {
      textContent: "", classList: {add() {}}, addEventListener() {}
    });
    return elements.get(id);
  },
  querySelectorAll() { return []; }
};
const context = vm.createContext({
  document, setInterval() {}, clearInterval() {},
  sessionStorage: {getItem() {return null;}, setItem() {}},
  fetch: async () => ({ok: true, json: async () => ({
    active_channels: 1, max_active_channels: 120
  })})
});
vm.runInContext(fs.readFileSync(0, "utf8"), context);
vm.runInContext('applyLease({channel: "ch-test", caller: "trial-test", token: "private-test-token", expires_at: 9999999999})', context);
for (const id of ["prompt-t1", "prompt-t2"]) {
  const prompt = elements.get(id).textContent;
  assert.ok(prompt.includes('stream "ch-test"'));
  assert.ok(prompt.includes('--url "https://example.test/try/mcp"'));
  assert.ok(prompt.includes("minymaton-wait.py"));
  assert.ok(prompt.includes("background-command tool"));
  assert.ok(prompt.includes("wait: false"));
  assert.ok(!prompt.includes("private-test-token"));
  assert.ok(!prompt.includes("then call receive (wait: true"));
}
""",
            ],
            input=script,
            text=True,
            check=True,
        )


if __name__ == "__main__":
    unittest.main()
