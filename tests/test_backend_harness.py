import pytest

from brokoli.testing import BackendProcess


def test_backend_process_rejects_an_empty_command():
    with pytest.raises(ValueError, match="must not be empty"):
        BackendProcess([])


def test_backend_process_starts_a_real_health_server(tmp_path):
    script = tmp_path / "backend.py"
    script.write_text(
        "import os\n"
        "from http.server import BaseHTTPRequestHandler, HTTPServer\n"
        "from urllib.parse import urlparse\n"
        "class H(BaseHTTPRequestHandler):\n"
        "    def do_GET(self):\n"
        "        self.send_response(200); self.end_headers()\n"
        "    def log_message(self, *_): pass\n"
        "port = urlparse(os.environ['BROKOLI_TEST_SERVER_URL']).port\n"
        "HTTPServer(('127.0.0.1', port), H).serve_forever()\n"
    )
    with BackendProcess(
        ["python3", str(script)],
        server="http://127.0.0.1:18765",
    ):
        assert True
