import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from zipfile import ZipFile


class CEUploadTest(unittest.TestCase):
    def submit(self, upload_status):
        requests = []

        class API(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                requests.append((self.path, self.headers, body))
                endpoints = {
                    "/api/v2/file-upload/start": (201, b'{"data":{"id":7}}'),
                    "/api/v2/file-upload/7": (upload_status, b""),
                    "/api/v2/file-upload/7/end": (200, b""),
                }
                status, reply = endpoints[self.path]
                self.send_response(status)
                self.end_headers()
                self.wfile.write(reply)

            def log_message(self, *args):
                pass

        with HTTPServer(("127.0.0.1", 0), API) as server, tempfile.TemporaryDirectory() as directory:
            archive = Path(directory) / "test bundle.zip"
            document = Path(directory) / "users.JSON"
            document.write_bytes(b'{"meta":{"type":"users","version":6},"data":[]}')
            with ZipFile(archive, "w") as output:
                output.write(document, "users.json")

            worker = threading.Thread(target=server.serve_forever)
            worker.start()
            try:
                result = subprocess.run(
                    [sys.executable, "-m", "bloodhound_import", "--ce-url",
                     f"http://127.0.0.1:{server.server_port}", str(archive), str(document)],
                    env=dict(os.environ, BLOODHOUND_CE_TOKEN="test.jwt.signature"),
                    capture_output=True, text=True, timeout=10,
                )
            finally:
                server.shutdown()
                worker.join()
            return result, requests, [archive.read_bytes(), document.read_bytes()]

    def test_submits_original_files_as_one_job(self):
        result, requests, files = self.submit(202)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Follow job 7", result.stdout)
        self.assertEqual([path for path, _, _ in requests], [
            "/api/v2/file-upload/start", "/api/v2/file-upload/7",
            "/api/v2/file-upload/7", "/api/v2/file-upload/7/end",
        ])
        self.assertEqual([body for _, _, body in requests[1:3]], files)
        for (_, headers, _), name, media_type in zip(
            requests[1:3], ["test bundle.zip", "users.JSON"], ["application/zip", "application/json"]
        ):
            self.assertEqual(headers["Authorization"], "Bearer test.jwt.signature")
            self.assertEqual(headers["X-File-Upload-Name"], name)
            self.assertEqual(headers["Content-Type"], media_type)

    def test_does_not_finalize_after_rejection(self):
        result, requests, _ = self.submit(400)
        self.assertEqual(result.returncode, 1)
        self.assertIn("job 7, uploading 'test bundle.zip'", result.stderr)
        self.assertIn("HTTP 400", result.stderr)
        self.assertEqual(len(requests), 2)
