"""Submit files to BloodHound CE's native ingestion pipeline."""

import http.client
import json
import os
import re
from pathlib import Path
from urllib.parse import quote, urlsplit


def upload_files(url, token, filenames):
    """Stream the files to a single CE job and return its ID when queued."""
    if not re.fullmatch(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", token):
        raise ValueError("BLOODHOUND_CE_TOKEN must contain a JWT")
    server = urlsplit(url)
    if (
        server.scheme not in ("http", "https")
        or not server.hostname
        or server.username is not None
        or server.query
        or server.fragment
    ):
        raise ValueError("provide an HTTP(S) base URL without credentials, query, or fragment")

    content_types = {".json": "application/json", ".zip": "application/zip"}
    files = [Path(name) for name in filenames]
    if not files:
        raise ValueError("provide at least one file")
    for path in files:
        if path.suffix.lower() not in content_types:
            raise ValueError(f"expected a JSON or ZIP file: {path}")
        if not path.is_file():
            raise ValueError(f"file not found or not a regular file: {path}")

    connect = http.client.HTTPSConnection if server.scheme == "https" else http.client.HTTPConnection
    connection = connect(server.hostname, server.port, timeout=120)
    endpoint = server.path.rstrip("/") + "/api/v2/file-upload"

    def post(path, status, body=None, **headers):
        headers["Authorization"] = f"Bearer {token}"
        connection.request("POST", endpoint + path, body=body, headers=headers)
        with connection.getresponse() as response:
            if response.status != status:
                raise RuntimeError(f"POST {endpoint}{path} returned HTTP {response.status}")
            return response.read()

    try:
        try:
            job_id = json.loads(post("/start", 201))["data"]["id"]
        except (ValueError, KeyError, TypeError) as exc:
            raise RuntimeError("invalid response when starting an upload job") from exc
        if type(job_id) is not int or job_id <= 0:
            raise RuntimeError("invalid job ID from BloodHound CE")

        step = "sending files"
        try:
            for path in files:
                step = f"uploading {path.name!r}"
                with path.open("rb") as body:
                    post(f"/{job_id}", 202, body, **{
                        "Content-Type": content_types[path.suffix.lower()],
                        "Content-Length": str(os.fstat(body.fileno()).st_size),
                        "X-File-Upload-Name": quote(path.name, safe=" ._-"),
                    })
            step = "finalizing the upload"
            post(f"/{job_id}/end", 200)
        except (OSError, ValueError, RuntimeError, http.client.HTTPException) as exc:
            raise RuntimeError(f"job {job_id}, {step}: {exc}") from exc
        return job_id
    except http.client.HTTPException as exc:
        raise RuntimeError(f"HTTP connection failed: {exc}") from exc
    finally:
        connection.close()
