#!/usr/bin/env python3
"""Isolated test-only Maven, OCI and Cloud endpoints; never used by publication clients."""
import base64
import hashlib
import http.server
import json
from pathlib import Path

root = Path("/fixture")
basic = "Basic " + base64.b64encode(b"proof:fixture-password").decode()
manifest = (root / "manifest.json").read_bytes()
manifest_digest = "sha256:" + hashlib.sha256(manifest).hexdigest()
jar = (root / "proof.jar").read_bytes()


class Fixture(http.server.BaseHTTPRequestHandler):
    def log_message(self, *arguments):
        pass

    def do_GET(self):
        if self.path == "/ready":
            self.send_response(200)
            self.end_headers()
            return
        if self.headers.get("Authorization") != basic:
            self.send_response(401)
            self.send_header("WWW-Authenticate", 'Basic realm="fixture"')
            self.end_headers()
            return
        if self.path == "/maven/example/proof/1.0.0/proof-1.0.0.jar":
            payload = jar
        elif self.path == "/maven/example/proof/1.0.0/proof-1.0.0.jar.sha1":
            payload = hashlib.sha1(jar).hexdigest().encode()
        elif self.path == "/v2/example/proof/manifests/" + manifest_digest:
            payload = manifest
        else:
            self.send_response(404)
            self.end_headers()
            return
        with (root / "requests.jsonl").open("a") as log:
            log.write(json.dumps(self.path) + "\n")
        self.send_response(200)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self):
        if self.headers.get("Authorization") != "Bearer fixture-cloud-token":
            self.send_response(401)
            self.end_headers()
            return
        if self.path != "/api/v1/organizations/example/applications/proof/environments/staging/deployments":
            self.send_response(404)
            self.end_headers()
            return
        body = self.rfile.read(int(self.headers["Content-Length"]))
        observation = dict(body=base64.b64encode(body).decode(), key=self.headers.get("Idempotency-Key"),
                           mediaType=self.headers.get("Content-Type"), mode=self.headers.get("X-TPF-Deployment-Mode"))
        with (root / "submitted.jsonl").open("a") as log:
            log.write(json.dumps(observation) + "\n")
        payload = b'{"deploymentId":"container-proof-1","status":"REGISTERED"}'
        self.send_response(201)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


http.server.HTTPServer(("0.0.0.0", 8080), Fixture).serve_forever()
