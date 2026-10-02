#!/usr/bin/env python3
"""Isolated test-only Maven, OCI and Cloud endpoints; never used by publication clients."""
import base64
import hashlib
import http.server
import json
import os
import ssl
from urllib.parse import parse_qs
from pathlib import Path

root = Path(os.environ.get("TPF_FIXTURE_ROOT", "/fixture"))
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

    def respond(self, status, value):
        payload = json.dumps(value).encode()
        self.send_response(status)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"]))
        if self.path.startswith("/oauth2/"):
            fields = parse_qs(body.decode())
            if self.path.endswith("device_authorization"):
                type(self).polls = 0
                host = (root / "verification-host").read_text() if (root / "verification-host").exists() else "127.0.0.1"
                self.respond(200, dict(device_code="fixture-device-secret", user_code="TPF-PROOF",
                    verification_uri=("http://" if host == "127.0.0.1" else "https://") + host + "/verify", expires_in=60, interval=1))
            elif fields.get("grant_type") == ["urn:ietf:params:oauth:grant-type:device_code"]:
                type(self).polls += 1
                if type(self).polls <= 2:
                    self.respond(400, dict(error="authorization_pending" if type(self).polls == 1 else "slow_down"))
                else:
                    self.respond(200, dict(token_type="Bearer", access_token="fixture-cloud-token", refresh_token="fixture-refresh-secret", expires_in=3600))
            elif fields.get("grant_type") == ["refresh_token"]:
                state = (root / "refresh-state").read_text() if (root / "refresh-state").exists() else "ok"
                if state != "ok": self.respond(400, dict(error=state))
                elif fields.get("refresh_token") != ["fixture-refresh-secret"]: self.respond(400, dict(error="invalid_grant"))
                else: self.respond(200, dict(token_type="Bearer", access_token="fixture-cloud-token", refresh_token="fixture-rotated-secret", expires_in=3600))
            elif fields.get("client_secret") == ["fixture-ci-secret"]:
                self.respond(200, dict(token_type="Bearer", access_token="fixture-cloud-token", expires_in=3600))
            else: self.respond(400, dict(error="access_denied"))
            return
        if self.headers.get("Authorization") != "Bearer fixture-cloud-token":
            self.respond(401, dict(error="unauthorized"))
            return
        if (root / "forbidden").exists():
            self.respond(403, dict(error="forbidden"))
            return
        if self.path != "/api/v1/organizations/example/applications/proof/environments/staging/deployments":
            self.send_response(404)
            self.end_headers()
            return
        observation = dict(body=base64.b64encode(body).decode(), key=self.headers.get("Idempotency-Key"),
                           mediaType=self.headers.get("Content-Type"), mode=self.headers.get("X-TPF-Deployment-Mode"))
        with (root / "submitted.jsonl").open("a") as log:
            log.write(json.dumps(observation) + "\n")
        payload = b'{"deploymentId":"container-proof-1","status":"REGISTERED"}'
        self.send_response(201)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


server = http.server.HTTPServer(("127.0.0.1" if os.environ.get("TPF_FIXTURE_ROOT") else "0.0.0.0", int(os.environ.get("TPF_FIXTURE_PORT", "8080"))), Fixture)
if os.environ.get("TPF_FIXTURE_CERT"):
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(os.environ["TPF_FIXTURE_CERT"], os.environ["TPF_FIXTURE_KEY"])
    server.socket = context.wrap_socket(server.socket, server_side=True)
server.serve_forever()
