#!/usr/bin/env python3
# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
"""
Daily Kite Connect login using Kite's standard browser flow.

Mirrors openalgo's pattern:

1. Binds a one-shot HTTP listener at the URL registered against the Kite Connect
   app (e.g. ``http://127.0.0.1:5000/zerodha/callback`` for our shared key).
2. Opens ``https://kite.trade/connect/login?api_key=…`` in the system browser.
   Zerodha handles their own login + 2FA on the Kite UI — we never see the
   password or TOTP.
3. Catches the post-login redirect, lifts ``request_token`` from the query string.
4. Exchanges the single-use ``request_token`` for a daily ``access_token`` via
   ``POST api.kite.trade/session/token`` (same SHA-256 checksum as
   ``nautilus_zerodha::auth::exchange_request_token``).
5. Writes ``ZERODHA_ACCESS_TOKEN=…`` back into the .env atomically.

Standard library only — no extra dependencies.

Usage::

    python examples/live/zerodha/login.py
    python examples/live/zerodha/login.py --env path/to/.env
    python examples/live/zerodha/login.py --print           # echo token, don't rewrite .env
    python examples/live/zerodha/login.py --no-browser      # just print the URL to open manually
"""

from __future__ import annotations

import argparse
import hashlib
import http.server
import json
import os
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from pathlib import Path

DEFAULT_ENV_PATH = Path(__file__).resolve().parent / ".env"

KITE_LOGIN_URL = "https://kite.trade/connect/login"
SESSION_TOKEN_URL = "https://api.kite.trade/session/token"
KITE_VERSION = "3"

CALLBACK_TIMEOUT_SECS = 300  # request_token TTL is ~5 minutes per Kite docs

REQUIRED_FIELDS = (
    "ZERODHA_API_KEY",
    "ZERODHA_API_SECRET",
    "ZERODHA_REDIRECT_URL",
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Zerodha Kite Connect daily login — browser flow + local callback listener.",
    )
    parser.add_argument(
        "--env",
        type=Path,
        default=DEFAULT_ENV_PATH,
        help="Path to the .env file holding the long-lived credentials.",
    )
    parser.add_argument(
        "--print",
        dest="print_only",
        action="store_true",
        help="Print the access_token to stdout instead of rewriting the .env file.",
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Don't try to open the browser — just print the login URL to open manually.",
    )
    args = parser.parse_args()

    env = load_env(args.env)
    missing = [k for k in REQUIRED_FIELDS if not env.get(k)]
    if missing:
        print(f"[login] missing fields in {args.env}: {', '.join(missing)}", file=sys.stderr)
        return 2

    api_key = env["ZERODHA_API_KEY"]
    api_secret = env["ZERODHA_API_SECRET"]
    redirect_url = env["ZERODHA_REDIRECT_URL"]

    parsed = urllib.parse.urlparse(redirect_url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname or not parsed.port:
        print(
            f"[login] ZERODHA_REDIRECT_URL must be a full URL with host + port (got {redirect_url!r})",
            file=sys.stderr,
        )
        return 2

    host = parsed.hostname
    port = parsed.port
    expected_path = parsed.path or "/"

    login_url = f"{KITE_LOGIN_URL}?api_key={urllib.parse.quote(api_key)}"

    print(f"[login] starting callback listener on {host}:{port}{expected_path}")
    try:
        request_token = capture_request_token(host, port, expected_path, login_url, args.no_browser)
    except OSError as e:
        if e.errno == 48 or "Address already in use" in str(e):
            print(
                f"[login] {host}:{port} is already bound — is openalgo (or another listener) "
                f"running? Stop it and rerun.",
                file=sys.stderr,
            )
            return 1
        print(f"[login] listener error: {e}", file=sys.stderr)
        return 1
    except LoginError as e:
        print(f"[login] failed: {e}", file=sys.stderr)
        return 1

    print(f"[login] captured request_token (preview: {request_token[:4]}…{request_token[-4:]})")

    try:
        access_token = exchange_request_token(api_key, api_secret, request_token)
    except LoginError as e:
        print(f"[login] session/token exchange failed: {e}", file=sys.stderr)
        return 1
    except urllib.error.URLError as e:
        print(f"[login] session/token transport error: {e}", file=sys.stderr)
        return 1

    if args.print_only:
        print(access_token)
    else:
        env["ZERODHA_ACCESS_TOKEN"] = access_token
        write_env_atomic(args.env, env)
        print(
            f"[login] ZERODHA_ACCESS_TOKEN written to {args.env} "
            f"(token preview: {access_token[:4]}…{access_token[-4:]})"
        )
    return 0


class LoginError(RuntimeError):
    pass


def capture_request_token(
    host: str,
    port: int,
    expected_path: str,
    login_url: str,
    no_browser: bool,
) -> str:
    """Bind a one-shot HTTP server, open the Kite login URL, return `request_token`."""
    captured: dict[str, str | None] = {"token": None, "error": None}
    completed = threading.Event()

    class _Handler(http.server.BaseHTTPRequestHandler):
        # Quieter than the default; suppress per-request stderr logging.
        def log_message(self, format: str, *args: object) -> None:  # noqa: A002
            return

        def do_GET(self) -> None:  # noqa: N802
            parsed = urllib.parse.urlparse(self.path)
            if parsed.path != expected_path:
                self.send_response(404)
                self.end_headers()
                return

            qs = urllib.parse.parse_qs(parsed.query)
            status = (qs.get("status") or [""])[0]
            token = (qs.get("request_token") or [""])[0]
            error_reason = (qs.get("error_message") or qs.get("error") or [""])[0]

            if status == "success" and token:
                captured["token"] = token
                _respond_html(
                    self,
                    200,
                    "Login captured",
                    "You can close this tab and return to the terminal.",
                )
            else:
                captured["error"] = error_reason or f"status={status!r} token={token!r}"
                _respond_html(
                    self,
                    400,
                    "Login failed",
                    captured["error"] or "no request_token in callback",
                )
            completed.set()

    server = http.server.HTTPServer((host, port), _Handler)
    thread = threading.Thread(target=server.serve_forever, name="zerodha-login-cb", daemon=True)
    thread.start()
    try:
        if no_browser:
            print(f"[login] open this URL in your browser to begin:\n  {login_url}")
        else:
            print(f"[login] opening browser → {login_url}")
            if not webbrowser.open(login_url):
                print(f"[login] webbrowser.open returned False; open manually:\n  {login_url}")

        if not completed.wait(timeout=CALLBACK_TIMEOUT_SECS):
            raise LoginError(
                f"timed out after {CALLBACK_TIMEOUT_SECS}s waiting for Kite callback"
            )
    finally:
        server.shutdown()
        server.server_close()

    if captured["error"]:
        raise LoginError(f"callback reported failure: {captured['error']}")
    token = captured["token"]
    if not token:
        raise LoginError("callback completed without a request_token")
    return token


def _respond_html(handler: http.server.BaseHTTPRequestHandler, code: int, title: str, message: str) -> None:
    body = (
        f"<!doctype html><html><head><meta charset=utf-8><title>{title}</title>"
        f"<style>body{{font-family:system-ui;padding:2rem;max-width:32rem;margin:auto;}}"
        f"h1{{margin-bottom:1rem}}p{{line-height:1.4}}</style></head>"
        f"<body><h1>{title}</h1><p>{message}</p></body></html>"
    ).encode()
    handler.send_response(code)
    handler.send_header("Content-Type", "text/html; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def exchange_request_token(api_key: str, api_secret: str, request_token: str) -> str:
    """Mirror of `nautilus_zerodha::auth::exchange_request_token`."""
    checksum = hashlib.sha256(
        f"{api_key}{request_token}{api_secret}".encode()
    ).hexdigest()
    payload = urllib.parse.urlencode({
        "api_key": api_key,
        "request_token": request_token,
        "checksum": checksum,
    }).encode()
    req = urllib.request.Request(
        SESSION_TOKEN_URL,
        data=payload,
        headers={"X-Kite-Version": KITE_VERSION, "User-Agent": "nautilus-zerodha-login/1.0"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body_bytes = e.read() if e.fp else b""
        raise LoginError(f"session/token HTTP {e.code}: {body_bytes[:400]!r}") from e
    except json.JSONDecodeError as e:
        raise LoginError(f"session/token returned non-JSON body: {e}") from e

    token = (body.get("data") or {}).get("access_token")
    if not isinstance(token, str) or not token:
        raise LoginError(f"session/token missing access_token in {body!r}")
    return token


def load_env(path: Path) -> dict[str, str]:
    if not path.exists():
        raise LoginError(f".env not found at {path}")
    env: dict[str, str] = {}
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        env[key.strip()] = value.strip().strip("'").strip('"')
    return env


def write_env_atomic(path: Path, env: dict[str, str]) -> None:
    """Rewrite .env preserving comments, replacing the value for any key in `env`.

    SIGINT mid-write must not leave a truncated .env, so we write to .env.new and rename.
    """
    seen: set[str] = set()
    new_lines: list[str] = []
    for raw in path.read_text().splitlines():
        stripped = raw.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            new_lines.append(raw)
            continue
        key = stripped.partition("=")[0].strip()
        if key in env:
            new_lines.append(f"{key}={env[key]}")
            seen.add(key)
        else:
            new_lines.append(raw)
    for key, value in env.items():
        if key not in seen:
            new_lines.append(f"{key}={value}")

    tmp = path.with_suffix(path.suffix + ".new")
    tmp.write_text("\n".join(new_lines) + "\n")
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)


if __name__ == "__main__":
    sys.exit(main())
