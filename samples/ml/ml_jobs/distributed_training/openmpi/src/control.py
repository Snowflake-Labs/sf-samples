#!/usr/bin/env python3
"""Send or receive a small completion message between ML Job entrypoints."""

from __future__ import annotations

import argparse
import socket
import time


def probe_connection(host: str, port: int, timeout: float) -> None:
    """Verify that a TCP listener is reachable without testing SSH authentication."""
    started_at = time.monotonic()
    with socket.create_connection((host, port), timeout=timeout):
        pass
    elapsed = time.monotonic() - started_at
    print(
        f"[probe] TCP connection to {host}:{port} succeeded in {elapsed:.3f}s",
        flush=True,
    )


def wait_for_status(port: int, timeout: float) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", port))
        server.listen()
        server.settimeout(timeout)
        try:
            connection, address = server.accept()
        except socket.timeout:
            print(
                f"[control] no completion received within {timeout:.0f}s; treating as failure",
                flush=True,
            )
            return 1
        with connection:
            message = connection.recv(32).decode("ascii", "replace").strip()
    try:
        status = int(message)
    except ValueError:
        print(
            f"[control] malformed completion message {message!r}; treating as failure",
            flush=True,
        )
        return 1
    print(f"[control] completion from {address[0]}: status={status}", flush=True)
    return status


def send_status(host: str, port: int, status: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            with socket.create_connection((host, port), timeout=5) as connection:
                connection.sendall(f"{status}\n".encode("ascii"))
            return
        except OSError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    wait_parser = subparsers.add_parser("wait")
    wait_parser.add_argument("--port", type=int, required=True)
    wait_parser.add_argument("--timeout", type=float, default=7200)

    send_parser = subparsers.add_parser("send")
    send_parser.add_argument("--host", required=True)
    send_parser.add_argument("--port", type=int, required=True)
    send_parser.add_argument("--status", type=int, required=True)
    send_parser.add_argument("--timeout", type=float, default=60)

    probe_parser = subparsers.add_parser("probe")
    probe_parser.add_argument("--host", required=True)
    probe_parser.add_argument("--port", type=int, required=True)
    probe_parser.add_argument("--timeout", type=float, default=5)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command == "wait":
        raise SystemExit(wait_for_status(args.port, args.timeout))
    if args.command == "send":
        send_status(args.host, args.port, args.status, args.timeout)
        return
    probe_connection(args.host, args.port, args.timeout)


if __name__ == "__main__":
    main()
