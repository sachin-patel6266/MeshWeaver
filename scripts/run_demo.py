"""Start ten isolated local nodes and submit a CPU-bound example task."""

from __future__ import annotations

import subprocess
import sys
import tempfile
import time
from pathlib import Path

from meshweaver.identity import Identity, write_certificate_bundle


def main() -> int:
    processes: list[subprocess.Popen[bytes]] = []
    with tempfile.TemporaryDirectory(prefix="meshweaver-demo-") as raw_directory:
        directory = Path(raw_directory)
        identities = [
            Identity.create(directory / f"node-{index:02d}", f"meshweaver-demo-{index:02d}")
            for index in range(10)
        ]
        client = Identity.create(directory / "client", "meshweaver-demo-client")
        bundle = directory / "trusted-peers.pem"
        write_certificate_bundle(
            [identity.certificate_pem for identity in [*identities, client]], bundle
        )
        for index in range(10):
            command = [
                sys.executable,
                "-m",
                "meshweaver",
                "node",
                "--identity-dir",
                str(directory / f"node-{index:02d}"),
                "--trusted-certs",
                str(bundle),
                "--host",
                "127.0.0.1",
                "--port",
                str(8765 + index),
                "--udp-port",
                "8766",
                "--no-dashboard",
            ]
            if index:
                command.extend(["--bootstrap", "127.0.0.1:8765"])
            processes.append(
                subprocess.Popen(
                    command,
                    cwd=Path(__file__).resolve().parents[1],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            )
        try:
            time.sleep(7)
            submit = [
                sys.executable,
                "-m",
                "meshweaver",
                "submit",
                "--identity-dir",
                str(directory / "client"),
                "--trusted-certs",
                str(bundle),
                "--bootstrap",
                "127.0.0.1:8765",
                "--function",
                "meshweaver.examples:count_primes",
                "--args",
                "[50000]",
                "--timeout",
                "60",
            ]
            result = subprocess.run(
                submit,
                cwd=Path(__file__).resolve().parents[1],
                check=False,
                timeout=75,
            )
            if result.returncode == 0:
                print("Ten-node demonstration completed successfully.")
            return result.returncode
        finally:
            for process in processes:
                process.terminate()
            for process in processes:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


if __name__ == "__main__":
    raise SystemExit(main())

