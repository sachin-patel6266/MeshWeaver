"""Peer identities, certificate creation, TLS contexts, and message signatures."""

from __future__ import annotations

import base64
import hashlib
import ipaddress
import json
import ssl
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.exceptions import InvalidSignature
from cryptography.x509.oid import NameOID


def canonical_json(value: Any) -> bytes:
    """Return deterministic UTF-8 JSON bytes for signing."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")


class Identity:
    """An Ed25519 identity whose self-signed certificate is its TLS identity."""

    def __init__(
        self,
        private_key: Ed25519PrivateKey,
        certificate: x509.Certificate,
        key_path: Path,
        cert_path: Path,
    ):
        self.private_key = private_key
        self.certificate = certificate
        self.key_path = key_path
        self.cert_path = cert_path
        self.certificate_pem = certificate.public_bytes(serialization.Encoding.PEM)
        self.fingerprint = hashlib.sha256(
            certificate.public_bytes(serialization.Encoding.DER)
        ).hexdigest()
        self.node_id = self.fingerprint

    @classmethod
    def create(cls, directory: str | Path, common_name: str) -> "Identity":
        """Create and persist a new Ed25519 key and self-signed TLS certificate."""
        path = Path(directory)
        path.mkdir(parents=True, exist_ok=True)
        key_path = path / "identity.key"
        cert_path = path / "identity.crt"
        if key_path.exists() or cert_path.exists():
            raise FileExistsError(
                f"Identity already exists in {path}; refusing to overwrite it."
            )

        private_key = Ed25519PrivateKey.generate()
        subject = issuer = x509.Name(
            [x509.NameAttribute(NameOID.COMMON_NAME, common_name)]
        )
        certificate = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(private_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.now(timezone.utc) - timedelta(minutes=1))
            .not_valid_after(datetime.now(timezone.utc) + timedelta(days=3650))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(
                x509.SubjectAlternativeName(
                    [x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
                ),
                critical=False,
            )
            .sign(private_key, algorithm=None)
        )
        key_path.write_bytes(
            private_key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )
        cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
        return cls(private_key, certificate, key_path, cert_path)

    @classmethod
    def load(cls, directory: str | Path) -> "Identity":
        """Load an existing identity from its directory."""
        path = Path(directory)
        private_key = serialization.load_pem_private_key(
            (path / "identity.key").read_bytes(), password=None
        )
        certificate = x509.load_pem_x509_certificate((path / "identity.crt").read_bytes())
        if not isinstance(private_key, Ed25519PrivateKey):
            raise TypeError("MeshWeaver identities must use Ed25519 private keys.")
        if not isinstance(certificate.public_key(), Ed25519PublicKey):
            raise TypeError("MeshWeaver certificates must contain Ed25519 public keys.")
        if private_key.public_key().public_bytes(
            serialization.Encoding.Raw, serialization.PublicFormat.Raw
        ) != certificate.public_key().public_bytes(
            serialization.Encoding.Raw, serialization.PublicFormat.Raw
        ):
            raise ValueError("The private key does not match the identity certificate.")
        return cls(private_key, certificate, path / "identity.key", path / "identity.crt")

    def sign(self, value: Any) -> str:
        """Sign canonical JSON and return a base64-encoded Ed25519 signature."""
        return base64.b64encode(self.private_key.sign(canonical_json(value))).decode("ascii")


def load_trusted_certificates(path: str | Path) -> dict[str, x509.Certificate]:
    """Load PEM certificates explicitly trusted for TLS and signed RPC."""
    contents = Path(path).read_bytes()
    certificates: dict[str, x509.Certificate] = {}
    marker = b"-----END CERTIFICATE-----"
    for section in contents.split(marker):
        if b"-----BEGIN CERTIFICATE-----" not in section:
            continue
        pem = section + marker + b"\n"
        cert = x509.load_pem_x509_certificate(pem)
        fingerprint = hashlib.sha256(
            cert.public_bytes(serialization.Encoding.DER)
        ).hexdigest()
        certificates[fingerprint] = cert
    if not certificates:
        raise ValueError(f"No PEM certificates found in trust bundle {path}.")
    return certificates


def write_certificate_bundle(
    certificates: list[bytes], destination: str | Path, *, overwrite: bool = False
) -> None:
    """Write PEM certificates to a trust bundle without replacing files by default."""
    path = Path(destination)
    if path.exists() and not overwrite:
        raise FileExistsError(f"Trust bundle already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"".join(certificates))


def tls_contexts(
    identity: Identity, trusted_certificates_path: str | Path
) -> tuple[ssl.SSLContext, ssl.SSLContext]:
    """Build mutual-TLS contexts rooted only in the supplied trust bundle."""
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.minimum_version = ssl.TLSVersion.TLSv1_2
    server.verify_mode = ssl.CERT_REQUIRED
    server.load_cert_chain(
        certfile=str(identity.cert_path),
        keyfile=str(identity.key_path),
    )
    server.load_verify_locations(cafile=str(trusted_certificates_path))

    client = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    client.minimum_version = ssl.TLSVersion.TLSv1_2
    client.check_hostname = False
    client.load_verify_locations(cafile=str(trusted_certificates_path))
    client.load_cert_chain(
        certfile=str(identity.cert_path),
        keyfile=str(identity.key_path),
    )
    return server, client


def verify_signature(
    certificate: x509.Certificate, value: Any, signature: str
) -> bool:
    """Verify a signed RPC body against its trusted certificate."""
    public_key = certificate.public_key()
    if not isinstance(public_key, Ed25519PublicKey):
        return False
    try:
        public_key.verify(base64.b64decode(signature, validate=True), canonical_json(value))
    except (ValueError, TypeError, InvalidSignature):
        return False
    return True
