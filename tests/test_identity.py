from __future__ import annotations

from meshweaver.identity import (
    Identity,
    load_trusted_certificates,
    verify_signature,
    write_certificate_bundle,
)


def test_identity_signing_and_trust_bundle(tmp_path):
    first = Identity.create(tmp_path / "first", "first")
    second = Identity.create(tmp_path / "second", "second")
    bundle = tmp_path / "trust.pem"
    write_certificate_bundle([first.certificate_pem, second.certificate_pem], bundle)
    trusted = load_trusted_certificates(bundle)

    body = {"sender": first.node_id, "message": {"type": "ping"}}
    signature = first.sign(body)
    assert verify_signature(trusted[first.node_id], body, signature)
    assert not verify_signature(
        trusted[first.node_id], {**body, "message": {"type": "task_submit"}}, signature
    )
    assert second.node_id in trusted

