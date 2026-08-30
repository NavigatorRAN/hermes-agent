from __future__ import annotations

import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from plugins.platforms.a2a.signal_exact import SignalExactError, SignalExactService


def _request(body: str = "SYNTHETIC TEST ONLY", key: str = "canary-1") -> dict:
    return {
        "recipientAlias": "Veronika Warren",
        "body": body,
        "contentSHA256": hashlib.sha256(body.encode("utf-8")).hexdigest(),
        "idempotencyKey": key,
    }


def test_same_idempotency_key_replays_receipt_without_second_signal_send(
    monkeypatch, tmp_path
):
    service = SignalExactService()
    service._state_path = tmp_path / "signal-exact-receipts.json"
    monkeypatch.setattr(
        service,
        "_fetch_contacts",
        lambda: [{"name": "Veronika Warren", "number": "+61000004959"}],
        raising=False,
    )
    sends = []

    def post_message(recipient: str, body: str) -> dict:
        sends.append((recipient, body))
        return {"timestamp": "1788000000000"}

    monkeypatch.setattr(service, "_post_message", post_message, raising=False)

    try:
        first = service.send_exact(_request())
        replay = service.send_exact(_request())
    except SignalExactError as exc:
        pytest.fail(f"exact send remained unavailable: {exc}")

    assert first == replay
    assert first == {
        "recipientAlias": "Veronika Warren",
        "state": "ACCEPTED",
        "externalReference": "signal:1788000000000",
        "deliveredContentSHA256": hashlib.sha256(
            b"SYNTHETIC TEST ONLY"
        ).hexdigest(),
    }
    assert sends == [("+61000004959", "SYNTHETIC TEST ONLY")]


def test_content_hash_mismatch_is_rejected_before_contact_lookup(monkeypatch, tmp_path):
    service = SignalExactService()
    service._state_path = tmp_path / "signal-exact-receipts.json"
    looked_up = False

    def fetch_contacts():
        nonlocal looked_up
        looked_up = True
        return []

    monkeypatch.setattr(service, "_fetch_contacts", fetch_contacts, raising=False)
    request = _request()
    request["contentSHA256"] = "0" * 64

    with pytest.raises(SignalExactError, match="hash"):
        service.send_exact(request)

    assert looked_up is False


def test_reusing_idempotency_key_for_changed_body_is_rejected(monkeypatch, tmp_path):
    service = SignalExactService()
    service._state_path = tmp_path / "signal-exact-receipts.json"
    monkeypatch.setattr(
        service,
        "_fetch_contacts",
        lambda: [{"name": "Veronika Warren", "number": "+61000004959"}],
        raising=False,
    )
    monkeypatch.setattr(
        service,
        "_post_message",
        lambda recipient, body: {"timestamp": "1788000000001"},
        raising=False,
    )
    service.send_exact(_request())

    with pytest.raises(SignalExactError, match="different request"):
        service.send_exact(_request(body="CHANGED", key="canary-1"))


def test_ambiguous_contact_name_is_rejected_without_sending(monkeypatch, tmp_path):
    service = SignalExactService()
    service._state_path = tmp_path / "signal-exact-receipts.json"
    monkeypatch.setattr(
        service,
        "_fetch_contacts",
        lambda: [
            {"name": "Veronika Warren", "number": "+61000004959"},
            {"profile_name": "Veronika Warren", "number": "+61000001234"},
        ],
        raising=False,
    )
    sends = []
    monkeypatch.setattr(
        service,
        "_post_message",
        lambda recipient, body: sends.append((recipient, body)),
        raising=False,
    )

    with pytest.raises(SignalExactError, match="ambiguous"):
        service.send_exact(_request())

    assert sends == []


def test_concurrent_same_key_calls_cannot_send_twice(monkeypatch, tmp_path):
    service = SignalExactService()
    service._state_path = tmp_path / "signal-exact-receipts.json"
    contact_barrier = threading.Barrier(2)

    def fetch_contacts():
        contact_barrier.wait(timeout=2)
        return [{"name": "Veronika Warren", "number": "+61000004959"}]

    monkeypatch.setattr(service, "_fetch_contacts", fetch_contacts, raising=False)
    sends = []
    send_lock = threading.Lock()

    def post_message(recipient: str, body: str) -> dict:
        with send_lock:
            sends.append((recipient, body))
        return {"timestamp": "1788000000002"}

    monkeypatch.setattr(service, "_post_message", post_message, raising=False)

    def attempt():
        try:
            return service.send_exact(_request())
        except SignalExactError as exc:
            return str(exc)

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(lambda _: attempt(), range(2)))

    assert len(sends) == 1
    assert any(isinstance(outcome, dict) for outcome in outcomes)
    assert all(
        isinstance(outcome, dict) or "unknown" in outcome
        for outcome in outcomes
    )
