"""Structured exact-content Signal delivery for trusted A2A peers."""

from __future__ import annotations

import hashlib
import json
import os
import threading
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import quote

from hermes_constants import get_hermes_home
from utils import atomic_json_write


_STATE_LOCK = threading.Lock()


class SignalExactError(Exception):
    """A structured Signal send could not be completed safely."""


class SignalExactService:
    NOTES_TO_SELF_ALIAS = "Notes to Self"

    def __init__(self) -> None:
        self._state_path = get_hermes_home() / "signal-exact-receipts.json"

    def is_available(self) -> bool:
        try:
            http_url, account = self._configuration()
            with urllib.request.urlopen(f"{http_url}/v1/about", timeout=2) as response:
                about = json.loads(response.read().decode("utf-8"))
            with urllib.request.urlopen(f"{http_url}/v1/accounts", timeout=2) as response:
                accounts = json.loads(response.read().decode("utf-8"))
            return isinstance(about, dict) and isinstance(accounts, list) and account in accounts
        except Exception:
            return False

    def send_exact(self, request: dict) -> dict:
        recipient_alias = self._required(request, "recipientAlias")
        body = self._required(request, "body")
        content_hash = self._required(request, "contentSHA256").lower()
        idempotency_key = self._required(request, "idempotencyKey")
        actual_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()
        if content_hash != actual_hash:
            raise SignalExactError("Signal content hash does not match the exact body")

        fingerprint = hashlib.sha256(
            json.dumps(
                {
                    "recipientAlias": recipient_alias,
                    "body": body,
                    "contentSHA256": content_hash,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()

        with _STATE_LOCK:
            state = self._load_state()
            existing = state.get(idempotency_key)
            if existing:
                if existing.get("fingerprint") != fingerprint:
                    raise SignalExactError(
                        "The idempotency key is already bound to a different request"
                    )
                if existing.get("receipt"):
                    return dict(existing["receipt"])
                if existing.get("status") == "PENDING":
                    raise SignalExactError(
                        "The earlier delivery outcome is unknown; reconcile it before retrying"
                    )

        recipient = self._resolve_contact(recipient_alias)

        with _STATE_LOCK:
            state = self._load_state()
            existing = state.get(idempotency_key)
            if existing:
                if existing.get("fingerprint") != fingerprint:
                    raise SignalExactError(
                        "The idempotency key is already bound to a different request"
                    )
                if existing.get("receipt"):
                    return dict(existing["receipt"])
                if existing.get("status") == "PENDING":
                    raise SignalExactError(
                        "The earlier delivery outcome is unknown; reconcile it before retrying"
                    )
            state[idempotency_key] = {
                "fingerprint": fingerprint,
                "status": "PENDING",
            }
            self._save_state(state)

        try:
            response = self._post_message(recipient, body)
            timestamp = str(response.get("timestamp") or "").strip()
            if not timestamp:
                raise SignalExactError("Signal bridge did not return a message timestamp")
            receipt = {
                "recipientAlias": recipient_alias,
                "state": "ACCEPTED",
                "externalReference": f"signal:{timestamp}",
                "deliveredContentSHA256": content_hash,
            }
        except SignalExactError:
            raise
        except Exception as exc:
            raise SignalExactError(
                "Signal delivery outcome is unknown; reconcile it before retrying"
            ) from exc

        with _STATE_LOCK:
            state = self._load_state()
            state[idempotency_key] = {
                "fingerprint": fingerprint,
                "status": "ACCEPTED",
                "receipt": receipt,
            }
            self._save_state(state)
        return receipt

    def list_recipients(self) -> list[dict]:
        """Return unambiguous Signal contacts plus the local self destination."""
        _, account = self._configuration()
        aliases: dict[str, tuple[str, set[str]]] = {}
        for contact in self._fetch_contacts():
            if not isinstance(contact, dict):
                continue
            alias = self._preferred_alias(contact)
            number = str(contact.get("number") or "").strip()
            if not alias or not number or number == account:
                continue
            key = alias.casefold()
            if key not in aliases:
                aliases[key] = (alias, set())
            aliases[key][1].add(number)

        contacts = [
            {"alias": alias, "displayName": alias, "kind": "CONTACT"}
            for alias, numbers in aliases.values()
            if len(numbers) == 1
        ]
        contacts.sort(key=lambda item: item["displayName"].casefold())
        return [
            {
                "alias": self.NOTES_TO_SELF_ALIAS,
                "displayName": self.NOTES_TO_SELF_ALIAS,
                "kind": "SELF",
            },
            *contacts,
        ]

    @staticmethod
    def _required(request: dict, field: str) -> str:
        value = request.get(field)
        if not isinstance(value, str) or not value.strip():
            raise SignalExactError(f"{field} is required")
        return value.strip() if field != "body" else value

    def _resolve_contact(self, alias: str) -> str:
        if alias.strip().casefold() == self.NOTES_TO_SELF_ALIAS.casefold():
            _, account = self._configuration()
            return account
        matches: set[str] = set()
        normalized = alias.strip().casefold()
        for contact in self._fetch_contacts():
            if not isinstance(contact, dict):
                continue
            names = {
                str(contact.get(key) or "").strip().casefold()
                for key in ("name", "profile_name", "nickname", "username")
            }
            combined = " ".join(
                part
                for part in (
                    str(contact.get("given_name") or "").strip(),
                    str(contact.get("family_name") or "").strip(),
                )
                if part
            ).casefold()
            if combined:
                names.add(combined)
            number = str(contact.get("number") or "").strip()
            if normalized in names and number:
                matches.add(number)
        if not matches:
            raise SignalExactError(f"Signal contact {alias!r} was not found")
        if len(matches) != 1:
            raise SignalExactError(f"Signal contact {alias!r} is ambiguous")
        return next(iter(matches))

    @staticmethod
    def _preferred_alias(contact: dict) -> str:
        for key in ("name", "profile_name", "nickname", "username"):
            value = str(contact.get(key) or "").strip()
            if value:
                return value
        return " ".join(
            part
            for part in (
                str(contact.get("given_name") or "").strip(),
                str(contact.get("family_name") or "").strip(),
            )
            if part
        )

    def _fetch_contacts(self) -> list[dict]:
        http_url, account = self._configuration()
        url = (
            f"{http_url}/v1/contacts/{quote(account, safe='')}"
            "?all_recipients=true"
        )
        try:
            with urllib.request.urlopen(url, timeout=60) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except Exception as exc:
            raise SignalExactError("Signal contact lookup failed") from exc
        if not isinstance(payload, list):
            raise SignalExactError("Signal contact lookup returned an invalid response")
        return payload

    def _post_message(self, recipient: str, body: str) -> dict:
        http_url, account = self._configuration()
        payload = json.dumps(
            {"number": account, "recipients": [recipient], "message": body}
        ).encode("utf-8")
        request = urllib.request.Request(
            f"{http_url}/v2/send",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                result = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raise SignalExactError(f"Signal bridge rejected the send ({exc.code})") from exc
        except Exception as exc:
            raise SignalExactError(
                "Signal delivery outcome is unknown; reconcile it before retrying"
            ) from exc
        if not isinstance(result, dict):
            raise SignalExactError("Signal bridge returned an invalid send response")
        return result

    @staticmethod
    def _configuration() -> tuple[str, str]:
        http_url = os.getenv("SIGNAL_HTTP_URL", "").strip().rstrip("/")
        account = os.getenv("SIGNAL_ACCOUNT", "").strip()
        if not http_url or not account:
            raise SignalExactError("Exact Signal delivery is unavailable")
        return http_url, account

    def _load_state(self) -> dict:
        path = Path(self._state_path)
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SignalExactError("Signal idempotency state is unreadable") from exc
        return payload if isinstance(payload, dict) else {}

    def _save_state(self, state: dict) -> None:
        atomic_json_write(self._state_path, state, mode=0o600)
