"""Expo push notification sender.

Uses Expo's push API (https://exp.host/--/api/v2/push/send) rather than
direct Firebase since the mobile app uses Expo managed workflow.
"""
import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

EXPO_PUSH_URL = "https://exp.host/--/api/v2/push/send"


def send_push(
    token: str,
    title: str,
    body: str,
    data: Optional[dict] = None,
) -> bool:
    """Send a single Expo push notification. Returns True on success."""
    if not token or not token.startswith("ExponentPushToken["):
        return False
    try:
        payload = {
            "to": token,
            "title": title,
            "body": body,
            "sound": "default",
            "data": data or {},
        }
        r = httpx.post(EXPO_PUSH_URL, json=payload, timeout=10)
        result = r.json()
        # Expo returns {"data": [{"status": "ok", ...}]}
        items = result.get("data", [{}])
        return items[0].get("status") == "ok" if items else False
    except Exception as exc:
        logger.warning("Push send failed: %s", exc)
        return False


def send_push_bulk(messages: list[dict]) -> None:
    """Fire-and-forget bulk push (up to 100 at a time, per Expo limit)."""
    for i in range(0, len(messages), 100):
        batch = messages[i : i + 100]
        try:
            httpx.post(EXPO_PUSH_URL, json=batch, timeout=15)
        except Exception as exc:
            logger.warning("Bulk push batch failed: %s", exc)
