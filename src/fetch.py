import json
import logging
import os
import time
from collections import deque
from threading import Lock
from urllib.error import HTTPError
from urllib.request import HTTPRedirectHandler, Request, build_opener

from pymongo import MongoClient


_LOGGER = logging.getLogger(__name__)
_FETCH_LOCK = Lock()

class _RateLimiter:
    """Share this limiter across all requests, including repeated imports."""

    def __init__(self):
        self._requests = deque(maxlen=30)
        self._lock = Lock()

    def wait(self):
        with self._lock:
            while self._requests:
                now = time.monotonic()
                delay = self._requests[-1] + 2.01 - now
                if len(self._requests) == 30:
                    delay = max(delay, self._requests[0] + 60.01 - now)
                if delay <= 0:
                    break
                time.sleep(delay)
            self._requests.append(time.monotonic())


class _NoRedirects(HTTPRedirectHandler):
    """Prevent automatic extra requests from bypassing the rate limiter."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_LIMITER = _RateLimiter()


def _get_json(url, headers):
    request = Request(url, headers={"Accept": "application/json", **headers})
    opener = build_opener(_NoRedirects())
    _LIMITER.wait()
    with opener.open(request, timeout=30) as response:
        return json.load(response)


def fetch_items() -> int:
    """Fetch and upsert item details, returning the number saved.

    All API requests in this process share the rate limit. Fetches are serialized
    to prevent overlapping runs. A 404 is skipped because an item can lose its
    usual price between requests. Other HTTP or database errors propagate; items
    already saved remain stored and calling again updates them without duplicates.
    Run only one importer process per API key to preserve the shared rate limit.
    """
    with _FETCH_LOCK:
        api_key = os.environ.get("COZY_MARKETS_API_KEY", "").strip()
        mongo_url = os.environ.get("MONGO_CONNECTION_URL", "").strip()
        if not api_key or not mongo_url:
            raise ValueError(
                "COZY_MARKETS_API_KEY and MONGO_CONNECTION_URL must be configured"
            )

        usual_url = os.environ.get(
            "COZY_MARKET_USUAL_URL",
            "https://cozy.markets/trade-api/dev/v1/usual-prices",
        )
        items_url = os.environ.get(
            "COZY_MARKET_ITEMS_URL", "https://cozy.markets/trade-api/dev/v1/items"
        ).rstrip("/")
        headers = {"Authorization": f"Bearer {api_key}"}
        saved = 0

        with MongoClient(mongo_url, serverSelectionTimeoutMS=30_000) as client:
            # Check the database before spending the API's limited request quota.
            client.admin.command("ping")
            collection = client["maplestreet"]["items"]
            usual_prices = _get_json(usual_url, headers)
            items = usual_prices["items"]
            if not isinstance(items, list):
                raise ValueError("usual-prices response must contain an items array")

            for summary in items:
                item_id = summary["item_id"]
                if type(item_id) is not int or item_id <= 0:
                    raise ValueError("Cozy Markets item_id must be a positive integer")
                try:
                    details = _get_json(f"{items_url}/{item_id}", headers)
                except HTTPError as exc:
                    if exc.code != 404:
                        raise
                    exc.close()
                    _LOGGER.warning("Item %s no longer has a usual price; skipping", item_id)
                    continue

                item = details["item"]
                if not isinstance(item, dict) or item.get("item_id") != item_id:
                    raise ValueError(f"Item response does not match item_id {item_id}")
                # Preserve attribution and snapshot dates alongside all item fields.
                document = {key: value for key, value in details.items() if key != "item"}
                document.update(item)
                document["_id"] = item_id
                collection.replace_one({"_id": item_id}, document, upsert=True)
                saved += 1

        return saved
