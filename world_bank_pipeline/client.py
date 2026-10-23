from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable


@dataclass(frozen=True)
class ApiPage:
    request_url: str
    fetched_at: str
    body: bytes
    payload: list
    page_number: int
    total_pages: int


class WorldBankApiError(RuntimeError):
    pass


class WorldBankClient:
    def __init__(
        self,
        base_url: str,
        timeout_seconds: int = 30,
        max_retries: int = 4,
        retry_backoff_seconds: float = 1.0,
        opener: Callable = urllib.request.urlopen,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self.opener = opener
        self.sleep = sleep

    def get_pages(self, endpoint: str, params: dict[str, str]) -> list[ApiPage]:
        pages: list[ApiPage] = []
        page_number = 1
        total_pages = 1
        while page_number <= total_pages:
            query = {**params, "format": "json", "page": str(page_number)}
            url = f"{self.base_url}/{endpoint.lstrip('/')}?{urllib.parse.urlencode(query)}"
            body = self._get(url)
            fetched_at = datetime.now(timezone.utc).isoformat()
            try:
                payload = json.loads(body)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise WorldBankApiError(f"Invalid JSON returned by {url}") from error
            if not isinstance(payload, list) or len(payload) != 2:
                raise WorldBankApiError(f"Unexpected World Bank response structure at {url}")
            metadata, records = payload
            if isinstance(metadata, dict) and "message" in metadata:
                raise WorldBankApiError(f"World Bank API error at {url}: {metadata['message']}")
            if not isinstance(metadata, dict) or not isinstance(records, list):
                raise WorldBankApiError(f"Missing pagination metadata or records at {url}")
            try:
                total_pages = max(1, int(metadata.get("pages", 1)))
            except (TypeError, ValueError) as error:
                raise WorldBankApiError(f"Invalid page count returned by {url}") from error
            pages.append(ApiPage(url, fetched_at, body, records, page_number, total_pages))
            page_number += 1
        return pages

    def _get(self, url: str) -> bytes:
        retryable_statuses = {408, 429, 500, 502, 503, 504}
        for attempt in range(self.max_retries + 1):
            try:
                with self.opener(url, timeout=self.timeout_seconds) as response:
                    return response.read()
            except urllib.error.HTTPError as error:
                error.close()
                if error.code not in retryable_statuses or attempt == self.max_retries:
                    raise WorldBankApiError(f"HTTP {error.code} from {url}") from error
            except (urllib.error.URLError, TimeoutError) as error:
                if attempt == self.max_retries:
                    raise WorldBankApiError(f"Request failed for {url}: {error}") from error
            self.sleep(self.retry_backoff_seconds * (2**attempt))
        raise WorldBankApiError(f"Request retries exhausted for {url}")