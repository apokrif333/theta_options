from __future__ import annotations

import os
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
import requests


class ThetaDataError(RuntimeError):
    pass


class ThetaNoData(ThetaDataError):
    pass


@dataclass(frozen=True)
class ThetaClient:
    base_url: str
    timeout_seconds: int = 120
    max_retries: int = 12
    retry_sleep_seconds: float = 5.0

    def get_csv(self, path: str, params: dict[str, Any]) -> str:
        url = f"{self.base_url}/{path.lstrip('/')}"
        request_params = dict(params)
        request_params.setdefault("format", "csv")

        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.get(url, params=request_params, timeout=self.timeout_seconds)
            except requests.RequestException as exc:
                last_error = exc
                time.sleep(self.retry_sleep_seconds)
                continue

            if response.status_code == 200:
                return response.text

            if response.status_code == 472:
                raise ThetaNoData(response.text)

            if response.status_code in {429, 474, 500, 502, 503, 504} and attempt < self.max_retries:
                time.sleep(self.retry_sleep_seconds)
                continue

            raise ThetaDataError(f"Theta request failed: {response.status_code} {response.text} url={response.url}")

        raise ThetaDataError(f"Theta request failed after {self.max_retries} attempts: {last_error}")

    def get_csv_file(self, path: str, params: dict[str, Any]) -> Path:
        url = f"{self.base_url}/{path.lstrip('/')}"
        request_params = dict(params)
        request_params.setdefault("format", "csv")

        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.get(url, params=request_params, timeout=self.timeout_seconds, stream=True)
            except requests.RequestException as exc:
                last_error = exc
                time.sleep(self.retry_sleep_seconds)
                continue

            with response:
                if response.status_code == 200:
                    try:
                        return _write_response_to_temp_csv(response)
                    except requests.RequestException as exc:
                        last_error = exc
                        if attempt < self.max_retries:
                            time.sleep(self.retry_sleep_seconds)
                            continue
                        raise ThetaDataError(f"Theta stream failed after {attempt} attempts: {exc}") from exc

                if response.status_code == 472:
                    raise ThetaNoData(response.text)

                if response.status_code in {429, 474, 500, 502, 503, 504} and attempt < self.max_retries:
                    time.sleep(self.retry_sleep_seconds)
                    continue

                raise ThetaDataError(f"Theta request failed: {response.status_code} {response.text} url={response.url}")

        raise ThetaDataError(f"Theta request failed after {self.max_retries} attempts: {last_error}")

    def get_csv_frame(self, path: str, params: dict[str, Any]) -> pl.DataFrame:
        csv_path = self.get_csv_file(path, params)
        try:
            if csv_path.stat().st_size == 0:
                return pl.DataFrame()
            return pl.read_csv(csv_path)
        finally:
            csv_path.unlink(missing_ok=True)


def _write_response_to_temp_csv(response: requests.Response) -> Path:
    fd, tmp_name = tempfile.mkstemp(prefix="theta_", suffix=".csv")
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        with tmp_path.open("wb") as tmp_file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    tmp_file.write(chunk)
    except Exception:
        _safe_unlink(tmp_path)
        raise
    return tmp_path


def _safe_unlink(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except PermissionError:
        # On Windows a handle can be released slightly later by the network stack.
        time.sleep(0.05)
        path.unlink(missing_ok=True)
