import json
import logging
import os
from typing import Any, Dict, Optional
from urllib import error, parse, request

from config import Config

logger = logging.getLogger(__name__)


class PlanDriverClient:
    def __init__(self, base_url: Optional[str] = None, token: Optional[str] = None):
        self.base_url = (base_url or Config.PLANDRIVER_BASE_URL).rstrip("/")
        self.token = token or Config.PLANDRIVER_TOKEN
        self.test_mode = Config.PLANDRIVER_TEST_MODE
        self.pending_tests_json_path = Config.PLANDRIVER_PENDING_TESTS_JSON

    def _load_pending_tests_from_file(self) -> Dict[str, Any]:
        path = os.path.abspath(self.pending_tests_json_path)
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        logger.info("PlanDriver test mode: loaded pending tests from local file %s", path)
        return payload

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _request(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
        query: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{parse.urlencode(query)}"

        data = None
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")

        req = request.Request(url=url, data=data, headers=self._headers(), method=method)
        try:
            with request.urlopen(req, timeout=20) as response:
                body = response.read().decode("utf-8")
                return json.loads(body) if body else {}
        except error.HTTPError as exc:
            response_body = exc.read().decode("utf-8", errors="ignore")
            logger.error("PlanDriver HTTP error %s for %s %s: %s", exc.code, method, path, response_body)
            raise
        except error.URLError as exc:
            logger.error("PlanDriver connection error for %s %s: %s", method, path, exc)
            raise

    def get_pending_tests(self) -> Dict[str, Any]:
        if self.test_mode:
            return self._load_pending_tests_from_file()
        return self._request("GET", "/api/bot/pending-tests")

    def send_test_result(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if self.test_mode:
            logger.info(
                "PlanDriver test mode: skipping POST /api/bot/test-result; payload=%s",
                json.dumps(payload, ensure_ascii=False),
            )
            return {
                "status": "test_mode_skipped",
                "message": "POST /api/bot/test-result skipped in PLANDRIVER_TEST_MODE",
                "payload": payload,
            }
        return self._request("POST", "/api/bot/test-result", payload=payload)
