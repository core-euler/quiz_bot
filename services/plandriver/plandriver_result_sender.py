import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Dict, TypeVar

from services.plandriver.plandriver_client import PlanDriverClient
from services.plandriver.plandriver_storage import PlanDriverStorage

logger = logging.getLogger(__name__)
T = TypeVar("T")


class PlanDriverResultSender:
    """Send PlanDriver test results and retry undelivered payloads."""

    def __init__(self, client: PlanDriverClient, storage: PlanDriverStorage):
        self.client = client
        self.storage = storage

    @staticmethod
    async def _run_blocking(
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Execute blocking storage operations outside the event loop."""
        return await asyncio.to_thread(func, *args, **kwargs)

    def _build_payload(
        self,
        *,
        violation_id: int,
        driver_id: int,
        attestation_id: int,
        violation_type_code: str,
        passed: bool,
        score: int,
        completed_at: str,
    ) -> Dict:
        """Build a PlanDriver result payload for a single violation."""
        incomplete = self.storage.get_incomplete_attestation_violations(attestation_id)
        all_passed = passed and not any(
            item.violation_id != violation_id for item in incomplete
        )
        return {
            "driver_id": driver_id,
            "attestation_id": attestation_id,
            "results": [
                {
                    "violation_id": violation_id,
                    "violation_type_code": violation_type_code,
                    "passed": passed,
                    "score": score,
                    "completed_at": completed_at,
                }
            ],
            "all_passed": all_passed,
        }

    async def send_result(
        self,
        *,
        violation_id: int,
        driver_id: int,
        attestation_id: int,
        violation_type_code: str,
        passed: bool,
        score: int,
    ) -> Dict:
        """Send a result immediately and persist retry metadata on failure."""
        current = await self._run_blocking(self.storage.get_violation, violation_id)
        if current and current.status == "completed":
            logger.info(
                "PlanDriver result already completed for violation_id=%s, "
                "skipping duplicate POST",
                violation_id,
            )
            return {"delivery_status": "already_completed"}

        completed_at = (
            datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )
        payload = await self._run_blocking(
            self._build_payload,
            violation_id=violation_id,
            driver_id=driver_id,
            attestation_id=attestation_id,
            violation_type_code=violation_type_code,
            passed=passed,
            score=score,
            completed_at=completed_at,
        )

        logger.info(
            "Sending PlanDriver result for violation_id=%s driver_id=%s passed=%s score=%s",
            violation_id,
            driver_id,
            passed,
            score,
        )
        await self._run_blocking(
            self.storage.update_violation_status,
            violation_id,
            status="result_pending",
            passed=passed,
            score=score,
            completed_at=completed_at,
            last_error="",
        )
        try:
            response = await self.client.send_test_result(payload)
        except Exception as exc:
            await self._run_blocking(
                self.storage.update_violation_status,
                violation_id,
                status="result_failed",
                passed=passed,
                score=score,
                completed_at=completed_at,
                last_error=str(exc),
            )
            logger.error(
                "PlanDriver result queued for retry violation_id=%s: %s",
                violation_id,
                exc,
            )
            return {"delivery_status": "queued_for_retry", "error": str(exc)}

        next_status = "completed" if passed else "sent"
        await self._run_blocking(
            self.storage.update_violation_status,
            violation_id,
            status=next_status,
            passed=passed,
            score=score,
            completed_at=completed_at,
            last_error="",
        )
        logger.info(
            "PlanDriver result accepted for violation_id=%s; local status=%s",
            violation_id,
            next_status,
        )
        return {"delivery_status": "sent", "response": response}

    async def retry_pending_results(self) -> None:
        """Retry results that were stored locally after a failed POST."""
        retry_candidates = await self._run_blocking(
            self.storage.get_result_retry_candidates
        )
        for violation in retry_candidates:
            if violation.passed is None or violation.score is None or not violation.completed_at:
                logger.warning(
                    "PlanDriver retry skipped for violation_id=%s due to "
                    "incomplete local payload",
                    violation.violation_id,
                )
                continue

            payload = await self._run_blocking(
                self._build_payload,
                violation_id=violation.violation_id,
                driver_id=violation.driver_id,
                attestation_id=violation.attestation_id,
                violation_type_code=violation.violation_type_code,
                passed=violation.passed,
                score=violation.score,
                completed_at=violation.completed_at,
            )
            try:
                await self.client.send_test_result(payload)
            except Exception as exc:
                await self._run_blocking(
                    self.storage.update_violation_status,
                    violation.violation_id,
                    status="result_failed",
                    passed=violation.passed,
                    score=violation.score,
                    completed_at=violation.completed_at,
                    last_error=str(exc),
                )
                logger.error(
                    "PlanDriver retry failed for violation_id=%s: %s",
                    violation.violation_id,
                    exc,
                )
                continue

            next_status = "completed" if violation.passed else "sent"
            await self._run_blocking(
                self.storage.update_violation_status,
                violation.violation_id,
                status=next_status,
                passed=violation.passed,
                score=violation.score,
                completed_at=violation.completed_at,
                last_error="",
            )
            logger.info(
                "PlanDriver retry delivered violation_id=%s; local status=%s",
                violation.violation_id,
                next_status,
            )
