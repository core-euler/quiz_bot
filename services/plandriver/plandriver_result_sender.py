from datetime import datetime, timezone
from typing import Dict

from services.plandriver.plandriver_client import PlanDriverClient
from services.plandriver.plandriver_storage import PlanDriverStorage
import logging

logger = logging.getLogger(__name__)


class PlanDriverResultSender:
    def __init__(self, client: PlanDriverClient, storage: PlanDriverStorage):
        self.client = client
        self.storage = storage

    def send_result(
        self,
        *,
        violation_id: int,
        driver_id: int,
        attestation_id: int,
        violation_type_code: str,
        passed: bool,
        score: int,
    ) -> Dict:
        incomplete = self.storage.get_incomplete_attestation_violations(attestation_id)
        all_passed = passed and len([item for item in incomplete if item.violation_id != violation_id]) == 0
        completed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

        payload = {
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

        logger.info(
            "Sending PlanDriver result for violation_id=%s driver_id=%s passed=%s score=%s",
            violation_id,
            driver_id,
            passed,
            score,
        )
        response = self.client.send_test_result(payload)
        next_status = "completed" if passed else "sent"
        self.storage.update_violation_status(
            violation_id,
            status=next_status,
            passed=passed,
            score=score,
            completed_at=completed_at,
            last_error="",
        )
        logger.info("PlanDriver result accepted for violation_id=%s; local status=%s", violation_id, next_status)
        return response
