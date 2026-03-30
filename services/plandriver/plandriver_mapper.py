from typing import List, Optional

from config import Config


class PlanDriverMapper:
    def __init__(self, mapping: Optional[dict] = None):
        self.mapping = mapping if mapping is not None else Config.PLANDRIVER_TEST_MAPPING

    def get_question_categories(self, violation_type_code: str) -> List[str]:
        configured = self.mapping.get(violation_type_code)
        if isinstance(configured, str):
            return [configured]
        if isinstance(configured, list):
            return [str(item) for item in configured if str(item).strip()]
        return [violation_type_code]

    def get_assignment_name(self, violation_type_code: str) -> str:
        return f"PlanDriver:{violation_type_code}"
