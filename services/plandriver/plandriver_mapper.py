from typing import Dict, List, Optional

from config import Config

DEFAULT_PLANDRIVER_TEST_MAPPING: Dict[str, str] = {
    "1": "Логистика",
    "2": "Логистика",
    "3": "Логистика",
    "5": "Техническая часть",
    "7": "Правила компании",
    "11": "Правила компании",
    "12": "Правила компании",
    "13": "Работа с документацией",
    "14": "Экономия топлива",
}
CRITICAL_VIOLATION_CODES: frozenset[str] = frozenset({"4", "6", "8", "9", "10"})


class PlanDriverMapper:
    """Maps PlanDriver violation codes to bot question categories."""

    def __init__(self, mapping: Optional[Dict[str, object]] = None):
        configured_mapping = (
            mapping if mapping is not None else Config.PLANDRIVER_TEST_MAPPING
        )
        normalized_mapping = self._normalize_mapping(configured_mapping)
        self.mapping = normalized_mapping or DEFAULT_PLANDRIVER_TEST_MAPPING.copy()

    @staticmethod
    def _normalize_mapping(mapping: Dict[str, object]) -> Dict[str, object]:
        normalized_mapping: Dict[str, object] = {}
        for raw_code, raw_value in mapping.items():
            code = str(raw_code).strip()
            if not code:
                continue
            normalized_mapping[code] = raw_value
        return normalized_mapping

    @staticmethod
    def normalize_violation_type_code(violation_type_code: object) -> str:
        """Return the canonical string representation of a violation code."""
        return str(violation_type_code).strip()

    def is_critical_violation(self, violation_type_code: object) -> bool:
        """Return whether a violation code is critical for bot processing."""
        code = self.normalize_violation_type_code(violation_type_code)
        return code in CRITICAL_VIOLATION_CODES

    def get_question_categories(self, violation_type_code: object) -> List[str]:
        """Return question categories for a non-critical violation code."""
        code = self.normalize_violation_type_code(violation_type_code)
        configured = self.mapping.get(code)
        if isinstance(configured, str):
            return [configured]
        if isinstance(configured, list):
            return [str(item) for item in configured if str(item).strip()]
        return [code]

    def get_assignment_name(self, violation_type_code: object) -> str:
        """Return the synthetic campaign name for a PlanDriver assignment."""
        code = self.normalize_violation_type_code(violation_type_code)
        return f"PlanDriver:{code}"
