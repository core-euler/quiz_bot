from dataclasses import dataclass, asdict
from typing import List, Optional
from datetime import datetime
from enum import Enum


class UserStatus(Enum):
    AWAITS = "ожидает"
    CONFIRMED = "подтверждён"
    REJECTED = "отклонён"


class CampaignType(Enum):
    TRAINING = "Обучение"
    TESTING = "Тестирование"


@dataclass
class Question:
    category: str
    question_text: str
    answer1: str
    answer2: str
    answer3: str
    answer4: str
    correct_answer: int  # 1-4
    is_critical: bool  # Критический вопрос
    explanation: Optional[str]  # Пояснение к ответу
    row_index: int  # индекс строки в таблице


@dataclass
class AdminConfig:
    num_questions: int  # N
    max_errors: int  # M
    retry_hours: int  # H
    seconds_per_question: int  # S
    motorcades: Optional[List[str]] = None


@dataclass
class Session:
    fio: str
    question_ids: List[int]
    current_index: int
    remaining_score: int
    correct_count: int
    started_at: float
    last_action_at: float
    per_question_deadline: Optional[float]
    admin_config_snapshot: dict
    campaign_name: Optional[str] = None  # Название кампании
    mode: Optional[CampaignType] = None  # Режим: Обучение/Тестирование

    def to_dict(self):
        # asdict не всегда корректно работает с Enum, поэтому преобразуем вручную
        data = asdict(self)
        if self.mode:
            data['mode'] = self.mode.value
        return data

    @classmethod
    def from_dict(cls, data: dict):
        # Преобразуем строковое значение mode обратно в Enum
        if 'mode' in data and data['mode'] is not None:
            try:
                data['mode'] = CampaignType(data['mode'])
            except ValueError:
                data['mode'] = None  # или установить значение по умолчанию
        return cls(**data)


@dataclass
class UserInfo:
    telegram_id: str
    phone: str
    fio: str
    motorcade: str
    status: UserStatus
    personnel_number: Optional[str] = None


@dataclass
class Campaign:
    name: str
    deadline: datetime
    type: CampaignType
    assignment: str  # "ВСЕ" or motorcade name (e.g., "АК-1")


@dataclass
class UserResult:
    telegram_id: str
    campaign_name: str
    final_status: str
    date: datetime
    result: str  # "Пройден" или "Не пройден" (для обратной совместимости можно вычислять из final_status)
    correct_count: int  # Количество верных ответов
    notes: Optional[str] = None  # Примечания


@dataclass
class CampaignStats:
    """Statistics for a campaign."""

    campaign_name: str
    total_attempts: int
    passed_count: int
    failed_count: int
    pass_rate: float
    avg_correct_answers: float


@dataclass
class DriverMapping:
    driver_id: int
    telegram_id: str
    personnel_number: Optional[str] = None
    full_name: Optional[str] = None
    updated_at: Optional[str] = None


@dataclass
class ExternalViolation:
    violation_id: int
    driver_id: int
    attestation_id: int
    violation_type_code: str
    violation_type_name: Optional[str] = None
    comment: Optional[str] = None
    deadline: Optional[str] = None
    driver_name: Optional[str] = None
    personnel_number: Optional[str] = None
    telegram_id: Optional[str] = None
    question_categories: Optional[List[str]] = None
    status: str = "new"
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    completed_at: Optional[str] = None
    passed: Optional[bool] = None
    score: Optional[int] = None
    last_error: Optional[str] = None
