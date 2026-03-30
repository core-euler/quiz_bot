import json
import os
import sqlite3
from datetime import datetime, timezone
from typing import List, Optional

from config import Config
from models import DriverMapping, ExternalViolation


class PlanDriverStorage:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or Config.PLANDRIVER_DB_PATH
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _init_db(self):
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS driver_mapping (
                    driver_id INTEGER PRIMARY KEY,
                    telegram_id TEXT NOT NULL,
                    personnel_number TEXT,
                    full_name TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS external_violations (
                    violation_id INTEGER PRIMARY KEY,
                    driver_id INTEGER NOT NULL,
                    attestation_id INTEGER NOT NULL,
                    violation_type_code TEXT NOT NULL,
                    violation_type_name TEXT,
                    comment TEXT,
                    deadline TEXT,
                    driver_name TEXT,
                    personnel_number TEXT,
                    telegram_id TEXT,
                    question_categories TEXT,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT,
                    passed INTEGER,
                    score INTEGER,
                    last_error TEXT
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_external_violations_telegram_status ON external_violations (telegram_id, status)"
            )

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _row_to_mapping(row: sqlite3.Row) -> DriverMapping:
        return DriverMapping(
            driver_id=row["driver_id"],
            telegram_id=row["telegram_id"],
            personnel_number=row["personnel_number"],
            full_name=row["full_name"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _row_to_violation(row: sqlite3.Row) -> ExternalViolation:
        categories = row["question_categories"]
        return ExternalViolation(
            violation_id=row["violation_id"],
            driver_id=row["driver_id"],
            attestation_id=row["attestation_id"],
            violation_type_code=row["violation_type_code"],
            violation_type_name=row["violation_type_name"],
            comment=row["comment"],
            deadline=row["deadline"],
            driver_name=row["driver_name"],
            personnel_number=row["personnel_number"],
            telegram_id=row["telegram_id"],
            question_categories=json.loads(categories) if categories else None,
            status=row["status"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            completed_at=row["completed_at"],
            passed=None if row["passed"] is None else bool(row["passed"]),
            score=row["score"],
            last_error=row["last_error"],
        )

    def get_driver_mapping(self, driver_id: int) -> Optional[DriverMapping]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM driver_mapping WHERE driver_id = ?",
                (driver_id,),
            ).fetchone()
        return self._row_to_mapping(row) if row else None

    def upsert_driver_mapping(
        self,
        driver_id: int,
        telegram_id: str,
        personnel_number: Optional[str],
        full_name: Optional[str],
    ) -> DriverMapping:
        updated_at = self._now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO driver_mapping (driver_id, telegram_id, personnel_number, full_name, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(driver_id) DO UPDATE SET
                    telegram_id = excluded.telegram_id,
                    personnel_number = excluded.personnel_number,
                    full_name = excluded.full_name,
                    updated_at = excluded.updated_at
                """,
                (driver_id, telegram_id, personnel_number, full_name, updated_at),
            )
        return DriverMapping(driver_id, telegram_id, personnel_number, full_name, updated_at)

    def get_violation(self, violation_id: int) -> Optional[ExternalViolation]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM external_violations WHERE violation_id = ?",
                (violation_id,),
            ).fetchone()
        return self._row_to_violation(row) if row else None

    def create_violation_if_missing(
        self,
        *,
        violation_id: int,
        driver_id: int,
        attestation_id: int,
        violation_type_code: str,
        violation_type_name: Optional[str],
        comment: Optional[str],
        deadline: Optional[str],
        driver_name: Optional[str],
        personnel_number: Optional[str],
        telegram_id: Optional[str],
        question_categories: Optional[List[str]],
        status: str,
        last_error: Optional[str] = None,
    ) -> bool:
        now = self._now()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO external_violations (
                    violation_id, driver_id, attestation_id, violation_type_code, violation_type_name,
                    comment, deadline, driver_name, personnel_number, telegram_id, question_categories,
                    status, created_at, updated_at, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    violation_id, driver_id, attestation_id, violation_type_code, violation_type_name,
                    comment, deadline, driver_name, personnel_number, telegram_id,
                    json.dumps(question_categories or []), status, now, now, last_error,
                ),
            )
            return cursor.rowcount > 0

    def update_violation_status(
        self,
        violation_id: int,
        *,
        status: str,
        telegram_id: Optional[str] = None,
        last_error: Optional[str] = None,
        passed: Optional[bool] = None,
        score: Optional[int] = None,
        completed_at: Optional[str] = None,
    ):
        fields = ["status = ?", "updated_at = ?"]
        values: List[object] = [status, self._now()]

        if telegram_id is not None:
            fields.append("telegram_id = ?")
            values.append(telegram_id)
        if last_error is not None:
            fields.append("last_error = ?")
            values.append(last_error)
        if passed is not None:
            fields.append("passed = ?")
            values.append(1 if passed else 0)
        if score is not None:
            fields.append("score = ?")
            values.append(score)
        if completed_at is not None:
            fields.append("completed_at = ?")
            values.append(completed_at)

        values.append(violation_id)
        with self._connect() as conn:
            conn.execute(
                f"UPDATE external_violations SET {', '.join(fields)} WHERE violation_id = ?",
                values,
            )

    def get_pending_assignments_for_user(self, telegram_id: str) -> List[ExternalViolation]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM external_violations
                WHERE telegram_id = ? AND status = 'sent'
                ORDER BY created_at ASC
                """,
                (telegram_id,),
            ).fetchall()
        return [self._row_to_violation(row) for row in rows]

    def get_incomplete_attestation_violations(self, attestation_id: int) -> List[ExternalViolation]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM external_violations
                WHERE attestation_id = ? AND status != 'completed'
                """,
                (attestation_id,),
            ).fetchall()
        return [self._row_to_violation(row) for row in rows]
