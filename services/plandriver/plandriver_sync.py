import asyncio
import logging
from collections import OrderedDict
from typing import Any, Callable, TypeVar

from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from services.google_sheets import GoogleSheetsService
from services.plandriver.plandriver_client import PlanDriverClient
from services.plandriver.plandriver_mapper import PlanDriverMapper
from services.plandriver.plandriver_storage import PlanDriverStorage

logger = logging.getLogger(__name__)
T = TypeVar("T")


class PlanDriverSyncService:
    """Synchronize pending PlanDriver assignments into Telegram."""

    def __init__(
        self,
        bot: Bot,
        google_sheets: GoogleSheetsService,
        storage: PlanDriverStorage,
        client: PlanDriverClient,
        mapper: PlanDriverMapper,
    ):
        self.bot = bot
        self.google_sheets = google_sheets
        self.storage = storage
        self.client = client
        self.mapper = mapper

    @staticmethod
    async def _run_blocking(
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Execute blocking Google Sheets and SQLite operations in a thread."""
        return await asyncio.to_thread(func, *args, **kwargs)

    @staticmethod
    def _unique_telegram_ids(matches: list[Any]) -> list[str]:
        """Extract unique Telegram IDs while preserving their original order."""
        unique = OrderedDict()
        for match in matches:
            telegram_id = str(match.telegram_id).strip()
            if telegram_id:
                unique[telegram_id] = None
        return list(unique.keys())

    @staticmethod
    def _filter_by_column(matches: list, column_name: str | None) -> list:
        """Фильтрует пользователей по автоколонне если column_name задан."""
        if not column_name:
            return matches
        normalized = column_name.strip().casefold()
        filtered = [
            match
            for match in matches
            if match.motorcade.strip().casefold() == normalized
        ]
        return filtered if filtered else matches

    async def _resolve_telegram_ids(
        self,
        driver_id: int,
        driver_name: str,
        personnel_number: str | None,
        column_name: str | None = None,
    ) -> list[str]:
        """Resolve Telegram users for a PlanDriver driver."""
        matches = await self._run_blocking(
            self.google_sheets.find_confirmed_users_by_fio,
            driver_name,
        )
        if len(matches) > 1:
            matches = self._filter_by_column(matches, column_name)
        telegram_ids = self._unique_telegram_ids(matches)
        if len(telegram_ids) == 1:
            telegram_id = telegram_ids[0]
            await self._run_blocking(
                self.storage.upsert_driver_mapping,
                driver_id,
                telegram_id,
                personnel_number,
                driver_name,
            )
            logger.info(
                "PlanDriver driver_id=%s mapped to telegram_id=%s by FIO+column",
                driver_id,
                telegram_id,
            )
            return telegram_ids
        if len(telegram_ids) > 1:
            logger.warning(
                "PlanDriver driver_id=%s resolved to multiple telegram_id values by fio='%s' column='%s': %s",
                driver_id,
                driver_name,
                column_name,
                telegram_ids,
            )
            return telegram_ids

        if personnel_number:
            matches = await self._run_blocking(
                self.google_sheets.find_confirmed_users_by_personnel_number,
                personnel_number,
            )
            telegram_ids = self._unique_telegram_ids(matches)
            if len(telegram_ids) == 1:
                telegram_id = telegram_ids[0]
                await self._run_blocking(
                    self.storage.upsert_driver_mapping,
                    driver_id,
                    telegram_id,
                    personnel_number,
                    driver_name,
                )
                logger.info(
                    "PlanDriver driver_id=%s mapped to telegram_id=%s by "
                    "personnel_number=%s fallback",
                    driver_id,
                    telegram_id,
                    personnel_number,
                )
                return telegram_ids
            if len(telegram_ids) > 1:
                logger.warning(
                    "PlanDriver driver_id=%s resolved to multiple telegram_id "
                    "values by personnel_number=%s fallback: %s",
                    driver_id,
                    personnel_number,
                    telegram_ids,
                )
                return telegram_ids

        existing = await self._run_blocking(
            self.storage.get_driver_mapping,
            driver_id,
        )
        if existing:
            return [existing.telegram_id]

        logger.warning(
            "PlanDriver user not found for driver_id=%s, fio='%s', column='%s'",
            driver_id,
            driver_name,
            column_name,
        )
        return []

    async def sync_pending_tests(self):
        """Pull pending PlanDriver tests and deliver non-critical assignments."""
        payload = await self.client.get_pending_tests()
        items = payload.get("data", [])
        if not items:
            logger.info("PlanDriver sync: no pending tests")
            return

        for driver in items:
            driver_id = driver["driver_id"]
            driver_name = driver.get("driver_name", "")
            personnel_number = driver.get("personnel_number")
            column_name = driver.get("column_name")
            attestation_id = driver["attestation_id"]
            deadline = driver.get("deadline")
            telegram_ids = await self._resolve_telegram_ids(
                driver_id,
                driver_name,
                personnel_number,
                column_name,
            )
            primary_telegram_id = telegram_ids[0] if telegram_ids else None

            for violation in driver.get("violations", []):
                violation_id = violation["violation_id"]
                existing = await self._run_blocking(
                    self.storage.get_violation,
                    violation_id,
                )
                known_recipient_ids = set(
                    await self._run_blocking(
                        self.storage.get_violation_recipient_ids,
                        violation_id,
                    )
                )
                violation_type_code = self.mapper.normalize_violation_type_code(
                    violation["violation_type_code"]
                )
                is_critical = bool(violation.get("is_critical")) or (
                    self.mapper.is_critical_violation(violation_type_code)
                )
                if existing and existing.status == "completed":
                    logger.info(
                        "PlanDriver violation_id=%s already completed locally, skipping",
                        violation_id,
                    )
                    continue
                if existing and existing.status in {"result_pending", "result_failed"}:
                    logger.info(
                        "PlanDriver violation_id=%s has pending local result delivery "
                        "(status=%s), skipping reassignment",
                        violation_id,
                        existing.status,
                    )
                    continue

                question_categories = None
                if not is_critical:
                    question_categories = self.mapper.get_question_categories(
                        violation_type_code
                    )

                if is_critical:
                    logger.warning(
                        "PlanDriver violation_id=%s code=%s is critical; "
                        "skipping Telegram test delivery",
                        violation_id,
                        violation_type_code,
                    )
                    if not existing:
                        await self._run_blocking(
                            self.storage.create_violation_if_missing,
                            violation_id=violation_id,
                            driver_id=driver_id,
                            attestation_id=attestation_id,
                            violation_type_code=violation_type_code,
                            violation_type_name=violation.get("violation_type_name"),
                            comment=violation.get("comment"),
                            deadline=deadline,
                            driver_name=driver_name,
                            personnel_number=personnel_number,
                            telegram_id=primary_telegram_id,
                            question_categories=None,
                            status="critical",
                            last_error="critical_violation",
                        )
                    else:
                        await self._run_blocking(
                            self.storage.update_violation_status,
                            violation_id,
                            status="critical",
                            telegram_id=primary_telegram_id,
                            last_error="critical_violation",
                        )
                    continue

                if not telegram_ids:
                    logger.warning(
                        "PlanDriver violation_id=%s skipped: telegram user not resolved for driver_id=%s",
                        violation_id,
                        driver_id,
                    )
                    if not existing:
                        await self._run_blocking(
                            self.storage.create_violation_if_missing,
                            violation_id=violation_id,
                            driver_id=driver_id,
                            attestation_id=attestation_id,
                            violation_type_code=violation_type_code,
                            violation_type_name=violation.get("violation_type_name"),
                            comment=violation.get("comment"),
                            deadline=deadline,
                            driver_name=driver_name,
                            personnel_number=personnel_number,
                            telegram_id=None,
                            question_categories=question_categories,
                            status="new",
                            last_error="driver_not_found",
                        )
                    else:
                        await self._run_blocking(
                            self.storage.update_violation_status,
                            violation_id,
                            status="new",
                            last_error="driver_not_found",
                        )
                    continue

                if existing:
                    await self._run_blocking(
                        self.storage.update_violation_status,
                        violation_id,
                        status="sent",
                        telegram_id=primary_telegram_id,
                        last_error="",
                    )
                else:
                    created = await self._run_blocking(
                        self.storage.create_violation_if_missing,
                        violation_id=violation_id,
                        driver_id=driver_id,
                        attestation_id=attestation_id,
                        violation_type_code=violation_type_code,
                        violation_type_name=violation.get("violation_type_name"),
                        comment=violation.get("comment"),
                        deadline=deadline,
                        driver_name=driver_name,
                        personnel_number=personnel_number,
                        telegram_id=primary_telegram_id,
                        question_categories=question_categories,
                        status="sent",
                    )
                    if not created:
                        continue

                await self._run_blocking(
                    self.storage.add_violation_recipients,
                    violation_id,
                    telegram_ids,
                )
                recipients_to_send = [
                    telegram_id
                    for telegram_id in telegram_ids
                    if telegram_id not in known_recipient_ids
                ]
                if (
                    existing
                    and existing.last_error
                    and existing.last_error.startswith("telegram_send_failed")
                ):
                    recipients_to_send = telegram_ids
                if not recipients_to_send:
                    logger.info(
                        "PlanDriver violation_id=%s already assigned to current "
                        "recipients, skipping duplicate",
                        violation_id,
                    )
                    continue

                text = (
                    "Вам назначен тест.\n\n"
                    f"Тип нарушения: {violation.get('violation_type_name') or violation_type_code}\n"
                    f"Комментарий: {violation.get('comment') or 'не указан'}\n"
                    f"Дедлайн: {deadline or 'не указан'}"
                )
                keyboard = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(
                        text="Начать тест",
                        callback_data=f"plandriver:start:{violation_id}"
                    )
                ]])
                send_errors: list[str] = []
                for telegram_id in recipients_to_send:
                    try:
                        await self.bot.send_message(
                            int(telegram_id),
                            text,
                            reply_markup=keyboard,
                        )
                        logger.info(
                            "PlanDriver violation_id=%s sent to telegram_id=%s",
                            violation_id,
                            telegram_id,
                        )
                    except Exception as exc:
                        logger.error(
                            "Failed to send PlanDriver assignment violation_id=%s "
                            "to telegram_id=%s: %s",
                            violation_id,
                            telegram_id,
                            exc,
                        )
                        send_errors.append(f"{telegram_id}: {exc}")

                if send_errors:
                    await self._run_blocking(
                        self.storage.update_violation_status,
                        violation_id,
                        status="sent",
                        last_error=f"telegram_send_failed: {'; '.join(send_errors)}",
                    )
                else:
                    await self._run_blocking(
                        self.storage.update_violation_status,
                        violation_id,
                        status="sent",
                        last_error="",
                    )
