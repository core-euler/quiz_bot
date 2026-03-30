import logging
from collections import OrderedDict

from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from services.google_sheets import GoogleSheetsService
from services.plandriver.plandriver_client import PlanDriverClient
from services.plandriver.plandriver_mapper import PlanDriverMapper
from services.plandriver.plandriver_storage import PlanDriverStorage

logger = logging.getLogger(__name__)


class PlanDriverSyncService:
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
    def _unique_telegram_ids(matches) -> list[str]:
        unique = OrderedDict()
        for match in matches:
            telegram_id = str(match.telegram_id).strip()
            if telegram_id:
                unique[telegram_id] = None
        return list(unique.keys())

    def _resolve_telegram_ids(self, driver_id: int, driver_name: str, personnel_number: str | None) -> list[str]:
        existing = self.storage.get_driver_mapping(driver_id)
        if personnel_number:
            matches = self.google_sheets.find_confirmed_users_by_personnel_number(personnel_number)
            telegram_ids = self._unique_telegram_ids(matches)
            if len(telegram_ids) == 1:
                telegram_id = telegram_ids[0]
                self.storage.upsert_driver_mapping(driver_id, telegram_id, personnel_number, driver_name)
                logger.info(
                    "PlanDriver driver_id=%s mapped to telegram_id=%s by personnel_number=%s",
                    driver_id,
                    telegram_id,
                    personnel_number,
                )
                return telegram_ids
            if len(telegram_ids) > 1:
                logger.warning(
                    "PlanDriver driver_id=%s resolved to multiple telegram_id values by personnel_number=%s: %s",
                    driver_id,
                    personnel_number,
                    telegram_ids,
                )
                return telegram_ids

        matches = self.google_sheets.find_confirmed_users_by_fio(driver_name)
        telegram_ids = self._unique_telegram_ids(matches)
        if len(telegram_ids) == 1:
            telegram_id = telegram_ids[0]
            self.storage.upsert_driver_mapping(driver_id, telegram_id, personnel_number, driver_name)
            logger.info(
                "PlanDriver driver_id=%s mapped to telegram_id=%s by exact FIO",
                driver_id,
                telegram_id,
            )
            return telegram_ids
        if len(telegram_ids) > 1:
            logger.warning(
                "PlanDriver driver_id=%s resolved to multiple telegram_id values by fio='%s': %s",
                driver_id,
                driver_name,
                telegram_ids,
            )
            return telegram_ids

        if existing:
            return [existing.telegram_id]

        logger.warning(
            "PlanDriver user not found for driver_id=%s, fio='%s'",
            driver_id,
            driver_name,
        )
        return []

    async def sync_pending_tests(self):
        payload = self.client.get_pending_tests()
        items = payload.get("data", [])
        if not items:
            logger.info("PlanDriver sync: no pending tests")
            return

        for driver in items:
            driver_id = driver["driver_id"]
            driver_name = driver.get("driver_name", "")
            personnel_number = driver.get("personnel_number")
            attestation_id = driver["attestation_id"]
            deadline = driver.get("deadline")
            telegram_ids = self._resolve_telegram_ids(driver_id, driver_name, personnel_number)
            primary_telegram_id = telegram_ids[0] if telegram_ids else None

            for violation in driver.get("violations", []):
                violation_id = violation["violation_id"]
                existing = self.storage.get_violation(violation_id)
                if existing and existing.status == "completed":
                    logger.info("PlanDriver violation_id=%s already completed locally, skipping", violation_id)
                    continue

                categories = self.mapper.get_question_categories(violation["violation_type_code"])
                if not telegram_ids:
                    logger.warning(
                        "PlanDriver violation_id=%s skipped: telegram user not resolved for driver_id=%s",
                        violation_id,
                        driver_id,
                    )
                    if not existing:
                        self.storage.create_violation_if_missing(
                            violation_id=violation_id,
                            driver_id=driver_id,
                            attestation_id=attestation_id,
                            violation_type_code=violation["violation_type_code"],
                            violation_type_name=violation.get("violation_type_name"),
                            comment=violation.get("comment"),
                            deadline=deadline,
                            driver_name=driver_name,
                            personnel_number=personnel_number,
                            telegram_id=None,
                            question_categories=categories,
                            status="new",
                            last_error="driver_not_found",
                        )
                    else:
                        self.storage.update_violation_status(
                            violation_id,
                            status="new",
                            last_error="driver_not_found",
                        )
                    continue

                if existing and existing.telegram_id and not (
                    existing.last_error and existing.last_error.startswith("telegram_send_failed")
                ):
                    logger.info("PlanDriver violation_id=%s already assigned to telegram_id=%s, skipping duplicate", violation_id, existing.telegram_id)
                    continue

                if existing:
                    self.storage.update_violation_status(
                        violation_id,
                        status="sent",
                        telegram_id=primary_telegram_id,
                        last_error="",
                    )
                else:
                    created = self.storage.create_violation_if_missing(
                        violation_id=violation_id,
                        driver_id=driver_id,
                        attestation_id=attestation_id,
                        violation_type_code=violation["violation_type_code"],
                        violation_type_name=violation.get("violation_type_name"),
                        comment=violation.get("comment"),
                        deadline=deadline,
                        driver_name=driver_name,
                        personnel_number=personnel_number,
                        telegram_id=primary_telegram_id,
                        question_categories=categories,
                        status="sent",
                    )
                    if not created:
                        continue

                text = (
                    "Вам назначен тест.\n\n"
                    f"Тип нарушения: {violation.get('violation_type_name') or violation['violation_type_code']}\n"
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
                for telegram_id in telegram_ids:
                    try:
                        await self.bot.send_message(int(telegram_id), text, reply_markup=keyboard)
                        logger.info("PlanDriver violation_id=%s sent to telegram_id=%s", violation_id, telegram_id)
                    except Exception as exc:
                        logger.error(
                            "Failed to send PlanDriver assignment violation_id=%s to telegram_id=%s: %s",
                            violation_id,
                            telegram_id,
                            exc,
                        )
                        send_errors.append(f"{telegram_id}: {exc}")

                if send_errors:
                    self.storage.update_violation_status(
                        violation_id,
                        status="sent",
                        last_error=f"telegram_send_failed: {'; '.join(send_errors)}",
                    )
