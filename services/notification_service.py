"""Notification service for deadline reminders."""
import logging
from datetime import datetime
from typing import List, Tuple

from models import UserInfo, Campaign, UserStatus
from services.google_sheets import GoogleSheetsService

logger = logging.getLogger(__name__)


class NotificationService:
    """Service for managing campaign deadline notifications."""

    def __init__(self, google_sheets: GoogleSheetsService):
        """Initialize notification service.

        Args:
            google_sheets: Google Sheets service instance
        """
        self.google_sheets = google_sheets

    def get_users_to_notify(self) -> List[Tuple[UserInfo, Campaign, int]]:
        """Get list of users who need deadline reminders.

        Returns:
            List of tuples (user_info, campaign, days_left)
        """
        users_to_notify = []
        today = datetime.now().date()

        try:
            # Get all active campaigns
            campaigns = self.google_sheets.get_all_campaigns()

            for campaign in campaigns:
                days_left = (campaign.deadline.date() - today).days

                # Only notify 3 days before or 1 day before
                if days_left not in [3, 1]:
                    continue

                # Get users who should receive notification for this campaign
                users = self._get_eligible_users_for_campaign(campaign)

                for user in users:
                    users_to_notify.append((user, campaign, days_left))

            logger.info(
                f"Found {len(users_to_notify)} users to notify "
                f"for {len(set(c.name for _, c, _ in users_to_notify))} campaigns"
            )

        except Exception as e:
            logger.error(f"Error getting users to notify: {e}", exc_info=True)

        return users_to_notify

    def _get_eligible_users_for_campaign(
        self, campaign: Campaign
    ) -> List[UserInfo]:
        """Get users eligible for this campaign who haven't completed it.

        Args:
            campaign: Campaign to check eligibility for

        Returns:
            List of eligible UserInfo objects
        """
        eligible_users = []

        try:
            # Get all users from Google Sheets
            # Note: We need to implement a method to get all users
            # For now, we'll use a workaround by getting user info individually
            # In production, you might want to add get_all_users() to GoogleSheetsService

            # Read all users from the Пользователи sheet
            range_name = "'Пользователи'!A:E"
            result = self.google_sheets._retry_request(
                self.google_sheets.service.spreadsheets().values().get,
                spreadsheetId=self.google_sheets.sheet_id,
                range=range_name,
            )
            values = result.get("values", [])

            if len(values) < 2:
                return []

            headers = [h.lower() for h in values[0]]
            id_col = headers.index("telegram_id")
            status_col = headers.index("статус")
            motorcade_col = headers.index("автоколонна")

            for row in values[1:]:
                if len(row) <= max(id_col, status_col, motorcade_col):
                    continue

                telegram_id = str(row[id_col])
                status_str = row[status_col].strip() if status_col < len(row) else ""

                # Only notify confirmed users
                if status_str != UserStatus.CONFIRMED.value:
                    continue

                # Only notify users who have passed the initial test
                if not self.google_sheets.has_passed_initial_test(telegram_id):
                    logger.debug(f"User {telegram_id} skipped for campaign '{campaign.name}' - initial test not passed.")
                    continue

                # Check assignment
                user_motorcade = row[motorcade_col] if motorcade_col < len(row) else ""

                # Check if campaign is assigned to this user
                if campaign.assignment.upper() != "ВСЕ":
                    if user_motorcade != campaign.assignment:
                        continue

                # Check if user has completed this campaign
                results = self.google_sheets.get_user_results(telegram_id)

                # Find latest result for this campaign
                campaign_results = [
                    r for r in results if r.campaign_name == campaign.name
                ]

                if campaign_results:
                    # Sort by date to get latest
                    campaign_results.sort(key=lambda r: r.date, reverse=True)
                    latest_status = campaign_results[0].final_status

                    # Only notify if not completed or allowed to retry
                    if latest_status not in [None, "разрешена пересдача"]:
                        continue

                # Get full user info
                user_info = self.google_sheets.get_user_info(telegram_id)
                if user_info:
                    eligible_users.append(user_info)

        except Exception as e:
            logger.error(
                f"Error getting eligible users for campaign "
                f"{campaign.name}: {e}",
                exc_info=True,
            )

        return eligible_users

    def build_reminder_message(self, campaign: Campaign, days_left: int) -> str:
        """Build reminder message based on days left.

        Args:
            campaign: Campaign to remind about
            days_left: Number of days until deadline

        Returns:
            Formatted reminder message in Russian
        """
        deadline_str = campaign.deadline.strftime("%d.%m.%Y")

        if days_left == 3:
            return (
                f"⏰ Напоминание!\n\n"
                f"До окончания кампании **{campaign.name}** осталось 3 дня.\n\n"
                f"Тип: {campaign.type.value}\n"
                f"Срок: до {deadline_str}\n\n"
                f"Не забудьте пройти тест! Используйте команду /start."
            )
        elif days_left == 1:
            return (
                f"🚨 СРОЧНО!\n\n"
                f"До окончания кампании **{campaign.name}** остался 1 день!\n\n"
                f"Тип: {campaign.type.value}\n"
                f"Крайний срок: {deadline_str}\n\n"
                f"⚠️ Пройдите тест сегодня! Команда /start."
            )
        else:
            return ""

    def build_new_campaign_message(self, campaign: Campaign, user_fio: str) -> str:
        """Builds a message to announce a new campaign.

        Args:
            campaign: The new campaign.
            user_fio: The full name (FIO) of the user.

        Returns:
            Formatted message string.
        """
        deadline_str = campaign.deadline.strftime("%d.%m.%Y")
        return (
            f"✨ Здравствуйте, {user_fio}!\n\n"
            f"Для вас доступна новая учебная кампания: **{campaign.name}**\n\n"
            f"Тип: {campaign.type.value}\n"
            f"Срок прохождения: до {deadline_str}\n\n"
            f"Чтобы начать, используйте команду /start"
        )
