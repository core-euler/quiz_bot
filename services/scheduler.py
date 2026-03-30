"""Scheduler service for background tasks."""
import logging
from datetime import datetime

from aiogram import Bot
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config import Config
from services.google_sheets import GoogleSheetsService
from services.notification_service import NotificationService
from services.plandriver.plandriver_client import PlanDriverClient
from services.plandriver.plandriver_mapper import PlanDriverMapper
from services.plandriver.plandriver_storage import PlanDriverStorage
from services.plandriver.plandriver_sync import PlanDriverSyncService
from services.redis_service import RedisService

logger = logging.getLogger(__name__)


class SchedulerService:
    """Service for scheduling background tasks."""

    def __init__(self, bot: Bot, google_sheets: GoogleSheetsService, redis_service: RedisService):
        self.bot = bot
        self.google_sheets = google_sheets
        self.redis_service = redis_service
        self.notification_service = NotificationService(google_sheets)
        self.scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
        self.plandriver_sync_service = None

        if Config.PLANDRIVER_ENABLED:
            self.plandriver_sync_service = PlanDriverSyncService(
                bot=bot,
                google_sheets=google_sheets,
                storage=PlanDriverStorage(),
                client=PlanDriverClient(),
                mapper=PlanDriverMapper(),
            )

        logger.info("SchedulerService initialized with timezone Europe/Moscow")

    def start(self):
        """Start the scheduler and add jobs."""
        self.scheduler.add_job(
            self.check_deadlines_job,
            "cron",
            hour=10,
            minute=0,
            id="deadline_check",
            name="Daily Deadline Check",
            replace_existing=True,
        )
        self.scheduler.add_job(
            self.check_new_campaigns_job,
            "interval",
            minutes=Config.CAMPAIGN_CHECK_INTERVAL_MINUTES,
            id="new_campaign_check",
            name=f"New Campaign Check (every {Config.CAMPAIGN_CHECK_INTERVAL_MINUTES}min)",
            replace_existing=True,
        )
        if self.plandriver_sync_service:
            self.scheduler.add_job(
                self.sync_plandriver_job,
                "interval",
                minutes=Config.PLANDRIVER_POLL_INTERVAL_MINUTES,
                id="plandriver_sync",
                name=f"PlanDriver Sync (every {Config.PLANDRIVER_POLL_INTERVAL_MINUTES}min)",
                replace_existing=True,
            )

        self.scheduler.start()
        logger.info("Scheduler started with %s jobs.", len(self.scheduler.get_jobs()))

    def shutdown(self):
        """Gracefully shutdown the scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown(wait=True)
            logger.info("Scheduler shut down successfully")

    async def sync_plandriver_job(self):
        if not self.plandriver_sync_service:
            return
        try:
            await self.plandriver_sync_service.sync_pending_tests()
        except Exception as e:
            logger.error("Error in sync_plandriver_job: %s", e, exc_info=True)

    async def check_new_campaigns_job(self):
        """Periodically checks for new campaigns and notifies users."""
        logger.info("Running new campaigns check job")
        try:
            all_campaigns = self.google_sheets.get_all_campaigns()
            today = datetime.now().date()
            active_campaigns = [c for c in all_campaigns if c.deadline.date() >= today]
            if not active_campaigns:
                logger.info("No active campaigns found.")
                return

            processed_campaign_names = await self.redis_service.get_processed_campaigns()
            new_campaigns = [c for c in active_campaigns if c.name not in processed_campaign_names]
            if not new_campaigns:
                logger.info("No new campaigns to announce.")
                return

            announced_campaign_names = []
            for campaign in new_campaigns:
                sent_count = 0
                error_count = 0
                target_users = self.google_sheets.get_target_users_for_campaign(campaign)
                if not target_users:
                    logger.info("No target users found for new campaign '%s'.", campaign.name)
                    announced_campaign_names.append(campaign.name)
                    continue

                eligible_users = []
                for user in target_users:
                    if self.google_sheets.has_passed_initial_test(user.telegram_id):
                        eligible_users.append(user)

                if not eligible_users:
                    logger.info("No eligible users (passed initial test) for campaign '%s'.", campaign.name)
                    announced_campaign_names.append(campaign.name)
                    continue

                for user in eligible_users:
                    try:
                        message = self.notification_service.build_new_campaign_message(campaign, user.fio)
                        await self.bot.send_message(int(user.telegram_id), message, parse_mode="Markdown")
                        sent_count += 1
                    except Exception as e:
                        error_count += 1
                        logger.error(
                            "Failed to send new campaign notification to %s for campaign '%s': %s",
                            user.telegram_id,
                            campaign.name,
                            e,
                        )

                logger.info(
                    "Announcement for campaign '%s' finished. Sent: %s, Errors: %s",
                    campaign.name,
                    sent_count,
                    error_count,
                )
                announced_campaign_names.append(campaign.name)

            if announced_campaign_names:
                await self.redis_service.add_processed_campaigns(*announced_campaign_names)
        except Exception as e:
            logger.error("Error in check_new_campaigns_job: %s", e, exc_info=True)

    async def check_deadlines_job(self):
        """Daily job to check campaign deadlines and send reminders."""
        logger.info("Running deadline check job")
        try:
            users_to_notify = self.notification_service.get_users_to_notify()
            if not users_to_notify:
                logger.info("No users to notify today")
                return

            sent_count = 0
            error_count = 0
            for user, campaign, days_left in users_to_notify:
                try:
                    message = self.notification_service.build_reminder_message(campaign, days_left)
                    if not message:
                        continue

                    await self.bot.send_message(int(user.telegram_id), message, parse_mode="Markdown")
                    sent_count += 1
                except Exception as e:
                    error_count += 1
                    logger.error(
                        "Failed to send reminder to %s for campaign %s: %s",
                        user.telegram_id,
                        campaign.name,
                        e,
                        exc_info=True,
                    )

            logger.info("Deadline check completed: %s sent, %s errors", sent_count, error_count)
        except Exception as e:
            logger.error("Error in check_deadlines_job: %s", e, exc_info=True)
