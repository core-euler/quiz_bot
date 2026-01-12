"""Scheduler service for background tasks."""
import logging
from datetime import datetime

from aiogram import Bot
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config import Config
from services.google_sheets import GoogleSheetsService
from services.notification_service import NotificationService
from services.redis_service import RedisService

logger = logging.getLogger(__name__)


class SchedulerService:
    """Service for scheduling background tasks."""

    def __init__(self, bot: Bot, google_sheets: GoogleSheetsService, redis_service: RedisService):
        """Initialize scheduler service.

        Args:
            bot: Bot instance for sending messages
            google_sheets: Google Sheets service instance
            redis_service: Redis service instance
        """
        self.bot = bot
        self.google_sheets = google_sheets
        self.redis_service = redis_service
        self.notification_service = NotificationService(google_sheets)
        self.scheduler = AsyncIOScheduler(timezone="Europe/Moscow")

        logger.info("SchedulerService initialized with timezone Europe/Moscow")

    def start(self):
        """Start the scheduler and add jobs."""
        # Add deadline check job - runs daily at 10:00 AM Moscow time
        self.scheduler.add_job(
            self.check_deadlines_job,
            "cron",
            hour=10,
            minute=0,
            id="deadline_check",
            name="Daily Deadline Check",
            replace_existing=True,
        )

        # Add new campaign check job
        self.scheduler.add_job(
            self.check_new_campaigns_job,
            "interval",
            minutes=Config.CAMPAIGN_CHECK_INTERVAL_MINUTES,
            id="new_campaign_check",
            name=f"New Campaign Check (every {Config.CAMPAIGN_CHECK_INTERVAL_MINUTES}min)",
            replace_existing=True,
        )

        self.scheduler.start()
        logger.info(f"Scheduler started with {len(self.scheduler.get_jobs())} jobs.")

    def shutdown(self):
        """Gracefully shutdown the scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown(wait=True)
            logger.info("Scheduler shut down successfully")

    async def check_new_campaigns_job(self):
        """Periodically checks for new campaigns and notifies users."""
        logger.info("Running new campaigns check job")
        try:
            # 1. Get all campaigns and filter for active ones
            all_campaigns = self.google_sheets.get_all_campaigns()
            today = datetime.now().date()
            active_campaigns = [
                c for c in all_campaigns if c.deadline.date() >= today
            ]
            if not active_campaigns:
                logger.info("No active campaigns found.")
                return

            # 2. Get already processed campaigns from Redis
            processed_campaign_names = await self.redis_service.get_processed_campaigns()

            # 3. Find campaigns that are new
            new_campaigns = [
                c for c in active_campaigns if c.name not in processed_campaign_names
            ]

            if not new_campaigns:
                logger.info("No new campaigns to announce.")
                return

            logger.info(f"Found {len(new_campaigns)} new campaigns to announce: {[c.name for c in new_campaigns]}")
            
            # 4. Process each new campaign
            announced_campaign_names = []
            for campaign in new_campaigns:
                sent_count = 0
                error_count = 0
                target_users = self.google_sheets.get_target_users_for_campaign(campaign)
                if not target_users:
                    logger.info(f"No target users found for new campaign '{campaign.name}'.")
                    # Add to announced list to prevent re-checking empty campaigns
                    announced_campaign_names.append(campaign.name)
                    continue

                for user in target_users:
                    try:
                        message = self.notification_service.build_new_campaign_message(campaign, user.fio)
                        await self.bot.send_message(
                            int(user.telegram_id), message, parse_mode="Markdown"
                        )
                        sent_count += 1
                    except Exception as e:
                        error_count += 1
                        logger.error(
                            f"Failed to send new campaign notification to {user.telegram_id} (FIO: {user.fio}) for campaign '{campaign.name}': {e}"
                        )
                
                logger.info(f"Announcement for campaign '{campaign.name}' finished. Sent: {sent_count}, Errors: {error_count}")
                announced_campaign_names.append(campaign.name)

            # 5. Update Redis with the list of announced campaigns
            if announced_campaign_names:
                await self.redis_service.add_processed_campaigns(*announced_campaign_names)
                logger.info(f"Updated Redis with {len(announced_campaign_names)} announced campaigns.")

        except Exception as e:
            logger.error(f"Error in check_new_campaigns_job: {e}", exc_info=True)

    async def check_deadlines_job(self):
        """Daily job to check campaign deadlines and send reminders.

        This job:
        1. Gets all active campaigns with deadlines in 3 or 1 day
        2. Finds users who haven't completed them
        3. Sends reminder messages via bot
        """
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
                    message = self.notification_service.build_reminder_message(
                        campaign, days_left
                    )

                    if not message:
                        logger.warning(
                            f"Empty message for campaign {campaign.name} "
                            f"with {days_left} days left"
                        )
                        continue

                    await self.bot.send_message(
                        int(user.telegram_id), message, parse_mode="Markdown"
                    )

                    sent_count += 1
                    logger.info(
                        f"Sent reminder to {user.telegram_id} "
                        f"for campaign {campaign.name} "
                        f"({days_left} days left)"
                    )

                except Exception as e:
                    error_count += 1
                    logger.error(
                        f"Failed to send reminder to {user.telegram_id} "
                        f"for campaign {campaign.name}: {e}",
                        exc_info=True,
                    )

            logger.info(
                f"Deadline check completed: {sent_count} sent, "
                f"{error_count} errors"
            )

        except Exception as e:
            logger.error(
                f"Error in check_deadlines_job: {e}", exc_info=True
            )
    async def check_deadlines_job(self):
        """Daily job to check campaign deadlines and send reminders.

        This job:
        1. Gets all active campaigns with deadlines in 3 or 1 day
        2. Finds users who haven't completed them
        3. Sends reminder messages via bot
        """
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
                    message = self.notification_service.build_reminder_message(
                        campaign, days_left
                    )

                    if not message:
                        logger.warning(
                            f"Empty message for campaign {campaign.name} "
                            f"with {days_left} days left"
                        )
                        continue

                    await self.bot.send_message(
                        int(user.telegram_id), message, parse_mode="Markdown"
                    )

                    sent_count += 1
                    logger.info(
                        f"Sent reminder to {user.telegram_id} "
                        f"for campaign {campaign.name} "
                        f"({days_left} days left)"
                    )

                except Exception as e:
                    error_count += 1
                    logger.error(
                        f"Failed to send reminder to {user.telegram_id} "
                        f"for campaign {campaign.name}: {e}",
                        exc_info=True,
                    )

            logger.info(
                f"Deadline check completed: {sent_count} sent, "
                f"{error_count} errors"
            )

        except Exception as e:
            logger.error(
                f"Error in check_deadlines_job: {e}", exc_info=True
            )
