"""Admin access filter for restricting commands to administrators."""
import logging
from aiogram.filters import Filter
from aiogram.types import Message

from config import Config

logger = logging.getLogger(__name__)


class IsAdmin(Filter):
    """Filter to check if user is administrator or owner.

    Both ADMIN(s) and OWNER have access to admin commands.
    Supports multiple admins via comma-separated ADMIN_TELEGRAM_ID env var.
    """

    async def __call__(self, message: Message) -> bool:
        """Check if message sender is admin or owner.

        Args:
            message: Incoming message

        Returns:
            True if user is admin or owner, False otherwise
        """
        user_id = str(message.from_user.id)

        # Check if user is in ADMIN list (supports multiple admins)
        is_admin = user_id in Config.ADMIN_TELEGRAM_IDS

        # Check if user is OWNER (owner has all admin rights too)
        is_owner = Config.OWNER_TELEGRAM_ID and user_id == Config.OWNER_TELEGRAM_ID

        has_access = is_admin or is_owner

        if not has_access:
            logger.info(
                f"Access denied for user {user_id} "
                f"to admin command: {message.text}"
            )
        else:
            role = "owner" if is_owner else "admin"
            logger.debug(f"Access granted to {user_id} ({role}) for command: {message.text}")

        return has_access
