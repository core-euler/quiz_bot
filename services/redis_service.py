import json
import logging
import redis.asyncio as redis
from typing import Optional
from models import Session
from config import Config

logger = logging.getLogger(__name__)


class RedisService:
    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None

    async def connect(self):
        """Подключается к Redis."""
        try:
            self.redis_client = redis.from_url(
                Config.REDIS_URL,
                decode_responses=False  # Храним как bytes для JSON
            )
            await self.redis_client.ping()
            logger.info("Подключение к Redis установлено")
        except Exception as e:
            logger.error(f"Ошибка подключения к Redis: {e}")
            # Fallback: продолжаем работу без Redis (сессии не будут сохраняться)
            self.redis_client = None

    async def disconnect(self):
        """Отключается от Redis."""
        if self.redis_client:
            await self.redis_client.close()

    def _get_key(self, telegram_id: int) -> str:
        return f"session:{telegram_id}"

    async def get_session(self, telegram_id: int) -> Optional[Session]:
        """Получает сессию пользователя."""
        if not self.redis_client:
            return None
        
        try:
            key = self._get_key(telegram_id)
            data = await self.redis_client.get(key)
            if data:
                session_dict = json.loads(data)
                return Session.from_dict(session_dict)
            return None
        except Exception as e:
            logger.error(f"Ошибка получения сессии из Redis: {e}")
            return None

    async def set_session(self, telegram_id: int, session: Session, ttl: int):
        """Сохраняет сессию пользователя с TTL."""
        if not self.redis_client:
            return
        
        try:
            key = self._get_key(telegram_id)
            data = json.dumps(session.to_dict())
            await self.redis_client.setex(key, ttl, data)
        except Exception as e:
            logger.error(f"Ошибка сохранения сессии в Redis: {e}")

    async def delete_session(self, telegram_id: int):
        """Удаляет сессию пользователя."""
        if not self.redis_client:
            return
        
        try:
            key = self._get_key(telegram_id)
            await self.redis_client.delete(key)
        except Exception as e:
            logger.error(f"Ошибка удаления сессии из Redis: {e}")

    async def has_active_session(self, telegram_id: int) -> bool:
        """Проверяет, есть ли активная сессия."""
        if not self.redis_client:
            return False
        
        try:
            key = self._get_key(telegram_id)
            return await self.redis_client.exists(key) > 0
        except Exception as e:
            logger.error(f"Ошибка проверки активной сессии: {e}")
            return False

    def _get_processed_campaigns_key(self) -> str:
        return "processed_campaigns"

    async def get_processed_campaigns(self) -> set[str]:
        """Возвращает множество названий обработанных кампаний."""
        if not self.redis_client:
            return set()
        
        try:
            key = self._get_processed_campaigns_key()
            campaigns_bytes = await self.redis_client.smembers(key)
            return {c.decode('utf-8') for c in campaigns_bytes}
        except Exception as e:
            logger.error(f"Ошибка получения обработанных кампаний из Redis: {e}")
            return set()

    async def add_processed_campaigns(self, *campaign_names: str):
        """Добавляет названия кампаний в множество обработанных."""
        if not self.redis_client or not campaign_names:
            return
        
        try:
            key = self._get_processed_campaigns_key()
            await self.redis_client.sadd(key, *campaign_names)
        except Exception as e:
            logger.error(f"Ошибка добавления обработанных кампаний в Redis: {e}")
