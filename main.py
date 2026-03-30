import asyncio
import logging
import sys
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.redis import RedisStorage
from config import Config
from handlers import common, test # Оставим пока, потом уберем fio и test, если они будут заменены новыми
from handlers import registration_handlers # Импортируем новый роутер
from handlers import appeals, admin # Импортируем appeals и admin handlers
from middlewares.access_middleware import AccessMiddleware # Импортируем middleware
from services.redis_service import RedisService
from services.google_sheets import GoogleSheetsService # Импортируем GoogleSheetsService
from services.plandriver.plandriver_mapper import PlanDriverMapper
from services.plandriver.plandriver_result_sender import PlanDriverResultSender
from services.plandriver.plandriver_storage import PlanDriverStorage
from services.plandriver.plandriver_client import PlanDriverClient
from services.scheduler import SchedulerService # Импортируем SchedulerService

# Настройка логирования
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

redis_service = RedisService()
google_sheets_service = GoogleSheetsService() # Создаем экземпляр GoogleSheetsService
plandriver_storage = PlanDriverStorage()
plandriver_mapper = PlanDriverMapper()
plandriver_result_sender = PlanDriverResultSender(
    client=PlanDriverClient(),
    storage=plandriver_storage,
)


async def main():
    """Главная функция запуска бота."""
    # Валидация конфигурации
    try:
        Config.validate()
    except ValueError as e:
        logger.error(f"Ошибка конфигурации: {e}")
        sys.exit(1)
    
    # Подключение к Redis
    await redis_service.connect()
    
    # Инициализация бота и диспетчера
    bot = Bot(token=Config.TELEGRAM_TOKEN)
    
    # Используем Redis storage для FSM
    storage = None
    if redis_service.redis_client:
        try:
            storage = RedisStorage.from_url(Config.REDIS_URL)
        except Exception as e:
            logger.warning(f"Не удалось создать RedisStorage, используем память: {e}")
            from aiogram.fsm.storage.memory import MemoryStorage
            storage = MemoryStorage()
    else:
        from aiogram.fsm.storage.memory import MemoryStorage
        storage = MemoryStorage()
        logger.warning("Используется MemoryStorage вместо Redis")
    
    dp = Dispatcher(storage=storage)
    
    # Регистрация middleware
    dp.message.middleware(AccessMiddleware())
    dp.callback_query.middleware(AccessMiddleware()) # Также применяем к callback_query

    # Регистрация роутеров. Важен порядок: registration_handlers должен идти раньше, чтобы перехватить /start
    dp.include_router(registration_handlers.router)
    dp.include_router(common.router)
    dp.include_router(test.router)
    dp.include_router(appeals.router)
    dp.include_router(admin.router)

    # Передача зависимостей
    dp["google_sheets"] = google_sheets_service
    dp["redis_service"] = redis_service # redis_service уже глобально, но для единообразия
    dp["plandriver_storage"] = plandriver_storage
    dp["plandriver_mapper"] = plandriver_mapper
    dp["plandriver_result_sender"] = plandriver_result_sender

    # Инициализация и запуск scheduler
    scheduler_service = SchedulerService(bot, google_sheets_service, redis_service)
    scheduler_service.start()
    logger.info("Scheduler started")

    logger.info("Бот запущен")

    try:
        # Запуск polling
        await dp.start_polling(bot, skip_updates=True)
    finally:
        scheduler_service.shutdown()
        await bot.session.close()
        await redis_service.disconnect()
        logger.info("Бот остановлен")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Остановка по запросу пользователя")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        sys.exit(1)
