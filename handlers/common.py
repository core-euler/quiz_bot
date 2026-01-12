import logging
import time
from datetime import datetime, timedelta

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (CallbackQuery, InlineKeyboardButton,
                           InlineKeyboardMarkup, Message, ReplyKeyboardRemove,
                           ReplyKeyboardMarkup, KeyboardButton)

from models import CampaignType
from services.google_sheets import AdminConfigError, GoogleSheetsService
from handlers.states import Registration, TestStates


logger = logging.getLogger(__name__)

router = Router()


@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext, google_sheets: GoogleSheetsService):
    """
    Единый обработчик команды /start.
    - Регистрирует новых пользователей.
    - Информирует пользователей в ожидании.
    - Запускает кампании для подтвержденных пользователей.
    """
    await state.clear()
    user_id = str(message.from_user.id)

    try:
        user_info = google_sheets.get_user_info(user_id)
        user_status = user_info.status.value if user_info else None

        # Сценарий 1: Новый пользователь
        if user_status is None:
            logger.info(f"Пользователь {user_id} не найден, запуск регистрации.")
            keyboard = ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="Отправить мой номер телефона", request_contact=True)]
                ],
                resize_keyboard=True,
                one_time_keyboard=True
            )
            await message.answer(
                "Добро пожаловать! Для регистрации, пожалуйста, нажмите кнопку ниже, чтобы отправить ваш номер телефона.",
                reply_markup=keyboard
            )
            await state.set_state(Registration.waiting_for_phone)
            return

        # Сценарий 2: Пользователь ожидает подтверждения или отклонен
        if user_status in ["ожидает", "отклонён"]:
            logger.info(f"Пользователь {user_id} имеет статус '{user_status}', доступ ограничен.")
            await message.answer(f"Ваша учетная запись находится в статусе '{user_status}'. Пожалуйста, дождитесь подтверждения администратором.")
            return

        # Сценарий 3: Подтвержденный пользователь -> сначала основной тест, потом кампании
        if user_status == "подтверждён":
            user_results = google_sheets.get_user_results(user_id)
            has_passed_init_test = any(
                not r.campaign_name and r.final_status == "успешно"
                for r in user_results
            )

            # 3.1. Если основной тест НЕ пройден, предлагаем его
            if not has_passed_init_test:
                has_taken_init_test = any(not r.campaign_name for r in user_results)

                if not has_taken_init_test:
                    # Пользователь еще не проходил основной тест - разрешаем
                    message_text = (
                        "👋 Добро пожаловать!\n\n"
                        "Для начала работы вам необходимо пройти основной тест. "
                        "Нажмите «Начать», чтобы приступить."
                    )
                    keyboard = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="Начать основной тест", callback_data="start_init_test")]
                    ])
                    await message.answer(message_text, reply_markup=keyboard)
                    logger.info(f"Пользователю {user_id} предложен основной тест (первый раз).")
                else:
                    # Пользователь уже проходил, но не сдал - проверяем cooldown
                    admin_config = google_sheets.read_admin_config()
                    last_test_time = google_sheets.get_last_test_time(int(user_id), campaign_name=None)
                    logger.info(f"Cooldown check for user {user_id}: last_test_time={last_test_time}, retry_hours={admin_config.retry_hours}")

                    if last_test_time:
                        hours_passed = (time.time() - last_test_time) / 3600
                        hours_required = admin_config.retry_hours
                        logger.info(f"Hours passed: {hours_passed:.2f}, required: {hours_required}")

                        if hours_passed < hours_required:
                            # Cooldown не прошел
                            hours_remaining = hours_required - hours_passed
                            if hours_remaining >= 1:
                                time_msg = f"{int(hours_remaining)} ч."
                            else:
                                minutes_remaining = int(hours_remaining * 60)
                                time_msg = f"{minutes_remaining} мин."

                            await message.answer(
                                f"⏳ Вы не сдали основной тест.\n\n"
                                f"Повторная попытка будет доступна через {time_msg}\n\n"
                                f"Правило: можно проходить тест раз в {hours_required} ч."
                            )
                            logger.info(f"Пользователь {user_id} заблокирован cooldown для основного теста (осталось {hours_remaining:.1f} ч.)")
                            return

                    # Cooldown прошел или не найден - разрешаем retry
                    message_text = (
                        "👋 Вы можете пройти основной тест повторно. "
                        "Нажмите «Начать», чтобы приступить."
                    )
                    keyboard = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="Начать основной тест", callback_data="start_init_test")]
                    ])
                    await message.answer(message_text, reply_markup=keyboard)
                    logger.info(f"Пользователю {user_id} разрешена повторная попытка основного теста.")
                return # Важно завершить обработку здесь

            # 3.2. Если основной тест пройден, ищем кампании
            else:
                campaign = google_sheets.get_active_campaign_for_user(user_id)
                if campaign:
                    user_data = {
                        "id": message.from_user.id, "username": message.from_user.username,
                        "first_name": message.from_user.first_name, "last_name": message.from_user.last_name,
                    }
                    await state.update_data(user_data=user_data, campaign_name=campaign.name, mode=campaign.type.value)

                    deadline_str = campaign.deadline.strftime("%d.%m.%Y")
                    message_text = (
                        f"👋 Здравствуйте!\n\n"
                        f"Для вас доступна учебная кампания: **{campaign.name}**\n\n"
                        f"🔹 **Тип:** {campaign.type.value}\n"
                        f"🔹 **Срок прохождения:** до {deadline_str}\n\n"
                        f"Нажмите «Начать», чтобы приступить."
                    )
                    keyboard = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="Начать", callback_data="start_campaign")]
                    ])
                    await message.answer(message_text, reply_markup=keyboard, parse_mode="Markdown")
                    logger.info(f"Пользователю {user_id} предложена кампания '{campaign.name}'")
                else:
                    # Основной тест пройден, кампаний нет
                    await message.answer("✅ Основной тест пройден. На данный момент для вас нет доступных кампаний.")
                    logger.info(f"Пользователь {user_id} прошел основной тест, нет доступных кампаний.")


    except AdminConfigError as e:
        logger.error(f"Критическая ошибка конфигурации: {e}")
        await message.answer("⚠️ Бот не настроен. Пожалуйста, обратитесь к администратору.")
    except Exception as e:
        logger.error(f"Произошла ошибка при обработке /start для {user_id}: {e}", exc_info=True)
        await message.answer("Произошла ошибка. Попробуйте позже.")


@router.callback_query(F.data.in_({"start_campaign", "start_init_test"}))
async def start_test_callback(callback_query: CallbackQuery, state: FSMContext, google_sheets: GoogleSheetsService):
    """
    Обрабатывает нажатие кнопки "Начать", получает ФИО пользователя из Google Sheets
    и сразу запускает подготовку к тесту, пропуская ручной ввод ФИО.
    """
    await callback_query.answer()
    user_id = str(callback_query.from_user.id)
    
    try:
        # 1. Проверяем базовые настройки
        admin_config = google_sheets.read_admin_config()
        all_questions = google_sheets.read_questions()

        if not all_questions:
            await callback_query.message.answer("❗️ В базе нет вопросов. Обратитесь к администратору.")
            await state.clear()
            return

        if len(all_questions) < admin_config.num_questions:
            logger.warning(
                "Недостаточно вопросов: доступно %s, требуется %s",
                len(all_questions), admin_config.num_questions
            )
            await callback_query.message.answer("⚠️ Временно недостаточно вопросов для старта. Обратитесь к администратору.")
            await state.clear()
            return

        # 2. Получаем информацию о пользователе, включая его ФИО
        user_info = google_sheets.get_user_info(user_id)
        if not user_info or not user_info.fio:
            await callback_query.message.answer("⚠️ Не удалось найти ваше ФИО в системе. Пожалуйста, обратитесь к администратору.")
            await state.clear()
            return
            
        # 3. Обновляем данные сессии в FSM, включая FIO и user_data
        user_data = {
            "id": callback_query.from_user.id,
            "username": callback_query.from_user.username,
            "first_name": callback_query.from_user.first_name,
            "last_name": callback_query.from_user.last_name,
        }
        await state.update_data(fio=user_info.fio, user_data=user_data)

        # Если это основной тест, еще раз убедимся, что данных кампании нет
        if callback_query.data == "start_init_test":
            await state.update_data(campaign_name=None, mode=None)

        logger.info(f"Пользователь {user_id} (ФИО: {user_info.fio}) начинает тест (callback: {callback_query.data}).")

        # 4. Сразу переходим к подготовке теста
        from handlers.test import prepare_test
        await state.set_state(TestStates.PREPARE_TEST)
        await prepare_test(callback_query.message, state)

    except AdminConfigError as e:
        logger.error(f"Отсутствуют настройки теста: {e}")
        await callback_query.message.answer("⚠️ У бота отсутствуют необходимые настройки. Обратитесь к администратору.")
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка при старте теста: {e}", exc_info=True)
        await callback_query.message.answer("Произошла ошибка при подготовке к тесту. Попробуйте позже.")
        await state.clear()