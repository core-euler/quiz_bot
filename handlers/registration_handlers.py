import logging

from aiogram import F, Router
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.types import (CallbackQuery, InlineKeyboardButton,
                           InlineKeyboardMarkup, Message, ReplyKeyboardRemove)

from handlers.states import Registration
from services.google_sheets import GoogleSheetsService

logger = logging.getLogger(__name__)
router = Router()


@router.message(StateFilter(Registration), Command("start"))
async def cancel_registration(message: Message, state: FSMContext):
    """
    Позволяет пользователю отменить процесс регистрации командой /start.
    """
    await state.clear()
    await message.answer(
        "Текущая регистрация отменена. Чтобы начать заново, пожалуйста, отправьте команду /start.",
        reply_markup=ReplyKeyboardRemove()
    )


@router.message(Registration.waiting_for_phone, F.contact)
async def process_phone(message: Message, state: FSMContext):
    """
    Обрабатывает полученный номер телефона из контакта Telegram.
    """
    phone_number = message.contact.phone_number
    await state.update_data(phone_number=phone_number)
    await message.answer("Спасибо! Теперь введите ваше полное ФИО.", reply_markup=ReplyKeyboardRemove())
    await state.set_state(Registration.waiting_for_fio)


@router.message(Registration.waiting_for_fio, F.text)
async def process_fio(message: Message, state: FSMContext, google_sheets: GoogleSheetsService):
    """
    Обрабатывает введенное ФИО и предлагает выбрать автоколонну.
    """
    fio = message.text
    await state.update_data(fio=fio)

    try:
        admin_config = google_sheets.read_admin_config()
        motorcades = admin_config.motorcades

        if motorcades:
            buttons = [
                [
                    InlineKeyboardButton(
                        text=mc,
                        callback_data=f"motorcade:{mc}",
                    )
                ]
                for mc in motorcades
            ]
            keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
            await message.answer(
                "Отлично! Теперь выберите вашу автоколонну из списка:",
                reply_markup=keyboard,
            )
        else:
            logger.warning("Список автоколонн не найден в настройках. Используется ручной ввод.")
            await message.answer("Отлично! Назовите вашу автоколонну.")

        await state.set_state(Registration.waiting_for_motorcade)

    except Exception as e:
        logger.error(f"Ошибка при получении списка автоколонн: {e}", exc_info=True)
        await message.answer("Произошла ошибка при загрузке списка автоколонн. Пожалуйста, введите название вашей автоколонны вручную.")
        await state.set_state(Registration.waiting_for_motorcade)


@router.callback_query(Registration.waiting_for_motorcade, F.data.startswith("motorcade:"))
async def process_motorcade_callback(callback_query: CallbackQuery, state: FSMContext, google_sheets: GoogleSheetsService):
    """
    Обрабатывает выбор автоколонны из inline-кнопки и завершает регистрацию.
    """
    motorcade = callback_query.data.split(":")[1]
    
    # Редактируем сообщение, чтобы убрать кнопки
    await callback_query.message.edit_text(f"Выбрана автоколонна: {motorcade}")

    data = await state.get_data()
    user_id = str(callback_query.from_user.id)
    phone_number = data.get("phone_number")
    fio = data.get("fio")

    try:
        google_sheets.add_user(user_id, phone_number, fio, motorcade, "ожидает")
        await callback_query.message.answer(
            "Регистрация завершена! Ваша учетная запись ожидает подтверждения администратором."
        )
    except Exception as e:
        logger.error(f"Ошибка завершения регистрации для {user_id}: {e}", exc_info=True)
        await callback_query.message.answer("Произошла критическая ошибка при завершении регистрации. Попробуйте позже.")
    finally:
        await state.clear()
        await callback_query.answer()


@router.message(Registration.waiting_for_motorcade, F.text)
async def process_motorcade_manual(message: Message, state: FSMContext, google_sheets: GoogleSheetsService):
    """
    Обрабатывает ручной ввод названия автоколонны и завершает регистрацию.
    (Используется как fallback, если список автоколонн не загрузился)
    """
    motorcade = message.text
    data = await state.get_data()
    user_id = str(message.from_user.id)
    phone_number = data.get("phone_number")
    fio = data.get("fio")

    try:
        google_sheets.add_user(user_id, phone_number, fio, motorcade, "ожидает")

        await message.answer(
            "Регистрация завершена! Ваша учетная запись ожидает подтверждения администратором."
        )
    except Exception as e:
        logger.error(f"Ошибка завершения регистрации (ручной ввод) для {user_id}: {e}", exc_info=True)
        await message.answer("Произошла критическая ошибка при завершении регистрации. Попробуйте позже.")
    finally:
        await state.clear()
