import logging
import re
import time
from datetime import datetime
from typing import List, Optional

import pytz
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from config import Config
from models import (AdminConfig, Campaign, CampaignStats, CampaignType,
                    Question, UserInfo, UserResult, UserStatus)

logger = logging.getLogger(__name__)


class AdminConfigError(Exception):
    """Ошибка, возникающая при отсутствии или некорректных настройках в листе ⚙️Настройки."""


USERS_SHEET = "👩‍👧‍👧Пользователи"
QUESTIONS_SHEET = "❓Вопросы"
ADMIN_SHEET = "⚙️Настройки"
RESULTS_SHEET = "📊Результаты"
CAMPAIGNS_SHEET = "🚚Кампании"


class GoogleSheetsService:
    def __init__(self):
        credentials = service_account.Credentials.from_service_account_info(
            Config.GOOGLE_CREDENTIALS,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        self.service = build('sheets', 'v4', credentials=credentials)
        self.sheet_id = Config.SHEET_ID
        self.max_retries = 3
        self.retry_delay = 1

    def _retry_request(self, func, *args, **kwargs):
        last_error = None
        for attempt in range(self.max_retries):
            try:
                request = func(*args, **kwargs)
                if hasattr(request, 'execute'):
                    return request.execute()
                return request
            except HttpError as e:
                last_error = e
                if e.resp.status in [429, 500, 502, 503, 504]:
                    delay = self.retry_delay * (2 ** attempt)
                    logger.warning(
                        f"Ошибка Google Sheets API (попытка {attempt + 1}/{self.max_retries}): {e}. Повтор через {delay}с")
                    time.sleep(delay)
                else:
                    raise
            except Exception as e:
                logger.error(f"Неожиданная ошибка при запросе к Google Sheets: {e}")
                raise
        logger.error(f"Не удалось выполнить запрос после {self.max_retries} попыток")
        raise last_error

    @staticmethod
    def _find_optional_column(headers: List[str], *names: str) -> Optional[int]:
        for name in names:
            if name in headers:
                return headers.index(name)
        return None

    def add_user(self, telegram_id: str, phone_number: str, fio: str, motorcade: str, status: str = "ожидает"):
        try:
            values = [[telegram_id, phone_number, fio, motorcade, status]]
            body = {'values': values}
            self._retry_request(
                self.service.spreadsheets().values().append,
                spreadsheetId=self.sheet_id,
                range=f"{USERS_SHEET}!A:E",
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body=body
            )
            logger.info(f"Пользователь {telegram_id} добавлен в лист '{USERS_SHEET}' со статусом '{status}'")
        except Exception as e:
            logger.error(f"Ошибка добавления пользователя в лист '{USERS_SHEET}': {e}")
            raise

    @staticmethod
    def _normalize_status(status_str: str) -> str:
        """Нормализует статус пользователя.

        Заменяет 'е' на 'ё' в словах 'подтвержден' и 'отклонен' для совместимости.

        Args:
            status_str: Исходный статус из Google Sheets

        Returns:
            Нормализованный статус
        """
        normalized = status_str.strip()
        # Заменяем "подтвержден" -> "подтверждён"
        if normalized == "подтвержден":
            normalized = "подтверждён"
        # Заменяем "отклонен" -> "отклонён"
        elif normalized == "отклонен":
            normalized = "отклонён"
        return normalized

    def get_user_info(self, telegram_id: str) -> Optional[UserInfo]:
        try:
            range_name = f"{USERS_SHEET}!A:Z"
            result = self._retry_request(
                self.service.spreadsheets().values().get,
                spreadsheetId=self.sheet_id,
                range=range_name
            )
            values = result.get('values', [])
            if not values:
                return None

            headers = [h.lower() for h in values[0]]
            try:
                id_col = headers.index('telegram_id')
                phone_col = headers.index('телефон')
                fio_col = headers.index('фио')
                motorcade_col = headers.index('автоколонна')
                status_col = headers.index('статус')
                personnel_col = self._find_optional_column(headers, 'табельный номер', 'personnel_number')
            except ValueError as e:
                logger.error(f"В листе '{USERS_SHEET}' отсутствует обязательная колонка: {e}")
                return None

            for row in values[1:]:
                if len(row) > id_col and str(row[id_col]) == telegram_id:
                    try:
                        # Нормализуем статус (подтвержден -> подтверждён)
                        status_str = self._normalize_status(row[status_col])
                        status = UserStatus(status_str)
                        
                        return UserInfo(
                            telegram_id=str(row[id_col]),
                            phone=row[phone_col],
                            fio=row[fio_col],
                            motorcade=row[motorcade_col],
                            status=status,
                            personnel_number=row[personnel_col] if personnel_col is not None and personnel_col < len(row) else None,
                        )
                    except (ValueError, IndexError):
                        original_status = row[status_col] if status_col < len(row) else "[СТАТУС НЕ НАЙДЕН]"
                        logger.warning(
                            f"Некорректный статус ('{original_status}') для пользователя {telegram_id}. "
                            f"Пользователь будет считаться ожидающим подтверждения."
                        )
                        # Возвращаем пользователя со статусом 'ожидает', чтобы он не начал регистрацию заново
                        return UserInfo(
                            telegram_id=str(row[id_col]),
                            phone=row[phone_col],
                            fio=row[fio_col],
                            motorcade=row[motorcade_col],
                            status=UserStatus.AWAITS,
                            personnel_number=row[personnel_col] if personnel_col is not None and personnel_col < len(row) else None,
                        )
            return None
        except Exception as e:
            logger.error(f"Ошибка получения информации о пользователе {telegram_id}: {e}")
            return None

    def get_all_campaigns(self) -> List[Campaign]:
        campaigns = []
        try:
            range_name = f"{CAMPAIGNS_SHEET}!A:D"
            result = self._retry_request(self.service.spreadsheets().values().get, spreadsheetId=self.sheet_id,
                                          range=range_name)
            values = result.get('values', [])
            if len(values) < 2:
                return []

            headers = [h.lower().strip() for h in values[0]]
            try:
                name_col = headers.index('название кампании')
                deadline_col = headers.index('дедлайн')
                type_col = headers.index('тип')
                assignment_col = headers.index('назначение')
            except ValueError as e:
                logger.error(f"В листе '{CAMPAIGNS_SHEET}' отсутствует обязательная колонка: {e}")
                logger.error(f"Доступные заголовки: {headers}")
                return []

            for row_idx, row in enumerate(values[1:], start=2):
                try:
                    name = row[name_col]
                    if not name: continue

                    deadline = datetime.strptime(row[deadline_col], "%Y-%m-%d")
                    ctype = CampaignType(row[type_col])
                    assignment = row[assignment_col].strip() if assignment_col < len(row) and row[assignment_col] else ""

                    campaigns.append(
                        Campaign(name=name, deadline=deadline, type=ctype, assignment=assignment))
                except (ValueError, IndexError) as e:
                    logger.warning(f"Ошибка парсинга кампании в строке {row_idx}: {e}")
                    continue
            return campaigns
        except Exception as e:
            logger.error(f"Ошибка чтения кампаний из листа '{CAMPAIGNS_SHEET}': {e}")
            return []

    def _parse_datetime_str(self, date_str: str) -> Optional[datetime]:
        """Парсит строку с датой, поддерживая новый и старый (ISO) форматы."""
        if not date_str:
            return None
        # ВАЖНО: все временные метки считаются в 'Europe/Moscow'
        tz = pytz.timezone("Europe/Moscow")
        try:
            # Сначала пробуем новый формат "ГГГГ-ММ-ДД ЧЧ:ММ"
            naive_dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
            # Локализуем как московское время
            return tz.localize(naive_dt)
        except ValueError:
            try:
                # Фоллбэк на ISO формат для старых данных
                dt = datetime.fromisoformat(date_str)
                # Если в строке уже есть таймзона, fromisoformat вернет aware datetime.
                # Если нет - вернет naive, который нужно локализовать.
                if dt.tzinfo is None:
                    return tz.localize(dt)
                return dt
            except ValueError:
                logger.warning(f"Не удалось распознать формат даты '{date_str}' ни в одном из известных форматов.")
                return None

    def get_user_results(self, telegram_id: str) -> List[UserResult]:
        results = []
        try:
            range_name = f"{RESULTS_SHEET}!A:H"  # 8 columns
            result = self._retry_request(self.service.spreadsheets().values().get, spreadsheetId=self.sheet_id,
                                          range=range_name)
            values = result.get('values', [])
            if len(values) < 2:
                return []

            headers = [h.lower().strip() for h in values[0]]
            try:
                id_col = headers.index('telegram_id')
                date_col = headers.index('дата прохождения теста')
                fio_col = headers.index('фио')
                correct_col = headers.index('количество верных ответов')
                notes_col = headers.index('примечания')
                status_col = headers.index('итоговый статус')
                campaign_col = headers.index('название кампании')
            except ValueError as e:
                logger.error(f"В листе '{RESULTS_SHEET}' отсутствует обязательная колонка: {e}")
                logger.error(f"Доступные заголовки: {headers}")
                return []

            for row in values[1:]:
                if len(row) > id_col and str(row[id_col]) == telegram_id:
                    try:
                        date_str = row[date_col]
                        dt = self._parse_datetime_str(date_str)
                        if not dt:
                            logger.warning(f"Пропуск результата для {telegram_id} из-за неверной даты: '{date_str}'")
                            continue

                        campaign_name = row[campaign_col] if len(row) > campaign_col else ""
                        final_status = row[status_col] if len(row) > status_col else ""

                        # Вычисляем result из final_status для обратной совместимости
                        result = "Пройден" if final_status == "успешно" else "Не пройден"

                        # Читаем correct_count и notes
                        try:
                            correct_count = int(row[correct_col]) if len(row) > correct_col and row[correct_col] else 0
                        except ValueError:
                            correct_count = 0

                        notes = row[notes_col] if len(row) > notes_col else None

                        results.append(UserResult(
                            telegram_id=str(row[id_col]),
                            date=dt,
                            campaign_name=campaign_name,
                            final_status=final_status,
                            result=result,
                            correct_count=correct_count,
                            notes=notes
                        ))
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Ошибка парсинга результата для пользователя {telegram_id}: {e}")
                        continue
            return results
        except Exception as e:
            logger.error(f"Ошибка получения результатов пользователя {telegram_id}: {e}")
            return []

    def get_active_campaign_for_user(self, telegram_id: str) -> Optional[Campaign]:
        user_info = self.get_user_info(telegram_id)
        if not user_info:
            logger.warning(f"Для telegram_id {telegram_id} не найдена информация о пользователе.")
            return None

        all_campaigns = self.get_all_campaigns()
        user_results = self.get_user_results(telegram_id)
        
        user_results.sort(key=lambda r: r.date, reverse=True)
        latest_results = {res.campaign_name: res.final_status for res in reversed(user_results)}

        today = datetime.now()

        for campaign in all_campaigns:
            if campaign.deadline.date() < today.date():
                continue

            if campaign.assignment.upper() != "ВСЕ":
                if user_info.motorcade != campaign.assignment:
                    continue

            last_status = latest_results.get(campaign.name)

            if last_status is None:
                logger.info(f"Найдена активная кампания '{campaign.name}' для пользователя {telegram_id} (ранее не проходил).")
                return campaign

            if last_status == "разрешена пересдача":
                logger.info(f"Найдена активная кампания '{campaign.name}' для пользователя {telegram_id} (разрешена пересдача).")
                return campaign

        logger.info(f"Для пользователя {telegram_id} не найдено активных кампаний.")
        return None

    def has_passed_initial_test(self, user_id: str, user_results: Optional[List[UserResult]] = None) -> bool:
        """Проверяет, сдал ли пользователь успешно основной тест."""
        if user_results is None:
            user_results = self.get_user_results(user_id)
        
        return any(
            not r.campaign_name and r.final_status == "успешно"
            for r in user_results
        )

    def get_all_active_campaigns_for_user(self, user_id: str) -> List[Campaign]:
        """Возвращает список всех активных и доступных кампаний для пользователя."""
        user_info = self.get_user_info(user_id)
        if not user_info:
            logger.warning(f"Для telegram_id {user_id} не найдена информация о пользователе.")
            return []

        all_campaigns = self.get_all_campaigns()
        user_results = self.get_user_results(user_id)
        
        user_results.sort(key=lambda r: r.date, reverse=True)
        latest_results = {res.campaign_name: res.final_status for res in reversed(user_results)}

        today = datetime.now()
        available_campaigns = []

        for campaign in all_campaigns:
            if campaign.deadline.date() < today.date():
                continue

            if campaign.assignment.upper() != "ВСЕ":
                if user_info.motorcade != campaign.assignment:
                    continue

            last_status = latest_results.get(campaign.name)
            
            if last_status is None or last_status == "разрешена пересдача":
                available_campaigns.append(campaign)
        
        logger.info(f"Для пользователя {user_id} найдено {len(available_campaigns)} активных кампаний.")
        return available_campaigns

    def get_campaign_by_name(self, campaign_name: str) -> Optional[Campaign]:
        """Получает кампанию по имени.

        Args:
            campaign_name: Название кампании

        Returns:
            Campaign объект или None если не найдена
        """
        all_campaigns = self.get_all_campaigns()
        for campaign in all_campaigns:
            if campaign.name == campaign_name:
                logger.info(f"Найдена кампания '{campaign_name}'.")
                return campaign

        logger.warning(f"Кампания '{campaign_name}' не найдена.")
        return None

    def get_target_users_for_campaign(self, campaign: Campaign) -> List[UserInfo]:
        """Возвращает список подтвержденных пользователей, которым назначена кампания."""
        target_users = []
        try:
            range_name = f"{USERS_SHEET}!A:Z"
            result = self._retry_request(
                self.service.spreadsheets().values().get,
                spreadsheetId=self.sheet_id,
                range=range_name
            )
            values = result.get('values', [])
            if len(values) < 2:
                return []

            headers = [h.lower().strip() for h in values[0]]
            try:
                id_col = headers.index('telegram_id')
                phone_col = headers.index('телефон')
                fio_col = headers.index('фио')
                motorcade_col = headers.index('автоколонна')
                status_col = headers.index('статус')
                personnel_col = self._find_optional_column(headers, 'табельный номер', 'personnel_number')
            except ValueError as e:
                logger.error(f"В листе '{USERS_SHEET}' отсутствует обязательная колонка: {e}")
                return []

            for row in values[1:]:
                if not row or not row[id_col]:
                    continue

                try:
                    status_str = row[status_col].strip() if status_col < len(row) else ""
                    # Нормализуем статус (подтвержден -> подтверждён)
                    status_str = self._normalize_status(status_str)
                    if UserStatus(status_str) != UserStatus.CONFIRMED:
                        continue

                    user_motorcade = row[motorcade_col] if motorcade_col < len(row) else ""
                    is_target = False
                    if campaign.assignment.upper() == "ВСЕ":
                        is_target = True
                    elif user_motorcade == campaign.assignment:
                        is_target = True

                    if is_target:
                        target_users.append(UserInfo(
                            telegram_id=str(row[id_col]),
                            phone=row[phone_col] if phone_col < len(row) else "",
                            fio=row[fio_col] if fio_col < len(row) else "",
                            motorcade=user_motorcade,
                            status=UserStatus.CONFIRMED,
                            personnel_number=row[personnel_col] if personnel_col is not None and personnel_col < len(row) else None,
                        ))
                except (ValueError, IndexError) as e:
                    logger.warning(f"Ошибка парсинга пользователя в строке {row}: {e}")
                    continue
            
            logger.info(f"Для кампании '{campaign.name}' найдено {len(target_users)} целевых пользователей.")
            return target_users
        except Exception as e:
            logger.error(f"Ошибка получения целевых пользователей для кампании '{campaign.name}': {e}")
            return []

    def find_confirmed_users_by_fio(self, fio: str) -> List[UserInfo]:
        """Ищет подтвержденных пользователей по точному ФИО."""
        target_users = []
        normalized_fio = fio.strip().casefold()
        try:
            range_name = f"{USERS_SHEET}!A:Z"
            result = self._retry_request(
                self.service.spreadsheets().values().get,
                spreadsheetId=self.sheet_id,
                range=range_name
            )
            values = result.get('values', [])
            if len(values) < 2:
                return []

            headers = [h.lower().strip() for h in values[0]]
            id_col = headers.index('telegram_id')
            phone_col = headers.index('телефон')
            fio_col = headers.index('фио')
            motorcade_col = headers.index('автоколонна')
            status_col = headers.index('статус')
            personnel_col = self._find_optional_column(headers, 'табельный номер', 'personnel_number')

            for row in values[1:]:
                if len(row) <= max(id_col, fio_col, status_col):
                    continue

                row_fio = row[fio_col].strip().casefold()
                status_str = self._normalize_status(row[status_col])
                if row_fio != normalized_fio or UserStatus(status_str) != UserStatus.CONFIRMED:
                    continue

                target_users.append(UserInfo(
                    telegram_id=str(row[id_col]),
                    phone=row[phone_col] if phone_col < len(row) else "",
                    fio=row[fio_col],
                    motorcade=row[motorcade_col] if motorcade_col < len(row) else "",
                    status=UserStatus.CONFIRMED,
                    personnel_number=row[personnel_col] if personnel_col is not None and personnel_col < len(row) else None,
                ))
        except ValueError as e:
            logger.error(f"В листе '{USERS_SHEET}' отсутствует обязательная колонка: {e}")
        except Exception as e:
            logger.error(f"Ошибка поиска пользователя по ФИО '{fio}': {e}")
        return target_users

    def find_confirmed_users_by_personnel_number(self, personnel_number: str) -> List[UserInfo]:
        """Ищет подтвержденных пользователей по табельному номеру, если колонка есть в листе."""
        target_users = []
        target_number = personnel_number.strip()
        if not target_number:
            return []

        try:
            range_name = f"{USERS_SHEET}!A:Z"
            result = self._retry_request(
                self.service.spreadsheets().values().get,
                spreadsheetId=self.sheet_id,
                range=range_name
            )
            values = result.get('values', [])
            if len(values) < 2:
                return []

            headers = [h.lower().strip() for h in values[0]]
            personnel_col = self._find_optional_column(headers, 'табельный номер', 'personnel_number')
            if personnel_col is None:
                return []

            id_col = headers.index('telegram_id')
            phone_col = headers.index('телефон')
            fio_col = headers.index('фио')
            motorcade_col = headers.index('автоколонна')
            status_col = headers.index('статус')

            for row in values[1:]:
                if len(row) <= max(id_col, personnel_col, status_col):
                    continue
                if row[personnel_col].strip() != target_number:
                    continue

                status_str = self._normalize_status(row[status_col])
                if UserStatus(status_str) != UserStatus.CONFIRMED:
                    continue

                target_users.append(UserInfo(
                    telegram_id=str(row[id_col]),
                    phone=row[phone_col] if phone_col < len(row) else "",
                    fio=row[fio_col] if fio_col < len(row) else "",
                    motorcade=row[motorcade_col] if motorcade_col < len(row) else "",
                    status=UserStatus.CONFIRMED,
                    personnel_number=row[personnel_col],
                ))
        except ValueError as e:
            logger.error(f"В листе '{USERS_SHEET}' отсутствует обязательная колонка: {e}")
        except Exception as e:
            logger.error(f"Ошибка поиска пользователя по табельному номеру '{personnel_number}': {e}")
        return target_users

    def read_admin_config(self) -> AdminConfig:
        try:
            range_name = f"{ADMIN_SHEET}!A1:E2"
            result = self._retry_request(
                self.service.spreadsheets().values().get,
                spreadsheetId=self.sheet_id,
                range=range_name
            )
            values = result.get('values', [])

            if len(values) < 2:
                raise AdminConfigError("Лист Настройки должен содержать заголовки и значения")

            headers = values[0]
            data_row = values[1]
            config_dict = {header.lower(): data_row[i] for i, header in enumerate(headers) if i < len(data_row)}

            required_fields = {
                'количество вопросов': 'num_questions',
                'количество допустимых ошибок': 'max_errors',
                'как часто можно проходить тест (часов)': 'retry_hours',
                'количество секунд на одно задание': 'seconds_per_question',
            }
            parsed_values = {}
            missing_fields = []

            for header_key, attr_name in required_fields.items():
                raw_value = config_dict.get(header_key)
                if not raw_value or not str(raw_value).strip():
                    missing_fields.append(header_key)
                    continue
                try:
                    parsed_values[attr_name] = int(str(raw_value).strip())
                except ValueError:
                    raise AdminConfigError(f"Поле '{header_key}' должно быть целым числом")

            if missing_fields:
                raise AdminConfigError("Не заполнены обязательные поля: " + ", ".join(missing_fields))

            motorcades_raw = config_dict.get('автоколонны')
            if motorcades_raw and isinstance(motorcades_raw, str):
                motorcades_list = [mc.strip() for mc in motorcades_raw.split(';') if mc.strip()]
                if motorcades_list:
                    parsed_values['motorcades'] = motorcades_list

            return AdminConfig(**parsed_values)
        except AdminConfigError:
            raise
        except Exception as e:
            logger.error(f"Ошибка чтения конфигурации (Настройки): {e}")
            raise

    def read_questions(self) -> List[Question]:
        try:
            range_name = f"{QUESTIONS_SHEET}!A:J"
            result = self._retry_request(
                self.service.spreadsheets().values().get,
                spreadsheetId=self.sheet_id,
                range=range_name
            )
            values = result.get('values', [])
            if len(values) < 2: return []

            headers = [h.lower().strip() for h in values[0]]
            questions = []

            try:
                h = {
                    'cat': headers.index('категория'), 'q': headers.index('вопрос'),
                    'a1': headers.index('ответ 1'), 'a2': headers.index('ответ 2'),
                    'a3': headers.index('ответ 3'), 'a4': headers.index('ответ 4'),
                    'correct': headers.index('правильный ответ (1-4)'),
                    'crit': headers.index('критический вопрос'),
                    'exp': headers.index('пояснение')
                }
            except ValueError as e:
                logger.error(f"В листе '{QUESTIONS_SHEET}' отсутствует обязательная колонка: {e}")
                return []


            for row_idx, row in enumerate(values[1:], start=2):
                try:
                    get = lambda index: row[index].strip() if index < len(row) and row[index] else ""

                    question_text = get(h['q'])
                    if not get(h['cat']) or not question_text:
                        continue

                    answers = [get(h['a1']), get(h['a2']), get(h['a3']), get(h['a4'])]
                    if len([ans for ans in answers if ans]) < 2:
                        continue

                    correct_answer = int(get(h['correct']))
                    if not (1 <= correct_answer <= 4 and answers[correct_answer - 1]):
                        continue

                    is_critical = get(h['crit']).upper() == 'ДА'
                    explanation = get(h['exp'])

                    questions.append(Question(
                        category=get(h['cat']), question_text=question_text,
                        answer1=answers[0], answer2=answers[1], answer3=answers[2], answer4=answers[3],
                        correct_answer=correct_answer, is_critical=is_critical,
                        explanation=explanation, row_index=row_idx
                    ))
                except (ValueError, IndexError) as e:
                    logger.warning(f"Ошибка парсинга вопроса в строке {row_idx}: {e}")
                    continue

            return questions
        except Exception as e:
            logger.error(f"Ошибка чтения вопросов ({QUESTIONS_SHEET}): {e}")
            return []

    def get_last_test_time(self, telegram_id: int, campaign_name: Optional[str] = None) -> Optional[float]:
        """Get timestamp of last test attempt for user."""
        try:
            range_name = f"{RESULTS_SHEET}!A:H"  # 8 columns
            result = self._retry_request(
                self.service.spreadsheets().values().get,
                spreadsheetId=self.sheet_id,
                range=range_name
            )
            values = result.get('values', [])
            if len(values) < 2: return None

            telegram_id_str = str(telegram_id)
            logger.info(f"get_last_test_time: Searching for user {telegram_id_str}, campaign filter: {repr(campaign_name)}")

            for row in reversed(values[1:]):
                if not row: continue
                if str(row[0]) != telegram_id_str:
                    continue

                # Колонка H "название кампании" = индекс 7 (8-я колонка, индексация с 0)
                row_campaign = row[7] if len(row) > 7 else ""
                logger.info(f"Found row for user {telegram_id_str}: row_campaign='{row_campaign}', date={row[2] if len(row) > 2 else 'N/A'}")

                if campaign_name is None or campaign_name == "":
                    if row_campaign:
                        logger.info(f"Skipping row - has campaign '{row_campaign}' but looking for initial test")
                        continue
                elif row_campaign != campaign_name:
                    logger.info(f"Skipping row - campaign mismatch: '{row_campaign}' != '{campaign_name}'")
                    continue

                if len(row) > 2 and row[2]:
                    dt = self._parse_datetime_str(row[2])
                    if dt:
                        timestamp = dt.timestamp()
                        logger.info(f"Found matching test: date={row[2]}, timestamp={timestamp}")
                        return timestamp

            logger.info(f"No matching test found for user {telegram_id_str}")
            return None
        except Exception as e:
            logger.error(f"Ошибка получения времени последнего теста: {e}")
            return None

    def write_result(self, telegram_id: int, display_name: str, test_date: str, fio: str,
                     correct_count: int, campaign_name: str, final_status: str, notes: Optional[str] = None):
        """Записывает результат теста в лист Результаты."""
        try:
            # Правильный порядок колонок A:H (8 колонок, БЕЗ колонки "Результат")
            values = [[
                str(telegram_id),       # A: telegram_id
                display_name or '',     # B: username
                test_date,              # C: дата
                fio,                    # D: ФИО
                str(correct_count),     # E: количество верных ответов
                notes or '',            # F: примечания
                final_status,           # G: итоговый статус (успешно/не пройдено/разрешена пересдача)
                campaign_name or ''     # H: название кампании
            ]]
            logger.info(f"Writing result: telegram_id={telegram_id}, campaign_name='{campaign_name}', final_status='{final_status}', date={test_date}")
            body = {'values': values}

            # Диапазон A:H (8 колонок)
            range_to_append = f"{RESULTS_SHEET}!A:H"

            append_result = self._retry_request(
                self.service.spreadsheets().values().append,
                spreadsheetId=self.sheet_id, range=range_to_append,
                valueInputOption='RAW', insertDataOption='INSERT_ROWS', body=body
            )

            updated_range = append_result.get('updates', {}).get('updatedRange', '')
            if updated_range:
                match = re.search(r'!?A(\d+):', updated_range)
                if match:
                    row_number = int(match.group(1))
                    try:
                        sheet_id = self._get_sheet_id(RESULTS_SHEET)
                        if sheet_id is not None:
                            clear_format_body = {'requests': [{'repeatCell': {
                                'range': {
                                    'sheetId': sheet_id,
                                    'startRowIndex': row_number - 1, 'endRowIndex': row_number,
                                    'startColumnIndex': 0, 'endColumnIndex': 8 # 8 колонок (A:H)
                                },
                                'cell': {'userEnteredFormat': {}},
                                'fields': 'userEnteredFormat'
                            }}]}
                            self._retry_request(
                                self.service.spreadsheets().batchUpdate,
                                spreadsheetId=self.sheet_id, body=clear_format_body
                            )
                    except Exception as e:
                        logger.warning(f"Не удалось очистить форматирование строки {row_number}: {e}")

            logger.info(f"Результат записан ({RESULTS_SHEET}) для telegram_id={telegram_id}")
        except Exception as e:
            logger.error(f"Ошибка записи результата ({RESULTS_SHEET}): {e}")
            raise

    def _get_sheet_id(self, sheet_name: str) -> Optional[int]:
        try:
            spreadsheet = self._retry_request(
                self.service.spreadsheets().get,
                spreadsheetId=self.sheet_id
            )
            for sheet in spreadsheet.get('sheets', []):
                if sheet.get('properties', {}).get('title') == sheet_name:
                    return sheet.get('properties', {}).get('sheetId')
            return None
        except Exception as e:
            logger.warning(f"Не удалось получить ID листа {sheet_name}: {e}")
            return None

    def get_campaign_statistics(
        self, campaign_name: Optional[str] = None
    ) -> List[CampaignStats]:
        """Get statistics for campaigns from Results sheet.

        Args:
            campaign_name: Optional campaign name to filter by.
                          If None, returns stats for all campaigns.

        Returns:
            List of CampaignStats objects
        """
        try:
            range_name = f"{RESULTS_SHEET}!A:H"  # 8 колонок
            result = self._retry_request(
                self.service.spreadsheets().values().get,
                spreadsheetId=self.sheet_id,
                range=range_name,
            )
            values = result.get("values", [])
            if len(values) < 2:
                return []

            headers = [h.lower().strip() for h in values[0]]
            try:
                campaign_col = headers.index("название кампании")  # Должна быть колонка H (индекс 7)
                status_col = headers.index("итоговый статус")      # Должна быть колонка G (индекс 6)
                correct_col = headers.index("количество верных ответов")  # Должна быть колонка E (индекс 4)
            except ValueError as e:
                logger.error(
                    f"В листе '{RESULTS_SHEET}' отсутствует обязательная "
                    f"колонка: {e}"
                )
                logger.error(f"Доступные заголовки: {headers}")
                return []

            # Group results by campaign
            campaign_data = {}
            for row in values[1:]:
                if len(row) <= max(campaign_col, status_col, correct_col):
                    continue

                c_name = row[campaign_col] if campaign_col < len(row) else ""
                if not c_name:
                    continue

                # Filter by campaign name if provided
                if campaign_name and c_name != campaign_name:
                    continue

                if c_name not in campaign_data:
                    campaign_data[c_name] = {
                        "total": 0,
                        "passed": 0,
                        "failed": 0,
                        "correct_answers": [],
                    }

                status = row[status_col] if status_col < len(row) else ""
                campaign_data[c_name]["total"] += 1

                if status == "успешно":
                    campaign_data[c_name]["passed"] += 1
                elif status == "не пройдено":
                    campaign_data[c_name]["failed"] += 1

                # Parse correct answers count
                try:
                    correct = (
                        int(row[correct_col]) if correct_col < len(row) else 0
                    )
                    campaign_data[c_name]["correct_answers"].append(correct)
                except (ValueError, IndexError):
                    pass

            # Build statistics list
            stats_list = []
            for c_name, data in campaign_data.items():
                total = data["total"]
                passed = data["passed"]
                failed = data["failed"]
                correct_answers = data["correct_answers"]

                pass_rate = (passed / total * 100) if total > 0 else 0.0
                avg_correct = (
                    sum(correct_answers) / len(correct_answers)
                    if correct_answers
                    else 0.0
                )

                stats_list.append(
                    CampaignStats(
                        campaign_name=c_name,
                        total_attempts=total,
                        passed_count=passed,
                        failed_count=failed,
                        pass_rate=pass_rate,
                        avg_correct_answers=avg_correct,
                    )
                )

            return stats_list
        except Exception as e:
            logger.error(f"Ошибка получения статистики кампаний: {e}", exc_info=True)
            return []
