# PlanDriver Traceability

Документ фиксирует соответствие текущей реализации требованиям из `SPEC.md` и контракту из `bot-integration.md`.

## SPEC.md

### 1. Текущее состояние / не ломать существующую логику тестов

Статус: реализовано

Как обеспечено:

- существующие сценарии `/start`, регистрации, основного теста и кампаний сохранены
- PlanDriver встроен отдельным модулем
- назначение PlanDriver приходит отдельным сообщением, а не через переписывание основного меню

Код:

- `handlers/common.py`
- `handlers/test.py`
- `services/plandriver/`

### 2. Добавить новый модуль PlanDriver Integration Module

Статус: реализовано

Код:

- `services/plandriver/plandriver_client.py`
- `services/plandriver/plandriver_sync.py`
- `services/plandriver/plandriver_mapper.py`
- `services/plandriver/plandriver_result_sender.py`
- `services/plandriver/plandriver_storage.py`

### 3. Архитектура: polling -> модуль PlanDriver -> core bot logic

Статус: реализовано

Как обеспечено:

- polling запускается через scheduler
- sync логика живёт отдельно от Telegram handlers
- текущая система тестов используется повторно

Код:

- `services/scheduler.py`
- `services/plandriver/plandriver_sync.py`

### 4. Отдельный модуль `services/plandriver/`

Статус: реализовано

### 5. Получение заданий через polling `GET /api/bot/pending-tests`

Статус: реализовано

Код:

- `services/plandriver/plandriver_client.py`
- `services/plandriver/plandriver_sync.py`
- `services/scheduler.py`

### 6. Для каждого нового нарушения:

- проверить, не обрабатывалось ли ранее
- найти пользователя в Telegram
- определить тест по `violation_type_code`
- запустить тест через текущую систему

Статус: реализовано в коде, требует живой проверки

Код:

- `services/plandriver/plandriver_storage.py`
- `services/plandriver/plandriver_sync.py`
- `services/plandriver/plandriver_mapper.py`
- `handlers/common.py`
- `handlers/test.py`

### 7. Маппинг `violation_type_code -> тест`

Статус: механизм реализован, реальные данные не заполнены

Как обеспечено:

- маппинг хранится в `PLANDRIVER_TEST_MAPPING`
- значения маппятся в категории вопросов

Блокер:

- нет подтверждённых реальных категорий вопросов

### 8. Маппинг водителей `driver_id ↔ telegram_id`

Статус: реализовано

Как обеспечено:

- локальная таблица `driver_mapping`
- поиск по `personnel_number`, если колонка уже есть в Google Sheets
- fallback по точному ФИО

Код:

- `services/plandriver/plandriver_storage.py`
- `services/plandriver/plandriver_sync.py`
- `services/google_sheets.py`

### 9. Защита от дублей `external_violations`

Статус: реализовано

Как обеспечено:

- ключ по `violation_id`
- локальные статусы `new / sent / completed`

### 10. Завершение теста и отправка результата

Статус: реализовано в коде, требует живой проверки

Код:

- `handlers/test.py`
- `services/plandriver/plandriver_result_sender.py`

### 11. Логика результатов

- `passed=true` -> нарушение закрывается
- `passed=false` -> нарушение остаётся активным
- повторные отправки допустимы

Статус: реализовано в коде, требует живой проверки

Как обеспечено:

- при `passed=true` локальный статус `completed`
- при `passed=false` локальный статус `sent`
- повторная отправка допускается контрактом backend

### 12. Обработка ошибок

Статус: реализовано частично в коде, требует живой проверки

Покрыто:

- водитель не найден
- duplicate violation
- timeout
- ошибка отправки результата
- API PlanDriver недоступен

Требует реальной проверки:

- поведение на боевых данных и ошибках сети/API

### 13. Ограничения

Статус: соблюдено

Проверка:

- текущая система тестов не переписана
- основной UX не заменён
- webhook не добавлялся
- критические нарушения отдельно не обрабатываются

### 14. Требования к реализации

Статус: реализовано в коде

Проверка:

- используется текущий backend проекта
- реализация модульная
- Bearer авторизация есть
- polling worker есть
- идемпотентность учтена
- логирование действий есть

### 15. Результат

Статус: требует живой проверки

Причина:

- код готов, но сценарий ещё не прогнан на реальном backend и реальных данных

### 16. Ключевой принцип: интеграция не должна ломать текущую логику

Статус: соблюдено

## bot-integration.md

### Base URL и Bearer token

Статус: реализовано

Код:

- `services/plandriver/plandriver_client.py`
- `config.py`

### `GET /api/bot/pending-tests`

Статус: реализовано

Используемые поля:

- `driver_id`
- `driver_name`
- `personnel_number`
- `attestation_id`
- `deadline`
- `violations[].violation_id`
- `violations[].violation_type_code`
- `violations[].violation_type_name`
- `violations[].comment`

### `POST /api/bot/test-result`

Статус: реализовано

Отправляем:

- `driver_id`
- `attestation_id`
- `results[].violation_id`
- `results[].violation_type_code`
- `results[].passed`
- `results[].score`
- `results[].completed_at`
- `all_passed`

### Критические нарушения не попадают в `pending-tests`

Статус: учтено архитектурно

Как обеспечено:

- бот работает только с тем, что пришло из `pending-tests`

### `personnel_number` есть в контракте Driver

Статус: учтено

Как обеспечено:

- при наличии соответствующей колонки в Google Sheets маппинг использует `personnel_number` первым

## Остаток До Готовности

- получить реальный `PLANDRIVER_TOKEN`
- получить реальные категории вопросов из Google Sheets
- заполнить `PLANDRIVER_TEST_MAPPING`
- прогнать живой сценарий end-to-end
