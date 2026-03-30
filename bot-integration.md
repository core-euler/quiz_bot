# Интеграция Telegram-бота тестирования с PlanDriver

## Общая схема

PlanDriver — система управления водительским составом. Бот тестирования — внешний сервис, который опрашивает API PlanDriver, получает задания на тестирование водителей и возвращает результаты. PlanDriver ничего не отправляет в бота — только отвечает на запросы.

**Base URL:** `https://prog.lagrangegroup.ru`
**OpenAPI документация:** https://prog.lagrangegroup.ru/docs
**Авторизация:** `Authorization: Bearer {token}` (Laravel Sanctum)

---

## Бизнес-логика

Водители получают нарушения. Нарушения делятся на **критические** и **некритические**.

### Некритические нарушения

```
Нарушение зафиксировано
  → Водитель ставится на контроль, создаётся аттестация (pending, дедлайн 7 дней)
  → Бот при polling видит задание → отправляет водителю тест по типу нарушения
  → Водитель проходит тест:
      Пройден → нарушение удаляется
        → Нарушений не осталось → аттестация passed, водитель снят с контроля
        → Ещё есть нарушения → ждём прохождения остальных тестов
      Не пройден → можно пересдать до дедлайна
  → Дедлайн истёк, тест не пройден:
      → Водитель в "список на разбор" (reason: test_not_passed)
```

### Критические нарушения

Критические типы: `DRUNK` (алкогольное опьянение), `INSUBORDINATION` (неподчинение).

```
Нарушение зафиксировано
  → Водитель сразу попадает в "список на разбор" (reason: critical_violation)
  → Разбор в комиссии (дедлайн 14 дней):
      Прошёл → водитель остаётся
      Не прошёл или дедлайн истёк → увольнение
```

Критические нарушения **не попадают** в `GET /api/bot/pending-tests` — бот с ними не работает.

---

## API для бота

### GET /api/bot/pending-tests

Возвращает водителей с pending-аттестациями и некритическими нарушениями.

**Query-параметры:**
- `driver_id` (int, опционально) — фильтр по конкретному водителю

**Пример ответа:**

```json
{
  "data": [
    {
      "driver_id": 42,
      "driver_name": "Иванов Иван Иванович",
      "personnel_number": "12345",
      "column_name": "Колонна №3",
      "attestation_id": 15,
      "deadline": "2026-03-20",
      "violations": [
        {
          "violation_id": 101,
          "violation_type_code": "LATE",
          "violation_type_name": "Опоздание",
          "event_date": "2026-03-10",
          "comment": "Опоздание на 30 минут"
        },
        {
          "violation_id": 98,
          "violation_type_code": "TRAFFIC_VIOLATION",
          "violation_type_name": "Нарушение ПДД",
          "event_date": "2026-03-05",
          "comment": ""
        }
      ]
    }
  ]
}
```

Если заданий нет — `"data": []`.

---

### POST /api/bot/test-result

Принимает результаты тестирования. Пройденные нарушения удаляются из системы. Если у водителя не осталось нарушений — аттестация автоматически закрывается как пройденная.

**Запрос:**

```json
{
  "driver_id": 42,
  "attestation_id": 15,
  "results": [
    {
      "violation_id": 101,
      "violation_type_code": "LATE",
      "passed": true,
      "score": 85,
      "completed_at": "2026-03-15T14:30:00Z"
    },
    {
      "violation_id": 98,
      "violation_type_code": "TRAFFIC_VIOLATION",
      "passed": false,
      "score": 40,
      "completed_at": "2026-03-15T14:45:00Z"
    }
  ],
  "all_passed": false
}
```

| Поле | Тип | Обязательно | Описание |
|------|-----|-------------|----------|
| `driver_id` | int | да | ID водителя |
| `attestation_id` | int | да | ID аттестации |
| `results` | array | да | Массив результатов |
| `results[].violation_id` | int | да | ID нарушения |
| `results[].violation_type_code` | string | да | Код типа нарушения |
| `results[].passed` | bool | да | Тест пройден |
| `results[].score` | int | нет | Балл (0-100) |
| `results[].completed_at` | string | нет | ISO 8601 дата/время |
| `all_passed` | bool | да | Все тесты пройдены |

**Ответ:**

```json
{
  "status": "ok",
  "violations_cleared": [101],
  "violations_remaining": [98],
  "attestation_status": "pending",
  "message": "1 тест(ов) принято, осталось нарушений: 1"
}
```

Когда все нарушения закрыты:

```json
{
  "status": "ok",
  "violations_cleared": [101, 98],
  "violations_remaining": [],
  "attestation_status": "passed",
  "message": "Все тесты пройдены, аттестация завершена"
}
```

Эндпоинт идемпотентен — повторная отправка того же результата не вызывает ошибку.

---

## Авторизация

Для бота создаётся сервисный API-токен. Токен передаётся в заголовке каждого запроса:

```
Authorization: Bearer {BOT_TOKEN}
```

---

## Типы нарушений

| code | name | is_critical |
|------|------|-------------|
| `LATE` | Опоздание | нет |
| `ABSENCE` | Неявка | нет |
| `DRUNK` | Алкогольное опьянение | **да** |
| `TRAFFIC_VIOLATION` | Нарушение ПДД | нет |
| `VEHICLE_DAMAGE` | Повреждение ТС | нет |
| `CARGO_DAMAGE` | Повреждение груза | нет |
| `INSUBORDINATION` | Неподчинение | **да** |
| `DOCUMENTATION` | Нарушение документооборота | нет |
| `OTHER` | Прочее | нет |

Актуальный список: `GET /api/violation-types` (публичный, без авторизации).

---

## Сценарий работы бота

```
1. Бот запрашивает GET /api/bot/pending-tests
2. Для каждого задания находит водителя в Telegram
   (маппинг driver_id ↔ Telegram ведётся на стороне бота)
3. Отправляет водителю тест по violation_type_code
4. Водитель проходит тест → бот отправляет POST /api/bot/test-result
5. Повтор
```

Рекомендуемый интервал polling: 1-5 минут.

---

## Модель данных (справочно)

### Driver

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | int | PK |
| `full_name` | string | ФИО |
| `personnel_number` | string | Табельный номер (уникальный) |
| `column_id` | int | Колонна (подразделение) |
| `status` | string | `working` / `fired` |

Модель Driver не содержит `telegram_id` — привязка к Telegram на стороне бота.

### Violation

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | int | PK |
| `driver_id` | int | FK → Driver |
| `violation_type_id` | int | FK → ViolationType |
| `event_date` | date | Дата нарушения |
| `comment` | text | Комментарий |

### Дополнительные эндпоинты

| Метод | URL | Описание |
|-------|-----|----------|
| `GET` | `/api/drivers` | Список водителей |
| `GET` | `/api/drivers/{id}` | Карточка водителя |
| `GET` | `/api/violations` | Список нарушений |
| `GET` | `/api/violation-types` | Справочник типов нарушений |
| `GET` | `/api/attestations` | Список аттестаций |

Полная документация: https://prog.lagrangegroup.ru/docs
