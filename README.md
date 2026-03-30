# Telegram Quiz Bot

A configurable Telegram bot for conducting quizzes, with questions, settings, and results managed through a Google Sheet. The bot is designed to be robust, featuring timed questions, error limits, and a sophisticated question selection mechanism.

## Features

- **Dynamic Configuration**: All quiz parameters (number of questions, error limits, cooldown periods, etc.) are configured directly in a Google Sheet, allowing for easy adjustments without code changes.
- **Google Sheets Integration**: Seamlessly reads questions and settings from a Google Sheet and writes back the results in real-time.
- **Timed Questions**: Each question has a configurable time limit for answering.
- **Error Limiting**: The quiz automatically ends if a user exceeds the configured number of incorrect answers.
- **Cooldown Mechanism**: Prevents users from retaking the test for a configurable number of hours.
- **Proportional Question Distribution**: A smart algorithm selects questions proportionally from different categories, ensuring the quiz composition reflects the overall structure of the question database.
- **State Management**: Uses Redis to manage user sessions, making the quiz process resilient.
- **Easy Deployment**: Can be run locally with Python or as a containerized application using Docker and Docker Compose.

## How It Works

The bot guides the user through a structured quiz process from start to finish.

### 1. Starting the Quiz
- The user initiates the interaction with the `/start` command.
- The bot immediately reads the configuration from the "⚙️Настройки" (Settings) sheet in Google Sheets.
- It performs initial checks to ensure that the configuration is valid and that there are enough questions available to build a test.

### 2. User Identification
- The bot prompts the user to enter their full name (ФИО), which is required to proceed.
- The user confirms the entered name before the test begins.

### 3. Test Preparation
- **Cooldown Check**: The bot checks the "📊Результаты" (Results) sheet to find the user's last attempt. If the cooldown period (e.g., 24 hours) has not yet passed, the bot informs the user how much time is remaining.
- **Question Selection**: The core logic for question selection is triggered. The bot fetches all questions and distributes them according to the algorithm described below.
- **Session Creation**: A new quiz session is created and stored in Redis, containing the selected questions, user's score, and timers.

### 4. The Quiz
- The bot sends questions one by one, each with a custom inline keyboard for the answers.
- A timer runs for each question. If the user doesn't answer in time, the quiz ends.
- The bot tracks the number of correct and incorrect answers. If the user exceeds the maximum number of allowed errors, the quiz ends.

### 5. Finishing the Test
- Once the quiz is complete (either by answering all questions, running out of time, or making too many mistakes), the bot displays the final result.
- The result (user's name, Telegram ID, date, score, etc.) is written as a new row in the "📊Результаты" (Results) sheet.
- The user's session is cleared from Redis.

## Proportional Question Distribution

Instead of picking a fixed number of questions from a few random categories, the bot uses a more balanced and fair algorithm:

1.  **Grouping**: All questions from the "❓Вопросы" (Questions) sheet are grouped by their specified category.
2.  **Proportional Quotas**: The bot calculates a "quota" for each category based on its share of the total number of questions. For example, if 50% of all questions belong to "Category A", then approximately 50% of the questions in the quiz will be drawn from "Category A".
3.  **Adjustment**: The algorithm intelligently handles cases where a category might not have enough questions to fulfill its quota, borrowing the deficit from other available categories.
4.  **Random Selection**: Once quotas are determined, the bot randomly selects the required number of questions from each category.
5.  **Final Shuffle**: The final list of selected questions is shuffled to ensure a random order for the user.

This approach ensures that the quiz is always a representative sample of the entire question database, automatically adapting as new questions or categories are added.

## Google Sheets Setup

To use the bot, you need to create a Google Sheet with three specific tabs:

### 1. `⚙️Настройки` (Settings)
This sheet holds the main configuration. It must contain a header row and a data row.

| количество вопросов | количество допустимых ошибок | как часто можно проходить тест (часов) | количество секунд на одно задание |
| ------------------- | ---------------------------- | -------------------------------------- | --------------------------------- |
| 20                  | 2                            | 24                                     | 60                                |

- `количество вопросов`: Total questions per quiz.
- `количество допустимых ошибок`: Max incorrect answers allowed.
- `как часто можно проходить тест (часов)`: Cooldown period in hours.
- `количество секунд на одно задание`: Time limit per question in seconds.

### 2. `❓Вопросы` (Questions)
This sheet contains the question database.

| Категория | Вопрос | Ответ 1 | Ответ 2 | Ответ 3 | Ответ 4 | Правильный ответ |
| --------- | ------ | ------- | ------- | ------- | ------- | ---------------- |
| History   | ...    | ...     | ...     | ...     | ...     | 2                |
| Geography | ...    | ...     | ...     | ...     | ...     | 4                |

- `Категория`: The category of the question.
- `Вопрос`: The text of the question.
- `Ответ 1` - `Ответ 4`: The answer options.
- `Правильный ответ`: A number from 1 to 4 indicating the correct answer column.

### 3. `📊Результаты` (Results)
This sheet is where the bot writes the quiz results. The bot will create and populate this sheet automatically. The columns are:

- Telegram ID
- Display Name (Username or First/Last Name)
- Test Date
- Full Name (from user input)
- Result (Passed/Failed)
- Correct Count
- Notes (e.g., if the test timed out)

## Installation and Setup

### Prerequisites
- Python 3.9+
- Docker and Docker Compose (for containerized deployment)
- A Telegram Bot Token
- Google Cloud Service Account credentials with access to the Google Sheets API.

### 1. Clone the Repository
```bash
git clone <repository-url>
cd quiz_bot
```

### 2. Configure Environment Variables
Create a `.env` file in the project root and fill it with your credentials:

```env
# --- Telegram ---
TELEGRAM_TOKEN="YOUR_TELEGRAM_BOT_TOKEN"

# --- Google Sheets ---
SHEET_ID="YOUR_GOOGLE_SHEET_ID"
# Your Google credentials JSON, as a single line string or a path to the file.
GOOGLE_CREDENTIALS='{"type": "service_account", "project_id": "...", ...}'

# --- Redis ---
REDIS_URL="redis://redis:6379/0"

# --- Logging ---
LOG_LEVEL="INFO"

# --- PlanDriver ---
PLANDRIVER_ENABLED=true
PLANDRIVER_BASE_URL="https://prog.lagrangegroup.ru"
PLANDRIVER_TOKEN="YOUR_PLANDRIVER_BOT_TOKEN"
PLANDRIVER_POLL_INTERVAL_MINUTES=5
PLANDRIVER_DB_PATH="/absolute/path/to/plandriver.db"
# Fill after confirming real question categories in Google Sheets
PLANDRIVER_TEST_MAPPING='{}'
```

### 3. Running the Bot

#### Option A: With Docker (Recommended)
This is the easiest way to get the bot and its Redis dependency running.

```bash
docker-compose up --build
```

To run in the background:
```bash
docker-compose up -d --build
```

#### Option B: Locally with Python
1.  **Set up a virtual environment:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```
2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
3.  **Run a local Redis instance:**
    You need a running Redis server. If you have Docker, you can run:
    ```bash
    docker run -d -p 6379:6379 redis:alpine
    ```
    Then, update your `REDIS_URL` in the `.env` file to `redis://localhost:6379/0`.

4.  **Start the bot:**
    ```bash
    python main.py
    ```

## PlanDriver Integration

To verify the backend path to PlanDriver, the bot only needs valid env values and access to the target Google Sheet.

Minimal steps:

1. Set `PLANDRIVER_ENABLED=true`
2. Fill `PLANDRIVER_TOKEN`
3. Start the bot
4. Watch logs for:
   - successful `PlanDriver Sync`
   - response from `GET /api/bot/pending-tests`
   - driver mapping result
   - assignment creation in local SQLite
   - Telegram delivery result

At first start, `PLANDRIVER_TEST_MAPPING='{}'` is acceptable if the goal is only to confirm requests to the external backend. A real test launch still requires confirmed question categories in Google Sheets.

## Project Structure

- `main.py`: The main entry point for the application.
- `config.py`: Handles loading and validation of environment variables.
- `handlers/`: Contains the bot's command and message handlers (e.g., `start`, FIO processing, test logic).
- `services/`: Contains services for interacting with external systems like Google Sheets (`google_sheets.py`) and Redis (`redis_service.py`).
- `utils/`: Includes utility functions, such as the question distribution algorithm.
- `models.py`: Defines the data structures (dataclasses) used throughout the application.
- `docker-compose.yml`: Defines the services for containerized deployment (the bot and Redis).
- `Dockerfile`: Instructions for building the bot's Docker image.
- `requirements.txt`: A list of Python dependencies.
