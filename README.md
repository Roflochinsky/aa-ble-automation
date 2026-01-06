# AA_BLE Automation

Автоматизация отчётов AA_BLE для анализа присутствия сотрудников по данным BLE-маячков.

## Возможности

- 📥 Автоматическая загрузка файлов AA_BLE из множества папок Google Drive
- 📊 Получение справочных данных (Журнал BLE, Привязка людей) из Google Sheets
- 📈 Генерация интерактивных SVG-таймлайнов с динамическим viewport
- 🎯 KPI-карточки: Зоны работ / Перерывы / Вне BLE / Прочее
- 📋 Accordion с детализацией по зонам и меткам
- 📤 **Отправка HTML-отчётов через Telegram-бота**
- 🏢 Поддержка множества площадок

## Быстрый старт

### 1. Установка

```bash
git clone <repository-url>
cd aa-ble-automation

python -m venv venv
source venv/bin/activate  # Linux/macOS
# или
.\venv\Scripts\activate   # Windows

pip install -r requirements.txt
```

### 2. Настройка

```bash
cp .env.example .env
cp facilities_config.json facilities_config.local.json
```

Заполните:
- `.env` — глобальные настройки (Google credentials, Telegram, справочники)
- `facilities_config.json` — конфигурация площадок (папки GDrive)

### 3. Запуск

```bash
# Обработка всех активных площадок за сегодня
python -m src.main

# Обработка за конкретную дату
python -m src.main --date 24.12.2025

# Обработка за диапазон дат
python -m src.main --date-from 20.12.2025 --date-to 24.12.2025

# Обработка конкретных площадок
python -m src.main --facilities MAGNIT_AUTO MAGNIT_BUTOVO
```

## Конфигурация

### .env (глобальные настройки)

```ini
GOOGLE_CREDENTIALS_PATH=credentials.json
TELEGRAM_BOT_TOKEN=123456:ABC...
TELEGRAM_CHAT_ID=-100123456789
GSHEETS_BLE_JOURNAL_ID=1abc...
GSHEETS_PEOPLE_MAPPING_ID=1def...
```

### facilities_config.json (площадки)

```json
{
  "global": {
    "telegram_bot_token": "...",
    "telegram_chat_id": "...",
    "gsheets_ble_journal_id": "...",
    "gsheets_people_mapping_id": "..."
  },
  "facilities": [
    {
      "name": "MAGNIT_AUTO",
      "input_folder_id": "1abc...",
      "enabled": true
    },
    {
      "name": "MAGNIT_BUTOVO",
      "input_folder_id": "2ghi...",
      "enabled": true
    }
  ]
}
```

## Выходные файлы

Отчёты отправляются в Telegram-чат как документы:
- Имя файла: `AA_BLE_{facility_name}_{dd-mm-yyyy}.html`
- Пример: `AA_BLE_MAGNIT_AUTO_24-12-2025.html`

> 💡 Если Telegram недоступен, файлы сохраняются локально.

## Структура проекта

```
aa-ble-automation/
├── src/
│   ├── main.py              # Главный оркестратор
│   ├── config.py            # Управление конфигурацией
│   ├── clients/
│   │   ├── gdrive.py        # Клиент Google Drive
│   │   ├── gsheets.py       # Клиент Google Sheets
│   │   └── telegram.py      # Telegram (логи + отчёты)
│   ├── processing/
│   │   ├── loader.py        # Загрузка данных
│   │   ├── processor.py     # Обработка данных
│   │   └── timeline.py      # Построение таймлайнов
│   └── reports/
│       └── svg_generator.py # SVG-таймлайны
├── facilities_config.json   # Конфигурация площадок
├── .env.example
├── requirements.txt
├── Dockerfile
└── docker-compose.yml
```

## Docker

```bash
docker-compose up --build
```

## Тестирование

```bash
pytest -v
```

## Документация

- [Руководство по развёртыванию](docs/DEPLOYMENT.md)
