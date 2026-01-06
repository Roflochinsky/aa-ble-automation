"""
Property-based tests for ConfigManager.

Feature: aa-ble-automation, Property 10: Missing config termination
Validates: Requirements 5.5
"""

import os
import pytest
from hypothesis import given, strategies as st, settings
from unittest.mock import patch


# Список всех обязательных параметров конфигурации
REQUIRED_PARAMS = [
    'GOOGLE_CREDENTIALS_PATH',
    'GDRIVE_INPUT_FOLDER_ID',
    'GDRIVE_OUTPUT_FOLDER_ID',
    'GSHEETS_BLE_JOURNAL_ID',
    'GSHEETS_BLE_JOURNAL_SHEET',
    'GSHEETS_PEOPLE_MAPPING_ID',
    'GSHEETS_PEOPLE_MAPPING_SHEET',
    'TELEGRAM_BOT_TOKEN',
    'TELEGRAM_CHAT_ID',
]


def create_valid_env() -> dict:
    """Создаёт полный набор валидных переменных окружения."""
    return {
        'GOOGLE_CREDENTIALS_PATH': 'credentials.json',
        'GDRIVE_INPUT_FOLDER_ID': 'folder_id_123',
        'GDRIVE_OUTPUT_FOLDER_ID': 'folder_id_456',
        'GSHEETS_BLE_JOURNAL_ID': 'sheet_id_789',
        'GSHEETS_BLE_JOURNAL_SHEET': 'Sheet1',
        'GSHEETS_PEOPLE_MAPPING_ID': 'sheet_id_012',
        'GSHEETS_PEOPLE_MAPPING_SHEET': 'Sheet1',
        'TELEGRAM_BOT_TOKEN': '123456:ABC-token',
        'TELEGRAM_CHAT_ID': '-1001234567890',
        'TIME_WINDOW_START': '6:00',
        'TIME_WINDOW_END': '21:00',
        'ROW_HEIGHT': '60',
    }


@settings(max_examples=100)
@given(missing_param_indices=st.lists(
    st.integers(min_value=0, max_value=len(REQUIRED_PARAMS) - 1),
    min_size=1,
    max_size=len(REQUIRED_PARAMS),
    unique=True
))
def test_missing_config_termination(missing_param_indices):
    """
    Feature: aa-ble-automation, Property 10: Missing config termination
    Validates: Requirements 5.5
    
    For any configuration missing a required parameter, 
    the system SHALL terminate with exit code 1.
    """
    from src.config import ConfigManager
    
    # Создаём валидное окружение
    env = create_valid_env()
    
    # Удаляем выбранные обязательные параметры
    missing_params = [REQUIRED_PARAMS[i] for i in missing_param_indices]
    for param in missing_params:
        env.pop(param, None)
    
    # Патчим os.getenv чтобы возвращать наши значения
    def mock_getenv(key, default=None):
        return env.get(key, default)
    
    # Патчим load_dotenv чтобы не загружать реальный .env файл
    with patch('src.config.load_dotenv'):
        with patch('os.getenv', side_effect=mock_getenv):
            with pytest.raises(SystemExit) as exc_info:
                ConfigManager.load()
            
            # Проверяем, что exit code равен 1
            assert exc_info.value.code == 1


def test_valid_config_loads_successfully():
    """
    Проверяет, что при наличии всех обязательных параметров
    конфигурация загружается успешно.
    """
    from src.config import ConfigManager
    
    env = create_valid_env()
    
    def mock_getenv(key, default=None):
        return env.get(key, default)
    
    with patch('src.config.load_dotenv'):
        with patch('os.getenv', side_effect=mock_getenv):
            config = ConfigManager.load()
            
            assert config.google_credentials_path == 'credentials.json'
            assert config.gdrive_input_folder_id == 'folder_id_123'
            assert config.telegram_bot_token == '123456:ABC-token'
            assert config.time_window_start == (6, 0)
            assert config.time_window_end == (21, 0)
            assert config.row_height == 60


@settings(max_examples=100)
@given(empty_value=st.sampled_from(['', '   ', '\t', '\n']))
def test_empty_or_whitespace_config_terminates(empty_value):
    """
    Feature: aa-ble-automation, Property 10: Missing config termination
    Validates: Requirements 5.5
    
    For any configuration with empty or whitespace-only required parameter,
    the system SHALL terminate with exit code 1.
    """
    from src.config import ConfigManager
    
    env = create_valid_env()
    # Устанавливаем пустое значение для первого обязательного параметра
    env['GOOGLE_CREDENTIALS_PATH'] = empty_value
    
    def mock_getenv(key, default=None):
        return env.get(key, default)
    
    with patch('src.config.load_dotenv'):
        with patch('os.getenv', side_effect=mock_getenv):
            with pytest.raises(SystemExit) as exc_info:
                ConfigManager.load()
            
            assert exc_info.value.code == 1
