"""
ConfigManager - Управление конфигурацией приложения AA_BLE Automation.

Поддерживает:
- Глобальные настройки из .env
- Множество площадок из facilities_config.json
"""

import json
import os
import sys
from dataclasses import dataclass, field
from typing import Optional, Tuple, List
from dotenv import load_dotenv


@dataclass
class FacilityConfig:
    """Конфигурация одной площадки."""
    name: str
    input_folder_id: str
    enabled: bool = True
    
    def __post_init__(self):
        """Валидация после инициализации."""
        if not self.name:
            raise ValueError("Facility name cannot be empty")


@dataclass
class ConfigManager:
    """Загрузка и валидация конфигурации."""
    
    # Google Cloud
    google_credentials_path: str
    
    # Google Sheets (общие справочники)
    gsheets_ble_journal_id: str
    gsheets_ble_journal_sheet: str
    gsheets_people_mapping_id: str
    gsheets_people_mapping_sheet: str
    
    # Telegram
    telegram_bot_token: str
    telegram_chat_id: str
    
    # Processing
    time_window_start: Tuple[int, int]
    time_window_end: Tuple[int, int]
    row_height: int
    
    # Площадки
    facilities: List[FacilityConfig] = field(default_factory=list)
    
    # Обязательные параметры (глобальные)
    REQUIRED_GLOBAL_PARAMS = [
        'google_credentials_path',
        'telegram_bot_token',
        'telegram_chat_id',
        'gsheets_ble_journal_id',
        'gsheets_people_mapping_id',
    ]
    
    @classmethod
    def load(
        cls, 
        env_path: Optional[str] = None,
        facilities_config_path: str = 'facilities_config.json'
    ) -> 'ConfigManager':
        """
        Загрузка конфигурации из .env и facilities_config.json.
        
        Args:
            env_path: Путь к .env файлу
            facilities_config_path: Путь к JSON-файлу с конфигурацией площадок
            
        Returns:
            ConfigManager с загруженной конфигурацией
            
        Raises:
            SystemExit: При отсутствии обязательных параметров (exit code 1)
        """
        # Загружаем .env
        load_dotenv(dotenv_path=env_path)
        
        # Загружаем facilities_config.json
        facilities_data = cls._load_facilities_config(facilities_config_path)
        global_config = facilities_data.get('global', {})
        facilities_list = facilities_data.get('facilities', [])
        
        # Приоритет: .env > sites_config.json > defaults
        def get_param(env_key: str, json_key: str, default: str = '') -> str:
            return os.getenv(env_key) or global_config.get(json_key, default)
        
        # Собираем конфигурацию
        google_credentials_path = get_param(
            'GOOGLE_CREDENTIALS_PATH', 'google_credentials_path', 'credentials.json'
        )
        telegram_bot_token = get_param('TELEGRAM_BOT_TOKEN', 'telegram_bot_token')
        telegram_chat_id = get_param('TELEGRAM_CHAT_ID', 'telegram_chat_id')
        gsheets_ble_journal_id = get_param('GSHEETS_BLE_JOURNAL_ID', 'gsheets_ble_journal_id')
        gsheets_ble_journal_sheet = get_param(
            'GSHEETS_BLE_JOURNAL_SHEET', 'gsheets_ble_journal_sheet', 'Sheet1'
        )
        gsheets_people_mapping_id = get_param('GSHEETS_PEOPLE_MAPPING_ID', 'gsheets_people_mapping_id')
        gsheets_people_mapping_sheet = get_param(
            'GSHEETS_PEOPLE_MAPPING_SHEET', 'gsheets_people_mapping_sheet', 'Sheet1'
        )
        
        # Временное окно
        time_start_str = get_param('TIME_WINDOW_START', 'time_window_start', '6:00')
        time_end_str = get_param('TIME_WINDOW_END', 'time_window_end', '23:00')
        time_window_start = cls._parse_time(time_start_str)
        time_window_end = cls._parse_time(time_end_str)
        
        # Высота строки
        row_height_str = get_param('ROW_HEIGHT', 'row_height', '60')
        row_height = int(row_height_str)
        
        # Парсим площадки
        facilities = []
        for facility_data in facilities_list:
            try:
                facility = FacilityConfig(
                    name=facility_data.get('name', ''),
                    input_folder_id=facility_data.get('input_folder_id', ''),
                    enabled=facility_data.get('enabled', True),
                )
                facilities.append(facility)
            except ValueError as e:
                print(f"WARNING: Skipping invalid facility config: {e}", file=sys.stderr)
        
        config = cls(
            google_credentials_path=google_credentials_path,
            telegram_bot_token=telegram_bot_token,
            telegram_chat_id=telegram_chat_id,
            gsheets_ble_journal_id=gsheets_ble_journal_id,
            gsheets_ble_journal_sheet=gsheets_ble_journal_sheet,
            gsheets_people_mapping_id=gsheets_people_mapping_id,
            gsheets_people_mapping_sheet=gsheets_people_mapping_sheet,
            time_window_start=time_window_start,
            time_window_end=time_window_end,
            row_height=row_height,
            facilities=facilities,
        )
        
        # Валидация
        missing = config._validate_required()
        if missing:
            error_msg = f"Missing required configuration: {', '.join(missing)}"
            print(f"ERROR: {error_msg}", file=sys.stderr)
            sys.exit(1)
        
        return config
    
    @classmethod
    def _load_facilities_config(cls, path: str) -> dict:
        """Загрузка JSON-конфигурации площадок."""
        if not os.path.exists(path):
            print(f"WARNING: Facilities config not found at {path}, using empty config", 
                  file=sys.stderr)
            return {'global': {}, 'facilities': []}
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            print(f"ERROR: Invalid JSON in {path}: {e}", file=sys.stderr)
            sys.exit(1)
    
    @staticmethod
    def _parse_time(time_str: str) -> Tuple[int, int]:
        """Парсит строку времени в формате HH:MM."""
        parts = time_str.strip().split(':')
        hours = int(parts[0])
        minutes = int(parts[1]) if len(parts) > 1 else 0
        return (hours, minutes)
    
    def _validate_required(self) -> List[str]:
        """Проверка обязательных параметров."""
        missing = []
        
        if not self.google_credentials_path:
            missing.append('google_credentials_path')
        if not self.telegram_bot_token:
            missing.append('telegram_bot_token')
        if not self.telegram_chat_id:
            missing.append('telegram_chat_id')
        if not self.gsheets_ble_journal_id:
            missing.append('gsheets_ble_journal_id')
        if not self.gsheets_people_mapping_id:
            missing.append('gsheets_people_mapping_id')
        
        return missing
    
    def get_enabled_facilities(self) -> List[FacilityConfig]:
        """Возвращает список активных площадок."""
        return [f for f in self.facilities if f.enabled]
    
    def get_facility_by_name(self, name: str) -> Optional[FacilityConfig]:
        """Поиск площадки по имени."""
        for facility in self.facilities:
            if facility.name == name:
                return facility
        return None
    
    def validate(self) -> bool:
        """Полная валидация конфигурации."""
        # Проверяем глобальные параметры
        if self._validate_required():
            return False
        
        # Проверяем временное окно
        if not (0 <= self.time_window_start[0] <= 23):
            return False
        if not (0 <= self.time_window_end[0] <= 23):
            return False
        
        # Проверяем высоту строки
        if self.row_height <= 0:
            return False
        
        # Проверяем что есть хотя бы одна активная площадка
        if not self.get_enabled_facilities():
            return False
        
        # Проверяем что у активных площадок заполнены folder_id
        for facility in self.get_enabled_facilities():
            if not facility.input_folder_id:
                return False
        
        return True
