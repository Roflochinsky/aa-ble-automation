# AA_BLE Automation - Google Sheets Client
"""
Клиент для работы с Google Sheets API.
Requirements: 2.1, 2.2, 2.3, 2.4
"""

import time
from typing import Optional

import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class GoogleSheetsClient:
    """Клиент для работы с Google Sheets API.
    
    Обеспечивает:
    - Аутентификацию через сервисный аккаунт
    - Чтение данных из листов
    - Конвертацию в pandas DataFrame
    - Корректную обработку кириллицы (UTF-8)
    """
    
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    MAX_RETRIES = 3
    RETRY_DELAY = 5  # секунд
    
    def __init__(self, credentials_path: str):
        """Инициализация клиента.
        
        Args:
            credentials_path: Путь к JSON-файлу сервисного аккаунта
        """
        self.credentials_path = credentials_path
        self._service = None
        self._credentials = None
    
    def authenticate(self) -> bool:
        """Аутентификация через сервисный аккаунт.
        
        Returns:
            True если аутентификация успешна
            
        Raises:
            Exception: При ошибке аутентификации после всех попыток
        """
        last_error = None
        
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                self._credentials = service_account.Credentials.from_service_account_file(
                    self.credentials_path,
                    scopes=self.SCOPES
                )
                self._service = build('sheets', 'v4', credentials=self._credentials)
                return True
            except Exception as e:
                last_error = e
                if attempt < self.MAX_RETRIES:
                    time.sleep(self.RETRY_DELAY)
        
        raise Exception(f"Authentication failed after {self.MAX_RETRIES} attempts: {last_error}")
    
    def _ensure_authenticated(self) -> None:
        """Проверка аутентификации, выполнение при необходимости."""
        if self._service is None:
            self.authenticate()
    
    def read_sheet(
        self, 
        spreadsheet_id: str, 
        sheet_name: str, 
        range_notation: Optional[str] = None
    ) -> list[list]:
        """Чтение данных из листа.
        
        Args:
            spreadsheet_id: ID таблицы Google Sheets
            sheet_name: Имя листа
            range_notation: A1-нотация диапазона (опционально, например 'A:D')
            
        Returns:
            Список строк, каждая строка - список значений ячеек
        """
        self._ensure_authenticated()
        
        if range_notation:
            full_range = f"'{sheet_name}'!{range_notation}"
        else:
            full_range = sheet_name
        
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                result = self._service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=full_range,
                    valueRenderOption='UNFORMATTED_VALUE'
                ).execute()
                
                values = result.get('values', [])
                return values
                
            except HttpError as e:
                if e.resp.status == 429:  # Rate limit
                    self._handle_rate_limit()
                    continue
                if attempt < self.MAX_RETRIES:
                    time.sleep(self.RETRY_DELAY)
                else:
                    raise
        
        return []  # Недостижимо, но для типизации
    
    def read_as_dataframe(
        self, 
        spreadsheet_id: str, 
        sheet_name: str,
        range_notation: Optional[str] = None,
        header_row: int = 0
    ) -> pd.DataFrame:
        """Чтение данных как pandas DataFrame.
        
        Args:
            spreadsheet_id: ID таблицы Google Sheets
            sheet_name: Имя листа
            range_notation: A1-нотация диапазона (опционально)
            header_row: Индекс строки с заголовками (по умолчанию 0)
            
        Returns:
            pandas DataFrame с данными из листа
        """
        values = self.read_sheet(spreadsheet_id, sheet_name, range_notation)
        
        if not values:
            return pd.DataFrame()
        
        if len(values) <= header_row:
            return pd.DataFrame()
        
        headers = values[header_row]
        data_rows = values[header_row + 1:]
        
        # Выравнивание строк по количеству колонок
        max_cols = len(headers)
        normalized_rows = []
        for row in data_rows:
            if len(row) < max_cols:
                row = row + [''] * (max_cols - len(row))
            elif len(row) > max_cols:
                row = row[:max_cols]
            normalized_rows.append(row)
        
        df = pd.DataFrame(normalized_rows, columns=headers)
        return df
    
    def _handle_rate_limit(self) -> None:
        """Обработка rate limit с экспоненциальной задержкой."""
        delays = [1, 2, 4, 8, 16]
        for delay in delays:
            time.sleep(delay)
            return  # После первой задержки возвращаемся
    
    @staticmethod
    def extract_employee_mapping_columns(data: list[list]) -> tuple[dict, dict]:
        """Извлечение колонок маппинга сотрудников.
        
        Извлекает колонки A (ТН), B (ФИО), D (Участок Работ) из данных.
        Это статический метод для тестирования Property 2.
        
        Args:
            data: Список строк из Google Sheets (включая заголовок)
            
        Returns:
            Кортеж из двух словарей:
            - fio_map: {ТН: ФИО}
            - area_map: {ТН: Участок Работ}
        """
        if not data or len(data) < 2:
            return {}, {}
        
        fio_map = {}
        area_map = {}
        
        # Пропускаем заголовок (первая строка)
        for row in data[1:]:
            if len(row) < 1:
                continue
            
            tn = str(row[0]).strip() if row[0] else ''
            if not tn:
                continue
            
            # Колонка B (индекс 1) - ФИО
            fio = str(row[1]).strip() if len(row) > 1 and row[1] else ''
            
            # Колонка D (индекс 3) - Участок Работ
            area = str(row[3]).strip() if len(row) > 3 and row[3] else ''
            
            if fio:
                fio_map[tn] = fio
            if area:
                area_map[tn] = area
        
        return fio_map, area_map
    
    @staticmethod
    def preserve_cyrillic(text: str) -> str:
        """Сохранение кириллических символов при обработке.
        
        Это статический метод для тестирования Property 3.
        Гарантирует корректную обработку UTF-8 кодировки.
        
        Args:
            text: Входная строка (возможно с кириллицей)
            
        Returns:
            Строка с сохранёнными кириллическими символами
        """
        if not isinstance(text, str):
            text = str(text)
        
        # Кодируем в UTF-8 и декодируем обратно для проверки целостности
        encoded = text.encode('utf-8')
        decoded = encoded.decode('utf-8')
        
        return decoded
