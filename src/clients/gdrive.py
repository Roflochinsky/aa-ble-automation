# AA_BLE Automation - Google Drive Client
"""
Клиент для работы с Google Drive API.
Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 4.5
"""

import io
import os
import re
import time
from datetime import date
from typing import Optional

from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from googleapiclient.errors import HttpError

from src.clients.oauth_helper import get_oauth_credentials


# Паттерн для извлечения даты из имени файла (YYYY-MM-DD)
DATE_PATTERN = re.compile(r'(\d{4}-\d{2}-\d{2})')


class GoogleDriveClient:
    """Клиент для работы с Google Drive API.
    
    Обеспечивает:
    - Аутентификацию через сервисный аккаунт или OAuth2
    - Получение списка файлов с фильтрацией по дате
    - Скачивание и загрузку файлов
    - Retry-логику для обработки ошибок API
    """
    
    SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
    MAX_RETRIES = 3
    RETRY_DELAY = 5  # секунд
    
    def __init__(
        self, 
        credentials_path: str, 
        impersonate_email: Optional[str] = None,
        use_oauth: bool = False,
        oauth_token_path: str = 'token.json'
    ):
        """Инициализация клиента.
        
        Args:
            credentials_path: Путь к JSON-файлу (service account или OAuth client secrets)
            impersonate_email: Email пользователя для impersonation (только для service account)
            use_oauth: Использовать OAuth2 вместо service account
            oauth_token_path: Путь к файлу с OAuth токеном
        """
        self.credentials_path = credentials_path
        self.impersonate_email = impersonate_email
        self.use_oauth = use_oauth
        self.oauth_token_path = oauth_token_path
        self._service = None
        self._credentials = None
    
    def authenticate(self) -> bool:
        """Аутентификация через сервисный аккаунт или OAuth2.
        
        Returns:
            True если аутентификация успешна
            
        Raises:
            Exception: При ошибке аутентификации после всех попыток
        """
        last_error = None
        
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                if self.use_oauth:
                    # OAuth2 авторизация
                    self._credentials = get_oauth_credentials(
                        client_secrets_path=self.credentials_path,
                        token_path=self.oauth_token_path
                    )
                    if not self._credentials:
                        raise Exception("OAuth2 авторизация не удалась. Запустите: python -m src.clients.oauth_helper")
                else:
                    # Service Account
                    self._credentials = service_account.Credentials.from_service_account_file(
                        self.credentials_path,
                        scopes=self.SCOPES
                    )
                    
                    # Domain-wide delegation
                    if self.impersonate_email:
                        self._credentials = self._credentials.with_subject(self.impersonate_email)
                
                self._service = build('drive', 'v3', credentials=self._credentials)
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
    
    def list_files(
        self, 
        folder_id: str, 
        name_pattern: Optional[str] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None
    ) -> list[dict]:
        """Получение списка файлов в папке.
        
        Args:
            folder_id: ID папки Google Drive
            name_pattern: Паттерн для фильтрации по имени (опционально)
            date_from: Начальная дата для фильтрации (опционально)
            date_to: Конечная дата для фильтрации (опционально)
            
        Returns:
            Список словарей с информацией о файлах:
            [{'id': str, 'name': str, 'mimeType': str, 'file_date': date|None}, ...]
        """
        self._ensure_authenticated()
        
        query = f"'{folder_id}' in parents and trashed = false"
        if name_pattern:
            query += f" and name contains '{name_pattern}'"
        
        files = []
        page_token = None
        
        while True:
            try:
                response = self._service.files().list(
                    q=query,
                    spaces='drive',
                    fields='nextPageToken, files(id, name, mimeType)',
                    pageToken=page_token
                ).execute()
                
                for file_info in response.get('files', []):
                    file_date = self._extract_date_from_filename(file_info['name'])
                    
                    # Фильтрация по диапазону дат
                    if file_date:
                        if date_from and file_date < date_from:
                            continue
                        if date_to and file_date > date_to:
                            continue
                    
                    files.append({
                        'id': file_info['id'],
                        'name': file_info['name'],
                        'mimeType': file_info['mimeType'],
                        'file_date': file_date
                    })
                
                page_token = response.get('nextPageToken')
                if not page_token:
                    break
                    
            except HttpError as e:
                print(f"❌ API Error при доступе к папке {folder_id}: {e.resp.status} - {e.content}")
                if e.resp.status == 429:  # Rate limit
                    self._handle_rate_limit()
                    continue
                raise
        
        return files
    
    @staticmethod
    def _extract_date_from_filename(filename: str) -> Optional[date]:
        """Извлечение даты из имени файла.
        
        Args:
            filename: Имя файла
            
        Returns:
            Объект date или None если дата не найдена
        """
        match = DATE_PATTERN.search(filename)
        if match:
            try:
                return date.fromisoformat(match.group(1))
            except ValueError:
                return None
        return None

    
    def download_file(self, file_id: str) -> bytes:
        """Скачивание файла по ID.
        
        Args:
            file_id: ID файла в Google Drive
            
        Returns:
            Содержимое файла в байтах
            
        Raises:
            HttpError: При ошибке API (file not found и др.)
        """
        self._ensure_authenticated()
        
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                request = self._service.files().get_media(fileId=file_id)
                buffer = io.BytesIO()
                downloader = MediaIoBaseDownload(buffer, request)
                
                done = False
                while not done:
                    _, done = downloader.next_chunk()
                
                return buffer.getvalue()
                
            except HttpError as e:
                if e.resp.status == 404:
                    raise  # File not found - не повторяем
                if e.resp.status == 429:  # Rate limit
                    self._handle_rate_limit()
                    continue
                if attempt < self.MAX_RETRIES:
                    time.sleep(self.RETRY_DELAY)
                else:
                    raise
        
        return b''  # Недостижимо, но для типизации
    
    def upload_file(
        self, 
        folder_id: str, 
        filename: str, 
        content: bytes, 
        mimetype: str
    ) -> str:
        """Загрузка файла в папку.
        
        Args:
            folder_id: ID папки назначения
            filename: Имя файла
            content: Содержимое файла в байтах
            mimetype: MIME-тип файла
            
        Returns:
            ID загруженного файла
        """
        self._ensure_authenticated()
        
        file_metadata = {
            'name': filename,
            'parents': [folder_id]
        }
        
        media = MediaIoBaseUpload(
            io.BytesIO(content),
            mimetype=mimetype,
            resumable=True
        )
        
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                file = self._service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                
                return file.get('id', '')
                
            except HttpError as e:
                if e.resp.status == 429:  # Rate limit
                    self._handle_rate_limit()
                    continue
                if attempt < self.MAX_RETRIES:
                    time.sleep(self.RETRY_DELAY)
                else:
                    raise
        
        return ''  # Недостижимо
    
    def _handle_rate_limit(self) -> None:
        """Обработка rate limit с экспоненциальной задержкой."""
        delays = [1, 2, 4, 8, 16]
        for delay in delays:
            time.sleep(delay)
            return  # После первой задержки возвращаемся
    
    @staticmethod
    def filter_files_by_date_pattern(filenames: list[str]) -> list[str]:
        """Фильтрация списка имён файлов по наличию даты в формате YYYY-MM-DD.
        
        Это статический метод для тестирования Property 1.
        
        Args:
            filenames: Список имён файлов
            
        Returns:
            Список имён файлов, содержащих дату в формате YYYY-MM-DD
        """
        result = []
        for filename in filenames:
            if DATE_PATTERN.search(filename):
                result.append(filename)
        return result
