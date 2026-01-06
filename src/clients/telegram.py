# AA_BLE Automation - Telegram Logger
"""
Модуль для отправки логов и уведомлений в Telegram.
Использует requests вместо python-telegram-bot для надёжности.
"""

import logging
import time
import traceback
from typing import Optional

import requests

logger = logging.getLogger(__name__)


class TelegramLogger:
    """
    Отправка логов и уведомлений в Telegram через HTTP API.
    
    Поддерживает:
    - Информационные сообщения (info)
    - Предупреждения (warning)
    - Сообщения об ошибках с трассировкой (error)
    - Отправку файлов (send_document)
    - Автоматический retry при flood control
    """
    
    MAX_MESSAGE_LENGTH = 4096
    API_BASE = "https://api.telegram.org/bot{token}"
    
    def __init__(self, bot_token: str, chat_id: str):
        """
        Args:
            bot_token: Токен Telegram-бота
            chat_id: ID чата для отправки сообщений
        """
        self.bot_token = bot_token
        self.chat_id = chat_id
        self._enabled = bool(bot_token and chat_id)
        
        if not self._enabled:
            logger.warning("Telegram bot_token или chat_id не указаны, логирование отключено")
    
    @property
    def api_url(self) -> str:
        return self.API_BASE.format(token=self.bot_token)
    
    def _send_request(self, method: str, data: dict = None, files: dict = None, retries: int = 3) -> bool:
        """Отправка запроса к Telegram API с retry."""
        if not self._enabled:
            return False
        
        url = f"{self.api_url}/{method}"
        
        for attempt in range(retries):
            try:
                if files:
                    response = requests.post(url, data=data, files=files, timeout=60)
                else:
                    response = requests.post(url, json=data, timeout=30)
                
                result = response.json()
                
                if result.get('ok'):
                    return True
                
                error_desc = result.get('description', 'Unknown error')
                
                # Flood control - ждём и повторяем
                if 'retry after' in error_desc.lower() or 'flood' in error_desc.lower():
                    retry_after = result.get('parameters', {}).get('retry_after', 15)
                    logger.warning(f"Flood control, ждём {retry_after} сек...")
                    time.sleep(retry_after + 1)
                    continue
                
                logger.error(f"Telegram API error: {error_desc}")
                return False
                
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout, попытка {attempt + 1}/{retries}")
                if attempt < retries - 1:
                    time.sleep(5)
                    continue
                return False
            except Exception as e:
                logger.error(f"Ошибка запроса к Telegram: {e}")
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
                return False
        
        return False
    
    def _split_message(self, message: str) -> list[str]:
        """Разбиение длинного сообщения на части."""
        if not message:
            return []
        
        if len(message) <= self.MAX_MESSAGE_LENGTH:
            return [message]
        
        parts = []
        remaining = message
        
        while remaining:
            if len(remaining) <= self.MAX_MESSAGE_LENGTH:
                parts.append(remaining)
                break
            
            split_pos = remaining.rfind('\n', 0, self.MAX_MESSAGE_LENGTH)
            if split_pos == -1 or split_pos == 0:
                split_pos = self.MAX_MESSAGE_LENGTH
            
            parts.append(remaining[:split_pos])
            remaining = remaining[split_pos:].lstrip('\n')
        
        return parts
    
    def _send_message(self, text: str) -> bool:
        """Отправка текстового сообщения."""
        return self._send_request('sendMessage', {
            'chat_id': self.chat_id,
            'text': text
        })

    def info(self, message: str) -> None:
        """Отправка информационного сообщения."""
        parts = self._split_message(f"ℹ️ INFO\n{message}")
        for part in parts:
            self._send_message(part)
            time.sleep(0.5)  # Небольшая задержка между сообщениями
    
    def warning(self, message: str) -> None:
        """Отправка предупреждения."""
        parts = self._split_message(f"⚠️ WARNING\n{message}")
        for part in parts:
            self._send_message(part)
            time.sleep(0.5)
    
    def error(self, message: str, exception: Optional[Exception] = None) -> None:
        """Отправка сообщения об ошибке."""
        error_text = f"❌ ERROR\n{message}"
        
        if exception:
            tb = traceback.format_exception(type(exception), exception, exception.__traceback__)
            error_text += f"\n\nStack trace:\n{''.join(tb)}"
        
        parts = self._split_message(error_text)
        for part in parts:
            self._send_message(part)
            time.sleep(0.5)
    
    def send_file(self, filepath: str, caption: Optional[str] = None) -> bool:
        """Отправка файла в чат."""
        if not self._enabled:
            return False
        
        try:
            with open(filepath, 'rb') as f:
                files = {'document': f}
                data = {'chat_id': self.chat_id}
                if caption:
                    data['caption'] = caption
                return self._send_request('sendDocument', data=data, files=files)
        except FileNotFoundError:
            logger.error(f"Файл не найден: {filepath}")
            return False

    def send_document(
        self, 
        content: bytes, 
        filename: str, 
        caption: Optional[str] = None
    ) -> bool:
        """Отправка документа из памяти (bytes) в чат."""
        if not self._enabled:
            logger.warning("Telegram не настроен, файл не отправлен")
            return False
        
        files = {'document': (filename, content)}
        data = {'chat_id': self.chat_id}
        if caption:
            data['caption'] = caption
        
        return self._send_request('sendDocument', data=data, files=files)
