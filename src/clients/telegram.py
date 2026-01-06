# AA_BLE Automation - Telegram Logger
"""
Модуль для отправки логов и уведомлений в Telegram.
Requirements: 3.1, 3.2, 3.3, 3.6
"""

import asyncio
import logging
import traceback
from typing import Optional

try:
    from telegram import Bot
    from telegram.error import TelegramError
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False
    Bot = None
    TelegramError = Exception


logger = logging.getLogger(__name__)


class TelegramLogger:
    """
    Отправка логов и уведомлений в Telegram.
    
    Поддерживает:
    - Информационные сообщения (info)
    - Предупреждения (warning)
    - Сообщения об ошибках с трассировкой (error)
    - Отправку файлов (send_file)
    - Автоматическое разбиение длинных сообщений
    """
    
    MAX_MESSAGE_LENGTH = 4096
    
    def __init__(self, bot_token: str, chat_id: str):
        """
        Инициализация TelegramLogger.
        
        Args:
            bot_token: Токен Telegram-бота
            chat_id: ID чата для отправки сообщений
        """
        self.bot_token = bot_token
        self.chat_id = chat_id
        self._bot: Optional[Bot] = None
        self._enabled = True
        
        if not TELEGRAM_AVAILABLE:
            logger.warning("python-telegram-bot не установлен, Telegram-логирование отключено")
            self._enabled = False
        elif not bot_token or not chat_id:
            logger.warning("Telegram bot_token или chat_id не указаны, логирование отключено")
            self._enabled = False

    @property
    def bot(self) -> Optional[Bot]:
        """Ленивая инициализация бота."""
        if self._bot is None and self._enabled and TELEGRAM_AVAILABLE:
            self._bot = Bot(token=self.bot_token)
        return self._bot
    
    def _split_message(self, message: str) -> list[str]:
        """
        Разбиение длинного сообщения на части.
        
        Каждая часть не превышает MAX_MESSAGE_LENGTH символов.
        Разбиение происходит по границам строк, если возможно.
        
        Args:
            message: Исходное сообщение
            
        Returns:
            Список частей сообщения
        """
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
            
            # Ищем последний перенос строки в пределах лимита
            split_pos = remaining.rfind('\n', 0, self.MAX_MESSAGE_LENGTH)
            
            if split_pos == -1 or split_pos == 0:
                # Нет переноса строки - режем по лимиту
                split_pos = self.MAX_MESSAGE_LENGTH
            
            parts.append(remaining[:split_pos])
            remaining = remaining[split_pos:].lstrip('\n')
        
        return parts
    
    def _send_message_sync(self, text: str, parse_mode: Optional[str] = None, retries: int = 3) -> bool:
        """
        Синхронная отправка сообщения с retry.
        
        Args:
            text: Текст сообщения
            parse_mode: Режим парсинга (HTML, Markdown, None)
            retries: Количество попыток
            
        Returns:
            True если отправка успешна
        """
        if not self._enabled or not self.bot:
            return False
        
        import time
        
        for attempt in range(retries):
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(
                        self.bot.send_message(
                            chat_id=self.chat_id,
                            text=text,
                            parse_mode=parse_mode
                        )
                    )
                    return True
                finally:
                    loop.close()
            except TelegramError as e:
                error_msg = str(e)
                if "Flood control" in error_msg or "Retry in" in error_msg:
                    # Извлекаем время ожидания из сообщения
                    wait_time = 15
                    if "Retry in" in error_msg:
                        try:
                            wait_time = int(''.join(filter(str.isdigit, error_msg.split("Retry in")[1][:5]))) + 1
                        except:
                            wait_time = 15
                    logger.warning(f"Flood control, ждём {wait_time} сек...")
                    time.sleep(wait_time)
                    continue
                logger.error(f"Ошибка отправки в Telegram: {e}")
                return False
            except Exception as e:
                logger.error(f"Неожиданная ошибка при отправке в Telegram: {e}")
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
                return False
        
        return False

    def info(self, message: str) -> None:
        """
        Отправка информационного сообщения.
        
        Requirements: 3.1, 3.2
        
        Args:
            message: Текст сообщения
        """
        parts = self._split_message(f"ℹ️ INFO\n{message}")
        for part in parts:
            self._send_message_sync(part)
    
    def warning(self, message: str) -> None:
        """
        Отправка предупреждения.
        
        Requirements: 3.4, 3.5
        
        Args:
            message: Текст предупреждения
        """
        parts = self._split_message(f"⚠️ WARNING\n{message}")
        for part in parts:
            self._send_message_sync(part)
    
    def error(self, message: str, exception: Optional[Exception] = None) -> None:
        """
        Отправка сообщения об ошибке.
        
        Requirements: 3.3
        
        Args:
            message: Текст ошибки
            exception: Исключение (опционально) для добавления трассировки
        """
        error_text = f"❌ ERROR\n{message}"
        
        if exception:
            tb = traceback.format_exception(type(exception), exception, exception.__traceback__)
            error_text += f"\n\nStack trace:\n{''.join(tb)}"
        
        parts = self._split_message(error_text)
        for part in parts:
            self._send_message_sync(part)
    
    def send_file(self, filepath: str, caption: Optional[str] = None) -> bool:
        """
        Отправка файла в чат.
        
        Args:
            filepath: Путь к файлу
            caption: Подпись к файлу (опционально)
            
        Returns:
            True если отправка успешна
        """
        if not self._enabled or not self.bot:
            return False
        
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                with open(filepath, 'rb') as f:
                    loop.run_until_complete(
                        self.bot.send_document(
                            chat_id=self.chat_id,
                            document=f,
                            caption=caption
                        )
                    )
                return True
            finally:
                loop.close()
        except FileNotFoundError:
            logger.error(f"Файл не найден: {filepath}")
            return False
        except TelegramError as e:
            logger.error(f"Ошибка отправки файла в Telegram: {e}")
            return False
        except Exception as e:
            logger.error(f"Неожиданная ошибка при отправке файла: {e}")
            return False

    def send_document(
        self, 
        content: bytes, 
        filename: str, 
        caption: Optional[str] = None,
        retries: int = 3
    ) -> bool:
        """
        Отправка документа из памяти (bytes) в чат с retry.
        
        Args:
            content: Содержимое файла в байтах
            filename: Имя файла для отображения
            caption: Подпись к файлу (опционально)
            retries: Количество попыток
            
        Returns:
            True если отправка успешна
        """
        if not self._enabled or not self.bot:
            logger.warning("Telegram не настроен, файл не отправлен")
            return False
        
        import time
        from io import BytesIO
        
        for attempt in range(retries):
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    file_obj = BytesIO(content)
                    file_obj.name = filename
                    
                    loop.run_until_complete(
                        self.bot.send_document(
                            chat_id=self.chat_id,
                            document=file_obj,
                            filename=filename,
                            caption=caption
                        )
                    )
                    return True
                finally:
                    loop.close()
            except TelegramError as e:
                error_msg = str(e)
                if "Flood control" in error_msg or "Retry in" in error_msg:
                    wait_time = 15
                    if "Retry in" in error_msg:
                        try:
                            wait_time = int(''.join(filter(str.isdigit, error_msg.split("Retry in")[1][:5]))) + 1
                        except:
                            wait_time = 15
                    logger.warning(f"Flood control, ждём {wait_time} сек...")
                    time.sleep(wait_time)
                    continue
                logger.error(f"Ошибка отправки документа в Telegram: {e}")
                return False
            except Exception as e:
                logger.error(f"Неожиданная ошибка при отправке документа: {e}")
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
                return False
        
        return False
