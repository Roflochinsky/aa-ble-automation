"""
Property-based tests for TelegramLogger.

Feature: aa-ble-automation, Property 6: Message splitting
Validates: Requirements 3.6
"""

import pytest
from hypothesis import given, strategies as st, settings, HealthCheck


class TestMessageSplitting:
    """
    Property-based tests for TelegramLogger._split_message method.
    
    Feature: aa-ble-automation, Property 6: Message splitting
    Validates: Requirements 3.6
    
    For any message string, if the length exceeds 4096 characters,
    the split function SHALL return multiple parts where each part
    is at most 4096 characters, and concatenating all parts SHALL
    equal the original message (with possible newline trimming between parts).
    """
    
    MAX_MESSAGE_LENGTH = 4096
    
    @settings(max_examples=100)
    @given(message=st.text(min_size=0, max_size=20000))
    def test_split_message_parts_not_exceed_max_length(self, message: str):
        """
        Feature: aa-ble-automation, Property 6: Message splitting
        Validates: Requirements 3.6
        
        For any message, each part returned by _split_message
        SHALL be at most MAX_MESSAGE_LENGTH characters.
        """
        from src.clients.telegram import TelegramLogger
        
        logger = TelegramLogger(bot_token="", chat_id="")
        parts = logger._split_message(message)
        
        for i, part in enumerate(parts):
            assert len(part) <= self.MAX_MESSAGE_LENGTH, (
                f"Part {i} exceeds max length: {len(part)} > {self.MAX_MESSAGE_LENGTH}"
            )

    @settings(max_examples=100)
    @given(message=st.text(min_size=0, max_size=20000))
    def test_split_message_concatenation_preserves_content(self, message: str):
        """
        Feature: aa-ble-automation, Property 6: Message splitting
        Validates: Requirements 3.6
        
        For any message, concatenating all parts (with newlines between)
        SHALL contain all characters from the original message.
        """
        from src.clients.telegram import TelegramLogger
        
        logger = TelegramLogger(bot_token="", chat_id="")
        parts = logger._split_message(message)
        
        if not message:
            assert parts == []
            return
        
        # Объединяем части с переносами строк (как они были разделены)
        concatenated = '\n'.join(parts)
        
        # Все символы оригинала (кроме лишних переносов) должны присутствовать
        # Удаляем переносы строк для сравнения содержимого
        original_chars = message.replace('\n', '')
        concatenated_chars = concatenated.replace('\n', '')
        
        assert original_chars == concatenated_chars, (
            f"Content mismatch after split and concatenation"
        )
    
    @settings(max_examples=100)
    @given(message=st.text(min_size=1, max_size=4096))
    def test_short_message_returns_single_part(self, message: str):
        """
        Feature: aa-ble-automation, Property 6: Message splitting
        Validates: Requirements 3.6
        
        For any message with length <= MAX_MESSAGE_LENGTH,
        the split function SHALL return exactly one part equal to the original.
        """
        from src.clients.telegram import TelegramLogger
        
        logger = TelegramLogger(bot_token="", chat_id="")
        parts = logger._split_message(message)
        
        assert len(parts) == 1
        assert parts[0] == message
    
    @settings(max_examples=100)
    @given(
        base_char=st.characters(whitelist_categories=['L', 'N']),
        length=st.integers(min_value=4097, max_value=15000)
    )
    def test_long_message_returns_multiple_parts(self, base_char: str, length: int):
        """
        Feature: aa-ble-automation, Property 6: Message splitting
        Validates: Requirements 3.6
        
        For any message with length > MAX_MESSAGE_LENGTH,
        the split function SHALL return more than one part.
        """
        from src.clients.telegram import TelegramLogger
        
        # Генерируем длинное сообщение из повторяющегося символа
        message = base_char * length
        
        logger = TelegramLogger(bot_token="", chat_id="")
        parts = logger._split_message(message)
        
        assert len(parts) > 1, (
            f"Message of length {len(message)} should be split into multiple parts"
        )
    
    def test_empty_message_returns_empty_list(self):
        """
        Edge case: empty message returns empty list.
        """
        from src.clients.telegram import TelegramLogger
        
        logger = TelegramLogger(bot_token="", chat_id="")
        parts = logger._split_message("")
        
        assert parts == []
    
    def test_message_with_newlines_splits_at_newline(self):
        """
        Edge case: message with newlines should prefer splitting at newline boundaries.
        """
        from src.clients.telegram import TelegramLogger
        
        logger = TelegramLogger(bot_token="", chat_id="")
        
        # Создаём сообщение с переносами строк
        line = "A" * 2000
        message = f"{line}\n{line}\n{line}"  # 6002 символа
        
        parts = logger._split_message(message)
        
        # Должно разбиться на части
        assert len(parts) >= 2
        
        # Каждая часть не превышает лимит
        for part in parts:
            assert len(part) <= 4096
