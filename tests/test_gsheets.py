"""
Property-based tests for GoogleSheetsClient.

Feature: aa-ble-automation
Property 2: Employee mapping column extraction
Property 3: Cyrillic character preservation
Validates: Requirements 2.2, 2.4
"""

import pytest
from hypothesis import given, strategies as st, settings, assume

from src.clients.gsheets import GoogleSheetsClient


# Стратегия для генерации табельных номеров (ТН)
tn_strategy = st.text(
    alphabet='0123456789',
    min_size=1,
    max_size=10
)

# Стратегия для генерации ФИО (кириллица)
cyrillic_alphabet = 'АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯабвгдеёжзийклмнопрстуфхцчшщъыьэюя '
fio_strategy = st.text(
    alphabet=cyrillic_alphabet,
    min_size=1,
    max_size=50
).filter(lambda x: x.strip())  # Не пустые после strip

# Стратегия для генерации участков работ
area_strategy = st.text(
    alphabet=cyrillic_alphabet + '0123456789-/',
    min_size=1,
    max_size=30
).filter(lambda x: x.strip())

# Стратегия для генерации строки данных сотрудника (минимум 4 колонки: A, B, C, D)
employee_row_strategy = st.tuples(
    tn_strategy,           # Колонка A - ТН
    fio_strategy,          # Колонка B - ФИО
    st.text(min_size=0, max_size=20),  # Колонка C - что-то другое
    area_strategy          # Колонка D - Участок Работ
).map(list)


class TestEmployeeMappingColumnExtraction:
    """
    Property-based tests for employee mapping column extraction.
    
    Feature: aa-ble-automation, Property 2: Employee mapping column extraction
    Validates: Requirements 2.2
    
    For any Google Sheets data with at least 4 columns, the mapping extraction
    SHALL correctly extract column A as ТН, column B as ФИО, and column D as Участок Работ.
    """

    @settings(max_examples=100)
    @given(employee_rows=st.lists(employee_row_strategy, min_size=1, max_size=20))
    def test_extracts_correct_columns(self, employee_rows: list[list]):
        """
        Feature: aa-ble-automation, Property 2: Employee mapping column extraction
        Validates: Requirements 2.2
        
        For any list of employee data rows, the extraction function SHALL
        correctly map ТН to ФИО and ТН to Участок Работ.
        Note: When duplicate ТН values exist, the last value wins.
        """
        # Добавляем заголовок
        header = ['ТН', 'ФИО', 'Должность', 'Участок Работ']
        data = [header] + employee_rows
        
        fio_map, area_map = GoogleSheetsClient.extract_employee_mapping_columns(data)
        
        # Строим ожидаемые маппинги (последнее значение для каждого ТН)
        expected_fio = {}
        expected_area = {}
        for row in employee_rows:
            tn = str(row[0]).strip()
            fio = str(row[1]).strip()
            area = str(row[3]).strip()
            
            if tn and fio:
                expected_fio[tn] = fio
            if tn and area:
                expected_area[tn] = area
        
        # Проверяем, что результат соответствует ожиданиям
        assert fio_map == expected_fio, f"FIO map mismatch"
        assert area_map == expected_area, f"Area map mismatch"

    @settings(max_examples=100)
    @given(
        tn=tn_strategy,
        fio=fio_strategy,
        area=area_strategy
    )
    def test_single_employee_extraction(self, tn: str, fio: str, area: str):
        """
        Feature: aa-ble-automation, Property 2: Employee mapping column extraction
        Validates: Requirements 2.2
        
        For any single employee record, extraction SHALL correctly map
        the ТН to both ФИО and Участок Работ.
        Note: Values are stripped of leading/trailing whitespace.
        """
        header = ['ТН', 'ФИО', 'Должность', 'Участок Работ']
        row = [tn, fio, 'Инженер', area]
        data = [header, row]
        
        fio_map, area_map = GoogleSheetsClient.extract_employee_mapping_columns(data)
        
        # Ожидаемые значения после strip()
        expected_tn = tn.strip()
        expected_fio = fio.strip()
        expected_area = area.strip()
        
        assert expected_tn in fio_map, f"ТН '{expected_tn}' should be in fio_map"
        assert fio_map[expected_tn] == expected_fio, f"FIO should be '{expected_fio}'"
        
        assert expected_tn in area_map, f"ТН '{expected_tn}' should be in area_map"
        assert area_map[expected_tn] == expected_area, f"Area should be '{expected_area}'"

    def test_empty_data_returns_empty_dicts(self):
        """
        Edge case: empty data returns empty dictionaries.
        """
        fio_map, area_map = GoogleSheetsClient.extract_employee_mapping_columns([])
        assert fio_map == {}
        assert area_map == {}

    def test_header_only_returns_empty_dicts(self):
        """
        Edge case: data with only header returns empty dictionaries.
        """
        header = ['ТН', 'ФИО', 'Должность', 'Участок Работ']
        fio_map, area_map = GoogleSheetsClient.extract_employee_mapping_columns([header])
        assert fio_map == {}
        assert area_map == {}

    def test_rows_with_missing_columns(self):
        """
        Edge case: rows with fewer than 4 columns are handled gracefully.
        """
        header = ['ТН', 'ФИО', 'Должность', 'Участок Работ']
        data = [
            header,
            ['12345', 'Иванов Иван'],  # Только 2 колонки
            ['67890', 'Петров Пётр', 'Инженер', 'Участок 1'],  # Полная строка
        ]
        
        fio_map, area_map = GoogleSheetsClient.extract_employee_mapping_columns(data)
        
        # Первая строка - только ФИО, без участка
        assert '12345' in fio_map
        assert fio_map['12345'] == 'Иванов Иван'
        assert '12345' not in area_map  # Нет колонки D
        
        # Вторая строка - полная
        assert '67890' in fio_map
        assert fio_map['67890'] == 'Петров Пётр'
        assert '67890' in area_map
        assert area_map['67890'] == 'Участок 1'

    def test_real_world_data(self):
        """
        Example test with realistic employee data.
        """
        data = [
            ['ТН', 'ФИО', 'Должность', 'Участок Работ'],
            ['001234', 'Иванов Иван Иванович', 'Инженер', 'Участок №1'],
            ['005678', 'Петрова Мария Сергеевна', 'Техник', 'Участок №2'],
            ['009012', 'Сидоров Алексей Петрович', 'Мастер', 'Участок №1'],
        ]
        
        fio_map, area_map = GoogleSheetsClient.extract_employee_mapping_columns(data)
        
        assert fio_map == {
            '001234': 'Иванов Иван Иванович',
            '005678': 'Петрова Мария Сергеевна',
            '009012': 'Сидоров Алексей Петрович',
        }
        
        assert area_map == {
            '001234': 'Участок №1',
            '005678': 'Участок №2',
            '009012': 'Участок №1',
        }


class TestCyrillicCharacterPreservation:
    """
    Property-based tests for Cyrillic character preservation.
    
    Feature: aa-ble-automation, Property 3: Cyrillic character preservation
    Validates: Requirements 2.4
    
    For any string containing Cyrillic characters, parsing and re-encoding
    SHALL preserve all original characters without corruption.
    """

    @settings(max_examples=100)
    @given(text=st.text(alphabet=cyrillic_alphabet, min_size=0, max_size=200))
    def test_cyrillic_preserved_after_encoding(self, text: str):
        """
        Feature: aa-ble-automation, Property 3: Cyrillic character preservation
        Validates: Requirements 2.4
        
        For any Cyrillic text, the preserve function SHALL return
        the exact same text without any character corruption.
        """
        result = GoogleSheetsClient.preserve_cyrillic(text)
        
        assert result == text, (
            f"Cyrillic text was corrupted: expected '{text}', got '{result}'"
        )

    @settings(max_examples=100)
    @given(text=st.text(min_size=0, max_size=200))
    def test_mixed_text_preserved(self, text: str):
        """
        Feature: aa-ble-automation, Property 3: Cyrillic character preservation
        Validates: Requirements 2.4
        
        For any text (including mixed Cyrillic and Latin), the preserve
        function SHALL return the exact same text.
        """
        result = GoogleSheetsClient.preserve_cyrillic(text)
        
        assert result == text, (
            f"Text was corrupted: expected '{text}', got '{result}'"
        )

    @settings(max_examples=100)
    @given(
        prefix=st.text(alphabet='abcdefghijklmnopqrstuvwxyz', min_size=0, max_size=20),
        cyrillic=st.text(alphabet=cyrillic_alphabet, min_size=1, max_size=50),
        suffix=st.text(alphabet='0123456789', min_size=0, max_size=10)
    )
    def test_cyrillic_in_context_preserved(self, prefix: str, cyrillic: str, suffix: str):
        """
        Feature: aa-ble-automation, Property 3: Cyrillic character preservation
        Validates: Requirements 2.4
        
        For any text with Cyrillic characters surrounded by other characters,
        the preserve function SHALL maintain all characters intact.
        """
        text = f"{prefix}{cyrillic}{suffix}"
        result = GoogleSheetsClient.preserve_cyrillic(text)
        
        assert result == text
        assert cyrillic in result, "Cyrillic portion should be preserved in result"

    def test_common_cyrillic_names(self):
        """
        Example test with common Russian names.
        """
        names = [
            'Иванов Иван Иванович',
            'Петрова Мария Сергеевна',
            'Сидоров Алексей Петрович',
            'Козлова Анна Владимировна',
            'Новиков Дмитрий Александрович',
        ]
        
        for name in names:
            result = GoogleSheetsClient.preserve_cyrillic(name)
            assert result == name, f"Name '{name}' was corrupted"

    def test_cyrillic_with_special_chars(self):
        """
        Example test with Cyrillic text containing special characters.
        """
        texts = [
            'Участок №1',
            'Зона "А"',
            'Отдел (основной)',
            'Склад-1/2',
        ]
        
        for text in texts:
            result = GoogleSheetsClient.preserve_cyrillic(text)
            assert result == text, f"Text '{text}' was corrupted"

    def test_empty_string(self):
        """
        Edge case: empty string is preserved.
        """
        result = GoogleSheetsClient.preserve_cyrillic('')
        assert result == ''

    def test_non_string_input(self):
        """
        Edge case: non-string input is converted to string.
        """
        result = GoogleSheetsClient.preserve_cyrillic(12345)
        assert result == '12345'
