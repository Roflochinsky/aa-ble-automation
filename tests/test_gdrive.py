"""
Property-based tests for GoogleDriveClient.

Feature: aa-ble-automation, Property 1: File name date pattern filtering
Validates: Requirements 1.2
"""

import pytest
from datetime import date
from hypothesis import given, strategies as st, settings, assume

from src.clients.gdrive import GoogleDriveClient, DATE_PATTERN


# Стратегия для генерации валидных дат в формате YYYY-MM-DD
valid_date_strategy = st.dates(
    min_value=date(2000, 1, 1),
    max_value=date(2099, 12, 31)
).map(lambda d: d.isoformat())

# Стратегия для генерации имён файлов без дат
filename_without_date_strategy = st.text(
    alphabet=st.characters(
        whitelist_categories=['L', 'N', 'P'],
        blacklist_characters='0123456789-'
    ),
    min_size=1,
    max_size=50
)

# Стратегия для генерации имён файлов с датой
filename_with_date_strategy = st.tuples(
    st.text(alphabet='abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_', min_size=0, max_size=20),
    valid_date_strategy,
    st.text(alphabet='abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_.', min_size=0, max_size=20)
).map(lambda t: f"{t[0]}{t[1]}{t[2]}")


class TestFileDatePatternFiltering:
    """
    Property-based tests for file name date pattern filtering.
    
    Feature: aa-ble-automation, Property 1: File name date pattern filtering
    Validates: Requirements 1.2
    
    For any list of file names, the file filter function SHALL return only files
    whose names contain a date in YYYY-MM-DD format, and all returned files
    SHALL match the pattern.
    """

    @settings(max_examples=100)
    @given(filenames_with_dates=st.lists(filename_with_date_strategy, min_size=0, max_size=20))
    def test_all_files_with_dates_are_returned(self, filenames_with_dates: list[str]):
        """
        Feature: aa-ble-automation, Property 1: File name date pattern filtering
        Validates: Requirements 1.2
        
        For any list of file names containing dates in YYYY-MM-DD format,
        the filter function SHALL return all of them.
        """
        result = GoogleDriveClient.filter_files_by_date_pattern(filenames_with_dates)
        
        # Все файлы с датами должны быть в результате
        assert len(result) == len(filenames_with_dates), (
            f"Expected {len(filenames_with_dates)} files, got {len(result)}"
        )
        
        for filename in filenames_with_dates:
            assert filename in result, f"File '{filename}' should be in result"
    
    @settings(max_examples=100)
    @given(filenames_without_dates=st.lists(filename_without_date_strategy, min_size=0, max_size=20))
    def test_files_without_dates_are_excluded(self, filenames_without_dates: list[str]):
        """
        Feature: aa-ble-automation, Property 1: File name date pattern filtering
        Validates: Requirements 1.2
        
        For any list of file names NOT containing dates in YYYY-MM-DD format,
        the filter function SHALL return an empty list.
        """
        # Дополнительно проверяем, что файлы действительно не содержат дат
        for filename in filenames_without_dates:
            assume(not DATE_PATTERN.search(filename))
        
        result = GoogleDriveClient.filter_files_by_date_pattern(filenames_without_dates)
        
        assert result == [], f"Expected empty list, got {result}"
    
    @settings(max_examples=100)
    @given(
        filenames_with_dates=st.lists(filename_with_date_strategy, min_size=1, max_size=10),
        filenames_without_dates=st.lists(filename_without_date_strategy, min_size=1, max_size=10)
    )
    def test_mixed_list_returns_only_dated_files(
        self, 
        filenames_with_dates: list[str], 
        filenames_without_dates: list[str]
    ):
        """
        Feature: aa-ble-automation, Property 1: File name date pattern filtering
        Validates: Requirements 1.2
        
        For any mixed list of file names, the filter function SHALL return
        only files containing dates in YYYY-MM-DD format.
        """
        # Убеждаемся, что файлы без дат действительно не содержат дат
        for filename in filenames_without_dates:
            assume(not DATE_PATTERN.search(filename))
        
        mixed_list = filenames_with_dates + filenames_without_dates
        result = GoogleDriveClient.filter_files_by_date_pattern(mixed_list)
        
        # Результат должен содержать только файлы с датами
        assert len(result) == len(filenames_with_dates), (
            f"Expected {len(filenames_with_dates)} files with dates, got {len(result)}"
        )
        
        for filename in result:
            assert DATE_PATTERN.search(filename), (
                f"File '{filename}' in result does not contain date pattern"
            )
    
    @settings(max_examples=100)
    @given(filenames=st.lists(st.text(min_size=0, max_size=100), min_size=0, max_size=30))
    def test_all_returned_files_match_pattern(self, filenames: list[str]):
        """
        Feature: aa-ble-automation, Property 1: File name date pattern filtering
        Validates: Requirements 1.2
        
        For any list of file names, ALL returned files SHALL match
        the YYYY-MM-DD date pattern.
        """
        result = GoogleDriveClient.filter_files_by_date_pattern(filenames)
        
        for filename in result:
            assert DATE_PATTERN.search(filename), (
                f"Returned file '{filename}' does not match date pattern YYYY-MM-DD"
            )
    
    @settings(max_examples=100)
    @given(date_str=valid_date_strategy)
    def test_extract_date_from_filename_valid(self, date_str: str):
        """
        Feature: aa-ble-automation, Property 1: File name date pattern filtering
        Validates: Requirements 1.2
        
        For any valid date string in YYYY-MM-DD format embedded in a filename,
        the extraction function SHALL return the correct date object.
        """
        filename = f"report_{date_str}.xlsx"
        extracted = GoogleDriveClient._extract_date_from_filename(filename)
        
        assert extracted is not None, f"Failed to extract date from '{filename}'"
        assert extracted.isoformat() == date_str, (
            f"Extracted date {extracted} does not match expected {date_str}"
        )
    
    def test_extract_date_from_filename_no_date(self):
        """
        Edge case: filename without date returns None.
        """
        filename = "report_without_date.xlsx"
        extracted = GoogleDriveClient._extract_date_from_filename(filename)
        
        assert extracted is None
    
    def test_filter_empty_list(self):
        """
        Edge case: empty list returns empty list.
        """
        result = GoogleDriveClient.filter_files_by_date_pattern([])
        assert result == []
    
    def test_filter_real_world_filenames(self):
        """
        Example test with realistic AA_BLE filenames.
        """
        filenames = [
            "11_отчет по АА_BLE со склейкой_MAGNIT_AUTO_!NEW!_2025-12-11.xlsx",
            "report_2024-01-15_final.xlsx",
            "data_export.csv",
            "backup_2023-06-30.zip",
            "notes.txt",
            "AA_BLE_2025-01-01_processed.xlsx"
        ]
        
        result = GoogleDriveClient.filter_files_by_date_pattern(filenames)
        
        expected = [
            "11_отчет по АА_BLE со склейкой_MAGNIT_AUTO_!NEW!_2025-12-11.xlsx",
            "report_2024-01-15_final.xlsx",
            "backup_2023-06-30.zip",
            "AA_BLE_2025-01-01_processed.xlsx"
        ]
        
        assert sorted(result) == sorted(expected)
