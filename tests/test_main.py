"""
Property-based tests for AABLEReportOrchestrator.

Feature: aa-ble-automation, Property 11: Date range filtering
Validates: Requirements 6.3
"""

import pytest
from datetime import date, timedelta
from hypothesis import given, strategies as st, settings, assume

from src.main import filter_files_by_date_range


# Стратегия для генерации валидных дат
date_strategy = st.dates(
    min_value=date(2020, 1, 1),
    max_value=date(2030, 12, 31)
)

# Стратегия для генерации файла с датой
def file_with_date_strategy():
    """Генерация файла с датой."""
    return st.builds(
        lambda d, name: {
            'id': f'file_{d.isoformat()}',
            'name': f'{name}_{d.isoformat()}.xlsx',
            'mimeType': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'file_date': d
        },
        d=date_strategy,
        name=st.text(alphabet='abcdefghijklmnopqrstuvwxyz', min_size=3, max_size=10)
    )

# Стратегия для генерации файла без даты
def file_without_date_strategy():
    """Генерация файла без даты."""
    return st.builds(
        lambda name: {
            'id': f'file_{name}',
            'name': f'{name}.xlsx',
            'mimeType': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'file_date': None
        },
        name=st.text(alphabet='abcdefghijklmnopqrstuvwxyz', min_size=3, max_size=10)
    )


class TestDateRangeFiltering:
    """
    Property-based tests for date range filtering.
    
    Feature: aa-ble-automation, Property 11: Date range filtering
    Validates: Requirements 6.3
    
    For any date range specified via arguments, the system SHALL process
    only files with dates within that range (inclusive).
    """

    @settings(max_examples=100)
    @given(
        files=st.lists(file_with_date_strategy(), min_size=1, max_size=20),
        date_from=date_strategy,
        date_to=date_strategy
    )
    def test_all_returned_files_within_range(
        self, 
        files: list[dict], 
        date_from: date, 
        date_to: date
    ):
        """
        Feature: aa-ble-automation, Property 11: Date range filtering
        Validates: Requirements 6.3
        
        For any date range, ALL returned files SHALL have dates within
        that range (inclusive).
        """
        # Ensure date_from <= date_to
        if date_from > date_to:
            date_from, date_to = date_to, date_from
        
        result = filter_files_by_date_range(files, date_from, date_to)
        
        for file_info in result:
            file_date = file_info.get('file_date')
            assert file_date is not None, "Returned file should have a date"
            assert date_from <= file_date <= date_to, (
                f"File date {file_date} is outside range [{date_from}, {date_to}]"
            )

    @settings(max_examples=100)
    @given(
        files=st.lists(file_with_date_strategy(), min_size=1, max_size=20),
        date_from=date_strategy,
        date_to=date_strategy
    )
    def test_no_files_within_range_excluded(
        self, 
        files: list[dict], 
        date_from: date, 
        date_to: date
    ):
        """
        Feature: aa-ble-automation, Property 11: Date range filtering
        Validates: Requirements 6.3
        
        For any date range, NO files with dates within that range
        SHALL be excluded from the result.
        """
        # Ensure date_from <= date_to
        if date_from > date_to:
            date_from, date_to = date_to, date_from
        
        result = filter_files_by_date_range(files, date_from, date_to)
        result_dates = {f.get('file_date') for f in result}
        
        # Check that all files within range are included
        for file_info in files:
            file_date = file_info.get('file_date')
            if file_date is not None and date_from <= file_date <= date_to:
                assert file_date in result_dates, (
                    f"File with date {file_date} within range [{date_from}, {date_to}] "
                    f"was excluded from result"
                )

    @settings(max_examples=100)
    @given(
        files=st.lists(file_with_date_strategy(), min_size=1, max_size=20),
        date_from=date_strategy,
        date_to=date_strategy
    )
    def test_files_outside_range_excluded(
        self, 
        files: list[dict], 
        date_from: date, 
        date_to: date
    ):
        """
        Feature: aa-ble-automation, Property 11: Date range filtering
        Validates: Requirements 6.3
        
        For any date range, files with dates OUTSIDE that range
        SHALL NOT be included in the result.
        """
        # Ensure date_from <= date_to
        if date_from > date_to:
            date_from, date_to = date_to, date_from
        
        result = filter_files_by_date_range(files, date_from, date_to)
        
        for file_info in result:
            file_date = file_info.get('file_date')
            if file_date is not None:
                assert file_date >= date_from, (
                    f"File date {file_date} is before range start {date_from}"
                )
                assert file_date <= date_to, (
                    f"File date {file_date} is after range end {date_to}"
                )

    @settings(max_examples=100)
    @given(
        files=st.lists(file_with_date_strategy(), min_size=1, max_size=20),
        single_date=date_strategy
    )
    def test_single_date_range_inclusive(
        self, 
        files: list[dict], 
        single_date: date
    ):
        """
        Feature: aa-ble-automation, Property 11: Date range filtering
        Validates: Requirements 6.3
        
        When date_from equals date_to, files with exactly that date
        SHALL be included (boundary is inclusive).
        """
        result = filter_files_by_date_range(files, single_date, single_date)
        
        # All returned files should have exactly the single_date
        for file_info in result:
            file_date = file_info.get('file_date')
            assert file_date == single_date, (
                f"File date {file_date} does not match single date {single_date}"
            )
        
        # All files with single_date should be in result
        expected_count = sum(
            1 for f in files 
            if f.get('file_date') == single_date
        )
        assert len(result) == expected_count, (
            f"Expected {expected_count} files with date {single_date}, got {len(result)}"
        )

    @settings(max_examples=100)
    @given(files=st.lists(file_with_date_strategy(), min_size=0, max_size=20))
    def test_no_range_returns_all_dated_files(self, files: list[dict]):
        """
        Feature: aa-ble-automation, Property 11: Date range filtering
        Validates: Requirements 6.3
        
        When no date range is specified (both None), all files with dates
        SHALL be returned.
        """
        result = filter_files_by_date_range(files, None, None)
        
        # All files with dates should be returned
        expected = [f for f in files if f.get('file_date') is not None]
        assert len(result) == len(expected), (
            f"Expected {len(expected)} files, got {len(result)}"
        )

    @settings(max_examples=100)
    @given(
        files=st.lists(file_with_date_strategy(), min_size=1, max_size=20),
        date_from=date_strategy
    )
    def test_only_date_from_filters_correctly(
        self, 
        files: list[dict], 
        date_from: date
    ):
        """
        Feature: aa-ble-automation, Property 11: Date range filtering
        Validates: Requirements 6.3
        
        When only date_from is specified, all files with dates >= date_from
        SHALL be returned.
        """
        result = filter_files_by_date_range(files, date_from, None)
        
        for file_info in result:
            file_date = file_info.get('file_date')
            if file_date is not None:
                assert file_date >= date_from, (
                    f"File date {file_date} is before date_from {date_from}"
                )

    @settings(max_examples=100)
    @given(
        files=st.lists(file_with_date_strategy(), min_size=1, max_size=20),
        date_to=date_strategy
    )
    def test_only_date_to_filters_correctly(
        self, 
        files: list[dict], 
        date_to: date
    ):
        """
        Feature: aa-ble-automation, Property 11: Date range filtering
        Validates: Requirements 6.3
        
        When only date_to is specified, all files with dates <= date_to
        SHALL be returned.
        """
        result = filter_files_by_date_range(files, None, date_to)
        
        for file_info in result:
            file_date = file_info.get('file_date')
            if file_date is not None:
                assert file_date <= date_to, (
                    f"File date {file_date} is after date_to {date_to}"
                )

    def test_empty_list_returns_empty(self):
        """
        Edge case: empty file list returns empty list.
        """
        result = filter_files_by_date_range([], date(2024, 1, 1), date(2024, 12, 31))
        assert result == []

    def test_files_without_dates_excluded_when_range_specified(self):
        """
        Edge case: files without dates are excluded when a date range is specified.
        """
        files = [
            {'id': '1', 'name': 'file1.xlsx', 'file_date': date(2024, 6, 15)},
            {'id': '2', 'name': 'file2.xlsx', 'file_date': None},
            {'id': '3', 'name': 'file3.xlsx', 'file_date': date(2024, 6, 20)},
        ]
        
        result = filter_files_by_date_range(files, date(2024, 6, 1), date(2024, 6, 30))
        
        # Only files with dates within range should be returned
        assert len(result) == 2
        result_ids = {f['id'] for f in result}
        assert '1' in result_ids
        assert '3' in result_ids
        assert '2' not in result_ids

    def test_boundary_dates_included(self):
        """
        Example test: boundary dates are inclusive.
        """
        files = [
            {'id': '1', 'name': 'file1.xlsx', 'file_date': date(2024, 1, 1)},
            {'id': '2', 'name': 'file2.xlsx', 'file_date': date(2024, 1, 15)},
            {'id': '3', 'name': 'file3.xlsx', 'file_date': date(2024, 1, 31)},
            {'id': '4', 'name': 'file4.xlsx', 'file_date': date(2024, 2, 1)},
        ]
        
        result = filter_files_by_date_range(files, date(2024, 1, 1), date(2024, 1, 31))
        
        # Files 1, 2, 3 should be included (boundary dates are inclusive)
        assert len(result) == 3
        result_ids = {f['id'] for f in result}
        assert '1' in result_ids  # date_from boundary
        assert '2' in result_ids  # middle
        assert '3' in result_ids  # date_to boundary
        assert '4' not in result_ids  # outside range

    def test_real_world_scenario(self):
        """
        Example test with realistic AA_BLE file scenario.
        """
        files = [
            {
                'id': 'abc123',
                'name': '11_отчет по АА_BLE со склейкой_MAGNIT_AUTO_!NEW!_2025-12-11.xlsx',
                'mimeType': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'file_date': date(2025, 12, 11)
            },
            {
                'id': 'def456',
                'name': 'AA_BLE_2025-12-10.xlsx',
                'mimeType': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'file_date': date(2025, 12, 10)
            },
            {
                'id': 'ghi789',
                'name': 'AA_BLE_2025-12-09.xlsx',
                'mimeType': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'file_date': date(2025, 12, 9)
            },
        ]
        
        # Filter for December 10-11
        result = filter_files_by_date_range(files, date(2025, 12, 10), date(2025, 12, 11))
        
        assert len(result) == 2
        result_dates = {f['file_date'] for f in result}
        assert date(2025, 12, 10) in result_dates
        assert date(2025, 12, 11) in result_dates
        assert date(2025, 12, 9) not in result_dates
