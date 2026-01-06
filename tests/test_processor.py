# AA_BLE Automation - Property-based tests for DataProcessor
"""
Property-based tests for DataProcessor module.
Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 3.4, 3.5
"""

from datetime import date, datetime, time, timedelta
from typing import Optional

import pandas as pd
import pytest
from hypothesis import given, strategies as st, settings, assume

from src.processing.processor import (
    DataProcessor,
    normalize_columns,
    parse_time,
    round_051,
    infer_file_date,
    create_minute_bin,
    STANDARD_COLUMNS,
    COLUMN_MAPPING,
    ZONE_NAMES,
)


# =============================================================================
# Strategies for generating test data
# =============================================================================

# Стратегия для генерации вариантов названий колонок
def column_name_variants():
    """Генерирует различные варианты написания названий колонок."""
    variants = list(COLUMN_MAPPING.keys())
    return st.sampled_from(variants)


# Стратегия для генерации времени в формате HH:MM:SS
@st.composite
def time_string_hms(draw):
    """Генерирует строку времени в формате HH:MM:SS."""
    h = draw(st.integers(min_value=0, max_value=23))
    m = draw(st.integers(min_value=0, max_value=59))
    s = draw(st.integers(min_value=0, max_value=59))
    return f"{h:02d}:{m:02d}:{s:02d}"


# Стратегия для генерации времени в формате HH:MM
@st.composite
def time_string_hm(draw):
    """Генерирует строку времени в формате HH:MM."""
    h = draw(st.integers(min_value=0, max_value=23))
    m = draw(st.integers(min_value=0, max_value=59))
    return f"{h:02d}:{m:02d}"


# Стратегия для генерации Excel serial time (дробная часть дня)
@st.composite
def excel_serial_time(draw):
    """Генерирует Excel serial time (0.0 - 0.99999)."""
    return draw(st.floats(min_value=0.0, max_value=0.99999, allow_nan=False, allow_infinity=False))


# Стратегия для генерации валидного time объекта
@st.composite
def valid_time(draw):
    """Генерирует валидный time объект."""
    h = draw(st.integers(min_value=0, max_value=23))
    m = draw(st.integers(min_value=0, max_value=59))
    s = draw(st.integers(min_value=0, max_value=59))
    return time(hour=h, minute=m, second=s)


# Стратегия для генерации валидной даты
@st.composite
def valid_date(draw):
    """Генерирует валидную дату."""
    return draw(st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31)))


# Стратегия для генерации табельного номера
def tn_number():
    """Генерирует табельный номер."""
    return st.text(
        alphabet=st.characters(whitelist_categories=('Nd',)),
        min_size=4,
        max_size=10
    ).filter(lambda x: x.strip() != '')


# =============================================================================
# Property 12: Column normalization
# Validates: Requirements 7.1
# =============================================================================

@settings(max_examples=100)
@given(
    tn_col=st.sampled_from(['тн', 'ТН', 'табельный номер', 'tn', 'tn_number']),
    shift_col=st.sampled_from(['день смены', 'День смены', 'дата смены', 'shift_day', 'дата']),
    tag_col=st.sampled_from(['ble-метка', 'BLE-метка', 'ble метка', 'метка', 'ble_tag', 'tag']),
    zone_col=st.sampled_from(['зона', 'Зона', 'zone', 'zone_id', 'id зоны']),
    time_col=st.sampled_from(['время на объекте', 'Время на объекте', 'время', 'time', 'time_only']),
    num_rows=st.integers(min_value=1, max_value=10)
)
def test_column_normalization(tn_col, shift_col, tag_col, zone_col, time_col, num_rows):
    """
    Feature: aa-ble-automation, Property 12: Column normalization
    Validates: Requirements 7.1
    
    For any AA_BLE input data regardless of column naming variations,
    the normalization function SHALL produce a DataFrame with standardized
    columns: tn_number, shift_day, ble_tag, zone_id, time_only.
    """
    # Создаём DataFrame с различными вариантами названий колонок
    data = {
        tn_col: [f'TN{i}' for i in range(num_rows)],
        shift_col: ['2024-01-01'] * num_rows,
        tag_col: list(range(num_rows)),
        zone_col: [1] * num_rows,
        time_col: ['08:00:00'] * num_rows,
    }
    df = pd.DataFrame(data)
    
    # Нормализуем колонки
    result = normalize_columns(df)
    
    # Проверяем, что результат содержит все стандартные колонки
    assert set(result.columns) == set(STANDARD_COLUMNS), \
        f"Expected columns {STANDARD_COLUMNS}, got {list(result.columns)}"
    
    # Проверяем, что данные сохранились
    assert len(result) == num_rows
    
    # Проверяем порядок колонок
    assert list(result.columns) == STANDARD_COLUMNS


# =============================================================================
# Property 13: Time format parsing
# Validates: Requirements 7.2
# =============================================================================

@settings(max_examples=100)
@given(time_str=time_string_hms())
def test_time_parsing_hms_format(time_str):
    """
    Feature: aa-ble-automation, Property 13: Time format parsing
    Validates: Requirements 7.2
    
    For any time value in format HH:MM:SS, the parse function SHALL return
    a valid time object representing the same time.
    """
    result = parse_time(time_str)
    
    assert result is not None, f"Failed to parse time string: {time_str}"
    assert isinstance(result, time), f"Expected time object, got {type(result)}"
    
    # Проверяем, что время совпадает
    parts = time_str.split(':')
    expected_h, expected_m, expected_s = int(parts[0]), int(parts[1]), int(parts[2])
    
    assert result.hour == expected_h, f"Hour mismatch: {result.hour} != {expected_h}"
    assert result.minute == expected_m, f"Minute mismatch: {result.minute} != {expected_m}"
    assert result.second == expected_s, f"Second mismatch: {result.second} != {expected_s}"


@settings(max_examples=100)
@given(time_str=time_string_hm())
def test_time_parsing_hm_format(time_str):
    """
    Feature: aa-ble-automation, Property 13: Time format parsing
    Validates: Requirements 7.2
    
    For any time value in format HH:MM, the parse function SHALL return
    a valid time object representing the same time.
    """
    result = parse_time(time_str)
    
    assert result is not None, f"Failed to parse time string: {time_str}"
    assert isinstance(result, time), f"Expected time object, got {type(result)}"
    
    # Проверяем, что время совпадает
    parts = time_str.split(':')
    expected_h, expected_m = int(parts[0]), int(parts[1])
    
    assert result.hour == expected_h, f"Hour mismatch: {result.hour} != {expected_h}"
    assert result.minute == expected_m, f"Minute mismatch: {result.minute} != {expected_m}"
    assert result.second == 0, f"Second should be 0 for HH:MM format"


@settings(max_examples=100)
@given(excel_time=excel_serial_time())
def test_time_parsing_excel_format(excel_time):
    """
    Feature: aa-ble-automation, Property 13: Time format parsing
    Validates: Requirements 7.2
    
    For any time value in Excel serial date format, the parse function SHALL
    return a valid time object representing the same time.
    """
    result = parse_time(excel_time)
    
    assert result is not None, f"Failed to parse Excel time: {excel_time}"
    assert isinstance(result, time), f"Expected time object, got {type(result)}"
    
    # Проверяем, что время в допустимых пределах
    assert 0 <= result.hour <= 23
    assert 0 <= result.minute <= 59
    assert 0 <= result.second <= 59
    
    # Проверяем обратное преобразование (с допуском на округление)
    total_seconds = result.hour * 3600 + result.minute * 60 + result.second
    expected_seconds = int(excel_time * 24 * 60 * 60)
    
    # Допускаем погрешность в 1 секунду из-за округления
    assert abs(total_seconds - expected_seconds) <= 1, \
        f"Time conversion mismatch: {total_seconds} vs {expected_seconds}"


@settings(max_examples=100)
@given(t=valid_time())
def test_time_parsing_time_object(t):
    """
    Feature: aa-ble-automation, Property 13: Time format parsing
    Validates: Requirements 7.2
    
    For any time object, the parse function SHALL return the same time object.
    """
    result = parse_time(t)
    
    assert result is not None
    assert result == t, f"Time object should be returned as-is: {result} != {t}"


# =============================================================================
# Property 14: Duration rounding (0.51 rule)
# Validates: Requirements 7.3
# =============================================================================

@settings(max_examples=100)
@given(value=st.floats(min_value=0, max_value=1000, allow_nan=False, allow_infinity=False))
def test_duration_rounding_051_rule(value):
    """
    Feature: aa-ble-automation, Property 14: Duration rounding (0.51 rule)
    Validates: Requirements 7.3
    
    For any float value, the rounding function SHALL round up if the
    fractional part is >= 0.51, and round down otherwise.
    Note: Testing only non-negative values as duration is always positive.
    """
    import math
    result = round_051(value)
    
    assert isinstance(result, int), f"Expected int, got {type(result)}"
    
    integer_part = math.floor(value)
    fractional_part = value - integer_part
    
    # Учитываем epsilon для сравнения с плавающей точкой
    if fractional_part >= 0.51 - 1e-9:
        expected = integer_part + 1
    else:
        expected = integer_part
    
    assert result == expected, \
        f"round_051({value}) = {result}, expected {expected} (frac={fractional_part})"


@settings(max_examples=100)
@given(integer=st.integers(min_value=0, max_value=100))
def test_rounding_boundary_050(integer):
    """
    Feature: aa-ble-automation, Property 14: Duration rounding (0.51 rule)
    Validates: Requirements 7.3
    
    Values with fractional part exactly 0.50 should round down.
    Note: Testing only non-negative values as duration is always positive.
    """
    value = integer + 0.50
    result = round_051(value)
    
    assert result == integer, f"round_051({value}) should be {integer}, got {result}"


@settings(max_examples=100)
@given(integer=st.integers(min_value=0, max_value=100))
def test_rounding_boundary_051(integer):
    """
    Feature: aa-ble-automation, Property 14: Duration rounding (0.51 rule)
    Validates: Requirements 7.3
    
    Values with fractional part exactly 0.51 should round up.
    Note: Testing only non-negative values as duration is always positive.
    """
    value = integer + 0.51
    result = round_051(value)
    
    assert result == integer + 1, f"round_051({value}) should be {integer + 1}, got {result}"


# =============================================================================
# Property 15: Minute bin creation
# Validates: Requirements 7.4
# =============================================================================

@settings(max_examples=100)
@given(
    tn=st.text(min_size=1, max_size=10).filter(lambda x: x.strip() != ''),
    shift_day=valid_date(),
    t=valid_time(),
    ble_tag=st.integers(min_value=0, max_value=1000),
    zone_id=st.integers(min_value=0, max_value=13)
)
def test_minute_bin_creation(tn, shift_day, t, ble_tag, zone_id):
    """
    Feature: aa-ble-automation, Property 15: Minute bin creation
    Validates: Requirements 7.4
    
    For any BLE tag reading, the segment builder SHALL create exactly one
    segment with duration of 1 minute.
    """
    segment = create_minute_bin(tn, shift_day, t, ble_tag, zone_id)
    
    # Проверяем, что сегмент создан
    assert segment is not None
    
    # Проверяем длительность ровно 1 минута
    assert segment['duration_minutes'] == 1.0, \
        f"Duration should be 1.0, got {segment['duration_minutes']}"
    
    # Проверяем, что end = start + 1 минута
    expected_end = segment['start'] + timedelta(minutes=1)
    assert segment['end'] == expected_end, \
        f"End time should be start + 1 minute: {segment['end']} != {expected_end}"
    
    # Проверяем, что данные сохранены корректно
    assert segment['tn_number'] == tn
    assert segment['date'] == shift_day
    assert segment['ble_tag'] == ble_tag
    assert segment['zone_id'] == zone_id
    
    # Проверяем, что zone_name определено
    assert 'zone_name' in segment
    assert segment['zone_name'] is not None


@settings(max_examples=100)
@given(
    num_readings=st.integers(min_value=1, max_value=20),
    tn=st.text(min_size=1, max_size=10).filter(lambda x: x.strip() != ''),
    shift_day=valid_date(),
    zone_id=st.integers(min_value=0, max_value=13)
)
def test_multiple_readings_create_multiple_bins(num_readings, tn, shift_day, zone_id):
    """
    Feature: aa-ble-automation, Property 15: Minute bin creation
    Validates: Requirements 7.4
    
    For N BLE tag readings, the processor SHALL create exactly N segments,
    each with duration of 1 minute.
    """
    processor = DataProcessor()
    
    # Создаём DataFrame с несколькими записями
    data = {
        'tn_number': [tn] * num_readings,
        'shift_day': [shift_day] * num_readings,
        'ble_tag': list(range(num_readings)),
        'zone_id': [zone_id] * num_readings,
        'time_only': [time(hour=8, minute=i % 60, second=0) for i in range(num_readings)],
    }
    df = pd.DataFrame(data)
    
    # Строим сегменты
    segments = processor.build_segments(df, {}, {})
    
    # Проверяем количество сегментов
    assert len(segments) == num_readings, \
        f"Expected {num_readings} segments, got {len(segments)}"
    
    # Проверяем, что каждый сегмент имеет длительность 1 минута
    for _, seg in segments.iterrows():
        assert seg['duration_minutes'] == 1.0


# =============================================================================
# Property 16: File date inference
# Validates: Requirements 7.5
# =============================================================================

@settings(max_examples=100)
@given(
    file_date=valid_date(),
    num_rows=st.integers(min_value=1, max_value=10)
)
def test_file_date_inference_from_date_column(file_date, num_rows):
    """
    Feature: aa-ble-automation, Property 16: File date inference
    Validates: Requirements 7.5
    
    For any DataFrame with a 'date' or 'дата' column, the file date inference
    function SHALL return the date from the first non-null value.
    """
    # Тестируем колонку 'date'
    df = pd.DataFrame({
        'date': [file_date] * num_rows,
        'other_col': ['data'] * num_rows,
    })
    
    result = infer_file_date(df)
    
    assert result == file_date, f"Expected {file_date}, got {result}"


@settings(max_examples=100)
@given(
    file_date=valid_date(),
    num_rows=st.integers(min_value=1, max_value=10)
)
def test_file_date_inference_from_shift_day(file_date, num_rows):
    """
    Feature: aa-ble-automation, Property 16: File date inference
    Validates: Requirements 7.5
    
    For any DataFrame with a 'shift_day' column, the file date inference
    function SHALL return the date from the first non-null value.
    """
    df = pd.DataFrame({
        'shift_day': [file_date] * num_rows,
        'other_col': ['data'] * num_rows,
    })
    
    result = infer_file_date(df)
    
    assert result == file_date, f"Expected {file_date}, got {result}"


@settings(max_examples=100)
@given(
    date1=valid_date(),
    date2=valid_date(),
    num_rows=st.integers(min_value=2, max_value=10)
)
def test_file_date_inference_priority(date1, date2, num_rows):
    """
    Feature: aa-ble-automation, Property 16: File date inference
    Validates: Requirements 7.5
    
    The 'date' column SHALL have higher priority than 'shift_day'.
    """
    assume(date1 != date2)  # Убеждаемся, что даты разные
    
    df = pd.DataFrame({
        'date': [date1] * num_rows,
        'shift_day': [date2] * num_rows,
    })
    
    result = infer_file_date(df)
    
    # 'date' имеет приоритет над 'shift_day'
    assert result == date1, f"Expected {date1} (from 'date' column), got {result}"


# =============================================================================
# Property 4: Tag 0 warning threshold
# Validates: Requirements 3.4
# =============================================================================

@settings(max_examples=100)
@given(
    count_above=st.integers(min_value=101, max_value=500),
    count_below=st.integers(min_value=1, max_value=100)
)
def test_tag_zero_warning_above_threshold(count_above, count_below):
    """
    Feature: aa-ble-automation, Property 4: Tag 0 warning threshold
    Validates: Requirements 3.4
    
    For any dataset, the system SHALL send a warning notification if and only if
    an employee has more than 100 records with tag 0.
    """
    processor = DataProcessor()
    
    # Создаём данные: один сотрудник выше порога, один ниже
    data = {
        'tn_number': ['EMP_ABOVE'] * count_above + ['EMP_BELOW'] * count_below,
        'ble_tag': [0] * count_above + [0] * count_below,
    }
    df = pd.DataFrame(data)
    
    result = processor.analyze_zero_tags(df)
    
    # EMP_ABOVE должен быть в результате (> 100)
    assert 'EMP_ABOVE' in result.index, \
        f"Employee with {count_above} tag 0 records should be in warning list"
    assert result['EMP_ABOVE'] == count_above
    
    # EMP_BELOW не должен быть в результате (<= 100)
    assert 'EMP_BELOW' not in result.index, \
        f"Employee with {count_below} tag 0 records should NOT be in warning list"


@settings(max_examples=100)
@given(count=st.integers(min_value=1, max_value=100))
def test_tag_zero_no_warning_at_threshold(count):
    """
    Feature: aa-ble-automation, Property 4: Tag 0 warning threshold
    Validates: Requirements 3.4
    
    Employees with exactly 100 or fewer tag 0 records should NOT trigger warning.
    """
    processor = DataProcessor()
    
    data = {
        'tn_number': ['EMP1'] * count,
        'ble_tag': [0] * count,
    }
    df = pd.DataFrame(data)
    
    result = processor.analyze_zero_tags(df)
    
    # Не должно быть предупреждений
    assert len(result) == 0, \
        f"No warning expected for {count} tag 0 records, but got {len(result)}"


def test_tag_zero_exactly_100_no_warning():
    """
    Feature: aa-ble-automation, Property 4: Tag 0 warning threshold
    Validates: Requirements 3.4
    
    Exactly 100 tag 0 records should NOT trigger warning (threshold is > 100).
    """
    processor = DataProcessor()
    
    data = {
        'tn_number': ['EMP1'] * 100,
        'ble_tag': [0] * 100,
    }
    df = pd.DataFrame(data)
    
    result = processor.analyze_zero_tags(df)
    
    assert len(result) == 0, "100 records should not trigger warning"


def test_tag_zero_exactly_101_triggers_warning():
    """
    Feature: aa-ble-automation, Property 4: Tag 0 warning threshold
    Validates: Requirements 3.4
    
    Exactly 101 tag 0 records SHOULD trigger warning.
    """
    processor = DataProcessor()
    
    data = {
        'tn_number': ['EMP1'] * 101,
        'ble_tag': [0] * 101,
    }
    df = pd.DataFrame(data)
    
    result = processor.analyze_zero_tags(df)
    
    assert len(result) == 1, "101 records should trigger warning"
    assert 'EMP1' in result.index


# =============================================================================
# Property 5: Time gap detection
# Validates: Requirements 3.5
# =============================================================================

@settings(max_examples=100)
@given(
    gap_minutes=st.integers(min_value=2, max_value=30),
    base_hour=st.integers(min_value=6, max_value=18)
)
def test_time_gap_detection_above_threshold(gap_minutes, base_hour):
    """
    Feature: aa-ble-automation, Property 5: Time gap detection
    Validates: Requirements 3.5
    
    For any sequence of time records for an employee, the system SHALL detect
    and report all gaps greater than 1 minute between consecutive records.
    """
    processor = DataProcessor()
    
    # Используем timedelta для корректного добавления минут
    base_dt = datetime(2024, 1, 15, base_hour, 0, 0)
    
    # Создаём сегменты с разрывом
    segments = pd.DataFrame([
        {
            'tn_number': 'EMP1',
            'start': base_dt,
            'end': base_dt + timedelta(minutes=1),
        },
        {
            'tn_number': 'EMP1',
            'start': base_dt + timedelta(minutes=1 + gap_minutes),
            'end': base_dt + timedelta(minutes=2 + gap_minutes),
        },
    ])
    
    result = processor.analyze_time_gaps(segments, {})
    
    # Должен быть обнаружен разрыв
    assert 'EMP1' in result, f"Gap of {gap_minutes} minutes should be detected"
    assert len(result['EMP1']) == 1, f"Expected 1 gap, got {len(result['EMP1'])}"
    assert result['EMP1'][0]['gap_minutes'] == gap_minutes


@settings(max_examples=100)
@given(num_consecutive=st.integers(min_value=2, max_value=20))
def test_no_gap_for_consecutive_minutes(num_consecutive):
    """
    Feature: aa-ble-automation, Property 5: Time gap detection
    Validates: Requirements 3.5
    
    Consecutive 1-minute segments should NOT be reported as gaps.
    """
    processor = DataProcessor()
    
    # Создаём последовательные сегменты без разрывов
    segments_data = []
    for i in range(num_consecutive):
        segments_data.append({
            'tn_number': 'EMP1',
            'start': datetime(2024, 1, 15, 8, i, 0),
            'end': datetime(2024, 1, 15, 8, i + 1, 0),
        })
    
    segments = pd.DataFrame(segments_data)
    
    result = processor.analyze_time_gaps(segments, {})
    
    # Не должно быть разрывов
    assert 'EMP1' not in result or len(result.get('EMP1', [])) == 0, \
        f"No gaps expected for consecutive segments, but got {result}"


@settings(max_examples=100)
@given(
    num_gaps=st.integers(min_value=1, max_value=5),
    gap_size=st.integers(min_value=2, max_value=10)
)
def test_multiple_gaps_detected(num_gaps, gap_size):
    """
    Feature: aa-ble-automation, Property 5: Time gap detection
    Validates: Requirements 3.5
    
    All gaps greater than 1 minute should be detected.
    """
    processor = DataProcessor()
    
    # Создаём сегменты с несколькими разрывами
    segments_data = []
    current_minute = 0
    
    for i in range(num_gaps + 1):
        segments_data.append({
            'tn_number': 'EMP1',
            'start': datetime(2024, 1, 15, 8, current_minute, 0),
            'end': datetime(2024, 1, 15, 8, current_minute + 1, 0),
        })
        current_minute += 1 + gap_size  # Добавляем разрыв
    
    segments = pd.DataFrame(segments_data)
    
    result = processor.analyze_time_gaps(segments, {})
    
    # Должно быть обнаружено num_gaps разрывов
    assert 'EMP1' in result, "Gaps should be detected"
    assert len(result['EMP1']) == num_gaps, \
        f"Expected {num_gaps} gaps, got {len(result['EMP1'])}"
