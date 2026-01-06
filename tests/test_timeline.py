# AA_BLE Automation - Property-based tests for TimelineBuilder
"""
Property-based tests for TimelineBuilder module.
Requirements: 4.1, 4.2
"""

from datetime import date, datetime, time, timedelta
from typing import Optional

import pandas as pd
import pytest
from hypothesis import given, strategies as st, settings, assume

from src.processing.timeline import (
    TimelineBuilder,
    filter_by_time_window,
    calculate_zone_statistics,
    is_within_time_window,
    ZONE_PALETTES,
)
from src.processing.processor import ZONE_NAMES


# =============================================================================
# Strategies for generating test data
# =============================================================================

@st.composite
def valid_time(draw):
    """Генерирует валидный time объект."""
    h = draw(st.integers(min_value=0, max_value=23))
    m = draw(st.integers(min_value=0, max_value=59))
    s = draw(st.integers(min_value=0, max_value=59))
    return time(hour=h, minute=m, second=s)


@st.composite
def time_within_window(draw, window_start=(6, 0), window_end=(21, 0)):
    """Генерирует время внутри заданного окна."""
    start_minutes = window_start[0] * 60 + window_start[1]
    end_minutes = window_end[0] * 60 + window_end[1] - 1  # -1 чтобы не включать границу
    
    total_minutes = draw(st.integers(min_value=start_minutes, max_value=end_minutes))
    h = total_minutes // 60
    m = total_minutes % 60
    return time(hour=h, minute=m, second=0)


@st.composite
def time_outside_window(draw, window_start=(6, 0), window_end=(21, 0)):
    """Генерирует время вне заданного окна."""
    start_minutes = window_start[0] * 60 + window_start[1]
    end_minutes = window_end[0] * 60 + window_end[1]
    
    # Выбираем либо до начала окна, либо после конца
    before_window = draw(st.booleans())
    
    if before_window and start_minutes > 0:
        total_minutes = draw(st.integers(min_value=0, max_value=start_minutes - 1))
    else:
        # После окна (от end_minutes до 23:59)
        total_minutes = draw(st.integers(min_value=end_minutes, max_value=23 * 60 + 59))
    
    h = total_minutes // 60
    m = total_minutes % 60
    return time(hour=h, minute=m, second=0)


@st.composite
def valid_date(draw):
    """Генерирует валидную дату."""
    return draw(st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 31)))


@st.composite
def segment_dataframe(draw, num_rows=None, time_strategy=None):
    """Генерирует DataFrame с сегментами."""
    if num_rows is None:
        num_rows = draw(st.integers(min_value=1, max_value=20))
    
    if time_strategy is None:
        time_strategy = valid_time()
    
    base_date = draw(valid_date())
    
    segments = []
    for _ in range(num_rows):
        t = draw(time_strategy)
        zone_id = draw(st.integers(min_value=0, max_value=13))
        duration = draw(st.floats(min_value=0.5, max_value=5.0))
        
        start_dt = datetime.combine(base_date, t)
        end_dt = start_dt + timedelta(minutes=duration)
        
        segments.append({
            'tn_number': 'EMP1',
            'employee': 'Test Employee',
            'start': start_dt,
            'end': end_dt,
            'duration_minutes': duration,
            'zone_id': zone_id,
            'zone_name': ZONE_NAMES.get(zone_id, f'Зона {zone_id}'),
            'ble_tag': draw(st.integers(min_value=0, max_value=100)),
        })
    
    return pd.DataFrame(segments)


# =============================================================================
# Property 7: Timeline window filtering
# Validates: Requirements 4.1
# =============================================================================

@settings(max_examples=100)
@given(
    num_inside=st.integers(min_value=1, max_value=10),
    num_outside=st.integers(min_value=1, max_value=10),
    base_date=valid_date()
)
def test_timeline_window_filtering_only_includes_window_data(num_inside, num_outside, base_date):
    """
    Feature: aa-ble-automation, Property 7: Timeline window filtering
    Validates: Requirements 4.1
    
    For any set of timeline segments, the generated chart SHALL only include
    data points within the 6:00-21:00 time window.
    """
    builder = TimelineBuilder(window_start=(6, 0), window_end=(21, 0))
    
    # Создаём сегменты внутри окна
    inside_segments = []
    for i in range(num_inside):
        # Время внутри окна: 6:00 - 20:59
        hour = 6 + (i % 15)  # 6-20
        minute = (i * 7) % 60
        t = time(hour=hour, minute=minute)
        start_dt = datetime.combine(base_date, t)
        
        inside_segments.append({
            'tn_number': 'EMP1',
            'employee': 'Test',
            'start': start_dt,
            'end': start_dt + timedelta(minutes=1),
            'duration_minutes': 1.0,
            'zone_id': 1,
            'zone_name': 'Test Zone',
            'ble_tag': i,
        })
    
    # Создаём сегменты вне окна
    outside_segments = []
    for i in range(num_outside):
        # Время вне окна: 0:00-5:59 или 21:00-23:59
        if i % 2 == 0:
            hour = i % 6  # 0-5
        else:
            hour = 21 + (i % 3)  # 21-23
        minute = (i * 11) % 60
        t = time(hour=hour, minute=minute)
        start_dt = datetime.combine(base_date, t)
        
        outside_segments.append({
            'tn_number': 'EMP1',
            'employee': 'Test',
            'start': start_dt,
            'end': start_dt + timedelta(minutes=1),
            'duration_minutes': 1.0,
            'zone_id': 1,
            'zone_name': 'Test Zone',
            'ble_tag': 100 + i,
        })
    
    # Объединяем все сегменты
    all_segments = pd.DataFrame(inside_segments + outside_segments)
    
    # Фильтруем
    result = builder.filter_by_time_window(all_segments)
    
    # Проверяем, что результат содержит только сегменты внутри окна
    assert len(result) == num_inside, \
        f"Expected {num_inside} segments inside window, got {len(result)}"
    
    # Проверяем, что все результаты внутри окна
    for _, row in result.iterrows():
        start_time = row['start'].time()
        assert builder._is_within_window(start_time), \
            f"Time {start_time} should be within window 6:00-21:00"


@settings(max_examples=100)
@given(t=time_within_window())
def test_time_within_window_returns_true(t):
    """
    Feature: aa-ble-automation, Property 7: Timeline window filtering
    Validates: Requirements 4.1
    
    For any time within 6:00-21:00, is_within_window SHALL return True.
    """
    result = is_within_time_window(t, window_start=(6, 0), window_end=(21, 0))
    
    assert result is True, \
        f"Time {t} should be within window 6:00-21:00"


@settings(max_examples=100)
@given(t=time_outside_window())
def test_time_outside_window_returns_false(t):
    """
    Feature: aa-ble-automation, Property 7: Timeline window filtering
    Validates: Requirements 4.1
    
    For any time outside 6:00-21:00, is_within_window SHALL return False.
    """
    result = is_within_time_window(t, window_start=(6, 0), window_end=(21, 0))
    
    assert result is False, \
        f"Time {t} should be outside window 6:00-21:00"


@settings(max_examples=100)
@given(base_date=valid_date())
def test_window_boundary_start_included(base_date):
    """
    Feature: aa-ble-automation, Property 7: Timeline window filtering
    Validates: Requirements 4.1
    
    The start boundary (6:00) SHALL be included in the window.
    """
    builder = TimelineBuilder(window_start=(6, 0), window_end=(21, 0))
    
    # Создаём сегмент ровно в 6:00
    start_dt = datetime.combine(base_date, time(6, 0, 0))
    df = pd.DataFrame([{
        'tn_number': 'EMP1',
        'start': start_dt,
        'end': start_dt + timedelta(minutes=1),
        'duration_minutes': 1.0,
        'zone_id': 1,
    }])
    
    result = builder.filter_by_time_window(df)
    
    assert len(result) == 1, "6:00 should be included in window"


@settings(max_examples=100)
@given(base_date=valid_date())
def test_window_boundary_end_excluded(base_date):
    """
    Feature: aa-ble-automation, Property 7: Timeline window filtering
    Validates: Requirements 4.1
    
    The end boundary (21:00) SHALL be excluded from the window.
    """
    builder = TimelineBuilder(window_start=(6, 0), window_end=(21, 0))
    
    # Создаём сегмент ровно в 21:00
    start_dt = datetime.combine(base_date, time(21, 0, 0))
    df = pd.DataFrame([{
        'tn_number': 'EMP1',
        'start': start_dt,
        'end': start_dt + timedelta(minutes=1),
        'duration_minutes': 1.0,
        'zone_id': 1,
    }])
    
    result = builder.filter_by_time_window(df)
    
    assert len(result) == 0, "21:00 should be excluded from window"


# =============================================================================
# Property 8: Zone statistics calculation
# Validates: Requirements 4.2
# =============================================================================

@settings(max_examples=100)
@given(
    zone_id=st.integers(min_value=0, max_value=13),
    num_segments=st.integers(min_value=1, max_value=20),
    duration=st.floats(min_value=0.5, max_value=10.0, allow_nan=False, allow_infinity=False)
)
def test_zone_statistics_single_zone(zone_id, num_segments, duration):
    """
    Feature: aa-ble-automation, Property 8: Zone statistics calculation
    Validates: Requirements 4.2
    
    For any set of segments for an employee, the zone statistics SHALL equal
    the sum of duration_minutes grouped by zone_id.
    """
    # Создаём сегменты для одной зоны
    segments = pd.DataFrame([{
        'zone_id': zone_id,
        'duration_minutes': duration,
    } for _ in range(num_segments)])
    
    result = calculate_zone_statistics(segments)
    
    # Проверяем, что есть ровно одна запись
    assert len(result) == 1, f"Expected 1 zone, got {len(result)}"
    
    # Проверяем сумму
    expected_total = num_segments * duration
    actual_total = result['total_minutes'].iloc[0]
    
    assert abs(actual_total - expected_total) < 0.001, \
        f"Expected total {expected_total}, got {actual_total}"
    
    # Проверяем zone_id
    assert result['zone_id'].iloc[0] == zone_id


@settings(max_examples=100)
@given(
    durations_zone1=st.lists(
        st.floats(min_value=0.5, max_value=10.0, allow_nan=False, allow_infinity=False),
        min_size=1, max_size=10
    ),
    durations_zone2=st.lists(
        st.floats(min_value=0.5, max_value=10.0, allow_nan=False, allow_infinity=False),
        min_size=1, max_size=10
    )
)
def test_zone_statistics_multiple_zones(durations_zone1, durations_zone2):
    """
    Feature: aa-ble-automation, Property 8: Zone statistics calculation
    Validates: Requirements 4.2
    
    For multiple zones, statistics SHALL correctly sum duration_minutes per zone.
    """
    # Создаём сегменты для двух зон
    segments_data = []
    
    for d in durations_zone1:
        segments_data.append({'zone_id': 1, 'duration_minutes': d})
    
    for d in durations_zone2:
        segments_data.append({'zone_id': 4, 'duration_minutes': d})
    
    segments = pd.DataFrame(segments_data)
    
    result = calculate_zone_statistics(segments)
    
    # Проверяем, что есть 2 зоны
    assert len(result) == 2, f"Expected 2 zones, got {len(result)}"
    
    # Проверяем суммы
    expected_zone1 = sum(durations_zone1)
    expected_zone4 = sum(durations_zone2)
    
    zone1_row = result[result['zone_id'] == 1]
    zone4_row = result[result['zone_id'] == 4]
    
    assert len(zone1_row) == 1, "Zone 1 should be present"
    assert len(zone4_row) == 1, "Zone 4 should be present"
    
    assert abs(zone1_row['total_minutes'].iloc[0] - expected_zone1) < 0.001, \
        f"Zone 1: expected {expected_zone1}, got {zone1_row['total_minutes'].iloc[0]}"
    
    assert abs(zone4_row['total_minutes'].iloc[0] - expected_zone4) < 0.001, \
        f"Zone 4: expected {expected_zone4}, got {zone4_row['total_minutes'].iloc[0]}"


@settings(max_examples=100)
@given(
    zone_ids=st.lists(
        st.integers(min_value=0, max_value=13),
        min_size=1, max_size=50
    ),
    base_duration=st.floats(min_value=0.5, max_value=5.0, allow_nan=False, allow_infinity=False)
)
def test_zone_statistics_sum_equals_total(zone_ids, base_duration):
    """
    Feature: aa-ble-automation, Property 8: Zone statistics calculation
    Validates: Requirements 4.2
    
    The sum of all zone statistics SHALL equal the total duration of all segments.
    """
    # Создаём сегменты с разными зонами
    segments = pd.DataFrame([{
        'zone_id': zone_id,
        'duration_minutes': base_duration,
    } for zone_id in zone_ids])
    
    result = calculate_zone_statistics(segments)
    
    # Сумма всех статистик должна равняться общей сумме
    expected_total = len(zone_ids) * base_duration
    actual_total = result['total_minutes'].sum()
    
    assert abs(actual_total - expected_total) < 0.001, \
        f"Total mismatch: expected {expected_total}, got {actual_total}"


@settings(max_examples=100)
@given(zone_id=st.integers(min_value=0, max_value=13))
def test_zone_statistics_includes_zone_name(zone_id):
    """
    Feature: aa-ble-automation, Property 8: Zone statistics calculation
    Validates: Requirements 4.2
    
    Zone statistics SHALL include the zone name from ZONE_NAMES.
    """
    segments = pd.DataFrame([{
        'zone_id': zone_id,
        'duration_minutes': 5.0,
    }])
    
    result = calculate_zone_statistics(segments)
    
    assert 'zone_name' in result.columns, "zone_name column should be present"
    
    expected_name = ZONE_NAMES.get(zone_id, f'Зона {zone_id}')
    actual_name = result['zone_name'].iloc[0]
    
    assert actual_name == expected_name, \
        f"Zone name mismatch: expected '{expected_name}', got '{actual_name}'"


def test_zone_statistics_empty_dataframe():
    """
    Feature: aa-ble-automation, Property 8: Zone statistics calculation
    Validates: Requirements 4.2
    
    Empty DataFrame SHALL return empty statistics.
    """
    result = calculate_zone_statistics(pd.DataFrame())
    
    assert len(result) == 0, "Empty input should return empty statistics"
    assert list(result.columns) == ['zone_id', 'zone_name', 'total_minutes']


def test_zone_statistics_sorted_by_duration():
    """
    Feature: aa-ble-automation, Property 8: Zone statistics calculation
    Validates: Requirements 4.2
    
    Statistics SHALL be sorted by total_minutes in descending order.
    """
    segments = pd.DataFrame([
        {'zone_id': 1, 'duration_minutes': 10.0},
        {'zone_id': 2, 'duration_minutes': 30.0},
        {'zone_id': 3, 'duration_minutes': 20.0},
    ])
    
    result = calculate_zone_statistics(segments)
    
    # Проверяем порядок: 2 (30), 3 (20), 1 (10)
    assert result['zone_id'].tolist() == [2, 3, 1], \
        f"Expected order [2, 3, 1], got {result['zone_id'].tolist()}"
