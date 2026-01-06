# AA_BLE Automation - Property-based tests for ReportGenerator
"""
Property-based tests for ReportGenerator module.
Requirements: 4.3, 4.4
"""

from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
import pytest
from hypothesis import given, strategies as st, settings, assume

from src.reports.generator import (
    ReportGenerator,
    generate_html_report,
    validate_html_structure,
)
from src.processing.timeline import TimelineBuilder


# =============================================================================
# Strategies for generating test data
# =============================================================================

@st.composite
def valid_section_id(draw):
    """Генерирует валидный ID секции."""
    # ID должен быть валидным HTML id (буквы, цифры, дефисы, подчёркивания)
    prefix = draw(st.sampled_from(['section', 'emp', 'user', 'report']))
    num = draw(st.integers(min_value=1, max_value=1000))
    return f"{prefix}-{num}"


@st.composite
def valid_section_title(draw):
    """Генерирует валидный заголовок секции."""
    # Используем простые ASCII строки для надёжности
    name = draw(st.text(
        alphabet=st.characters(whitelist_categories=('L', 'N', 'Zs')),
        min_size=1,
        max_size=50
    ).filter(lambda x: x.strip()))
    return name


@st.composite
def html_section(draw):
    """Генерирует секцию для HTML-отчёта."""
    section_id = draw(valid_section_id())
    title = draw(valid_section_title())
    
    # Опционально добавляем статистику
    has_stats = draw(st.booleans())
    stats_html = ""
    if has_stats:
        stats_html = "<div class='zone-stats'><h4>Статистика</h4><ul><li>Зона 1: 30м</li></ul></div>"
    
    # Опционально добавляем график (простой placeholder)
    has_chart = draw(st.booleans())
    chart_html = ""
    if has_chart:
        chart_html = "<div class='plotly-graph-div'>Chart placeholder</div>"
    
    return {
        'id': section_id,
        'title': title,
        'stats_html': stats_html,
        'chart_html': chart_html,
    }


@st.composite
def sections_list(draw, min_size=1, max_size=10):
    """Генерирует список секций."""
    num_sections = draw(st.integers(min_value=min_size, max_value=max_size))
    sections = []
    
    for i in range(num_sections):
        section = draw(html_section())
        # Гарантируем уникальность ID
        section['id'] = f"{section['id']}-{i}"
        sections.append(section)
    
    return sections


# =============================================================================
# Property 9: HTML report structure
# Validates: Requirements 4.3
# =============================================================================

@settings(max_examples=100)
@given(sections=sections_list(min_size=1, max_size=5))
def test_html_report_has_table_of_contents(sections):
    """
    Feature: aa-ble-automation, Property 9: HTML report structure
    Validates: Requirements 4.3
    
    For any generated HTML report, the output SHALL contain a table of contents element.
    """
    html = generate_html_report(sections, title="Test Report")
    
    validation = validate_html_structure(html)
    
    assert validation['has_toc'], \
        "HTML report must contain a table of contents"
    
    # Проверяем, что оглавление содержит ссылки на все секции
    for section in sections:
        section_id = section['id']
        assert f'href="#{section_id}"' in html, \
            f"Table of contents must contain link to section '{section_id}'"


@settings(max_examples=100)
@given(sections=sections_list(min_size=1, max_size=5))
def test_html_report_has_zone_stats_panel(sections):
    """
    Feature: aa-ble-automation, Property 9: HTML report structure
    Validates: Requirements 4.3
    
    For any generated HTML report, the output SHALL contain zone statistics panel.
    """
    html = generate_html_report(sections, title="Test Report")
    
    validation = validate_html_structure(html)
    
    assert validation['has_zone_stats_panel'], \
        "HTML report must contain zone statistics panel"
    
    # Количество панелей должно соответствовать количеству секций
    assert validation['stats_panels_count'] == len(sections), \
        f"Expected {len(sections)} stats panels, got {validation['stats_panels_count']}"


@settings(max_examples=100)
@given(sections=sections_list(min_size=1, max_size=5))
def test_html_report_has_plotly_chart_div(sections):
    """
    Feature: aa-ble-automation, Property 9: HTML report structure
    Validates: Requirements 4.3
    
    For any generated HTML report, the output SHALL contain at least one Plotly chart div.
    """
    html = generate_html_report(sections, title="Test Report")
    
    validation = validate_html_structure(html)
    
    # Проверяем наличие контейнера для графиков
    assert 'class="plotly-chart"' in html, \
        "HTML report must contain plotly-chart container"


@settings(max_examples=100)
@given(
    title=st.text(min_size=1, max_size=100).filter(lambda x: x.strip()),
    sections=sections_list(min_size=1, max_size=3)
)
def test_html_report_contains_title(title, sections):
    """
    Feature: aa-ble-automation, Property 9: HTML report structure
    Validates: Requirements 4.3
    
    For any generated HTML report, the output SHALL contain the specified title.
    """
    # Фильтруем специальные HTML символы для корректного сравнения
    safe_title = title.replace('<', '').replace('>', '').replace('&', '').strip()
    assume(len(safe_title) > 0)
    
    html = generate_html_report(sections, title=safe_title)
    
    assert f"<title>{safe_title}</title>" in html, \
        f"HTML report must contain title '{safe_title}'"
    
    assert f"<h1>{safe_title}</h1>" in html, \
        f"HTML report must contain h1 with title '{safe_title}'"


@settings(max_examples=100)
@given(sections=sections_list(min_size=1, max_size=5))
def test_html_report_sections_have_correct_ids(sections):
    """
    Feature: aa-ble-automation, Property 9: HTML report structure
    Validates: Requirements 4.3
    
    For any generated HTML report, each section SHALL have the correct id attribute.
    """
    html = generate_html_report(sections, title="Test Report")
    
    for section in sections:
        section_id = section['id']
        # Проверяем, что секция с таким id существует
        assert f'id="{section_id}"' in html, \
            f"HTML report must contain section with id '{section_id}'"


@settings(max_examples=100)
@given(sections=sections_list(min_size=1, max_size=5))
def test_html_report_sections_have_titles(sections):
    """
    Feature: aa-ble-automation, Property 9: HTML report structure
    Validates: Requirements 4.3
    
    For any generated HTML report, each section SHALL contain its title.
    """
    html = generate_html_report(sections, title="Test Report")
    
    for section in sections:
        section_title = section['title']
        # Проверяем, что заголовок секции присутствует
        assert section_title in html, \
            f"HTML report must contain section title '{section_title}'"


@settings(max_examples=100)
@given(num_sections=st.integers(min_value=1, max_value=10))
def test_html_report_toc_links_count_matches_sections(num_sections):
    """
    Feature: aa-ble-automation, Property 9: HTML report structure
    Validates: Requirements 4.3
    
    For any generated HTML report, the number of TOC links SHALL equal the number of sections.
    """
    sections = [
        {
            'id': f'section-{i}',
            'title': f'Section {i}',
            'stats_html': '',
            'chart_html': '',
        }
        for i in range(num_sections)
    ]
    
    html = generate_html_report(sections, title="Test Report")
    
    # Считаем количество ссылок в оглавлении
    toc_link_count = sum(1 for section in sections if f'href="#{section["id"]}"' in html)
    
    assert toc_link_count == num_sections, \
        f"Expected {num_sections} TOC links, got {toc_link_count}"


def test_html_report_empty_sections_list():
    """
    Feature: aa-ble-automation, Property 9: HTML report structure
    Validates: Requirements 4.3
    
    Empty sections list SHALL still produce valid HTML with TOC structure.
    """
    html = generate_html_report([], title="Empty Report")
    
    validation = validate_html_structure(html)
    
    # Даже пустой отчёт должен иметь структуру оглавления
    assert validation['has_toc'], \
        "Empty HTML report must still contain TOC structure"
    
    # Проверяем базовую HTML структуру
    assert '<!DOCTYPE html>' in html
    assert '<html' in html
    assert '</html>' in html


def test_html_report_includes_plotly_js():
    """
    Feature: aa-ble-automation, Property 9: HTML report structure
    Validates: Requirements 4.3
    
    HTML report SHALL include Plotly.js library.
    """
    sections = [{'id': 'test', 'title': 'Test', 'stats_html': '', 'chart_html': ''}]
    html = generate_html_report(sections, title="Test Report")
    
    assert 'plotly' in html.lower(), \
        "HTML report must include Plotly.js reference"


def test_html_report_has_proper_encoding():
    """
    Feature: aa-ble-automation, Property 9: HTML report structure
    Validates: Requirements 4.3
    
    HTML report SHALL specify UTF-8 encoding for Cyrillic support.
    """
    sections = [{'id': 'test', 'title': 'Тест', 'stats_html': '', 'chart_html': ''}]
    html = generate_html_report(sections, title="Тестовый отчёт")
    
    assert 'charset="UTF-8"' in html or 'charset=UTF-8' in html, \
        "HTML report must specify UTF-8 encoding"
    
    # Проверяем, что кириллица сохранена
    assert 'Тест' in html, "Cyrillic characters must be preserved"


# =============================================================================
# Unit tests for ReportGenerator
# =============================================================================

def test_report_generator_initialization():
    """Test ReportGenerator initialization."""
    builder = TimelineBuilder()
    generator = ReportGenerator(timeline_builder=builder)
    
    assert generator.timeline_builder is builder
    assert generator.logger_tg is None


def test_fig_to_div_with_none():
    """Test fig_to_div handles None figure."""
    builder = TimelineBuilder()
    generator = ReportGenerator(timeline_builder=builder)
    
    result = generator.fig_to_div(None, "Test Title")
    
    assert "Test Title" in result
    assert "Нет данных" in result


def test_validate_html_structure_function():
    """Test validate_html_structure helper function."""
    html = """
    <html>
    <nav class="table-of-contents" id="toc">
        <h2>Оглавление</h2>
    </nav>
    <div class="zone-stats-panel">Stats</div>
    <div class="plotly-chart">Chart</div>
    </html>
    """
    
    result = validate_html_structure(html)
    
    assert result['has_toc'] is True
    assert result['has_zone_stats_panel'] is True
    assert result['stats_panels_count'] == 1
