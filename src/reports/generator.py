# AA_BLE Automation - Report Generator
"""
Модуль генерации HTML и Excel отчётов.
Requirements: 4.3, 4.4
"""

from datetime import date
from io import BytesIO
from typing import Optional
import logging

import pandas as pd
import plotly.graph_objects as go

from src.processing.timeline import TimelineBuilder
from src.clients.telegram import TelegramLogger


logger = logging.getLogger(__name__)


class ReportGenerator:
    """
    Генерация HTML и Excel отчётов.
    
    Обеспечивает:
    - Генерацию объединённого HTML-отчёта с оглавлением
    - Генерацию Excel-отчёта с листами событий и сводными таблицами
    - Конвертацию Plotly-фигур в HTML
    """
    
    def __init__(
        self, 
        timeline_builder: TimelineBuilder,
        logger_tg: Optional[TelegramLogger] = None
    ):
        """
        Инициализация генератора отчётов.
        
        Args:
            timeline_builder: Построитель таймлайнов
            logger_tg: Telegram-логгер для уведомлений
        """
        self.timeline_builder = timeline_builder
        self.logger_tg = logger_tg
    
    def fig_to_div(self, fig: go.Figure, title: str = "") -> str:
        """
        Конвертация Plotly-фигуры в HTML div.
        
        Args:
            fig: Plotly Figure объект
            title: Заголовок для div
            
        Returns:
            HTML-строка с div содержащим график
        """
        if fig is None:
            return f"<div class='chart-container'><h3>{title}</h3><p>Нет данных</p></div>"
        
        # Генерируем HTML для графика без полной страницы
        chart_html = fig.to_html(
            full_html=False,
            include_plotlyjs=False,
            div_id=f"chart-{hash(title) % 10000}"
        )
        
        return f"""<div class='chart-container'>
    <h3>{title}</h3>
    {chart_html}
</div>"""

    def generate_combined_html(
        self, 
        sections: list[dict], 
        filename: str,
        title: str = "AA_BLE Report"
    ) -> str:
        """
        Генерация объединённого HTML-отчёта.
        
        Requirement: 4.3 - HTML с оглавлением, статистикой и графиками
        
        Args:
            sections: Список секций, каждая содержит:
                - id: уникальный идентификатор
                - title: заголовок секции
                - chart_html: HTML графика (опционально)
                - stats_html: HTML статистики (опционально)
                - employee: имя сотрудника (опционально)
            filename: Имя выходного файла
            title: Заголовок отчёта
            
        Returns:
            Путь к созданному файлу
        """
        # Генерируем оглавление
        toc_items = []
        for section in sections:
            section_id = section.get('id', '')
            section_title = section.get('title', 'Без названия')
            toc_items.append(f'<li><a href="#{section_id}">{section_title}</a></li>')
        
        toc_html = f"""<nav class="table-of-contents" id="toc">
    <h2>Оглавление</h2>
    <ul>
        {''.join(toc_items)}
    </ul>
</nav>"""
        
        # Генерируем секции контента
        content_sections = []
        for section in sections:
            section_id = section.get('id', '')
            section_title = section.get('title', 'Без названия')
            chart_html = section.get('chart_html', '')
            stats_html = section.get('stats_html', '')
            
            section_html = f"""<section id="{section_id}" class="report-section">
    <h2>{section_title}</h2>
    <div class="zone-stats-panel">
        {stats_html}
    </div>
    <div class="plotly-chart">
        {chart_html}
    </div>
</section>"""
            content_sections.append(section_html)
        
        # Собираем полный HTML
        html_content = self._generate_html_template(
            title=title,
            toc_html=toc_html,
            content_html='\n'.join(content_sections)
        )
        
        # Записываем в файл
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            if self.logger_tg:
                self.logger_tg.info(f"HTML-отчёт создан: {filename}")
            
            return filename
        except Exception as e:
            logger.error(f"Ошибка создания HTML-отчёта: {e}")
            if self.logger_tg:
                self.logger_tg.error(f"Ошибка создания HTML-отчёта: {e}", e)
            raise
    
    def _generate_html_template(
        self, 
        title: str, 
        toc_html: str, 
        content_html: str
    ) -> str:
        """Генерация HTML-шаблона с CSS и Plotly.js."""
        return f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        h1 {{
            color: #333;
            border-bottom: 2px solid #007bff;
            padding-bottom: 10px;
        }}
        .table-of-contents {{
            background: #fff;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }}
        .table-of-contents h2 {{
            margin-top: 0;
            color: #007bff;
        }}
        .table-of-contents ul {{
            list-style-type: none;
            padding-left: 0;
        }}
        .table-of-contents li {{
            padding: 5px 0;
        }}
        .table-of-contents a {{
            color: #333;
            text-decoration: none;
        }}
        .table-of-contents a:hover {{
            color: #007bff;
            text-decoration: underline;
        }}
        .report-section {{
            background: #fff;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }}
        .report-section h2 {{
            color: #333;
            border-bottom: 1px solid #ddd;
            padding-bottom: 10px;
        }}
        .zone-stats-panel {{
            background: #f8f9fa;
            padding: 15px;
            border-radius: 4px;
            margin-bottom: 15px;
        }}
        .zone-stats h4 {{
            margin-top: 0;
            color: #495057;
        }}
        .zone-stats ul {{
            list-style-type: none;
            padding-left: 0;
            margin: 0;
        }}
        .zone-stats li {{
            padding: 3px 0;
        }}
        .plotly-chart {{
            width: 100%;
            overflow-x: auto;
        }}
        .chart-container {{
            margin: 10px 0;
        }}
        .chart-container h3 {{
            color: #495057;
            margin-bottom: 10px;
        }}
    </style>
</head>
<body>
    <h1>{title}</h1>
    {toc_html}
    <main>
        {content_html}
    </main>
</body>
</html>"""

    def generate_excel(
        self, 
        data_by_date: dict[date, pd.DataFrame],
        filename: str
    ) -> str:
        """
        Генерация Excel-отчёта.
        
        Requirement: 4.4 - Excel с листами событий и сводными таблицами
        
        Args:
            data_by_date: Словарь {дата: DataFrame с сегментами}
            filename: Имя выходного файла
            
        Returns:
            Путь к созданному файлу
        """
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # Лист со всеми событиями
                all_events = []
                for report_date, df in data_by_date.items():
                    if not df.empty:
                        df_copy = df.copy()
                        df_copy['report_date'] = report_date
                        all_events.append(df_copy)
                
                if all_events:
                    events_df = pd.concat(all_events, ignore_index=True)
                    events_df.to_excel(writer, sheet_name='События', index=False)
                else:
                    # Создаём пустой лист
                    pd.DataFrame().to_excel(writer, sheet_name='События', index=False)
                
                # Сводная таблица по зонам
                self._create_zone_pivot_sheet(writer, data_by_date)
                
                # Сводная таблица по сотрудникам
                self._create_employee_pivot_sheet(writer, data_by_date)
            
            if self.logger_tg:
                self.logger_tg.info(f"Excel-отчёт создан: {filename}")
            
            return filename
        except Exception as e:
            logger.error(f"Ошибка создания Excel-отчёта: {e}")
            if self.logger_tg:
                self.logger_tg.error(f"Ошибка создания Excel-отчёта: {e}", e)
            raise
    
    def _create_zone_pivot_sheet(
        self, 
        writer: pd.ExcelWriter, 
        data_by_date: dict[date, pd.DataFrame]
    ) -> None:
        """Создание сводной таблицы по зонам."""
        all_data = []
        for report_date, df in data_by_date.items():
            if not df.empty and 'zone_id' in df.columns and 'duration_minutes' in df.columns:
                df_copy = df.copy()
                df_copy['report_date'] = report_date
                all_data.append(df_copy)
        
        if not all_data:
            pd.DataFrame(columns=['zone_name', 'total_minutes']).to_excel(
                writer, sheet_name='Сводка по зонам', index=False
            )
            return
        
        combined = pd.concat(all_data, ignore_index=True)
        
        # Группируем по зонам
        if 'zone_name' in combined.columns:
            pivot = combined.groupby('zone_name')['duration_minutes'].sum().reset_index()
            pivot.columns = ['Зона', 'Всего минут']
            pivot = pivot.sort_values('Всего минут', ascending=False)
        else:
            pivot = combined.groupby('zone_id')['duration_minutes'].sum().reset_index()
            pivot.columns = ['ID зоны', 'Всего минут']
            pivot = pivot.sort_values('Всего минут', ascending=False)
        
        pivot.to_excel(writer, sheet_name='Сводка по зонам', index=False)
    
    def _create_employee_pivot_sheet(
        self, 
        writer: pd.ExcelWriter, 
        data_by_date: dict[date, pd.DataFrame]
    ) -> None:
        """Создание сводной таблицы по сотрудникам."""
        all_data = []
        for report_date, df in data_by_date.items():
            if not df.empty and 'duration_minutes' in df.columns:
                df_copy = df.copy()
                df_copy['report_date'] = report_date
                all_data.append(df_copy)
        
        if not all_data:
            pd.DataFrame(columns=['employee', 'total_minutes']).to_excel(
                writer, sheet_name='Сводка по сотрудникам', index=False
            )
            return
        
        combined = pd.concat(all_data, ignore_index=True)
        
        # Определяем колонку с именем сотрудника
        employee_col = None
        for col in ['employee', 'ФИО', 'fio', 'tn_number']:
            if col in combined.columns:
                employee_col = col
                break
        
        if employee_col is None:
            pd.DataFrame(columns=['Сотрудник', 'Всего минут']).to_excel(
                writer, sheet_name='Сводка по сотрудникам', index=False
            )
            return
        
        # Группируем по сотрудникам
        pivot = combined.groupby(employee_col)['duration_minutes'].sum().reset_index()
        pivot.columns = ['Сотрудник', 'Всего минут']
        pivot = pivot.sort_values('Всего минут', ascending=False)
        
        pivot.to_excel(writer, sheet_name='Сводка по сотрудникам', index=False)
    
    def generate_html_string(
        self, 
        sections: list[dict], 
        title: str = "AA_BLE Report"
    ) -> str:
        """
        Генерация HTML-отчёта как строки (без записи в файл).
        
        Полезно для тестирования и проверки структуры.
        
        Args:
            sections: Список секций
            title: Заголовок отчёта
            
        Returns:
            HTML-строка
        """
        # Генерируем оглавление
        toc_items = []
        for section in sections:
            section_id = section.get('id', '')
            section_title = section.get('title', 'Без названия')
            toc_items.append(f'<li><a href="#{section_id}">{section_title}</a></li>')
        
        toc_html = f"""<nav class="table-of-contents" id="toc">
    <h2>Оглавление</h2>
    <ul>
        {''.join(toc_items)}
    </ul>
</nav>"""
        
        # Генерируем секции контента
        content_sections = []
        for section in sections:
            section_id = section.get('id', '')
            section_title = section.get('title', 'Без названия')
            chart_html = section.get('chart_html', '')
            stats_html = section.get('stats_html', '')
            
            section_html = f"""<section id="{section_id}" class="report-section">
    <h2>{section_title}</h2>
    <div class="zone-stats-panel">
        {stats_html}
    </div>
    <div class="plotly-chart">
        {chart_html}
    </div>
</section>"""
            content_sections.append(section_html)
        
        return self._generate_html_template(
            title=title,
            toc_html=toc_html,
            content_html='\n'.join(content_sections)
        )


# Вспомогательные функции для тестирования

def generate_html_report(
    sections: list[dict],
    title: str = "AA_BLE Report"
) -> str:
    """
    Функция-обёртка для генерации HTML-отчёта.
    
    Args:
        sections: Список секций отчёта
        title: Заголовок отчёта
        
    Returns:
        HTML-строка
    """
    from src.processing.timeline import TimelineBuilder
    builder = TimelineBuilder()
    generator = ReportGenerator(timeline_builder=builder)
    return generator.generate_html_string(sections, title)


def validate_html_structure(html: str) -> dict:
    """
    Проверка структуры HTML-отчёта.
    
    Requirement: 4.3 - проверка наличия обязательных элементов
    
    Args:
        html: HTML-строка
        
    Returns:
        Словарь с результатами проверки:
        - has_toc: наличие оглавления
        - has_zone_stats_panel: наличие панели статистики
        - has_plotly_chart: наличие Plotly-графика
        - toc_element: найденный элемент оглавления
        - stats_panels_count: количество панелей статистики
        - chart_divs_count: количество div с графиками
    """
    result = {
        'has_toc': False,
        'has_zone_stats_panel': False,
        'has_plotly_chart': False,
        'toc_element': None,
        'stats_panels_count': 0,
        'chart_divs_count': 0,
    }
    
    # Проверяем наличие оглавления
    if 'class="table-of-contents"' in html or 'id="toc"' in html:
        result['has_toc'] = True
        result['toc_element'] = 'nav.table-of-contents'
    
    # Проверяем наличие панели статистики по зонам
    stats_count = html.count('class="zone-stats-panel"')
    if stats_count > 0:
        result['has_zone_stats_panel'] = True
        result['stats_panels_count'] = stats_count
    
    # Проверяем наличие Plotly-графиков
    # Plotly генерирует div с классом plotly-graph-div или js-plotly-plot
    plotly_markers = [
        'class="plotly-chart"',
        'plotly-graph-div',
        'js-plotly-plot',
        'Plotly.newPlot',
    ]
    
    chart_count = 0
    for marker in plotly_markers:
        if marker in html:
            result['has_plotly_chart'] = True
            chart_count = max(chart_count, html.count(marker))
    
    result['chart_divs_count'] = chart_count
    
    return result
