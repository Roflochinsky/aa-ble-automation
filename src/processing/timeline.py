# AA_BLE Automation - Timeline Builder
"""
Модуль построения визуальных таймлайнов присутствия сотрудников.
Requirements: 4.1, 4.2
"""

from datetime import datetime, time, timedelta
from typing import Optional

import pandas as pd
import plotly.graph_objects as go

from src.processing.processor import ZONE_NAMES


# Цветовые палитры по зонам
ZONE_PALETTES = {
    0: ['#95A5A6', '#7F8C8D'],           # Вне зоны BLE-маячков - серый
    1: ['#27AE60', '#2ECC71', '#1ABC9C'], # Зоны проведения работ - зелёный
    2: ['#9B59B6', '#8E44AD'],           # Столовые - фиолетовый
    3: ['#C0392B', '#E74C3C'],           # Опасные зоны - тёмно-красный
    4: ['#E74C3C', '#FF6B6B', '#FF8E8E'], # Курилки - красный
    5: ['#3498DB', '#5DADE2'],           # Зоны отдыха - голубой
    6: ['#F39C12', '#F1C40F'],           # ВЖГ - жёлтый
    7: ['#1ABC9C', '#16A085'],           # Туалеты - бирюзовый
    8: ['#34495E', '#2C3E50'],           # Остановки автобусов - тёмно-синий
    9: ['#9B59B6', '#8E44AD'],           # Административные помещения - фиолетовый
    10: ['#1F77B4', '#2980B9', '#3498DB'], # Зона выдачи WW - синий
    11: ['#795548', '#8D6E63'],          # Склад - коричневый
    12: ['#607D8B', '#78909C'],          # Мастерские - серо-синий
    13: ['#E67E22', '#F39C12', '#D35400'], # КПП - оранжевый
}


class TimelineBuilder:
    """Построение визуальных таймлайнов присутствия сотрудников.
    
    Обеспечивает:
    - Создание Plotly-графиков таймлайнов
    - Цветовые палитры по зонам
    - Фильтрацию по временному окну 6:00-21:00
    - Расчёт статистики по зонам
    """
    
    def __init__(
        self, 
        row_height: int = 60,
        window_start: tuple[int, int] = (6, 0),
        window_end: tuple[int, int] = (21, 0)
    ):
        """Инициализация построителя таймлайнов.
        
        Args:
            row_height: Высота строки в пикселях
            window_start: Начало временного окна (час, минута)
            window_end: Конец временного окна (час, минута)
        """
        self.row_height = row_height
        self.window_start = time(hour=window_start[0], minute=window_start[1])
        self.window_end = time(hour=window_end[0], minute=window_end[1])

    def filter_by_time_window(self, df: pd.DataFrame) -> pd.DataFrame:
        """Фильтрация данных по временному окну.
        
        Requirement: 4.1 - только данные в окне 6:00-21:00
        
        Args:
            df: DataFrame с сегментами (должен содержать колонку 'start')
            
        Returns:
            DataFrame с данными только в пределах временного окна
        """
        if df.empty:
            return df
        
        if 'start' not in df.columns:
            return df
        
        result = df.copy()
        
        # Извлекаем время из datetime
        def get_time_from_datetime(dt):
            if isinstance(dt, datetime):
                return dt.time()
            elif isinstance(dt, time):
                return dt
            return None
        
        # Фильтруем по временному окну
        mask = result['start'].apply(
            lambda x: self._is_within_window(get_time_from_datetime(x))
        )
        
        return result[mask].reset_index(drop=True)
    
    def _is_within_window(self, t: Optional[time]) -> bool:
        """Проверка, находится ли время в пределах окна."""
        if t is None:
            return False
        return self.window_start <= t < self.window_end
    
    def calculate_zone_statistics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Расчёт статистики по зонам для сотрудника.
        
        Requirement: 4.2 - сумма минут по зонам
        
        Args:
            df: DataFrame с сегментами (должен содержать zone_id, duration_minutes)
            
        Returns:
            DataFrame со статистикой: zone_id, zone_name, total_minutes
        """
        if df.empty:
            return pd.DataFrame(columns=['zone_id', 'zone_name', 'total_minutes'])
        
        required_cols = ['zone_id', 'duration_minutes']
        if not all(col in df.columns for col in required_cols):
            return pd.DataFrame(columns=['zone_id', 'zone_name', 'total_minutes'])
        
        # Группируем по zone_id и суммируем duration_minutes
        stats = df.groupby('zone_id')['duration_minutes'].sum().reset_index()
        stats.columns = ['zone_id', 'total_minutes']
        
        # Добавляем названия зон
        stats['zone_name'] = stats['zone_id'].apply(
            lambda x: ZONE_NAMES.get(int(x), f'Зона {x}')
        )
        
        # Сортируем по убыванию времени
        stats = stats.sort_values('total_minutes', ascending=False).reset_index(drop=True)
        
        return stats[['zone_id', 'zone_name', 'total_minutes']]
    
    def get_zone_color(self, zone_id: int, tag: int = 0) -> str:
        """Получение цвета для зоны/метки.
        
        Args:
            zone_id: ID зоны
            tag: Номер BLE-метки (для вариации цвета)
            
        Returns:
            Цвет в формате HEX
        """
        palette = ZONE_PALETTES.get(zone_id, ['#95A5A6'])
        # Используем tag для выбора оттенка из палитры
        color_index = tag % len(palette)
        return palette[color_index]

    def create_user_timeline(
        self, 
        df_user: pd.DataFrame, 
        tag_desc_map: dict[int, str]
    ) -> tuple[go.Figure, str]:
        """Создание таймлайна для одного сотрудника.
        
        Args:
            df_user: DataFrame с сегментами одного сотрудника
            tag_desc_map: Словарь {номер_метки: описание}
            
        Returns:
            Tuple (Plotly Figure, HTML-строка статистики)
        """
        if df_user.empty:
            fig = go.Figure()
            fig.update_layout(title="Нет данных")
            return fig, ""
        
        # Фильтруем по временному окну
        filtered_df = self.filter_by_time_window(df_user)
        
        if filtered_df.empty:
            fig = go.Figure()
            fig.update_layout(title="Нет данных в указанном временном окне")
            return fig, ""
        
        # Получаем информацию о сотруднике
        employee_name = filtered_df['employee'].iloc[0] if 'employee' in filtered_df.columns else 'Unknown'
        
        # Создаём фигуру
        fig = go.Figure()
        
        # Группируем по зонам для создания баров
        for _, row in filtered_df.iterrows():
            zone_id = int(row.get('zone_id', 0))
            ble_tag = int(row.get('ble_tag', 0))
            zone_name = row.get('zone_name', ZONE_NAMES.get(zone_id, f'Зона {zone_id}'))
            
            start = row['start']
            end = row['end']
            
            # Получаем описание метки
            tag_desc = tag_desc_map.get(ble_tag, f'Метка {ble_tag}')
            
            # Цвет для зоны
            color = self.get_zone_color(zone_id, ble_tag)
            
            # Добавляем бар
            fig.add_trace(go.Bar(
                x=[(end - start).total_seconds() / 60],  # Длительность в минутах
                y=[employee_name],
                orientation='h',
                base=[start.hour * 60 + start.minute],  # Начало в минутах от полуночи
                marker_color=color,
                name=zone_name,
                hovertemplate=(
                    f"<b>{zone_name}</b><br>"
                    f"Метка: {tag_desc}<br>"
                    f"Время: {start.strftime('%H:%M')} - {end.strftime('%H:%M')}<br>"
                    "<extra></extra>"
                ),
                showlegend=False,
            ))
        
        # Настраиваем layout
        window_start_minutes = self.window_start.hour * 60 + self.window_start.minute
        window_end_minutes = self.window_end.hour * 60 + self.window_end.minute
        
        # Создаём метки для оси X (часы)
        tickvals = list(range(window_start_minutes, window_end_minutes + 1, 60))
        ticktext = [f"{m // 60}:00" for m in tickvals]
        
        fig.update_layout(
            title=f"Таймлайн: {employee_name}",
            xaxis=dict(
                title="Время",
                range=[window_start_minutes, window_end_minutes],
                tickvals=tickvals,
                ticktext=ticktext,
            ),
            yaxis=dict(
                title="",
            ),
            height=self.row_height * 2,
            barmode='overlay',
            showlegend=False,
        )
        
        # Рассчитываем статистику
        stats = self.calculate_zone_statistics(filtered_df)
        stats_html = self._format_stats_html(stats)
        
        return fig, stats_html
    
    def _format_stats_html(self, stats: pd.DataFrame) -> str:
        """Форматирование статистики в HTML."""
        if stats.empty:
            return ""
        
        lines = ["<div class='zone-stats'>", "<h4>Статистика по зонам:</h4>", "<ul>"]
        
        for _, row in stats.iterrows():
            zone_id = int(row['zone_id'])
            zone_name = row['zone_name']
            total_minutes = row['total_minutes']
            color = self.get_zone_color(zone_id)
            
            hours = int(total_minutes // 60)
            minutes = int(total_minutes % 60)
            time_str = f"{hours}ч {minutes}м" if hours > 0 else f"{minutes}м"
            
            lines.append(
                f"<li><span style='color:{color}'>●</span> {zone_name}: {time_str}</li>"
            )
        
        lines.extend(["</ul>", "</div>"])
        return "\n".join(lines)


# Вспомогательные функции для использования в тестах

def filter_by_time_window(
    df: pd.DataFrame,
    window_start: tuple[int, int] = (6, 0),
    window_end: tuple[int, int] = (21, 0)
) -> pd.DataFrame:
    """Функция-обёртка для фильтрации по временному окну."""
    builder = TimelineBuilder(window_start=window_start, window_end=window_end)
    return builder.filter_by_time_window(df)


def calculate_zone_statistics(df: pd.DataFrame) -> pd.DataFrame:
    """Функция-обёртка для расчёта статистики по зонам."""
    builder = TimelineBuilder()
    return builder.calculate_zone_statistics(df)


def is_within_time_window(
    t: time,
    window_start: tuple[int, int] = (6, 0),
    window_end: tuple[int, int] = (21, 0)
) -> bool:
    """Проверка, находится ли время в пределах окна."""
    builder = TimelineBuilder(window_start=window_start, window_end=window_end)
    return builder._is_within_window(t)
