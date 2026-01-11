# AA_BLE Automation - Data Processor
"""
Модуль обработки и нормализации данных AA_BLE.
Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 3.4, 3.5
"""

import re
from datetime import date, datetime, time, timedelta
from typing import Optional, Union

import pandas as pd

from src.clients.telegram import TelegramLogger


# Справочник зон
ZONE_NAMES = {
    0: 'Вне зоны BLE-маячков',
    1: 'Зоны проведения работ',
    2: 'Столовые',
    3: 'Опасные зоны',
    4: 'Курилки',
    5: 'Зоны отдыха',
    6: 'ВЖГ',
    7: 'Туалеты',
    8: 'Остановки автобусов',
    9: 'Административные помещения',
    10: 'Зона выдачи WW',
    11: 'Склад',
    12: 'Мастерские',
    13: 'КПП',
}

# Маппинг вариантов названий колонок к стандартным именам
COLUMN_MAPPING = {
    # ТН (табельный номер)
    'тн': 'tn_number',
    'табельный номер': 'tn_number',
    'табельный': 'tn_number',
    'tn': 'tn_number',
    'tn_number': 'tn_number',
    
    # День смены
    'день смены': 'shift_day',
    'дата смены': 'shift_day',
    'shift_day': 'shift_day',
    'дата': 'shift_day',
    
    # BLE-метка
    'ble-метка': 'ble_tag',
    'ble метка': 'ble_tag',
    'метка': 'ble_tag',
    'metka': 'ble_tag',
    'ble_tag': 'ble_tag',
    'tag': 'ble_tag',
    
    # Зона
    'зона': 'zone_id',
    'zone': 'zone_id',
    'zone_id': 'zone_id',
    'id зоны': 'zone_id',
    
    # Время на объекте
    'время на объекте': 'time_only',
    'время': 'time_only',
    'time': 'time_only',
    'time_only': 'time_only',
}

# Стандартные колонки после нормализации
STANDARD_COLUMNS = ['tn_number', 'shift_day', 'ble_tag', 'zone_id', 'time_only']


class DataProcessor:
    """Обработка данных AA_BLE.
    
    Обеспечивает:
    - Нормализацию названий колонок
    - Парсинг дат и времени
    - Построение минутных сегментов
    - Анализ меток 0 и разрывов времени
    """
    
    # Порог для предупреждения о метках 0
    TAG_ZERO_WARNING_THRESHOLD = 100
    
    # Минимальный разрыв во времени для предупреждения (в минутах)
    TIME_GAP_THRESHOLD_MINUTES = 1
    
    def __init__(self, logger: Optional[TelegramLogger] = None):
        """Инициализация процессора данных.
        
        Args:
            logger: Telegram-логгер для уведомлений (опционально)
        """
        self.logger = logger
    
    # Стандартные колонки после нормализации
    STANDARD_COLUMNS = ['tn_number', 'shift_day', 'ble_tag', 'zone_id', 'time_only']

    # Позиции колонок по умолчанию (как в legacy скрипте)
    DEFAULT_POSITIONS = {
        'tn_number': 0,
        'shift_day': 3,
        'ble_tag': 10,
        'zone_id': 11,
        'time_only': 15
    }

    def normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Нормализация названий колонок.
        
        Requirement: 7.1 - извлечение колонок ТН, День смены, BLE-метка, Зона, Время на объекте
        Поддерживает поиск по имени и фоллбэк на позицию (как в legacy).
        
        Args:
            df: Исходный DataFrame с произвольными названиями колонок
            
        Returns:
            DataFrame со стандартизированными колонками
        """
        if df.empty:
            return pd.DataFrame(columns=self.STANDARD_COLUMNS)
        
        out = pd.DataFrame()
        
        # Вспомогательная функция для поиска колонки по имени
        def find_col_by_mapping(std_col_name: str) -> Optional[str]:
            for col in df.columns:
                col_lower = str(col).lower().strip()
                # Проверка по словарю
                if COLUMN_MAPPING.get(col_lower) == std_col_name:
                    return col
            return None

        # Формируем стандартные колонки
        for std_col in self.STANDARD_COLUMNS:
            # 1. Ищем по имени
            source_col = find_col_by_mapping(std_col)
            
            if source_col is not None:
                out[std_col] = df[source_col]
            else:
                # 2. Фоллбэк на позицию
                pos = self.DEFAULT_POSITIONS.get(std_col)
                if pos is not None and pos < df.shape[1]:
                    out[std_col] = df.iloc[:, pos]
                else:
                    out[std_col] = None
        
        # Сохраняем служебные колонки, если они есть
        for meta_col in ['_source_file', '_file_date']:
            if meta_col in df.columns:
                out[meta_col] = df[meta_col]
                
        return out

    @staticmethod
    def parse_time(value: Union[str, float, time, datetime, None]) -> Optional[time]:
        """Парсинг значения времени из различных форматов.
        
        Requirement: 7.2 - обработка форматов HH:MM:SS, HH:MM и Excel serial date
        
        Args:
            value: Значение времени в одном из поддерживаемых форматов
            
        Returns:
            Объект time или None при ошибке парсинга
        """
        if value is None:
            return None
        
        # Уже time объект
        if isinstance(value, time):
            return value
        
        # datetime объект - извлекаем время
        if isinstance(value, datetime):
            return value.time()
        
        # Excel serial date (float)
        if isinstance(value, (int, float)):
            try:
                # Excel хранит время как дробную часть дня
                # 0.5 = 12:00, 0.25 = 6:00 и т.д.
                float_val = float(value)
                
                # Если значение > 1, это может быть полная дата+время
                # Берём только дробную часть
                if float_val >= 1:
                    float_val = float_val % 1
                
                # Конвертируем в секунды
                total_seconds = int(float_val * 24 * 60 * 60)
                hours = total_seconds // 3600
                minutes = (total_seconds % 3600) // 60
                seconds = total_seconds % 60
                
                return time(hour=hours, minute=minutes, second=seconds)
            except (ValueError, OverflowError):
                return None
        
        # Строковое значение
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None
            
            # Пробуем формат HH:MM:SS
            match = re.match(r'^(\d{1,2}):(\d{2}):(\d{2})$', value)
            if match:
                try:
                    h, m, s = int(match.group(1)), int(match.group(2)), int(match.group(3))
                    return time(hour=h, minute=m, second=s)
                except ValueError:
                    pass
            
            # Пробуем формат HH:MM
            match = re.match(r'^(\d{1,2}):(\d{2})$', value)
            if match:
                try:
                    h, m = int(match.group(1)), int(match.group(2))
                    return time(hour=h, minute=m, second=0)
                except ValueError:
                    pass
            
            # Пробуем как float (Excel serial)
            try:
                float_val = float(value)
                return DataProcessor.parse_time(float_val)
            except ValueError:
                pass
        
        return None
    
    @staticmethod
    def round_051(value: float) -> int:
        """Округление по правилу 0.51.
        
        Requirement: 7.3 - округление вверх если дробная часть >= 0.51
        
        Args:
            value: Число для округления
            
        Returns:
            Округлённое целое число
        """
        import math
        integer_part = math.floor(value)
        fractional_part = value - integer_part
        
        # Используем небольшой epsilon для сравнения с плавающей точкой
        # чтобы 2.51 (который может быть 2.5099999...) корректно округлялся вверх
        if fractional_part >= 0.51 - 1e-9:
            return integer_part + 1
        else:
            return integer_part
    
    @staticmethod
    def infer_file_date(df: pd.DataFrame) -> Optional[date]:
        """Определение даты файла из данных.
        
        Requirement: 7.5 - определение даты из первого информативного значения
        
        Приоритет колонок:
        1. date / дата
        2. дата на объекте
        3. shift_day
        
        Args:
            df: DataFrame с данными
            
        Returns:
            Дата файла или None
        """
        if df.empty:
            return None
        
        # Приоритетные колонки для поиска даты
        priority_columns = [
            'date', 'дата',
            'дата на объекте',
            'shift_day', 'день смены'
        ]
        
        # Ищем первое непустое значение даты
        for col_name in priority_columns:
            # Ищем колонку (регистронезависимо)
            matching_col = None
            for col in df.columns:
                if str(col).lower().strip() == col_name.lower():
                    matching_col = col
                    break
            
            if matching_col is None:
                continue
            
            # Ищем первое непустое значение
            for val in df[matching_col]:
                if pd.isna(val) or val is None:
                    continue
                
                parsed_date = DataProcessor._parse_date_value(val)
                if parsed_date is not None:
                    return parsed_date
        
        return None
    
    @staticmethod
    def _parse_date_value(value) -> Optional[date]:
        """Парсинг значения даты из различных форматов."""
        if value is None or pd.isna(value):
            return None
        
        # Уже date объект
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        
        # datetime объект
        if isinstance(value, datetime):
            return value.date()
        
        # pd.Timestamp
        if isinstance(value, pd.Timestamp):
            return value.date()
        
        # Строка
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None
            
            # Пробуем различные форматы
            formats = [
                '%Y-%m-%d',
                '%d.%m.%Y',
                '%d/%m/%Y',
                '%Y/%m/%d',
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(value, fmt).date()
                except ValueError:
                    continue
        
        # Excel serial date (число)
        if isinstance(value, (int, float)):
            try:
                # Excel epoch: 1899-12-30
                excel_epoch = datetime(1899, 12, 30)
                return (excel_epoch + timedelta(days=int(value))).date()
            except (ValueError, OverflowError):
                pass
        
        return None

    def parse_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Парсинг дат и времени в DataFrame.
        
        Requirement: 7.2
        
        Args:
            df: DataFrame с нормализованными колонками
            
        Returns:
            DataFrame с распарсенными датами и временем
        """
        if df.empty:
            return df
        
        result = df.copy()
        
        # Парсим время
        if 'time_only' in result.columns:
            result['time_only'] = result['time_only'].apply(self.parse_time)
        
        # Парсим дату смены
        if 'shift_day' in result.columns:
            result['shift_day'] = result['shift_day'].apply(self._parse_date_value)
        
        return result
    
    def build_segments(
        self, 
        df: pd.DataFrame, 
        area_map: dict[str, str], 
        fio_map: dict[str, str]
    ) -> pd.DataFrame:
        """Построение минутных сегментов с учетом ночных смен.
        
        Requirement: 7.4 - создание 1-минутных бинов для каждого BLE tag reading.
        Поддерживает переход через полночь (night shift).
        
        Args:
            df: DataFrame с нормализованными и распарсенными данными
            area_map: Словарь {ТН: Участок Работ}
            fio_map: Словарь {ТН: ФИО}
            
        Returns:
            DataFrame с сегментами
        """
        if df.empty:
            return pd.DataFrame(columns=[
                'tn_number', 'employee', 'area', 'date', 'start', 'end',
                'duration_minutes', 'ble_tag', 'zone_id', 'zone_name'
            ])
        
        segments = []
        
        # Группируем по сотруднику и дате смены
        # sort=False чтобы сохранить порядок групп, хотя внутри будем сортировать по индексу
        grouped = df.groupby(['tn_number', 'shift_day'], sort=False)
        
        for (tn, shift_day), group in grouped:
            if shift_day is None or pd.isna(shift_day):
                continue
            
            # Убеждаемся что это date
            if isinstance(shift_day, datetime):
                shift_day = shift_day.date()
                
            tn = str(tn).strip()
            
            # Сортируем по индексу чтобы сохранить хронологию файла
            # (важно для детекции перехода через полночь, если данные идут последовательно)
            sorted_group = group.sort_index()
            
            prev_time = None
            day_offset = 0
            
            for _, row in sorted_group.iterrows():
                time_only = row.get('time_only')
                ble_tag = row.get('ble_tag')
                zone_id = row.get('zone_id')
                
                if time_only is None:
                    continue
                
                # Детекция перехода через полночь
                if prev_time is not None:
                    # Если время "откатилось" назад (например, 23:59 -> 00:00)
                    if time_only < prev_time:
                         day_offset += 1
                
                prev_time = time_only
                
                # Формируем datetime с учетом смещения дня
                current_date = shift_day + timedelta(days=day_offset)
                start_dt = datetime.combine(current_date, time_only)
                end_dt = start_dt + timedelta(minutes=1)
                
                # Получаем ФИО и участок
                employee = fio_map.get(tn, tn)
                area = area_map.get(tn, '')
                
                # Получаем название зоны
                try:
                    zone_id_int = int(zone_id) if zone_id is not None else 0
                except (ValueError, TypeError):
                    zone_id_int = 0
                zone_name = ZONE_NAMES.get(zone_id_int, f'Зона {zone_id_int}')
                
                segment = {
                    'tn_number': tn,
                    'employee': employee,
                    'area': area,
                    'date': shift_day, # Оставляем оригинальную дату смены для группировки
                    'start': start_dt,
                    'end': end_dt,
                    'duration_minutes': 1.0,
                    'ble_tag': ble_tag,
                    'zone_id': zone_id_int,
                    'zone_name': zone_name,
                }
                segments.append(segment)
        
        return pd.DataFrame(segments)
    
    def analyze_zero_tags(self, df: pd.DataFrame) -> pd.Series:
        """Анализ записей с меткой 0.
        
        Requirement: 3.4 - предупреждение если у сотрудника > 100 записей с tag 0
        
        Args:
            df: DataFrame с данными (нормализованный или сегменты)
            
        Returns:
            Series с количеством записей tag 0 по сотрудникам (только > порога)
        """
        if df.empty:
            return pd.Series(dtype=int)
        
        # Определяем колонку с меткой
        tag_col = 'ble_tag' if 'ble_tag' in df.columns else None
        tn_col = 'tn_number' if 'tn_number' in df.columns else None
        
        if tag_col is None or tn_col is None:
            return pd.Series(dtype=int)
        
        # Фильтруем записи с tag 0
        zero_tags = df[df[tag_col] == 0]
        
        if zero_tags.empty:
            return pd.Series(dtype=int)
        
        # Считаем по сотрудникам
        counts = zero_tags.groupby(tn_col).size()
        
        # Фильтруем только тех, у кого больше порога
        above_threshold = counts[counts > self.TAG_ZERO_WARNING_THRESHOLD]
        
        # Отправляем предупреждение если есть такие сотрудники
        if not above_threshold.empty and self.logger:
            employees_list = '\n'.join([
                f"  - {tn}: {count} записей" 
                for tn, count in above_threshold.items()
            ])
            self.logger.warning(
                f"Сотрудники с более чем {self.TAG_ZERO_WARNING_THRESHOLD} "
                f"записями tag 0:\n{employees_list}"
            )
        
        return above_threshold

    def analyze_time_gaps(
        self, 
        df: pd.DataFrame, 
        fio_map: dict[str, str]
    ) -> dict[str, list[dict]]:
        """Анализ разрывов во времени.
        
        Requirement: 3.5 - обнаружение и отчёт о разрывах > 1 минуты
        
        Args:
            df: DataFrame с сегментами (должен содержать start, tn_number)
            fio_map: Словарь {ТН: ФИО}
            
        Returns:
            Словарь {ТН: [список разрывов]}
        """
        if df.empty:
            return {}
        
        # Проверяем наличие необходимых колонок
        required_cols = ['tn_number', 'start']
        if not all(col in df.columns for col in required_cols):
            return {}
        
        gaps_by_employee = {}
        
        # Группируем по сотруднику
        for tn, group in df.groupby('tn_number'):
            # Сортируем по времени
            sorted_group = group.sort_values('start')
            
            gaps = []
            prev_end = None
            
            for _, row in sorted_group.iterrows():
                current_start = row['start']
                
                if prev_end is not None:
                    # Вычисляем разрыв
                    gap_minutes = (current_start - prev_end).total_seconds() / 60
                    
                    if gap_minutes > self.TIME_GAP_THRESHOLD_MINUTES:
                        gaps.append({
                            'from': prev_end,
                            'to': current_start,
                            'gap_minutes': gap_minutes,
                        })
                
                # Обновляем prev_end
                if 'end' in row:
                    prev_end = row['end']
                else:
                    # Если нет end, предполагаем 1 минуту
                    prev_end = current_start + timedelta(minutes=1)
            
            if gaps:
                gaps_by_employee[str(tn)] = gaps
        
        # Отправляем отчёт в Telegram если есть разрывы
        if gaps_by_employee and self.logger:
            self._send_gaps_report(gaps_by_employee, fio_map)
        
        return gaps_by_employee
    
    def _send_gaps_report(
        self, 
        gaps_by_employee: dict[str, list[dict]], 
        fio_map: dict[str, str]
    ) -> None:
        """Отправка отчёта о разрывах в Telegram."""
        if not self.logger:
            return
        
        lines = ["Обнаружены разрывы во времени:"]
        
        for tn, gaps in gaps_by_employee.items():
            employee_name = fio_map.get(tn, tn)
            lines.append(f"\n{employee_name} ({tn}):")
            
            for gap in gaps[:5]:  # Ограничиваем до 5 разрывов на сотрудника
                from_time = gap['from'].strftime('%H:%M') if hasattr(gap['from'], 'strftime') else str(gap['from'])
                to_time = gap['to'].strftime('%H:%M') if hasattr(gap['to'], 'strftime') else str(gap['to'])
                lines.append(f"  {from_time} - {to_time} ({gap['gap_minutes']:.0f} мин)")
            
            if len(gaps) > 5:
                lines.append(f"  ... и ещё {len(gaps) - 5} разрывов")
        
        self.logger.warning('\n'.join(lines))
    
    def process_full(
        self, 
        df: pd.DataFrame, 
        area_map: dict[str, str], 
        fio_map: dict[str, str]
    ) -> pd.DataFrame:
        """Полная обработка данных AA_BLE.
        
        Выполняет все этапы обработки:
        1. Нормализация колонок
        2. Парсинг дат и времени
        3. Построение сегментов
        4. Анализ меток 0
        5. Анализ разрывов времени
        
        Args:
            df: Исходный DataFrame
            area_map: Словарь {ТН: Участок Работ}
            fio_map: Словарь {ТН: ФИО}
            
        Returns:
            DataFrame с сегментами
        """
        # 1. Нормализация колонок
        normalized = self.normalize_columns(df)
        
        # 2. Парсинг дат и времени
        parsed = self.parse_dates(normalized)
        
        # 3. Построение сегментов
        segments = self.build_segments(parsed, area_map, fio_map)
        
        # 4. Анализ меток 0
        self.analyze_zero_tags(parsed)
        
        # 5. Анализ разрывов времени
        self.analyze_time_gaps(segments, fio_map)
        
        return segments


# Вспомогательные функции для использования в тестах

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Функция-обёртка для нормализации колонок."""
    processor = DataProcessor()
    return processor.normalize_columns(df)


def parse_time(value) -> Optional[time]:
    """Функция-обёртка для парсинга времени."""
    return DataProcessor.parse_time(value)


def round_051(value: float) -> int:
    """Функция-обёртка для округления по правилу 0.51."""
    return DataProcessor.round_051(value)


def infer_file_date(df: pd.DataFrame) -> Optional[date]:
    """Функция-обёртка для определения даты файла."""
    return DataProcessor.infer_file_date(df)


def create_minute_bin(
    tn: str, 
    shift_day: date, 
    time_only: time, 
    ble_tag: int, 
    zone_id: int
) -> dict:
    """Создание одного минутного бина.
    
    Requirement: 7.4 - каждый BLE tag reading создаёт ровно один сегмент в 1 минуту
    """
    start_dt = datetime.combine(shift_day, time_only)
    end_dt = start_dt + timedelta(minutes=1)
    
    zone_id_int = int(zone_id) if zone_id is not None else 0
    zone_name = ZONE_NAMES.get(zone_id_int, f'Зона {zone_id_int}')
    
    return {
        'tn_number': tn,
        'date': shift_day,
        'start': start_dt,
        'end': end_dt,
        'duration_minutes': 1.0,
        'ble_tag': ble_tag,
        'zone_id': zone_id_int,
        'zone_name': zone_name,
    }
