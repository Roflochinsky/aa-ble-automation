# AA_BLE Automation - Data Loader
"""
Модуль загрузки данных из внешних источников.
Requirements: 1.2, 1.3, 2.1, 2.2
"""

import io
import re
from datetime import date
from typing import Optional

import pandas as pd

from src.clients.gdrive import GoogleDriveClient
from src.clients.gsheets import GoogleSheetsClient
from src.clients.telegram import TelegramLogger


class DataLoader:
    """Загрузка данных из внешних источников.
    
    Обеспечивает:
    - Загрузку файлов AA_BLE из Google Drive (Excel, второй лист)
    - Загрузку журнала BLE из Google Sheets
    - Загрузку привязки людей из Google Sheets
    - Логирование операций через Telegram
    """
    
    # Паттерн для поиска даты в имени файла
    DATE_PATTERN = re.compile(r'(\d{4}-\d{2}-\d{2})')
    
    def __init__(
        self, 
        gdrive: GoogleDriveClient, 
        gsheets: GoogleSheetsClient, 
        logger: TelegramLogger
    ):
        """Инициализация загрузчика данных.
        
        Args:
            gdrive: Клиент Google Drive
            gsheets: Клиент Google Sheets
            logger: Telegram-логгер для уведомлений
        """
        self.gdrive = gdrive
        self.gsheets = gsheets
        self.logger = logger
    
    def load_aable_files(
        self, 
        folder_id: str, 
        date_from: Optional[date] = None, 
        date_to: Optional[date] = None,
        drive_id: Optional[str] = None
    ) -> pd.DataFrame:
        """Загрузка файлов AA_BLE из Google Drive.
        
        Ищет Excel-файлы в указанной папке, фильтрует по дате в имени файла,
        читает второй лист каждого файла и объединяет данные.
        
        Requirements: 1.2, 1.3
        
        Args:
            folder_id: ID папки Google Drive с файлами AA_BLE
            date_from: Начальная дата для фильтрации (включительно)
            date_to: Конечная дата для фильтрации (включительно)
            drive_id: ID Shared Drive (опционально)
            
        Returns:
            DataFrame с объединёнными данными из всех файлов
        """
        try:
            # Получаем список файлов с фильтрацией по дате
            files = self.gdrive.list_files(
                folder_id=folder_id,
                date_from=date_from,
                date_to=date_to,
                drive_id=drive_id
            )
            
            if not files:
                self.logger.warning(
                    f"Файлы AA_BLE не найдены в папке {folder_id}"
                    + (f" за период {date_from} - {date_to}" if date_from or date_to else "")
                )
                return pd.DataFrame()
            
            self.logger.info(f"Найдено {len(files)} файлов AA_BLE для обработки")
            
            all_dataframes = []
            
            for file_info in files:
                file_id = file_info['id']
                file_name = file_info['name']
                file_date = file_info.get('file_date')
                
                try:
                    # Скачиваем файл
                    content = self.gdrive.download_file(file_id)
                    
                    # Читаем второй лист Excel (индекс 1)
                    df = self._read_excel_second_sheet(content, file_name)
                    
                    if df is not None and not df.empty:
                        self.logger.info(f"Columns in {file_name}: {list(df.columns)}")
                        # Добавляем информацию о файле
                        df['_source_file'] = file_name
                        df['_file_date'] = file_date
                        all_dataframes.append(df)
                        
                except Exception as e:
                    self.logger.warning(
                        f"Ошибка при обработке файла {file_name}: {str(e)}"
                    )
                    continue
            
            if not all_dataframes:
                self.logger.warning("Не удалось загрузить данные ни из одного файла")
                return pd.DataFrame()
            
            # Объединяем все DataFrame
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            self.logger.info(f"Загружено {len(combined_df)} записей из {len(all_dataframes)} файлов")
            
            return combined_df
            
        except Exception as e:
            self.logger.error(f"Ошибка при загрузке файлов AA_BLE: {str(e)}", exception=e)
            return pd.DataFrame()
    
    def _read_excel_second_sheet(
        self, 
        content: bytes, 
        filename: str
    ) -> Optional[pd.DataFrame]:
        """Чтение второго листа Excel-файла.
        
        Requirement: 1.3 - читаем второй лист workbook для данных AA_BLE
        
        Args:
            content: Содержимое файла в байтах
            filename: Имя файла (для логирования)
            
        Returns:
            DataFrame с данными или None при ошибке
        """
        try:
            # Создаём буфер из байтов
            buffer = io.BytesIO(content)
            
            # Читаем Excel файл
            excel_file = pd.ExcelFile(buffer, engine='openpyxl')
            
            # Проверяем наличие второго листа
            sheet_names = excel_file.sheet_names
            if len(sheet_names) < 2:
                self.logger.warning(
                    f"Файл {filename} содержит менее 2 листов, "
                    f"доступные листы: {sheet_names}"
                )
                # Если есть хотя бы один лист, читаем его
                if sheet_names:
                    return pd.read_excel(excel_file, sheet_name=0)
                return None
            
            # Читаем второй лист (индекс 1)
            df = pd.read_excel(excel_file, sheet_name=1)
            return df
            
        except Exception as e:
            self.logger.warning(f"Ошибка чтения Excel {filename}: {str(e)}")
            return None

    
    def load_ble_journal(
        self, 
        spreadsheet_id: str, 
        sheet_name: str
    ) -> dict[str, str]:
        """Загрузка журнала BLE из Google Sheets.
        
        Журнал содержит соответствие номеров меток и их описаний/локаций.
        
        Requirement: 2.1
        
        Args:
            spreadsheet_id: ID таблицы Google Sheets
            sheet_name: Имя листа с журналом BLE
            
        Returns:
            Словарь {номер_метки: описание}
        """
        try:
            # Читаем данные из листа
            data = self.gsheets.read_sheet(spreadsheet_id, sheet_name)
            
            if not data:
                self.logger.warning(
                    f"Журнал BLE пуст или не найден: {spreadsheet_id}/{sheet_name}"
                )
                return {}
            
            tag_desc_map = {}
            
            # Пропускаем заголовок (первая строка)
            for row in data[1:]:
                if len(row) < 2:
                    continue
                
                # Первая колонка - номер метки, вторая - описание
                raw_tag = row[0]
                try:
                    # Если это число (float/int), приводим к int, чтобы убрать .0
                    if isinstance(raw_tag, (int, float)):
                        tag = str(int(raw_tag)).strip()
                    else:
                        tag = str(raw_tag).strip()
                except (ValueError, TypeError):
                    tag = str(raw_tag).strip()

                description = str(row[1]).strip() if row[1] else ''
                
                if tag and description:
                    tag_desc_map[tag] = description
            
            self.logger.info(f"Загружено {len(tag_desc_map)} записей из журнала BLE")
            return tag_desc_map
            
        except Exception as e:
            self.logger.error(
                f"Ошибка при загрузке журнала BLE: {str(e)}", 
                exception=e
            )
            return {}
    
    def load_people_mapping(
        self, 
        spreadsheet_id: str, 
        sheet_name: str
    ) -> tuple[dict[str, str], dict[str, str]]:
        """Загрузка привязки людей из Google Sheets.
        
        Извлекает колонки A (ТН), B (ФИО), D (Участок Работ).
        
        Requirement: 2.2
        
        Args:
            spreadsheet_id: ID таблицы Google Sheets
            sheet_name: Имя листа с привязкой людей
            
        Returns:
            Кортеж из двух словарей:
            - area_map: {ТН: Участок Работ}
            - fio_map: {ТН: ФИО}
        """
        try:
            # Читаем данные из листа
            data = self.gsheets.read_sheet(spreadsheet_id, sheet_name)
            
            if not data:
                self.logger.warning(
                    f"Привязка людей пуста или не найдена: {spreadsheet_id}/{sheet_name}"
                )
                return {}, {}
            
            # Используем статический метод из GoogleSheetsClient
            fio_map, area_map = GoogleSheetsClient.extract_employee_mapping_columns(data)
            
            self.logger.info(
                f"Загружено {len(fio_map)} записей ФИО и "
                f"{len(area_map)} записей участков работ"
            )
            
            return area_map, fio_map
            
        except Exception as e:
            self.logger.error(
                f"Ошибка при загрузке привязки людей: {str(e)}", 
                exception=e
            )
            return {}, {}
