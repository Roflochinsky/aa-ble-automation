# AA_BLE Automation - Main Orchestrator
"""
Главный оркестратор генерации отчётов AA_BLE.
Поддерживает обработку множества площадок.
"""

import argparse
import sys
import time
from datetime import date, datetime
from typing import Optional, List

import pandas as pd

from src.config import ConfigManager, FacilityConfig
from src.clients.gdrive import GoogleDriveClient
from src.clients.gsheets import GoogleSheetsClient
from src.clients.telegram import TelegramLogger
from src.processing.loader import DataLoader
from src.processing.processor import DataProcessor
from src.reports.svg_generator import SVGTimelineGenerator, generate_report_filename

# Тайминги ожидания перед следующим запуском (в секундах)
SLEEP_ON_SUCCESS = 16 * 60 * 60  # 16 часов при успехе
SLEEP_ON_FAILURE = 3 * 60 * 60   # 3 часа при неудаче


class AABLEReportOrchestrator:
    """
    Главный оркестратор генерации отчётов AA_BLE.
    
    Обрабатывает множество площадок, для каждой:
    - Загружает файлы AA_BLE из соответствующей папки GDrive
    - Генерирует SVG-таймлайны
    - Выгружает HTML-отчёт в выходную папку
    """
    
    def __init__(self, config: ConfigManager):
        """
        Args:
            config: Конфигурация приложения
        """
        self.config = config
        
        # Telegram-логгер
        self.logger = TelegramLogger(
            bot_token=config.telegram_bot_token,
            chat_id=config.telegram_chat_id
        )
        
        # Google API клиенты
        self.gdrive = GoogleDriveClient(config.google_credentials_path)
        self.gsheets = GoogleSheetsClient(config.google_credentials_path)
        
        # Загрузчик данных
        self.data_loader = DataLoader(
            gdrive=self.gdrive,
            gsheets=self.gsheets,
            logger=self.logger
        )
        
        # Процессор данных
        self.processor = DataProcessor(logger=self.logger)
        
        # Кэш справочников (загружаются один раз)
        self._tag_desc_map: Optional[dict] = None
        self._area_map: Optional[dict] = None
        self._fio_map: Optional[dict] = None

    def run(
        self, 
        date_from: Optional[date] = None, 
        date_to: Optional[date] = None,
        facilities: Optional[List[str]] = None
    ) -> bool:
        """
        Запуск генерации отчётов для всех активных площадок.
        
        Args:
            date_from: Начальная дата (по умолчанию — сегодня)
            date_to: Конечная дата (по умолчанию — сегодня)
            facilities: Список имён площадок для обработки (None = все активные)
            
        Returns:
            True если хотя бы один отчёт создан успешно
        """
        try:
            # Даты по умолчанию
            if date_from is None and date_to is None:
                date_from = date.today()
                date_to = date.today()
            elif date_from is None:
                date_from = date_to
            elif date_to is None:
                date_to = date_from
            
            self.logger.info(
                f"🚀 Запуск AA_BLE Automation\n"
                f"Период: {date_from.strftime('%d.%m.%Y')} — {date_to.strftime('%d.%m.%Y')}"
            )
            
            # Аутентификация
            self._authenticate()
            
            # Загрузка справочников (один раз для всех площадок)
            self._load_reference_data()
            
            # Определяем площадки для обработки
            facilities_to_process = self._get_facilities_to_process(facilities)
            
            if not facilities_to_process:
                self.logger.warning("Нет активных площадок для обработки")
                return False
            
            self.logger.info(f"Площадок для обработки: {len(facilities_to_process)}")
            
            # Обрабатываем каждую площадку
            success_count = 0
            for facility in facilities_to_process:
                if self._process_facility(facility, date_from, date_to):
                    success_count += 1
            
            # Итог
            if success_count > 0:
                self.logger.info(
                    f"✅ Обработка завершена\n"
                    f"Успешно: {success_count}/{len(facilities_to_process)} площадок\n"
                    f"Следующий запуск через 16 часов"
                )
            else:
                self.logger.warning(
                    f"⚠️ Обработка завершена\n"
                    f"Успешно: 0/{len(facilities_to_process)} площадок\n"
                    f"Следующий запуск через 3 часа"
                )
            
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"Критическая ошибка: {str(e)}", exception=e)
            return False
    
    def _authenticate(self) -> None:
        """Аутентификация в Google API."""
        self.gdrive.authenticate()
        self.gsheets.authenticate()
    
    def _load_reference_data(self) -> None:
        """Загрузка справочных данных (Журнал BLE, Привязка людей)."""
        self.logger.info("Загрузка справочников...")
        
        self._tag_desc_map = self.data_loader.load_ble_journal(
            spreadsheet_id=self.config.gsheets_ble_journal_id,
            sheet_name=self.config.gsheets_ble_journal_sheet
        )
        
        self._area_map, self._fio_map = self.data_loader.load_people_mapping(
            spreadsheet_id=self.config.gsheets_people_mapping_id,
            sheet_name=self.config.gsheets_people_mapping_sheet
        )
        
        self.logger.info(
            f"Справочники загружены: "
            f"меток={len(self._tag_desc_map)}, "
            f"сотрудников={len(self._fio_map)}"
        )
    
    def _get_facilities_to_process(self, facility_names: Optional[List[str]]) -> List[FacilityConfig]:
        """Получение списка площадок для обработки."""
        enabled_facilities = self.config.get_enabled_facilities()
        
        if facility_names is None:
            return enabled_facilities
        
        # Фильтруем по именам
        return [f for f in enabled_facilities if f.name in facility_names]
    
    def _process_facility(
        self, 
        facility: FacilityConfig, 
        date_from: date, 
        date_to: date
    ) -> bool:
        """
        Обработка одной площадки.
        
        Args:
            facility: Конфигурация площадки
            date_from: Начальная дата
            date_to: Конечная дата
            
        Returns:
            True если отчёт создан успешно
        """
        try:
            self.logger.info(f"📍 Обработка площадки: {facility.name}")
            
            # Загрузка файлов AA_BLE для этой площадки
            raw_data = self.data_loader.load_aable_files(
                folder_id=facility.input_folder_id,
                date_from=date_from,
                date_to=date_to
            )
            
            if raw_data is None or raw_data.empty:
                self.logger.warning(f"[{facility.name}] Нет данных за период")
                return False
            
            # Обработка данных
            segments = self.processor.process_full(
                raw_data, 
                self._area_map, 
                self._fio_map
            )
            
            if segments.empty:
                self.logger.warning(f"[{facility.name}] После обработки нет данных")
                return False
            
            # Группируем по датам и генерируем отчёты
            reports_created = 0
            for report_date, date_segments in self._group_by_date(segments).items():
                if self._generate_and_upload_report(facility, report_date, date_segments):
                    reports_created += 1
            
            if reports_created > 0:
                self.logger.info(f"[{facility.name}] Создано отчётов: {reports_created}")
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"[{facility.name}] Ошибка: {str(e)}", exception=e)
            return False
    
    def _group_by_date(self, segments: pd.DataFrame) -> dict[date, pd.DataFrame]:
        """Группировка сегментов по датам."""
        if segments.empty or 'date' not in segments.columns:
            return {}
        
        result = {}
        for report_date, group in segments.groupby('date'):
            if isinstance(report_date, datetime):
                report_date = report_date.date()
            if isinstance(report_date, date):
                result[report_date] = group.reset_index(drop=True)
        
        return result
    
    def _generate_and_upload_report(
        self, 
        facility: FacilityConfig, 
        report_date: date, 
        segments: pd.DataFrame
    ) -> bool:
        """
        Генерация и отправка HTML-отчёта через Telegram.
        
        Args:
            facility: Конфигурация площадки
            report_date: Дата отчёта
            segments: DataFrame с сегментами
            
        Returns:
            True если успешно
        """
        try:
            # Генератор SVG-таймлайнов
            generator = SVGTimelineGenerator(tag_desc_map=self._tag_desc_map)
            
            # Генерация HTML
            html_content = generator.generate_combined_html(
                segments_df=segments,
                site_name=facility.name,
                report_date=report_date
            )
            
            if not html_content:
                return False
            
            # Имя файла
            filename = generate_report_filename(facility.name, report_date)
            
            # Отправка через Telegram
            caption = f"📊 {facility.name} — {report_date.strftime('%d.%m.%Y')}"
            success = self.logger.send_document(
                content=html_content.encode('utf-8'),
                filename=filename,
                caption=caption
            )
            
            if success:
                self.logger.info(f"[{facility.name}] Отчёт отправлен: {filename}")
            else:
                # Fallback: сохраняем локально
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                self.logger.warning(f"[{facility.name}] Telegram недоступен, сохранён локально: {filename}")
            
            return True
            
        except Exception as e:
            self.logger.error(
                f"[{facility.name}] Ошибка генерации отчёта за {report_date}: {str(e)}", 
                exception=e
            )
            return False


def parse_date(date_str: str) -> date:
    """Парсинг строки даты (YYYY-MM-DD или DD.MM.YYYY)."""
    formats = ['%Y-%m-%d', '%d.%m.%Y']
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Неизвестный формат даты: {date_str}")


def filter_files_by_date_range(
    files: list[dict],
    date_from: Optional[date] = None,
    date_to: Optional[date] = None
) -> list[dict]:
    """Фильтрация файлов по диапазону дат (для тестов)."""
    if not files:
        return []
    
    result = []
    for file_info in files:
        file_date = file_info.get('file_date')
        if file_date is None:
            if date_from is None and date_to is None:
                result.append(file_info)
            continue
        if date_from is not None and file_date < date_from:
            continue
        if date_to is not None and file_date > date_to:
            continue
        result.append(file_info)
    
    return result


def main():
    """Точка входа приложения."""
    parser = argparse.ArgumentParser(
        description='AA_BLE Report Automation — генерация SVG-таймлайнов'
    )
    
    parser.add_argument(
        '--date-from', type=str,
        help='Начальная дата (YYYY-MM-DD или DD.MM.YYYY)'
    )
    parser.add_argument(
        '--date-to', type=str,
        help='Конечная дата (YYYY-MM-DD или DD.MM.YYYY)'
    )
    parser.add_argument(
        '--date', type=str,
        help='Конкретная дата (YYYY-MM-DD или DD.MM.YYYY)'
    )
    parser.add_argument(
        '--facilities', type=str, nargs='+',
        help='Имена площадок для обработки (по умолчанию — все активные)'
    )
    parser.add_argument(
        '--env', type=str, default=None,
        help='Путь к .env файлу'
    )
    parser.add_argument(
        '--config', type=str, default='facilities_config.json',
        help='Путь к JSON-конфигурации площадок'
    )
    parser.add_argument(
        '--once', action='store_true',
        help='Выполнить один раз без ожидания (для отладки)'
    )
    parser.add_argument(
        '--diagnose', action='store_true',
        help='Режим диагностики: показать все файлы в папках без фильтрации по дате'
    )
    
    args = parser.parse_args()
    
    # Парсим даты
    date_from = None
    date_to = None
    
    if args.date:
        try:
            single_date = parse_date(args.date)
            date_from = single_date
            date_to = single_date
        except ValueError as e:
            print(f"Ошибка: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        if args.date_from:
            try:
                date_from = parse_date(args.date_from)
            except ValueError as e:
                print(f"Ошибка: {e}", file=sys.stderr)
                sys.exit(1)
        if args.date_to:
            try:
                date_to = parse_date(args.date_to)
            except ValueError as e:
                print(f"Ошибка: {e}", file=sys.stderr)
                sys.exit(1)
    
    # Загружаем конфигурацию
    config = ConfigManager.load(env_path=args.env, facilities_config_path=args.config)
    
    # Режим диагностики
    if args.diagnose:
        run_diagnostics(config)
        sys.exit(0)
    
    # Запускаем оркестратор
    orchestrator = AABLEReportOrchestrator(config)
    success = orchestrator.run(
        date_from=date_from, 
        date_to=date_to,
        facilities=args.facilities
    )
    
    # Если --once, выходим сразу
    if args.once:
        sys.exit(0 if success else 1)
    
    # Иначе ждём перед следующим запуском (для Docker restart)
    sleep_time = SLEEP_ON_SUCCESS if success else SLEEP_ON_FAILURE
    hours = sleep_time // 3600
    print(f"Ожидание {hours} часов перед следующим запуском...")
    time.sleep(sleep_time)
    
    sys.exit(0 if success else 1)


def run_diagnostics(config: ConfigManager):
    """Режим диагностики — показывает все файлы в папках."""
    print("=" * 60)
    print("ДИАГНОСТИКА ДОСТУПА К GOOGLE DRIVE")
    print("=" * 60)
    
    gdrive = GoogleDriveClient(config.google_credentials_path)
    
    try:
        gdrive.authenticate()
        print("✅ Аутентификация успешна")
    except Exception as e:
        print(f"❌ Ошибка аутентификации: {e}")
        return
    
    for facility in config.get_enabled_facilities():
        print(f"\n📁 {facility.name}")
        print(f"   Folder ID: {facility.input_folder_id}")
        
        try:
            # Получаем ВСЕ файлы без фильтрации по дате
            files = gdrive.list_files(
                folder_id=facility.input_folder_id,
                date_from=None,
                date_to=None
            )
            
            if not files:
                print("   ⚠️ Папка пуста или нет доступа")
            else:
                print(f"   Найдено файлов: {len(files)}")
                for f in files[:10]:  # Показываем первые 10
                    date_str = f['file_date'].isoformat() if f.get('file_date') else 'NO DATE'
                    print(f"   - {f['name']} [{date_str}]")
                if len(files) > 10:
                    print(f"   ... и ещё {len(files) - 10} файлов")
                    
        except Exception as e:
            print(f"   ❌ Ошибка: {e}")


if __name__ == '__main__':
    main()
