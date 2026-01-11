# AA_BLE Automation - Main Orchestrator
"""
Главный оркестратор генерации отчётов AA_BLE.
Поддерживает автоматический запуск в 08:00 и интерактивные команды в Telegram.
"""

import argparse
import logging
import sys
import time
import threading
from datetime import date, datetime, timedelta
from typing import Optional, List

import schedule
import telebot
from src.config import ConfigManager, FacilityConfig
from src.clients.gdrive import GoogleDriveClient
from src.clients.gsheets import GoogleSheetsClient
from src.clients.telegram import TelegramLogger
from src.processing.loader import DataLoader
from src.processing.processor import DataProcessor
from src.reports.svg_generator import SVGTimelineGenerator, generate_report_filename
from src.utils.log_capturer import memory_handler

# Настройка базового логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        memory_handler  # Наш перехватчик для /logs
    ]
)
logger = logging.getLogger(__name__)

# Настройки планировщика
TARGET_HOUR = "08:00"
SLEEP_ON_FAILURE = 3 * 60 * 60   # 3 часа при неудаче

# Глобальный флаг состояния
processing_lock = threading.Lock()


class AABLEReportOrchestrator:
    """Главный оркестратор генерации отчётов AA_BLE."""
    
    def __init__(self, config: ConfigManager):
        self.config = config
        self.logger = TelegramLogger(
            bot_token=config.telegram_bot_token,
            chat_id=config.telegram_chat_id
        )
        self.gdrive = GoogleDriveClient(
            config.google_credentials_path,
            impersonate_email=config.google_impersonate_email or None,
            use_oauth=config.google_use_oauth,
            oauth_token_path=config.google_oauth_token_path
        )
        self.gsheets = GoogleSheetsClient(
            config.google_credentials_path,
            impersonate_email=config.google_impersonate_email or None,
            use_oauth=config.google_use_oauth,
            oauth_token_path=config.google_oauth_token_path
        )
        self.data_loader = DataLoader(gdrive=self.gdrive, gsheets=self.gsheets, logger=self.logger)
        self.processor = DataProcessor(logger=self.logger)
        
        self._tag_desc_map: Optional[dict] = None
        self._area_map: Optional[dict] = None
        self._fio_map: Optional[dict] = None

    def run(self, date_from=None, date_to=None, facilities=None) -> bool:
        """Запуск генерации отчётов."""
        if not processing_lock.acquire(blocking=False):
            logger.warning("Попытка запуска при уже работающем процессе")
            return False
            
        try:
            # Очищаем логи перед новым запуском
            memory_handler.clear()
            
            # Даты по умолчанию
            if date_from is None and date_to is None:
                date_from = date.today()
                date_to = date.today()
            elif date_from is None:
                date_from = date_to
            elif date_to is None:
                date_to = date_from
            
            start_msg = f"🚀 Запуск AA_BLE Automation\nПериод: {date_from.strftime('%d.%m.%Y')} — {date_to.strftime('%d.%m.%Y')}"
            self.logger.info(start_msg)
            logger.info(start_msg)
            
            self._authenticate()
            self._load_reference_data()
            
            facilities_to_process = self._get_facilities_to_process(facilities)
            if not facilities_to_process:
                self.logger.warning("Нет активных площадок для обработки")
                return False
            
            success_count = 0
            all_segments_list = []
            
            for facility in facilities_to_process:
                # Передаем список для накопления сегментов
                facility_segments = self._process_facility(facility, date_from, date_to)
                if facility_segments is not None:
                    success_count += 1
                    all_segments_list.append(facility_segments)
            
            # Генерация сводных отчетов (Cleaners, Autstaff)
            if all_segments_list:
                full_df = pd.concat(all_segments_list, ignore_index=True)
                self._generate_aggregated_reports(full_df)
            
            final_msg = (
                f"✅ Обработка завершена\n"
                f"Успешно: {success_count}/{len(facilities_to_process)} площадок\n"
                f"Период: {date_from.strftime('%d.%m.%Y')} — {date_to.strftime('%d.%m.%Y')}"
            )
            self.logger.info(final_msg)
            logger.info(final_msg)
            
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"Критическая ошибка: {str(e)}", exception=e)
            return False
        finally:
            processing_lock.release()

    def _authenticate(self) -> None:
        self.gdrive.authenticate()
        self.gsheets.authenticate()
    
    def _load_reference_data(self) -> None:
        self._tag_desc_map = self.data_loader.load_ble_journal(
            self.config.gsheets_ble_journal_id, self.config.gsheets_ble_journal_sheet
        )
        self._area_map, self._fio_map = self.data_loader.load_people_mapping(
            self.config.gsheets_people_mapping_id, self.config.gsheets_people_mapping_sheet
        )

    def _get_facilities_to_process(self, names: Optional[List[str]]) -> List[FacilityConfig]:
        enabled = self.config.get_enabled_facilities()
        if names is None: return enabled
        return [f for f in enabled if f.name in names]
    
    def _process_facility(self, facility, date_from, date_to) -> Optional[pd.DataFrame]:
        """Обрабатывает площадку и возвращает DataFrame с сегментами (или None)."""
        try:
            raw_data = self.data_loader.load_aable_files(
                folder_id=facility.input_folder_id,
                date_from=date_from,
                date_to=date_to,
                drive_id=facility.drive_id
            )
            if raw_data is None or raw_data.empty: return None
            
            segments = self.processor.process_full(raw_data, self._area_map, self._fio_map)
            if segments.empty: return None
            
            reports_created = 0
            for r_date, d_segments in self._group_by_date(segments).items():
                if self._generate_and_upload_report(facility.name, r_date, d_segments):
                    reports_created += 1
            
            return segments if reports_created > 0 else None
        except Exception as e:
            logger.error(f"[{facility.name}] Ошибка: {e}")
            return None

    def _generate_aggregated_reports(self, full_df: pd.DataFrame) -> None:
        """Генерация сводных отчетов по ключевым словам в ФИО."""
        logger.info("Генерация сводных отчетов (Cleaners, Autstaff)...")
        
        # Определяем фильтры
        # Cleaners: содержит "клинер"
        cleaners_mask = full_df['employee'].astype(str).str.lower().str.contains('клинер', na=False)
        
        # Autstaff: содержит "аутстаф" или "аутсорс"
        autstaff_mask = full_df['employee'].astype(str).str.lower().str.contains('аутстаф|аутсорс', na=False, regex=True)
        
        groups = [
            ('CLEANERS', cleaners_mask),
            ('AUTSTAFF', autstaff_mask)
        ]
        
        for name, mask in groups:
            filtered_df = full_df[mask]
            if filtered_df.empty:
                continue
                
            # Группируем по датам и отправляем
            for r_date, d_segments in self._group_by_date(filtered_df).items():
                self._generate_and_upload_report(name, r_date, d_segments)

    def _group_by_date(self, segments: pd.DataFrame) -> dict:
        if segments.empty or 'date' not in segments.columns: return {}
        res = {}
        for r_date, group in segments.groupby('date'):
            if isinstance(r_date, datetime): r_date = r_date.date()
            res[r_date] = group.reset_index(drop=True)
        return res

    def _generate_and_upload_report(self, site_name: str, report_date: date, segments: pd.DataFrame) -> bool:
        """Генерация и отправка отчета (универсальный метод)."""
        try:
            generator = SVGTimelineGenerator(tag_desc_map=self._tag_desc_map)
            html = generator.generate_combined_html(segments, site_name, report_date)
            if not html: return False
            
            filename = generate_report_filename(site_name, report_date)
            caption = f"📊 {site_name} — {report_date.strftime('%d.%m.%Y')}"
            return self.logger.send_document(html.encode('utf-8'), filename, caption)
        except Exception as e:
            logger.error(f"Ошибка генерации {site_name}: {e}")
            return False


def parse_date(date_str: str) -> date:
    for fmt in ['%Y-%m-%d', '%d.%m.%Y']:
        try: return datetime.strptime(date_str, fmt).date()
        except ValueError: continue
    raise ValueError(f"Неизвестный формат: {date_str}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--date-from', type=str)
    parser.add_argument('--date-to', type=str)
    parser.add_argument('--facilities', type=str, nargs='+')
    parser.add_argument('--env', type=str, default=None)
    parser.add_argument('--config', type=str, default='facilities_config.json')
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()

    config = ConfigManager.load(env_path=args.env, facilities_config_path=args.config)
    orchestrator = AABLEReportOrchestrator(config)

    if args.once:
        df = parse_date(args.date_from) if args.date_from else None
        dt = parse_date(args.date_to) if args.date_to else None
        orchestrator.run(date_from=df, date_to=dt, facilities=args.facilities)
        sys.exit(0)

    # Запуск бота в отдельном потоке
    bot = telebot.TeleBot(config.telegram_bot_token)
    
    @bot.message_handler(commands=['start'])
    def cmd_start(m):
        bot.reply_to(m, "🤖 Бот управления AA_BLE готов.\n\nКоманды:\n/makereport [дата] — создать отчет\n/logs — получить логи последнего запуска")

    @bot.message_handler(commands=['makereport'])
    def cmd_report(m):
        if processing_lock.locked():
            bot.reply_to(m, "⚠️ Процесс уже запущен. Подождите окончания.")
            return
            
        try:
            text = m.text.split()
            d_from = None
            if len(text) > 1:
                d_from = parse_date(text[1])
            
            bot.reply_to(m, f"🚀 Начинаю генерацию отчета за {d_from or 'сегодня'}...")
            
            def worker():
                orchestrator.run(date_from=d_from)
                
            threading.Thread(target=worker).start()
        except Exception as e:
            bot.reply_to(m, f"❌ Ошибка параметров: {e}")

    @bot.message_handler(commands=['logs'])
    def cmd_logs(m):
        logs = memory_handler.get_logs()
        if not logs:
            bot.reply_to(m, "📝 Логи пока пусты.")
            return
            
        if len(logs) < 4000:
            bot.reply_to(m, f"📝 Последние логи:\n\n```\n{logs}\n```", parse_mode='Markdown')
        else:
            bot.send_document(m.chat.id, ('logs.txt', logs.encode('utf-8')), caption="📝 Полный лог работы")

    def run_bot():
        print("🤖 Telegram Bot Polling started...")
        bot.infinity_polling()

    threading.Thread(target=run_bot, daemon=True).start()

    # Настройка планировщика на 08:00
    def job():
        logger.info("⏰ Плановый запуск (08:00)")
        orchestrator.run()

    schedule.every().day.at(TARGET_HOUR).do(job)
    
    print(f"🕒 Планировщик активен: запуск ежедневно в {TARGET_HOUR}")

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == '__main__':
    main()