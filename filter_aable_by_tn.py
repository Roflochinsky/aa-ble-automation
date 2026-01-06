# 🔍 Фильтрация AA_BLE по списку ТН
# Загружает несколько файлов AA_BLE, фильтрует по ТН из xlsx, сохраняет ВСЕ оригинальные колонки

from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

# ============ НАСТРОЙКИ ============
TN_LIST_FILE = "tn_list.xlsx"
AABLE_FOLDER = ""
OUTPUT_FILE = "filtered_aable.xlsx"
# ===================================


def normalize_tn(val):
    """Нормализация ТН к строке"""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if s.endswith('.0'):
        s = s[:-2]
    return s


def load_tn_list(filepath: str) -> set:
    """Загрузка списка ТН из xlsx (первая колонка)"""
    print(f"📋 Загрузка списка ТН из: {filepath}")
    df = pd.read_excel(filepath)
    tn_col = df.iloc[:, 0]  # Первая колонка
    tn_set = set(normalize_tn(x) for x in tn_col if pd.notna(x))
    print(f"   Найдено {len(tn_set)} уникальных ТН")
    return tn_set


def find_aable_files(folder: str = None) -> list:
    """Поиск файлов AA_BLE в папке или запрос у пользователя"""
    if folder and Path(folder).exists():
        files = list(Path(folder).glob("*.xlsx")) + list(Path(folder).glob("*.xls"))
        print(f"📁 Найдено {len(files)} файлов в папке {folder}")
        return [str(f) for f in files]
    
    # Ручной ввод путей
    print("📁 Введите пути к файлам AA_BLE (по одному на строку, пустая строка = конец):")
    files = []
    while True:
        path = input("   > ").strip()
        if not path:
            break
        if Path(path).exists():
            files.append(path)
        else:
            print(f"   ⚠️ Файл не найден: {path}")
    return files


def load_aable_second_sheet(filepath: str) -> pd.DataFrame:
    """Загрузка второго листа из файла AA_BLE (все колонки как есть)"""
    try:
        xls = pd.ExcelFile(filepath)
        sheet = xls.sheet_names[1] if len(xls.sheet_names) >= 2 else xls.sheet_names[0]
        df = xls.parse(sheet)
        return df
    except Exception as e:
        print(f"   ❌ Ошибка чтения {filepath}: {e}")
        return pd.DataFrame()


def find_tn_column(df: pd.DataFrame) -> int:
    """Поиск колонки с ТН"""
    cols = list(df.columns)
    for i, c in enumerate(cols):
        name = str(c).strip().lower()
        if name in ['тн', 'tn', 'табельный номер', 'табель'] or 'тн' in name or 'табел' in name:
            return i
    return 0  # По умолчанию первая колонка


def find_time_column(df: pd.DataFrame) -> int:
    """Поиск колонки с временем"""
    cols = list(df.columns)
    for i, c in enumerate(cols):
        name = str(c).strip().lower()
        if 'время на объекте' in name or name == 'время' or name == 'time':
            return i
    return 15  # По умолчанию колонка P


def find_shift_day_column(df: pd.DataFrame) -> int:
    """Поиск колонки с днём смены"""
    cols = list(df.columns)
    for i, c in enumerate(cols):
        name = str(c).strip().lower()
        if 'день смены' in name or 'shift' in name or name == 'дата':
            return i
    return 3  # По умолчанию колонка D


def parse_time_only(val):
    """Парсинг времени"""
    if pd.isna(val):
        return None
    if isinstance(val, datetime):
        return val.time()
    s = str(val).strip()
    for fmt in ('%H:%M:%S', '%H:%M'):
        try:
            return datetime.strptime(s, fmt).time()
        except:
            pass
    try:
        v = float(s)
        base = datetime(1899, 12, 30)
        return (base + timedelta(days=v)).time()
    except:
        return None


def log_time_gaps(df: pd.DataFrame, tn_col_idx: int, time_col_idx: int, shift_col_idx: int):
    """Вывод лога разрывов во времени для каждого ТН"""
    print("\n" + "=" * 70)
    print("🔍 АНАЛИЗ РАЗРЫВОВ ВО ВРЕМЕНИ ПО СОТРУДНИКАМ")
    print("=" * 70)
    
    # Создаём временный df для анализа
    temp = pd.DataFrame({
        'tn': df.iloc[:, tn_col_idx].apply(normalize_tn),
        'shift_day': pd.to_datetime(df.iloc[:, shift_col_idx], errors='coerce').dt.date,
        'time_only': df.iloc[:, time_col_idx].apply(parse_time_only)
    })
    
    gaps_found = False
    
    for tn, tn_group in temp.groupby('tn'):
        if pd.isna(tn):
            continue
        tn_str = str(tn).strip()
        
        shift_stats = {}
        shift_gaps = {}
        
        for shift_day, day_group in tn_group.groupby('shift_day'):
            if pd.isna(shift_day):
                continue
            
            times = day_group['time_only'].dropna().tolist()
            times_sorted = sorted([t for t in times if t is not None])
            
            if len(times_sorted) < 2:
                continue
            
            shift_stats[shift_day] = len(times_sorted)
            
            gaps = []
            for i in range(1, len(times_sorted)):
                prev_time = times_sorted[i - 1]
                curr_time = times_sorted[i]
                
                prev_mins = prev_time.hour * 60 + prev_time.minute
                curr_mins = curr_time.hour * 60 + curr_time.minute
                
                diff = curr_mins - prev_mins
                if diff > 1:
                    gaps.append({
                        'from': prev_time.strftime('%H:%M'),
                        'to': curr_time.strftime('%H:%M'),
                        'gap_minutes': diff - 1
                    })
            
            if gaps:
                shift_gaps[shift_day] = gaps
        
        if shift_gaps:
            gaps_found = True
            
            if shift_stats:
                max_shift = max(shift_stats, key=shift_stats.get)
                max_shift_mins = shift_stats[max_shift]
                max_shift_str = max_shift.strftime('%d.%m.%Y') if hasattr(max_shift, 'strftime') else str(max_shift)
            else:
                max_shift_str = "н/д"
                max_shift_mins = 0
            
            print(f"\n👤 ТН: {tn_str}")
            print(f"   📊 Самая большая смена: {max_shift_str} ({max_shift_mins} мин)")
            
            for shift_day, gaps in shift_gaps.items():
                shift_str = shift_day.strftime('%d.%m.%Y') if hasattr(shift_day, 'strftime') else str(shift_day)
                shift_mins = shift_stats.get(shift_day, 0)
                print(f"   📅 Смена {shift_str} ({shift_mins} мин):")
                for gap in gaps:
                    print(f"      ⚠️ Разрыв: {gap['from']} → {gap['to']} (пропущено {gap['gap_minutes']} мин)")
    
    if not gaps_found:
        print("✅ Разрывов во времени не обнаружено.")
    
    print("=" * 70 + "\n")


def main():
    print("=" * 70)
    print("🔍 ФИЛЬТРАЦИЯ AA_BLE ПО СПИСКУ ТН")
    print("=" * 70)
    
    # 1. Загрузка списка ТН
    tn_file = input(f"Путь к файлу с ТН [{TN_LIST_FILE}]: ").strip() or TN_LIST_FILE
    tn_set = load_tn_list(tn_file)
    
    if not tn_set:
        print("❌ Список ТН пуст!")
        return
    
    # 2. Поиск файлов AA_BLE
    folder = input(f"Папка с AA_BLE файлами [{AABLE_FOLDER or 'ввод вручную'}]: ").strip() or AABLE_FOLDER
    aable_files = find_aable_files(folder if folder else None)
    
    if not aable_files:
        print("❌ Файлы AA_BLE не найдены!")
        return
    
    # 3. Загрузка и объединение данных (ВСЕ КОЛОНКИ КАК ЕСТЬ)
    print(f"\n📈 Загрузка {len(aable_files)} файлов AA_BLE...")
    all_frames = []
    
    for filepath in aable_files:
        print(f"   📄 {Path(filepath).name}...")
        df = load_aable_second_sheet(filepath)
        if not df.empty:
            all_frames.append(df)
    
    if not all_frames:
        print("❌ Не удалось загрузить данные!")
        return
    
    combined = pd.concat(all_frames, ignore_index=True)
    print(f"   ✅ Всего загружено: {len(combined)} строк, {len(combined.columns)} колонок")
    
    # 4. Находим колонку с ТН
    tn_col_idx = find_tn_column(combined)
    time_col_idx = find_time_column(combined)
    shift_col_idx = find_shift_day_column(combined)
    print(f"   📍 Колонка ТН: {combined.columns[tn_col_idx]} (индекс {tn_col_idx})")
    
    # 5. Фильтрация по ТН (сохраняем ВСЕ оригинальные колонки)
    print(f"\n🎯 Фильтрация по {len(tn_set)} ТН...")
    tn_column = combined.iloc[:, tn_col_idx].apply(normalize_tn)
    mask = tn_column.isin(tn_set)
    filtered = combined[mask].copy()
    print(f"   ✅ Отфильтровано: {len(filtered)} строк")
    
    found_tn = set(tn_column[mask].dropna().unique())
    missing_tn = tn_set - found_tn
    
    if missing_tn:
        print(f"   ⚠️ Не найдены ТН: {', '.join(sorted(missing_tn))}")
    
    print(f"   ✅ Найдены ТН: {', '.join(sorted(found_tn))}")
    
    if filtered.empty:
        print("❌ Нет данных после фильтрации!")
        return
    
    # 6. Вывод лога разрывов
    log_time_gaps(filtered, tn_col_idx, time_col_idx, shift_col_idx)
    
    # 7. Сохранение результата (ВСЕ КОЛОНКИ КАК В ОРИГИНАЛЕ)
    output = input(f"Сохранить в файл [{OUTPUT_FILE}]: ").strip() or OUTPUT_FILE
    filtered.to_excel(output, index=False)
    print(f"✅ Результат сохранён: {output}")
    print(f"   (Все {len(filtered.columns)} колонок сохранены как в оригинале)")


if __name__ == '__main__':
    main()
