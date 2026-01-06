# AA_BLE Automation - SVG Timeline Generator
"""
Модуль генерации SVG-таймлайнов (адаптировано из lomonosov_6_23_v2.py).

Особенности:
- Динамический viewport от первой до последней минуты
- KPI-карточки: Зоны работ / Перерывы / Вне BLE / Прочее
- Accordion с детализацией по зонам и меткам
- Полностью оффлайн (без CDN)
"""

import json
from datetime import datetime, timedelta, time as dtime, date
from typing import Optional
import pandas as pd


# Константы цветов и названий зон
ZONE_COLORS = {
    0: '#95A5A6', 1: '#27AE60', 2: '#E67E22', 3: '#E74C3C',
    4: '#E74C3C', 5: '#8E44AD', 6: '#3498DB', 7: '#F1C40F',
    8: '#1ABC9C', 9: '#9B59B6', 10: '#1F77B4', 11: '#E67E22',
    12: '#2ECC71', 13: '#E67E22'
}

ZONE_NAMES = {
    0: 'Вне зоны BLE-маячков', 1: 'Зоны проведения работ', 2: 'Столовые',
    3: 'Опасные зоны', 4: 'Курилки', 5: 'Зоны отдыха', 6: 'ВЖГ',
    7: 'Туалеты', 8: 'Остановки автобусов', 9: 'Административные помещения',
    10: 'Зона выдачи WW', 11: 'Склад', 12: 'Мастерские', 13: 'КПП'
}

ZONE_ORDER = [1, 4, 10, 13, 2, 7, 0, 5, 3, 6, 8, 9, 11, 12]

# KPI группировка
KPI_GROUPS = {
    'work': [1],
    'breaks': [2, 4, 5, 7],
    'no_ble': [0],
    'other': [3, 6, 8, 9, 10, 11, 12, 13]
}


def lighten_color(hex_color: str, factor: float = 0.4) -> str:
    """Осветление цвета для вложенных элементов."""
    hex_color = hex_color.lstrip('#')
    r, g, b = int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
    r = int(r + (255 - r) * factor)
    g = int(g + (255 - g) * factor)
    b = int(b + (255 - b) * factor)
    return f'#{r:02x}{g:02x}{b:02x}'


def format_duration(mins: int) -> str:
    """Форматирование длительности в часы и минуты."""
    h = int(mins) // 60
    m = int(mins) % 60
    if h == 0:
        return f"{m} мин"
    return f"{h}ч {m:02d}м"


class SVGTimelineGenerator:
    """Генератор SVG-таймлайнов для отчётов AA_BLE."""
    
    def __init__(self, tag_desc_map: Optional[dict] = None):
        """
        Args:
            tag_desc_map: Словарь {номер_метки: описание}
        """
        self.tag_desc_map = tag_desc_map or {}
    
    def prepare_timeline_data(self, df_user: pd.DataFrame) -> Optional[dict]:
        """Подготовка данных для SVG-таймлайна (динамический viewport).
        
        Args:
            df_user: DataFrame с сегментами одного сотрудника
            
        Returns:
            Словарь с данными для генерации или None
        """
        if df_user is None or df_user.empty:
            return None
        
        df = df_user.copy()
        df['start'] = pd.to_datetime(df['start'])
        df['end'] = pd.to_datetime(df['end'])
        
        if df.empty:
            return None
        
        user = df['employee'].iloc[0]
        area = df['area'].iloc[0] if 'area' in df.columns else ''
        
        # Определяем базовую дату (начало суток)
        first_dt = df['start'].min()
        base_date = first_dt.date()
        base_midnight = datetime.combine(base_date, dtime(0, 0))
        
        # Собираем сегменты с абсолютными минутами от полуночи
        segments = []
        for _, row in df.iterrows():
            minute = int((row['start'] - base_midnight).total_seconds() // 60)
            zone = int(row['zone_id']) if pd.notna(row['zone_id']) else 0
            tag = int(row['ble_tag']) if pd.notna(row['ble_tag']) else 0
            desc = self.tag_desc_map.get(str(tag), '')
            segments.append({
                'minute': minute,
                'zone': zone,
                'tag': tag,
                'desc': desc,
                'datetime': row['start']
            })
        
        if not segments:
            return None
        
        # Динамический viewport
        all_minutes = [s['minute'] for s in segments]
        first_minute = min(all_minutes)
        last_minute = max(all_minutes)
        
        viewport_start = (first_minute // 60) * 60
        viewport_end = ((last_minute // 60) + 1) * 60
        
        # Статистика по зонам
        zone_stats = {}
        for seg in segments:
            z = seg['zone']
            if z not in zone_stats:
                zone_stats[z] = {'minutes': 0, 'tags': {}}
            zone_stats[z]['minutes'] += 1
            t = seg['tag']
            if t not in zone_stats[z]['tags']:
                zone_stats[z]['tags'][t] = {'minutes': 0, 'desc': seg['desc']}
            zone_stats[z]['tags'][t]['minutes'] += 1
        
        # KPI
        total = len(segments)
        kpi = {'work': 0, 'breaks': 0, 'no_ble': 0, 'other': 0}
        for seg in segments:
            z = seg['zone']
            if z in KPI_GROUPS['work']:
                kpi['work'] += 1
            elif z in KPI_GROUPS['breaks']:
                kpi['breaks'] += 1
            elif z in KPI_GROUPS['no_ble']:
                kpi['no_ble'] += 1
            else:
                kpi['other'] += 1
        
        return {
            'employee': user,
            'area': area,
            'base_date': base_date,
            'segments': segments,
            'zone_stats': zone_stats,
            'kpi': kpi,
            'total': total,
            'viewport_start': viewport_start,
            'viewport_end': viewport_end,
            'first_minute': first_minute,
            'last_minute': last_minute,
        }
    
    def _generate_svg_content(self, data: dict) -> tuple[str, int]:
        """Генерация SVG-контента с сегментами.
        
        Returns:
            Tuple (SVG rects string, svg_width)
        """
        segments = data['segments']
        viewport_start = data['viewport_start']
        viewport_end = data['viewport_end']
        
        svg_rects = []
        for seg in segments:
            x_pos = (seg['minute'] - viewport_start) * 20
            color = ZONE_COLORS.get(seg['zone'], '#95A5A6')
            svg_rects.append(
                f'<rect x="{x_pos}" y="0" width="19" height="100" '
                f'fill="{color}" rx="3" class="segment" '
                f'data-minute="{seg["minute"]}" data-zone="{seg["zone"]}" '
                f'data-tag="{seg["tag"]}" data-desc="{seg["desc"]}"/>'
            )
        
        svg_width = (viewport_end - viewport_start) * 20
        return '\n'.join(svg_rects), svg_width
    
    def _generate_zones_accordion(self, data: dict) -> str:
        """Генерация accordion с детализацией по зонам."""
        zone_stats = data['zone_stats']
        total = data['total']
        
        zones_html = []
        sorted_zones = sorted(zone_stats.keys(), 
                              key=lambda z: ZONE_ORDER.index(z) if z in ZONE_ORDER else 999)
        
        for zid in sorted_zones:
            zdata = zone_stats[zid]
            mins = zdata['minutes']
            zpct = round(mins / total * 100, 1) if total > 0 else 0
            color = ZONE_COLORS.get(zid, '#95A5A6')
            light_color = lighten_color(color)
            zname = ZONE_NAMES.get(zid, f'Зона {zid}')
            
            # Метки внутри зоны
            tags_html = []
            sorted_tags = sorted(zdata['tags'].items(), key=lambda x: x[1]['minutes'], reverse=True)
            for tag_id, tdata in sorted_tags:
                tmins = tdata['minutes']
                tpct = round(tmins / total * 100, 1) if total > 0 else 0
                tdesc = tdata['desc']
                tlabel = f"#{tag_id} ({tdesc})" if tdesc else f"#{tag_id}"
                tags_html.append(
                    f'<div class="tag-row" style="background:{light_color}">'
                    f'<span class="tag-name">{tlabel}</span>'
                    f'<span class="tag-value">{format_duration(tmins)} ({tpct}%)</span></div>'
                )
            
            zones_html.append(f'''
            <div class="zone-accordion">
              <div class="zone-header" onclick="toggleZone(this)">
                <span class="zone-toggle">▶</span>
                <div class="zone-dot" style="background:{color}"></div>
                <span class="zone-name">{zname}</span>
                <span class="zone-value">{format_duration(mins)} ({zpct}%)</span>
              </div>
              <div class="zone-content">{''.join(tags_html)}</div>
            </div>''')
        
        return ''.join(zones_html)

    def generate_single_timeline_html(self, data: dict) -> Optional[str]:
        """Генерация полного HTML для одного сотрудника.
        
        Args:
            data: Данные от prepare_timeline_data()
            
        Returns:
            HTML-строка или None
        """
        if data is None:
            return None
        
        employee = data['employee']
        area = data['area']
        base_date = data['base_date']
        kpi = data['kpi']
        total = data['total']
        viewport_start = data['viewport_start']
        viewport_end = data['viewport_end']
        
        date_str = base_date.strftime('%d.%m.%Y') if base_date else ''
        
        def pct(v):
            return round(v / total * 100) if total > 0 else 0
        
        svg_content, svg_width = self._generate_svg_content(data)
        zones_html = self._generate_zones_accordion(data)
        
        # JSON для JS
        segments_for_js = [{'minute': s['minute'], 'zone': s['zone'], 
                           'tag': s['tag'], 'desc': s['desc']} for s in data['segments']]
        js_data = json.dumps({
            'segments': segments_for_js,
            'zoneColors': ZONE_COLORS,
            'zoneNames': ZONE_NAMES,
            'viewportStart': viewport_start,
            'viewportEnd': viewport_end,
            'baseDate': date_str,
        }, ensure_ascii=False)
        
        return self._build_full_html(
            employee=employee,
            area=area,
            date_str=date_str,
            kpi=kpi,
            total=total,
            pct=pct,
            svg_content=svg_content,
            svg_width=svg_width,
            zones_html=zones_html,
            js_data=js_data,
            viewport_start=viewport_start,
            viewport_end=viewport_end
        )
    
    def _build_full_html(self, employee, area, date_str, kpi, total, pct,
                         svg_content, svg_width, zones_html, js_data,
                         viewport_start, viewport_end) -> str:
        """Сборка полного HTML документа."""
        
        total_minutes = viewport_end - viewport_start
        
        return f'''<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<title>AA_BLE Таймлайн — {employee}</title>
{self._get_css()}
</head>
<body>
<div class="container">
<div class="card">
<div class="header">
  <div class="header-left">
    <div class="title">{employee}</div>
    <div class="subtitle">Участок: {area if area else 'Не указан'}</div>
    <div class="time-display" id="timeRange">{date_str} 00:00 — 01:00</div>
  </div>
</div>
<div class="kpi-section">
  <div class="kpi-cards">
    <div class="kpi-card work"><div class="kpi-label">Зоны проведения работ</div><div class="kpi-value">{format_duration(kpi['work'])}</div><div class="kpi-pct">{pct(kpi['work'])}%</div></div>
    <div class="kpi-card breaks"><div class="kpi-label">Зоны перерывов</div><div class="kpi-value">{format_duration(kpi['breaks'])}</div><div class="kpi-pct">{pct(kpi['breaks'])}%</div></div>
    <div class="kpi-card no-ble"><div class="kpi-label">Вне BLE</div><div class="kpi-value">{format_duration(kpi['no_ble'])}</div><div class="kpi-pct">{pct(kpi['no_ble'])}%</div></div>
    <div class="kpi-card other"><div class="kpi-label">Прочее</div><div class="kpi-value">{format_duration(kpi['other'])}</div><div class="kpi-pct">{pct(kpi['other'])}%</div></div>
    <div class="kpi-card total"><div class="kpi-label">Итого</div><div class="kpi-value">{format_duration(total)}</div><div class="kpi-pct">100%</div></div>
  </div>
</div>
<div class="timeline-wrapper">
  <button class="nav-btn" id="btnLeft" onclick="scrollTimeline(-60)">&lt;</button>
  <div class="viewport">
    <div class="canvas-container" id="canvasContainer">
      <svg id="timelineSvg" width="{svg_width}" height="100">
        {svg_content}
      </svg>
    </div>
  </div>
  <button class="nav-btn" id="btnRight" onclick="scrollTimeline(60)">&gt;</button>
</div>
<div class="time-axis-wrapper">
  <div class="time-axis">
    <div class="time-axis-inner" id="timeAxis"></div>
  </div>
</div>
{self._get_details_section_css()}
<div class="details-section">
  <div class="details-toggle" onclick="toggleDetails()">
    <span class="details-icon" id="detailsIcon">▶</span>
    <span>Детализация по зонам и меткам</span>
  </div>
  <div class="details-content" id="detailsContent">
    {zones_html}
  </div>
</div>
</div>
</div>
<div class="tooltip" id="tooltip"></div>
{self._get_js_script(js_data, viewport_start, viewport_end, total_minutes)}
</body>
</html>'''

    def _get_css(self) -> str:
        """Возвращает CSS стили."""
        return '''<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Segoe UI', Arial, sans-serif; background: #f0f2f5; padding: 24px; color: #333; }
.container { max-width: 1400px; margin: 0 auto; }
.card { background: #fff; border-radius: 12px; box-shadow: 0 2px 12px rgba(0,0,0,0.08); padding: 20px; margin-bottom: 20px; }
.header { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 20px; }
.header-left { flex-shrink: 0; }
.title { font-size: 22px; font-weight: 600; color: #1a1a2e; }
.subtitle { font-size: 14px; color: #666; margin-top: 4px; }
.time-display { background: #1976d2; color: #fff; padding: 8px 16px; border-radius: 8px; font-weight: 600; font-size: 15px; margin-top: 10px; display: inline-block; }
.kpi-section { margin-bottom: 20px; }
.kpi-cards { display: flex; gap: 12px; flex-wrap: wrap; }
.kpi-card { flex: 1; min-width: 140px; padding: 16px; border-radius: 10px; text-align: center; position: relative; overflow: hidden; }
.kpi-card::before { content: ''; position: absolute; left: 0; top: 0; bottom: 0; width: 4px; }
.kpi-card.work { background: #e8f5e9; } .kpi-card.work::before { background: #27AE60; }
.kpi-card.breaks { background: #ffebee; } .kpi-card.breaks::before { background: #E74C3C; }
.kpi-card.no-ble { background: #f5f5f5; } .kpi-card.no-ble::before { background: #95A5A6; }
.kpi-card.other { background: #fff3e0; } .kpi-card.other::before { background: #E67E22; }
.kpi-card.total { background: #e3f2fd; } .kpi-card.total::before { background: #1976d2; }
.kpi-label { font-size: 12px; color: #666; margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.5px; }
.kpi-value { font-size: 20px; font-weight: 700; color: #1a1a2e; }
.kpi-pct { font-size: 14px; color: #666; margin-top: 2px; }
.timeline-wrapper { position: relative; display: flex; align-items: center; gap: 12px; margin-top: 20px; }
.nav-btn { width: 48px; height: 100px; border: none; border-radius: 8px; cursor: pointer; font-size: 24px; font-weight: bold; transition: all 0.2s; display: flex; align-items: center; justify-content: center; flex-shrink: 0; background: #e8f4fd; color: #1976d2; }
.nav-btn:hover { background: #bbdefb; }
.nav-btn:active { transform: scale(0.95); background: #90caf9; }
.nav-btn:disabled { background: #f5f5f5; color: #ccc; cursor: not-allowed; transform: none; }
.viewport { flex: 1; overflow: hidden; border-radius: 8px; background: #fafafa; border: 1px solid #e0e0e0; }
.canvas-container { transition: transform 0.3s cubic-bezier(0.4, 0, 0.2, 1); }
.segment { cursor: pointer; transition: filter 0.15s; }
.segment:hover { filter: brightness(1.15) saturate(1.1); }
.time-axis-wrapper { margin-left: 60px; margin-right: 60px; overflow: hidden; }
.time-axis { display: flex; margin-top: 8px; position: relative; height: 24px; }
.time-axis-inner { display: flex; transition: transform 0.3s cubic-bezier(0.4, 0, 0.2, 1); }
.time-label { font-size: 11px; color: #666; text-align: left; flex-shrink: 0; width: 300px; }
.tooltip { position: fixed; background: #1a1a2e; color: #fff; padding: 12px 16px; border-radius: 8px; font-size: 13px; pointer-events: none; opacity: 0; transition: opacity 0.2s; z-index: 1000; max-width: 280px; box-shadow: 0 4px 20px rgba(0,0,0,0.25); }
.tooltip.visible { opacity: 1; }
.tooltip-row { margin: 4px 0; display: flex; justify-content: space-between; gap: 16px; }
.tooltip-label { color: #aaa; }
.tooltip-value { font-weight: 600; }
.tooltip-header { font-weight: 600; font-size: 14px; margin-bottom: 8px; padding-bottom: 8px; border-bottom: 1px solid #333; }
</style>'''

    def _get_details_section_css(self) -> str:
        """CSS для секции детализации."""
        return '''<style>
.details-section { margin-top: 20px; border-top: 1px solid #eee; padding-top: 16px; }
.details-toggle { display: flex; align-items: center; gap: 8px; cursor: pointer; padding: 10px; border-radius: 8px; font-weight: 500; color: #555; transition: background 0.2s; }
.details-toggle:hover { background: #f5f5f5; }
.details-icon { font-size: 12px; transition: transform 0.2s; }
.details-icon.open { transform: rotate(90deg); }
.details-content { margin-top: 12px; display: none; }
.zone-accordion { margin-bottom: 2px; }
.zone-header { display: flex; align-items: center; padding: 8px 10px; cursor: pointer; border-radius: 6px; transition: background 0.2s; font-size: 13px; }
.zone-header:hover { background: #f5f5f5; }
.zone-toggle { font-size: 10px; margin-right: 8px; color: #666; transition: transform 0.2s; width: 12px; }
.zone-toggle.open { transform: rotate(90deg); }
.zone-dot { width: 12px; height: 12px; border-radius: 4px; margin-right: 10px; flex-shrink: 0; }
.zone-name { flex: 1; color: #333; }
.zone-value { font-weight: 600; color: #333; margin-left: 8px; white-space: nowrap; }
.zone-content { margin-left: 30px; padding-left: 10px; border-left: 2px solid #e0e0e0; display: none; }
.tag-row { display: flex; align-items: center; justify-content: space-between; padding: 6px 10px; margin: 3px 0; border-radius: 6px; font-size: 12px; }
.tag-name { flex: 1; color: #555; }
.tag-value { font-weight: 600; color: #333; margin-left: 8px; }
</style>'''

    def _get_js_script(self, js_data: str, viewport_start: int, 
                       viewport_end: int, total_minutes: int) -> str:
        """JavaScript для интерактивности."""
        return f'''
<script>
const DATA = {js_data};
const CONFIG = {{
  MINUTE_WIDTH: 20,
  SEGMENT_HEIGHT: 100,
  SCROLL_STEP: 60,
  WINDOW_START: {viewport_start},
  WINDOW_END: {viewport_end},
}};
const TOTAL_MINUTES = CONFIG.WINDOW_END - CONFIG.WINDOW_START;
let offset = 0;
let viewportMinutes = 60;

function calcViewportMinutes() {{
  const viewport = document.querySelector('.viewport');
  if (viewport) viewportMinutes = Math.floor(viewport.clientWidth / CONFIG.MINUTE_WIDTH);
}}

function formatTime(m) {{
  const h = Math.floor(m / 60) % 24;
  const mm = m % 60;
  return h.toString().padStart(2, '0') + ':' + mm.toString().padStart(2, '0');
}}

function formatDateTime(m) {{
  const baseDate = DATA.baseDate || '';
  const dayOffset = Math.floor(m / 1440);
  const timeStr = formatTime(m);
  if (dayOffset > 0 && baseDate) {{
    const parts = baseDate.split('.');
    if (parts.length === 3) {{
      const d = new Date(parts[2], parts[1] - 1, parseInt(parts[0]) + dayOffset);
      const newDate = d.getDate().toString().padStart(2, '0') + '.' + 
                      (d.getMonth() + 1).toString().padStart(2, '0') + '.' + d.getFullYear();
      return newDate + ' ' + timeStr;
    }}
  }}
  return baseDate + ' ' + timeStr;
}}

function renderTimeAxis() {{
  const axis = document.getElementById('timeAxis');
  axis.innerHTML = '';
  for (let m = 0; m <= TOTAL_MINUTES; m += 15) {{
    const label = document.createElement('div');
    label.className = 'time-label';
    label.style.width = (15 * CONFIG.MINUTE_WIDTH) + 'px';
    label.textContent = formatTime(CONFIG.WINDOW_START + m);
    axis.appendChild(label);
  }}
}}

function scrollTimeline(minutes) {{
  calcViewportMinutes();
  const maxOffset = Math.max(0, TOTAL_MINUTES - viewportMinutes);
  offset = Math.max(0, Math.min(offset + minutes, maxOffset));
  updateOffset();
}}

function updateOffset() {{
  calcViewportMinutes();
  const px = offset * CONFIG.MINUTE_WIDTH;
  document.getElementById('canvasContainer').style.transform = 'translateX(-' + px + 'px)';
  document.getElementById('timeAxis').style.transform = 'translateX(-' + px + 'px)';
  const maxOffset = Math.max(0, TOTAL_MINUTES - viewportMinutes);
  document.getElementById('btnLeft').disabled = (offset <= 0);
  document.getElementById('btnRight').disabled = (offset >= maxOffset);
  const startMins = CONFIG.WINDOW_START + offset;
  const endMins = startMins + 60;
  document.getElementById('timeRange').textContent = formatDateTime(startMins) + ' — ' + formatTime(endMins);
}}

function toggleDetails() {{
  const content = document.getElementById('detailsContent');
  const icon = document.getElementById('detailsIcon');
  if (content.style.display === 'none' || content.style.display === '') {{
    content.style.display = 'block';
    icon.classList.add('open');
  }} else {{
    content.style.display = 'none';
    icon.classList.remove('open');
  }}
}}

function toggleZone(header) {{
  const content = header.nextElementSibling;
  const toggle = header.querySelector('.zone-toggle');
  if (content.style.display === 'none' || content.style.display === '') {{
    content.style.display = 'block';
    toggle.classList.add('open');
  }} else {{
    content.style.display = 'none';
    toggle.classList.remove('open');
  }}
}}

function showTooltip(e) {{
  const tooltip = document.getElementById('tooltip');
  const rect = e.target;
  const minute = parseInt(rect.dataset.minute);
  const timeStr = formatDateTime(minute);
  const zone = parseInt(rect.dataset.zone);
  const tag = rect.dataset.tag;
  const desc = rect.dataset.desc || '';
  tooltip.innerHTML = '<div class="tooltip-header">' + timeStr + '</div>' +
    '<div class="tooltip-row"><span class="tooltip-label">Зона:</span><span class="tooltip-value">' + (DATA.zoneNames[zone] || 'Зона ' + zone) + '</span></div>' +
    '<div class="tooltip-row"><span class="tooltip-label">Метка:</span><span class="tooltip-value">#' + tag + '</span></div>' +
    '<div class="tooltip-row"><span class="tooltip-label">Описание:</span><span class="tooltip-value">' + desc + '</span></div>';
  tooltip.classList.add('visible');
  moveTooltip(e);
}}

function moveTooltip(e) {{
  const tooltip = document.getElementById('tooltip');
  const tooltipWidth = tooltip.offsetWidth || 280;
  const tooltipHeight = tooltip.offsetHeight || 150;
  let left = e.clientX + 15;
  let top = e.clientY + 15;
  if (left + tooltipWidth > window.innerWidth - 10) left = e.clientX - tooltipWidth - 15;
  if (top + tooltipHeight > window.innerHeight - 10) top = e.clientY - tooltipHeight - 15;
  if (left < 10) left = 10;
  if (top < 10) top = 10;
  tooltip.style.left = left + 'px';
  tooltip.style.top = top + 'px';
}}

function hideTooltip() {{
  document.getElementById('tooltip').classList.remove('visible');
}}

document.addEventListener('DOMContentLoaded', function() {{
  calcViewportMinutes();
  renderTimeAxis();
  updateOffset();
  document.querySelectorAll('.segment').forEach(function(el) {{
    el.addEventListener('mouseenter', showTooltip);
    el.addEventListener('mouseleave', hideTooltip);
    el.addEventListener('mousemove', moveTooltip);
  }});
}});

window.addEventListener('resize', function() {{
  calcViewportMinutes();
  updateOffset();
}});

document.addEventListener('keydown', function(e) {{
  if (e.key === 'ArrowLeft') scrollTimeline(-CONFIG.SCROLL_STEP);
  if (e.key === 'ArrowRight') scrollTimeline(CONFIG.SCROLL_STEP);
}});
</script>'''

    def generate_combined_html(
        self, 
        segments_df: pd.DataFrame,
        site_name: str,
        report_date: date
    ) -> Optional[str]:
        """Генерация объединённого HTML со всеми сотрудниками.
        
        Args:
            segments_df: DataFrame с сегментами всех сотрудников
            site_name: Название объекта (для заголовка)
            report_date: Дата отчёта
            
        Returns:
            HTML-строка или None
        """
        if segments_df is None or segments_df.empty:
            return None
        
        date_str = report_date.strftime('%d.%m.%Y') if report_date else ''
        page_title = f"AA_BLE {site_name} — {date_str}"
        
        # Подготавливаем данные для каждого сотрудника
        all_data = []
        for employee, group in segments_df.groupby('employee'):
            data = self.prepare_timeline_data(group)
            if data:
                all_data.append(data)
        
        if not all_data:
            return None
        
        # Генерируем оглавление и секции
        toc_items = []
        sections = []
        
        for i, data in enumerate(all_data, 1):
            anchor = f"sec{i}"
            employee = data['employee']
            toc_items.append(f'<li><a href="#{anchor}">{employee}</a></li>')
            
            section_html = self._generate_section_html(data, i)
            sections.append({
                'anchor': anchor,
                'html': section_html,
                'index': i,
                'viewport_start': data['viewport_start'],
                'viewport_end': data['viewport_end'],
                'js_data': self._prepare_js_data(data),
            })
        
        return self._build_combined_html(page_title, toc_items, sections)
    
    def _prepare_js_data(self, data: dict) -> str:
        """Подготовка JSON данных для JS."""
        segments_for_js = [{'minute': s['minute'], 'zone': s['zone'], 
                           'tag': s['tag'], 'desc': s['desc']} for s in data['segments']]
        return json.dumps({
            'segments': segments_for_js,
            'zoneColors': ZONE_COLORS,
            'zoneNames': ZONE_NAMES,
            'viewportStart': data['viewport_start'],
            'viewportEnd': data['viewport_end'],
            'baseDate': data['base_date'].strftime('%d.%m.%Y') if data['base_date'] else '',
        }, ensure_ascii=False)
    
    def _generate_section_html(self, data: dict, index: int) -> str:
        """Генерация HTML секции для одного сотрудника (в combined режиме)."""
        employee = data['employee']
        area = data['area']
        base_date = data['base_date']
        kpi = data['kpi']
        total = data['total']
        
        date_str = base_date.strftime('%d.%m.%Y') if base_date else ''
        
        def pct(v):
            return round(v / total * 100) if total > 0 else 0
        
        svg_content, svg_width = self._generate_svg_content(data)
        zones_html = self._generate_zones_accordion(data)
        
        # Уникальные ID для элементов
        return f'''
<div class="card">
<div class="header">
  <div class="header-left">
    <div class="title">{employee}</div>
    <div class="subtitle">Участок: {area if area else 'Не указан'}</div>
    <div class="time-display" id="timeRange{index}">{date_str} 00:00 — 01:00</div>
  </div>
</div>
<div class="kpi-section">
  <div class="kpi-cards">
    <div class="kpi-card work"><div class="kpi-label">Зоны проведения работ</div><div class="kpi-value">{format_duration(kpi['work'])}</div><div class="kpi-pct">{pct(kpi['work'])}%</div></div>
    <div class="kpi-card breaks"><div class="kpi-label">Зоны перерывов</div><div class="kpi-value">{format_duration(kpi['breaks'])}</div><div class="kpi-pct">{pct(kpi['breaks'])}%</div></div>
    <div class="kpi-card no-ble"><div class="kpi-label">Вне BLE</div><div class="kpi-value">{format_duration(kpi['no_ble'])}</div><div class="kpi-pct">{pct(kpi['no_ble'])}%</div></div>
    <div class="kpi-card other"><div class="kpi-label">Прочее</div><div class="kpi-value">{format_duration(kpi['other'])}</div><div class="kpi-pct">{pct(kpi['other'])}%</div></div>
    <div class="kpi-card total"><div class="kpi-label">Итого</div><div class="kpi-value">{format_duration(total)}</div><div class="kpi-pct">100%</div></div>
  </div>
</div>
<div class="timeline-wrapper">
  <button class="nav-btn" id="btnLeft{index}" onclick="scrollTimeline{index}(-60)">&lt;</button>
  <div class="viewport">
    <div class="canvas-container" id="canvasContainer{index}">
      <svg id="timelineSvg{index}" width="{svg_width}" height="100">
        {svg_content}
      </svg>
    </div>
  </div>
  <button class="nav-btn" id="btnRight{index}" onclick="scrollTimeline{index}(60)">&gt;</button>
</div>
<div class="time-axis-wrapper">
  <div class="time-axis">
    <div class="time-axis-inner" id="timeAxis{index}"></div>
  </div>
</div>
{self._get_details_section_css()}
<div class="details-section">
  <div class="details-toggle" onclick="toggleDetails{index}()">
    <span class="details-icon" id="detailsIcon{index}">▶</span>
    <span>Детализация по зонам и меткам</span>
  </div>
  <div class="details-content" id="detailsContent{index}">
    {zones_html.replace('onclick="toggleZone(this)"', f'onclick="toggleZone{index}(this)"')}
  </div>
</div>
</div>'''

    def _build_combined_html(self, page_title: str, toc_items: list, sections: list) -> str:
        """Сборка объединённого HTML документа."""
        
        sections_html = []
        sections_js = []
        
        for sec in sections:
            i = sec['index']
            sections_html.append(f'''
<div class="section" id="{sec['anchor']}">
  <div class="back-link"><a href="#toc">← К оглавлению</a></div>
  {sec['html']}
</div>''')
            sections_js.append(self._generate_section_js(
                sec['js_data'], i, sec['viewport_start'], sec['viewport_end']
            ))
        
        return f'''<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<title>{page_title}</title>
{self._get_css()}
<style>
.main-container {{ max-width: 1400px; margin: 0 auto; }}
.toc-card {{ background: #fff; border-radius: 12px; box-shadow: 0 2px 12px rgba(0,0,0,0.08); padding: 24px; margin-bottom: 24px; }}
.toc-title {{ font-size: 24px; font-weight: 600; color: #1a1a2e; margin-bottom: 16px; }}
.toc-list {{ list-style: none; }}
.toc-list li {{ margin: 8px 0; }}
.toc-list a {{ color: #1976d2; text-decoration: none; font-size: 15px; }}
.toc-list a:hover {{ text-decoration: underline; }}
.section {{ margin-bottom: 32px; }}
.back-link {{ margin-bottom: 12px; }}
.back-link a {{ color: #1976d2; text-decoration: none; font-size: 14px; }}
.back-link a:hover {{ text-decoration: underline; }}
</style>
</head>
<body>
<div class="main-container">
<div class="toc-card" id="toc">
  <div class="toc-title">{page_title}</div>
  <ul class="toc-list">{''.join(toc_items)}</ul>
</div>
{''.join(sections_html)}
</div>
{''.join(sections_js)}
</body>
</html>'''

    def _generate_section_js(self, js_data: str, index: int, 
                             viewport_start: int, viewport_end: int) -> str:
        """JavaScript для отдельной секции в combined режиме."""
        total_minutes = viewport_end - viewport_start
        
        return f'''
<script>
(function() {{
  const DATA{index} = {js_data};
  const CONFIG{index} = {{
    MINUTE_WIDTH: 20,
    SCROLL_STEP: 60,
    WINDOW_START: {viewport_start},
    WINDOW_END: {viewport_end},
  }};
  const TOTAL_MINUTES{index} = CONFIG{index}.WINDOW_END - CONFIG{index}.WINDOW_START;
  let offset{index} = 0;
  let viewportMinutes{index} = 60;

  function calcViewportMinutes{index}() {{
    const viewport = document.querySelector('#sec{index} .viewport');
    if (viewport) viewportMinutes{index} = Math.floor(viewport.clientWidth / 20);
  }}

  function formatTime{index}(m) {{
    const h = Math.floor(m / 60) % 24;
    const mm = m % 60;
    return h.toString().padStart(2, '0') + ':' + mm.toString().padStart(2, '0');
  }}

  function formatDateTime{index}(m) {{
    const baseDate = DATA{index}.baseDate || '';
    const dayOffset = Math.floor(m / 1440);
    const timeStr = formatTime{index}(m);
    if (dayOffset > 0 && baseDate) {{
      const parts = baseDate.split('.');
      if (parts.length === 3) {{
        const d = new Date(parts[2], parts[1] - 1, parseInt(parts[0]) + dayOffset);
        const newDate = d.getDate().toString().padStart(2, '0') + '.' + 
                        (d.getMonth() + 1).toString().padStart(2, '0') + '.' + d.getFullYear();
        return newDate + ' ' + timeStr;
      }}
    }}
    return baseDate + ' ' + timeStr;
  }}

  function renderTimeAxis{index}() {{
    const axis = document.getElementById('timeAxis{index}');
    if (!axis) return;
    axis.innerHTML = '';
    for (let m = 0; m <= TOTAL_MINUTES{index}; m += 15) {{
      const label = document.createElement('div');
      label.className = 'time-label';
      label.style.width = '300px';
      label.textContent = formatTime{index}(CONFIG{index}.WINDOW_START + m);
      axis.appendChild(label);
    }}
  }}

  window.scrollTimeline{index} = function(minutes) {{
    calcViewportMinutes{index}();
    const maxOffset = Math.max(0, TOTAL_MINUTES{index} - viewportMinutes{index});
    offset{index} = Math.max(0, Math.min(offset{index} + minutes, maxOffset));
    updateOffset{index}();
  }};

  function updateOffset{index}() {{
    calcViewportMinutes{index}();
    const px = offset{index} * 20;
    const container = document.getElementById('canvasContainer{index}');
    const axis = document.getElementById('timeAxis{index}');
    if (container) container.style.transform = 'translateX(-' + px + 'px)';
    if (axis) axis.style.transform = 'translateX(-' + px + 'px)';
    const maxOffset = Math.max(0, TOTAL_MINUTES{index} - viewportMinutes{index});
    const btnLeft = document.getElementById('btnLeft{index}');
    const btnRight = document.getElementById('btnRight{index}');
    if (btnLeft) btnLeft.disabled = (offset{index} <= 0);
    if (btnRight) btnRight.disabled = (offset{index} >= maxOffset);
    const startMins = CONFIG{index}.WINDOW_START + offset{index};
    const endMins = startMins + 60;
    const timeRange = document.getElementById('timeRange{index}');
    if (timeRange) timeRange.textContent = formatDateTime{index}(startMins) + ' — ' + formatTime{index}(endMins);
  }}

  window.toggleDetails{index} = function() {{
    const content = document.getElementById('detailsContent{index}');
    const icon = document.getElementById('detailsIcon{index}');
    if (!content || !icon) return;
    if (content.style.display === 'none' || content.style.display === '') {{
      content.style.display = 'block';
      icon.classList.add('open');
    }} else {{
      content.style.display = 'none';
      icon.classList.remove('open');
    }}
  }};

  window.toggleZone{index} = function(header) {{
    const content = header.nextElementSibling;
    const toggle = header.querySelector('.zone-toggle');
    if (!content || !toggle) return;
    if (content.style.display === 'none' || content.style.display === '') {{
      content.style.display = 'block';
      toggle.classList.add('open');
    }} else {{
      content.style.display = 'none';
      toggle.classList.remove('open');
    }}
  }};

  function showTooltip{index}(e) {{
    const tooltip = document.getElementById('tooltip{index}');
    if (!tooltip) return;
    const rect = e.target;
    const minute = parseInt(rect.dataset.minute);
    const timeStr = formatDateTime{index}(minute);
    const zone = parseInt(rect.dataset.zone);
    const tag = rect.dataset.tag;
    const desc = rect.dataset.desc || '';
    tooltip.innerHTML = '<div class="tooltip-header">' + timeStr + '</div>' +
      '<div class="tooltip-row"><span class="tooltip-label">Зона:</span><span class="tooltip-value">' + (DATA{index}.zoneNames[zone] || 'Зона ' + zone) + '</span></div>' +
      '<div class="tooltip-row"><span class="tooltip-label">Метка:</span><span class="tooltip-value">#' + tag + '</span></div>' +
      '<div class="tooltip-row"><span class="tooltip-label">Описание:</span><span class="tooltip-value">' + desc + '</span></div>';
    tooltip.classList.add('visible');
    moveTooltip{index}(e);
  }}

  function moveTooltip{index}(e) {{
    const tooltip = document.getElementById('tooltip{index}');
    if (!tooltip) return;
    let left = e.clientX + 15;
    let top = e.clientY + 15;
    if (left + 280 > window.innerWidth - 10) left = e.clientX - 295;
    if (top + 150 > window.innerHeight - 10) top = e.clientY - 165;
    if (left < 10) left = 10;
    if (top < 10) top = 10;
    tooltip.style.left = left + 'px';
    tooltip.style.top = top + 'px';
  }}

  function hideTooltip{index}() {{
    const tooltip = document.getElementById('tooltip{index}');
    if (tooltip) tooltip.classList.remove('visible');
  }}

  document.addEventListener('DOMContentLoaded', function() {{
    calcViewportMinutes{index}();
    renderTimeAxis{index}();
    updateOffset{index}();
    const svg = document.getElementById('timelineSvg{index}');
    if (svg) {{
      svg.querySelectorAll('.segment').forEach(function(el) {{
        el.addEventListener('mouseenter', showTooltip{index});
        el.addEventListener('mouseleave', hideTooltip{index});
        el.addEventListener('mousemove', moveTooltip{index});
      }});
    }}
    // Создаём tooltip для этой секции если его нет
    if (!document.getElementById('tooltip{index}')) {{
      const tooltip = document.createElement('div');
      tooltip.id = 'tooltip{index}';
      tooltip.className = 'tooltip';
      document.body.appendChild(tooltip);
    }}
  }});
  
  window.addEventListener('resize', function() {{
    calcViewportMinutes{index}();
    updateOffset{index}();
  }});
}})();
</script>'''


def generate_report_filename(site_name: str, report_date: date) -> str:
    """Генерация имени файла отчёта.
    
    Args:
        site_name: Название объекта
        report_date: Дата отчёта
        
    Returns:
        Имя файла в формате "AA_BLE_{site_name}_{dd-mm-yyyy}.html"
    """
    date_str = report_date.strftime('%d-%m-%Y') if report_date else 'unknown'
    # Очищаем имя от недопустимых символов
    safe_name = "".join(c for c in site_name if c.isalnum() or c in (' ', '-', '_')).strip()
    safe_name = safe_name.replace(' ', '_')
    return f"AA_BLE_{safe_name}_{date_str}.html"
