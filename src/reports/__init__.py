# AA_BLE Automation - Reports package
"""
Модули генерации отчётов: HTML (SVG-таймлайны).
"""

from src.reports.generator import (
    ReportGenerator,
    generate_html_report,
    validate_html_structure,
)

from src.reports.svg_generator import (
    SVGTimelineGenerator,
    generate_report_filename,
    ZONE_COLORS,
    ZONE_NAMES,
    KPI_GROUPS,
)
