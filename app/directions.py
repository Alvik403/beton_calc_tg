"""Реестр направлений расчёта. Добавление нового направления = новая запись в регистр."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from app.config import (
    load_materials_config,
    load_prices_config,
    load_recipes_config,
)

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = BASE_DIR / "config"

# Дефолты для ЖБИ (будут использованы при регистрации направления)
JBI_BASE_ITEM_NAME = "ЖБИ 1"
JBI_BASE_MATERIAL_NAMES = [
    "БСТ В40 F150 W8 (ЖБИ)",
    "Кладочная сетка 200*200 2,0*3,0 ф5 ГОСТ 23279-2012",
    "Опалубка под плиту перекрытия",
    "Опалубка под продольную стену тип А",
    "Опалубка под продольную стену тип В",
    "Опалубка под торцевую стену тип В",
    "Опалубка под торцевую стену тип А",
    "ЗД-2 закладная деталь",
    "Кладочная сетка 200*200 2,0*6,0 ф5",
    "Г-500-500-8 армозаготовка",
    "К-2930-70-8 армокаркас",
    "КА-2930-60-52-8 армокаркас",
    "КР-3400-300-10 армокаркас",
    "Смазка для распалубки Forma CS-10 (кг)",
    "КГ-2930-250-8А армокаркас",
    "КГ-2930-250-8В армокаркас",
    "6А500С L=3440 армозаготовка",
    "Проволока вязальная d=1,2мм (кг)",
    "ЗД-1 закладная деталь",
    "П-350-60-6 армозаготовка",
    "Г-500-500-8* армозаготовка",
    "6А500С L=2085 армозаготовка",
    "Г-350-350-6 армозаготовка",
    "Фиксатор потолочная опора 20/25/30/35, Усиленный",
    "6А500С L=2090 армозаготовка",
    'Фиксатор "Звездочка" 15 мм',
]
JBI_BASE_ALIASES: Dict[str, List[str]] = {
    "БСТ В40 F150 W8 (ЖБИ)": ["БСТ В40 F150 W8 (ЖБИ)"],
    "Кладочная сетка 200*200 2,0*3,0 ф5 ГОСТ 23279-2012": [
        "Кладочная сетка 200*200 2,0*3,0 ф5 ГОСТ 23279-2012",
        "Кладочная сетка 200*200 2,0*6,0 ф5",
    ],
    "Опалубка под плиту перекрытия": ["Опалубка под плиту перекрытия"],
    "Опалубка под продольную стену тип А": ["Опалубка под продольную стену тип А"],
    "Опалубка под продольную стену тип В": ["Опалубка под продольную стену тип В"],
    "Опалубка под торцевую стену тип В": ["Опалубка под торцевую стену тип В"],
    "Опалубка под торцевую стену тип А": ["Опалубка под торцевую стену тип А"],
    "ЗД-2 закладная деталь": ["ЗД-2 закладная деталь"],
    "Кладочная сетка 200*200 2,0*6,0 ф5": ["Кладочная сетка 200*200 2,0*6,0 ф5"],
    "Г-500-500-8 армозаготовка": ["Г-500-500-8 армозаготовка"],
    "К-2930-70-8 армокаркас": ["К-2930-70-8 армокаркас"],
    "КА-2930-60-52-8 армокаркас": ["КА-2930-60-52-8 армокаркас"],
    "КР-3400-300-10 армокаркас": ["КР-3400-300-10 армокаркас"],
    "Смазка для распалубки Forma CS-10 (кг)": [
        "Смазка для распалубки Forma CS-10 (кг)",
        "Смазка для опалубки ТираФорм (1 бочка=200л)",
    ],
    "КГ-2930-250-8А армокаркас": ["КГ-2930-250-8А армокаркас"],
    "КГ-2930-250-8В армокаркас": ["КГ-2930-250-8В армокаркас"],
    "6А500С L=3440 армозаготовка": [
        "6А500С L=3440 армозаготовка",
        "8А500С L=3440 армозаготовка",
    ],
    "Проволока вязальная d=1,2мм (кг)": ["Проволока вязальная d=1,2мм (кг)"],
    "ЗД-1 закладная деталь": ["ЗД-1 закладная деталь"],
    "П-350-60-6 армозаготовка": [
        "П-350-60-6 армозаготовка",
        "П-370-60-6 армозаготовка",
    ],
    "Г-500-500-8* армозаготовка": [
        "Г-500-500-8* армозаготовка",
        "Г-500-500-8 армозаготовка",
    ],
    "6А500С L=2085 армозаготовка": [
        "6А500С L=2085 армозаготовка",
        "6А500С L=2090 армозаготовка",
    ],
    "Г-350-350-6 армозаготовка": [
        "Г-350-350-6 армозаготовка",
        "Г-350-350-8 армозаготовка",
    ],
    "Фиксатор потолочная опора 20/25/30/35, Усиленный": [
        "Фиксатор потолочная опора 20/25/30/35, Усиленный",
    ],
    "6А500С L=2090 армозаготовка": ["6А500С L=2090 армозаготовка"],
    'Фиксатор "Звездочка" 15 мм': ['Фиксатор "Звездочка" 15 мм'],
}
JBI_BASE_UNIT_PRICES = [
    7139.38, 717.14, 3921.36, 3296.03, 3296.03, 2665.07, 2662.26, 688.47,
    1700.00, 29.93, 176.92, 270.09, 374.77, 163.22, 487.85, 487.85, 48.11,
    108.33, 106.54, 13.64, 29.93, 28.97, 21.45, 2.92, 29.91, 1.67,
]
JBI_BASE_COUNTS = [
    10, 10, 1, 1, 1, 1, 1, 2, 1, 21, 5, 3, 2, 10, 1, 1, 9, 3, 2, 21, 36, 2, 15, 0, 75, 0,
]
JBI_BASE_TOTAL_PRICE = 106388.03


@dataclass
class Direction:
    """Направление расчёта (бетон, ЖБИ и т.д.)."""
    id: str
    display_name: str
    profiles_path: Path
    calc_type: str  # "m3" | "units"
    concrete_source: Optional[str] = None  # id направления-источника бетона (для units)
    supports_excel: bool = True
    _get_default_config: Callable[[], Dict[str, Any]] = field(default=lambda: {}, repr=False)

    def get_default_config(self) -> Dict[str, Any]:
        """Дефолтные материалы, рецепты, цены."""
        return self._get_default_config()


def _beton_default_config() -> Dict[str, Any]:
    return {
        "materials": load_materials_config(),
        "recipes": load_recipes_config(),
        "prices": load_prices_config(),
    }


def _jbi_default_materials() -> List[Dict[str, Any]]:
    return [
        {"name": name, "aliases": JBI_BASE_ALIASES.get(name, [name])}
        for name in JBI_BASE_MATERIAL_NAMES
    ]


def _jbi_default_recipes() -> List[Dict[str, Any]]:
    return [
        {
            "name": JBI_BASE_ITEM_NAME,
            "materials": {
                name: count
                for name, count in zip(JBI_BASE_MATERIAL_NAMES, JBI_BASE_COUNTS)
            },
        }
    ]


def _jbi_default_prices() -> List[Dict[str, Any]]:
    material_prices = [
        {"name": name, "no_delivery_no_vat": p, "no_delivery_vat_22": round(p * 1.22, 2),
         "pickup_no_vat": p, "pickup_vat_22": round(p * 1.22, 2)}
        for name, p in zip(JBI_BASE_MATERIAL_NAMES, JBI_BASE_UNIT_PRICES)
    ]
    material_prices.append({
        "name": JBI_BASE_ITEM_NAME,
        "no_delivery_no_vat": JBI_BASE_TOTAL_PRICE,
        "no_delivery_vat_22": round(JBI_BASE_TOTAL_PRICE * 1.22, 2),
        "pickup_no_vat": JBI_BASE_TOTAL_PRICE,
        "pickup_vat_22": round(JBI_BASE_TOTAL_PRICE * 1.22, 2),
    })
    return material_prices


def _jbi_default_config() -> Dict[str, Any]:
    return {
        "materials": _jbi_default_materials(),
        "recipes": _jbi_default_recipes(),
        "prices": _jbi_default_prices(),
    }


_REGISTRY: Dict[str, Direction] = {}


def _register(
    id: str,
    display_name: str,
    profiles_path: Path,
    calc_type: str,
    concrete_source: Optional[str] = None,
    supports_excel: bool = True,
    get_default_config: Optional[Callable[[], Dict[str, Any]]] = None,
) -> None:
    cb = get_default_config if get_default_config is not None else lambda: {}
    direction = Direction(
        id=id,
        display_name=display_name,
        profiles_path=profiles_path,
        calc_type=calc_type,
        concrete_source=concrete_source,
        supports_excel=supports_excel,
        _get_default_config=cb,
    )
    _REGISTRY[id] = direction


def _init_registry() -> None:
    if _REGISTRY:
        return
    _register(
        id="beton",
        display_name="Расчет бетона по остаткам",
        profiles_path=CONFIG_DIR / "web_profiles.json",
        calc_type="m3",
        supports_excel=True,
        get_default_config=_beton_default_config,
    )
    _register(
        id="jbi",
        display_name="Расчет ЖБИ",
        profiles_path=CONFIG_DIR / "web_profiles_jbi.json",
        calc_type="units",
        concrete_source="beton",
        supports_excel=True,
        get_default_config=_jbi_default_config,
    )


def get_all_directions() -> List[Direction]:
    _init_registry()
    return list(_REGISTRY.values())


def get_direction(scope: str) -> Direction:
    _init_registry()
    if scope in _REGISTRY:
        return _REGISTRY[scope]
    return _REGISTRY["beton"]


def validate_scope(scope: Optional[str]) -> str:
    _init_registry()
    if scope and scope in _REGISTRY:
        return scope
    return "beton"
