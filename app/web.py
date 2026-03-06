from __future__ import annotations

import io
import json
import tempfile
import time
from decimal import Decimal, ROUND_FLOOR
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import Body, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse

from app.calculator import Recipe, calculate_max_cubic_meters
from app.config import (
    load_materials_config,
    load_prices_config,
    load_recipes_config,
)
from app.excel_parser import MaterialConfig, extract_balances


app = FastAPI(title="Бетон калькулятор")


RATE_LIMIT_SECONDS = 10
_last_request_per_ip: dict[str, float] = {}

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = BASE_DIR / "config"
PROFILES_PATH = CONFIG_DIR / "web_profiles.json"
JBI_PROFILES_PATH = CONFIG_DIR / "web_profiles_jbi.json"
CONFIG_PASSWORD = "06032026"
CONFIG_SCOPE_BETON = "beton"
CONFIG_SCOPE_JBI = "jbi"
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
JBI_BASE_UNIT_PRICES = [
    7139.38,
    717.14,
    3921.36,
    3296.03,
    3296.03,
    2665.07,
    2662.26,
    688.47,
    1700.00,
    29.93,
    176.92,
    270.09,
    374.77,
    163.22,
    487.85,
    487.85,
    48.11,
    108.33,
    106.54,
    13.64,
    29.93,
    28.97,
    21.45,
    2.92,
    29.91,
    1.67,
]
JBI_BASE_COUNTS = [
    10,
    10,
    1,
    1,
    1,
    1,
    1,
    2,
    1,
    21,
    5,
    3,
    2,
    10,
    1,
    1,
    9,
    3,
    2,
    21,
    36,
    2,
    15,
    0,
    75,
    0,
]
JBI_BASE_TOTAL_PRICE = 106388.03
JBI_BASE_ALIASES = {
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
    material_prices = []
    for name, price in zip(JBI_BASE_MATERIAL_NAMES, JBI_BASE_UNIT_PRICES):
        material_prices.append(
            {
                "name": name,
                "no_delivery_no_vat": price,
                "no_delivery_vat_22": round(price * 1.22, 2),
                "pickup_no_vat": price,
                "pickup_vat_22": round(price * 1.22, 2),
            }
        )
    material_prices.append(
        {
            "name": JBI_BASE_ITEM_NAME,
            "no_delivery_no_vat": JBI_BASE_TOTAL_PRICE,
            "no_delivery_vat_22": round(JBI_BASE_TOTAL_PRICE * 1.22, 2),
            "pickup_no_vat": JBI_BASE_TOTAL_PRICE,
            "pickup_vat_22": round(JBI_BASE_TOTAL_PRICE * 1.22, 2),
        }
    )
    return material_prices


def _validate_scope(scope: Optional[str]) -> str:
    if scope == CONFIG_SCOPE_JBI:
        return CONFIG_SCOPE_JBI
    return CONFIG_SCOPE_BETON


def _profiles_path(scope: Optional[str]) -> Path:
    return JBI_PROFILES_PATH if _validate_scope(scope) == CONFIG_SCOPE_JBI else PROFILES_PATH


def _default_config_payload(scope: Optional[str]) -> Dict[str, Any]:
    if _validate_scope(scope) == CONFIG_SCOPE_JBI:
        return {
            "materials": _jbi_default_materials(),
            "recipes": _jbi_default_recipes(),
            "prices": _jbi_default_prices(),
        }
    return {
        "materials": load_materials_config(),
        "recipes": load_recipes_config(),
        "prices": load_prices_config(),
    }


def _require_config_password(request: Request) -> None:
    if request.headers.get("X-Config-Password") != CONFIG_PASSWORD:
        raise HTTPException(status_code=401, detail="Неверный пароль конфигуратора")


def _load_profiles(scope: Optional[str] = None) -> Dict[str, Any]:
    path = _profiles_path(scope)
    if not path.exists():
        return {"profiles": [], "active": None}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, dict):
                return {"profiles": [], "active": None}
            data.setdefault("profiles", [])
            data.setdefault("active", None)
            return data
    except Exception:
        return {"profiles": [], "active": None}


def _save_profiles(data: Dict[str, Any], scope: Optional[str] = None) -> None:
    path = _profiles_path(scope)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _get_profile(data: Dict[str, Any], name: Optional[str]) -> Optional[Dict[str, Any]]:
    if not name:
        return None
    for p in data.get("profiles", []):
        if p.get("name") == name:
            return p
    return None


def _client_ip(request: Request) -> str:
    client = request.client
    return client.host if client else "unknown"


def _is_rate_limited(ip: str) -> bool:
    now = time.time()
    last = _last_request_per_ip.get(ip)
    if last is not None and now - last < RATE_LIMIT_SECONDS:
        return True
    _last_request_per_ip[ip] = now
    return False


def _money(volume_m3: Decimal, price_per_m3: float) -> Decimal:
    return Decimal(str(volume_m3)) * Decimal(str(price_per_m3))


def _normalize_name(text: str) -> str:
    text = text.strip().lower().replace("ё", "е")
    text = text.replace("в", "b").replace("з", "3")
    import re

    text = re.sub(r"\s+", " ", text)
    return text


def _load_materials(
    scope: Optional[str] = None, profile_name: Optional[str] = None
) -> list[MaterialConfig]:
    scope = _validate_scope(scope)
    profiles = _load_profiles(scope)
    active_name = profile_name if profile_name is not None else profiles.get("active")
    active = _get_profile(profiles, active_name)
    base = _default_config_payload(scope)["materials"]
    raw = active.get("materials") if active and "materials" in active else base
    materials: list[MaterialConfig] = []
    for item in raw:
        name = item.get("name", "").strip()
        aliases = [a for a in item.get("aliases", []) if a]
        if name and aliases:
            materials.append(MaterialConfig(name=name, aliases=aliases))
    return materials


def _load_recipes(
    scope: Optional[str] = None, profile_name: Optional[str] = None
) -> list[Recipe]:
    scope = _validate_scope(scope)
    profiles = _load_profiles(scope)
    active_name = profile_name if profile_name is not None else profiles.get("active")
    active = _get_profile(profiles, active_name)
    base = _default_config_payload(scope)["recipes"]
    raw = active.get("recipes") if active and "recipes" in active else base
    recipes: list[Recipe] = []
    for item in raw:
        name = item.get("name", "").strip()
        materials = item.get("materials", {})
        if name and materials:
            recipes.append(Recipe(name=name, materials=materials))
    return recipes


def _load_prices(
    scope: Optional[str] = None, profile_name: Optional[str] = None
) -> dict[str, dict[str, float]]:
    scope = _validate_scope(scope)
    profiles = _load_profiles(scope)
    active_name = profile_name if profile_name is not None else profiles.get("active")
    active = _get_profile(profiles, active_name)
    raw_list: List[Dict[str, Any]]
    if active and "prices" in active:
        raw_list = active.get("prices") or []
    else:
        raw_list = _default_config_payload(scope)["prices"]
    prices: dict[str, dict[str, float]] = {}
    for item in raw_list:
        name = item.get("name", "").strip()
        if not name:
            continue
        prices[_normalize_name(name)] = {
            "no_delivery_no_vat": float(item.get("no_delivery_no_vat", 0) or 0),
            "no_delivery_vat_22": float(item.get("no_delivery_vat_22", 0) or 0),
            "pickup_no_vat": float(item.get("pickup_no_vat", 0) or 0),
            "pickup_vat_22": float(item.get("pickup_vat_22", 0) or 0),
        }
    return prices


def _build_output_dataframe(
    recipes: list[Recipe], balances: dict[str, float]
) -> pd.DataFrame:
    all_materials = []
    for recipe in recipes:
        for material in recipe.materials:
            if material not in all_materials:
                all_materials.append(material)

    rows = []
    for recipe in recipes:
        max_m3, required = calculate_max_cubic_meters(recipe, balances)
        row = {"Наименование": recipe.name, "Максимум, м3": max_m3}
        for material in all_materials:
            value = required.get(material, Decimal("0"))
            row[f"Нужно, кг {material}"] = value
        rows.append(row)

    return pd.DataFrame(rows)


def _build_prices_dataframe(
    recipes: list[Recipe], balances: dict[str, float], prices: dict[str, dict[str, float]]
) -> pd.DataFrame:
    rows = []
    for recipe in recipes:
        max_m3, _ = calculate_max_cubic_meters(recipe, balances)
        price = prices.get(_normalize_name(recipe.name), {})
        price_no_delivery_no_vat = price.get("no_delivery_no_vat", 0.0)
        price_no_delivery_vat = price.get("no_delivery_vat_22", 0.0)
        price_pickup_no_vat = price.get("pickup_no_vat", 0.0)
        price_pickup_vat = price.get("pickup_vat_22", 0.0)

        row = {
            "Наименование": recipe.name,
            "Стоимость без доставки без НДС": _money(max_m3, price_no_delivery_no_vat),
            "Стоимость без доставки с НДС 22%": _money(max_m3, price_no_delivery_vat),
            "Стоимость самовывоз без НДС": _money(max_m3, price_pickup_no_vat),
            "Стоимость самовывоз с НДС 22%": _money(max_m3, price_pickup_vat),
            " ": "",
            "Округл. БЕЗ ДОСТАВКИ БЕЗ НДС": price_no_delivery_no_vat,
            "БЕЗ ДОСТАВКИ С НДС 22%": price_no_delivery_vat,
            "САМОВЫВОЗ БЕЗ НДС": price_pickup_no_vat,
            "ОКРУГЛ. САМОВЫВОЗ С НДС 22%": price_pickup_vat,
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    # добавляем по 1 пустой колонке между группами цен и отступ в 1 колонку от левого края
    BLANK_LEFT = "__blank_left__"
    BLANK_COST_1 = "__blank_cost_1__"
    BLANK_COST_2 = "__blank_cost_2__"
    BLANK_ROUND_1 = "__blank_round_1__"
    BLANK_ROUND_2 = "__blank_round_2__"

    df[BLANK_LEFT] = ""
    df[BLANK_COST_1] = ""
    df[BLANK_COST_2] = ""
    df[BLANK_ROUND_1] = ""
    df[BLANK_ROUND_2] = ""

    name_col = "Наименование"
    c1 = "Стоимость без доставки без НДС"
    c2 = "Стоимость без доставки с НДС 22%"
    c3 = "Стоимость самовывоз без НДС"
    c4 = "Стоимость самовывоз с НДС 22%"
    spacer = " "
    r1 = "Округл. БЕЗ ДОСТАВКИ БЕЗ НДС"
    r2 = "БЕЗ ДОСТАВКИ С НДС 22%"
    r3 = "САМОВЫВОЗ БЕЗ НДС"
    r4 = "ОКРУГЛ. САМОВЫВОЗ С НДС 22%"

    ordered = [
        name_col,
        BLANK_LEFT,
        c1,
        c2,
        BLANK_COST_1,
        c3,
        c4,
        spacer,
        r1,
        r2,
        BLANK_ROUND_1,
        BLANK_ROUND_2,
        r3,
        r4,
    ]

    df = df[ordered]
    return df


def _build_summary(
    balances: dict[str, float],
    scope: Optional[str] = None,
    profile_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Построить краткую сводку по объемам и ценам для вывода на сайт."""
    recipes = _load_recipes(scope=scope, profile_name=profile_name)
    prices = _load_prices(scope=scope, profile_name=profile_name)

    output_df = _build_output_dataframe(recipes, balances)
    prices_df = _build_prices_dataframe(recipes, balances, prices)

    name_col = "Наименование"
    m_col = "Максимум, м3"
    c1 = "Стоимость без доставки без НДС"
    c2 = "Стоимость без доставки с НДС 22%"
    c3 = "Стоимость самовывоз без НДС"
    c4 = "Стоимость самовывоз с НДС 22%"
    r1 = "Округл. БЕЗ ДОСТАВКИ БЕЗ НДС"
    r2 = "БЕЗ ДОСТАВКИ С НДС 22%"
    r3 = "САМОВЫВОЗ БЕЗ НДС"
    r4 = "ОКРУГЛ. САМОВЫВОЗ С НДС 22%"

    merged = pd.merge(
        output_df[[name_col, m_col]],
        prices_df[[name_col, c1, c2, c3, c4, r1, r2, r3, r4]],
        on=name_col,
        how="left",
    )

    items: list[Dict[str, Any]] = []
    total_volume = Decimal("0")
    for _, row in merged.iterrows():
        name = str(row.get(name_col, "") or "")
        max_m3 = Decimal(str(row.get(m_col, 0) or 0))
        total_volume += max_m3

        def _val(col: str) -> Optional[float]:
            v = row.get(col, None)
            if pd.isna(v):
                return None
            try:
                return float(v)
            except Exception:
                return None

        items.append(
            {
                "name": name,
                "max_m3": float(max_m3),
                "amounts": {
                    "no_delivery_no_vat": _val(c1),
                    "no_delivery_vat_22": _val(c2),
                    "pickup_no_vat": _val(c3),
                    "pickup_vat_22": _val(c4),
                },
                "unit_prices": {
                    "no_delivery_no_vat": _val(r1),
                    "no_delivery_vat_22": _val(r2),
                    "pickup_no_vat": _val(r3),
                    "pickup_vat_22": _val(r4),
                },
            }
        )

    return {
        "kind": "beton",
        "items": items,
        "total_volume": float(total_volume),
    }


def _build_jbi_summary(
    raw_balances: dict[str, float], profile_name: Optional[str] = None
) -> Dict[str, Any]:
    """Рассчитать максимум изделий ЖБИ с учетом доступного бетона."""
    jbi_recipes = _load_recipes(scope=CONFIG_SCOPE_JBI, profile_name=profile_name)
    jbi_prices = _load_prices(scope=CONFIG_SCOPE_JBI, profile_name=profile_name)

    beton_materials = _load_materials(scope=CONFIG_SCOPE_BETON)
    beton_recipes = _load_recipes(scope=CONFIG_SCOPE_BETON)
    beton_balances = {
        key: value for key, value in raw_balances.items() if key in {m.name for m in beton_materials}
    }

    concrete_limits: dict[str, float] = {}
    for recipe in beton_recipes:
        max_m3, _ = calculate_max_cubic_meters(recipe, beton_balances)
        concrete_limits[recipe.name] = float(max_m3)

    effective_balances = dict(raw_balances)
    effective_balances.update(concrete_limits)

    items: list[Dict[str, Any]] = []
    for recipe in jbi_recipes:
        max_units_raw, _ = calculate_max_cubic_meters(recipe, effective_balances)
        max_units = int(max_units_raw.to_integral_value(rounding=ROUND_FLOOR))
        price = jbi_prices.get(_normalize_name(recipe.name), {})
        unit_price = float(price.get("no_delivery_no_vat", 0.0) or 0.0)
        items.append(
            {
                "name": recipe.name,
                "max_units": max_units,
                "unit_price": unit_price,
                "total_price": float(Decimal(str(unit_price)) * Decimal(str(max_units))),
            }
        )

    return {
        "kind": "jbi",
        "items": items,
    }


def _build_excel(
    balances: dict[str, float],
    scope: Optional[str] = None,
    profile_name: Optional[str] = None,
) -> bytes:
    recipes = _load_recipes(scope=scope, profile_name=profile_name)
    prices = _load_prices(scope=scope, profile_name=profile_name)

    output_df = _build_output_dataframe(recipes, balances)
    prices_df = _build_prices_dataframe(recipes, balances, prices)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        output_df.to_excel(writer, index=False, startrow=0, sheet_name="Итог")
        start_row = len(output_df.index) + 5
        prices_df.to_excel(writer, index=False, startrow=start_row, sheet_name="Итог")

        ws = writer.book["Итог"]
        from openpyxl.styles import Alignment, Border, Font, PatternFill, Side

        header_font = Font(bold=True)
        header_align = Alignment(horizontal="justify", vertical="center", wrap_text=True)
        body_align = Alignment(horizontal="justify", vertical="center")
        thin = Side(style="thin")
        table_border = Border(left=thin, right=thin, top=thin, bottom=thin)
        spacer_border = Border(left=thin, right=thin)
        palette = [
            "EAF2F8",
            "E8F6F3",
            "FEF9E7",
            "FDEDEC",
            "F4ECF7",
            "FDF2E9",
            "EBF5FB",
            "EAECEE",
        ]
        palette_bright = [
            "BBDFF7",
            "BFEED4",
            "F9E79F",
            "F5B7B1",
            "D7BDE2",
            "F8CFA8",
            "BBDFF7",
            "CCD1D1",
        ]
        highlight_b = PatternFill(start_color="D6EAF8", end_color="D6EAF8", fill_type="solid")
        highlight_b_bright = PatternFill(
            start_color="A9D6F4", end_color="A9D6F4", fill_type="solid"
        )

        highlight_names = {
            _normalize_name(name)
            for name in [
                "БСТ ВЗ0 F150 W6",
                "БСТ В20 F150 W6",
                "БСТ В25 F150 W8",
                "БСТ B7,5 F50 W2",
                "БСТ В20",
                "БСТ В25 F150 W6",
            ]
        }

        output_header_row = 1
        output_start_row = output_header_row + 1
        output_end_row = output_start_row + len(output_df.index) - 1
        output_end_col = output_df.shape[1]

        prices_header_row = start_row + 1
        prices_start_row = prices_header_row + 1
        prices_end_row = prices_start_row + len(prices_df.index) - 1
        prices_end_col = prices_df.shape[1]

        for row_idx in range(output_header_row, output_end_row + 1):
            for cell in ws.iter_rows(
                min_row=row_idx, max_row=row_idx, min_col=1, max_col=output_end_col
            ):
                for c in cell:
                    is_highlight = False
                    if row_idx >= output_start_row:
                        data_idx = row_idx - output_start_row
                        if 0 <= data_idx < len(output_df.index):
                            name = output_df.iloc[data_idx, 0]
                            is_highlight = _normalize_name(str(name)) in highlight_names
                    if c.value is None or str(c.value).strip() == "":
                        c.border = spacer_border
                        c.fill = PatternFill(fill_type=None)
                    elif c.column == 2:
                        c.border = table_border
                        c.fill = highlight_b_bright if is_highlight else highlight_b
                    else:
                        c.border = table_border
                        fill_source = palette_bright if is_highlight else palette
                        fill_color = fill_source[(c.column - 1) % len(fill_source)]
                        c.fill = PatternFill(
                            start_color=fill_color,
                            end_color=fill_color,
                            fill_type="solid",
                        )
                    if row_idx == output_header_row:
                        c.font = header_font
                        c.alignment = header_align
                    else:
                        c.alignment = body_align
                        if isinstance(c.value, (int, float, Decimal)):
                            # Значение хранится полное, отображаем 2 знака после запятой.
                            c.number_format = "#,##0.00"

        blank_cols = set()
        for blank_name in [
            "__blank_left__",
            "__blank_cost_1__",
            "__blank_cost_2__",
            "__blank_round_1__",
            "__blank_round_2__",
            " ",
        ]:
            if blank_name in prices_df.columns:
                blank_cols.add(prices_df.columns.get_loc(blank_name) + 1)

        for row_idx in range(prices_header_row, prices_end_row + 1):
            for cell in ws.iter_rows(
                min_row=row_idx, max_row=row_idx, min_col=1, max_col=prices_end_col
            ):
                for c in cell:
                    is_highlight = False
                    if row_idx >= prices_start_row:
                        data_idx = row_idx - prices_start_row
                        if 0 <= data_idx < len(prices_df.index):
                            name = prices_df.iloc[data_idx, 0]
                            is_highlight = _normalize_name(str(name)) in highlight_names
                    if c.column in blank_cols:
                        c.border = Border()
                        c.fill = PatternFill(fill_type=None)
                    elif c.value is None or str(c.value).strip() == "":
                        c.border = spacer_border
                        c.fill = PatternFill(fill_type=None)
                    elif c.column == 2:
                        c.border = table_border
                        c.fill = highlight_b_bright if is_highlight else highlight_b
                    else:
                        c.border = table_border
                        fill_source = palette_bright if is_highlight else palette
                        fill_color = fill_source[(c.column - 1) % len(fill_source)]
                        c.fill = PatternFill(
                            start_color=fill_color,
                            end_color=fill_color,
                            fill_type="solid",
                        )
                    if row_idx == prices_header_row:
                        c.font = header_font
                        c.alignment = header_align
                    else:
                        c.alignment = body_align
                        if isinstance(c.value, (int, float, Decimal)):
                            c.number_format = "#,##0.00"

        # выделяем максимальные значения по каждой ценовой колонке жирным
        price_cols = [
            "Стоимость без доставки без НДС",
            "Стоимость без доставки с НДС 22%",
            "Стоимость самовывоз без НДС",
            "Стоимость самовывоз с НДС 22%",
        ]
        for col_name in price_cols:
            if col_name not in prices_df.columns:
                continue
            col_idx = prices_df.columns.get_loc(col_name) + 1  # 1-based in Excel
            try:
                series = prices_df[col_name]
                max_val = series.max()
            except Exception:
                continue
            if pd.isna(max_val):
                continue
            for row_offset, val in enumerate(series, start=0):
                if pd.isna(val):
                    continue
                if abs(Decimal(str(val)) - Decimal(str(max_val))) > Decimal("0.0000001"):
                    continue
                excel_row = prices_start_row + row_offset
                cell = ws.cell(row=excel_row, column=col_idx)
                cell.font = Font(bold=True)

        # очищаем заголовки у служебных пустых колонок
        for blank_name in [
            "__blank_left__",
            "__blank_cost_1__",
            "__blank_cost_2__",
            "__blank_round_1__",
            "__blank_round_2__",
        ]:
            if blank_name in prices_df.columns:
                b_col = prices_df.columns.get_loc(blank_name) + 1
                ws.cell(row=prices_header_row, column=b_col).value = ""

        # группирующие заголовки над блоками цен
        group_row = prices_header_row - 1
        # стоимости
        ws.merge_cells(start_row=group_row, start_column=3, end_row=group_row, end_column=4)
        g1 = ws.cell(row=group_row, column=3, value="для организации А")
        ws.merge_cells(start_row=group_row, start_column=6, end_row=group_row, end_column=7)
        g2 = ws.cell(row=group_row, column=6, value="для иных организаций")
        # округлённые цены
        ws.merge_cells(start_row=group_row, start_column=9, end_row=group_row, end_column=10)
        g3 = ws.cell(row=group_row, column=9, value="для организации А")
        ws.merge_cells(start_row=group_row, start_column=13, end_row=group_row, end_column=14)
        g4 = ws.cell(row=group_row, column=13, value="для иных организаций")

        title_font_size = (header_font.sz or 11) + 1
        for gcell in (g1, g2, g3, g4):
            gcell.font = Font(bold=True, size=title_font_size)
            gcell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

        for column_cells in ws.columns:
            max_len = 0
            col = column_cells[0].column_letter
            for cell in column_cells:
                if cell.value is None:
                    continue
                max_len = max(max_len, len(str(cell.value)))
            if max_len:
                auto_width = min(max_len + 2, 60)
                # Не сужаем числовые колонки, иначе Excel визуально округляет
                # значения (например, 472.621959... выглядит как 472.622).
                ws.column_dimensions[col].width = max(auto_width, 10)

    output.seek(0)
    return output.read()


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    html = """
    <!doctype html>
    <html lang="ru">
    <head>
        <meta charset="utf-8" />
        <title>Расчет бетона по остаткам</title>
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <style>
            * { box-sizing: border-box; }
            body {
                margin: 0;
                min-height: 100vh;
                font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
                background: linear-gradient(180deg, #eef6ff 0%, #f8fbff 55%, #ffffff 100%);
                color: #10233f;
                display: flex;
                justify-content: flex-start;
                align-items: flex-start;
            }
            .wrap {
                width: 100%;
                max-width: 1680px;
                padding: 24px 24px 32px;
            }
            .page-layout {
                display: grid;
                grid-template-columns: 380px minmax(0, 1fr);
                gap: 20px;
                align-items: start;
            }
            .left-rail {
                position: sticky;
                top: 18px;
                align-self: start;
            }
            .left-stack {
                display: flex;
                flex-direction: column;
                gap: 18px;
            }
            .rail-section-title {
                font-size: 20px;
                font-weight: 700;
                margin: 0 0 10px;
                color: #123c73;
            }
            .layout {
                display: flex;
                gap: 20px;
                align-items: flex-start;
                flex-wrap: wrap;
            }
            .stack-section {
                margin-top: 0;
            }
            .box {
                border-radius: 12px;
                border: 1px solid #cfe1f7;
                background: linear-gradient(180deg, #ffffff 0%, #f7fbff 100%);
                padding: 18px 18px 16px;
                position: relative;
                width: 100%;
                max-width: 380px;
                box-shadow: 0 14px 30px rgba(33, 93, 168, 0.08);
            }
            .result-box {
                flex: 1 1 0;
                min-width: 980px;
                border-radius: 12px;
                border: 1px solid #cfe1f7;
                background: linear-gradient(180deg, #ffffff 0%, #f7fbff 100%);
                padding: 14px 16px;
                font-size: 13px;
                box-shadow: 0 14px 30px rgba(33, 93, 168, 0.08);
                margin-top: 34px;
            }
            .result-box.has-result {
                margin-top: 0;
            }
            .result-title {
                font-size: 14px;
                font-weight: 600;
                margin-bottom: 6px;
            }
            .section-title {
                font-size: 22px;
                font-weight: 700;
                margin: 0 0 14px;
                color: #123c73;
            }
            .result-meta {
                font-size: 12px;
                color: #58708f;
                margin-bottom: 4px;
            }
            .result-ok {
                color: #1859a8;
            }
            .result-empty {
                color: #7f95b2;
            }
            .result-list {
                margin: 6px 0 0;
                padding-left: 16px;
            }
            .result-tables {
                margin-top: 14px;
                display: grid;
                grid-template-columns: repeat(2, minmax(0, 1fr));
                gap: 14px;
                align-items: start;
            }
            .result-table-card {
                border-radius: 20px;
                border: 1px solid #cfe1f7;
                background: linear-gradient(180deg, #ffffff 0%, #f4f9ff 100%);
                box-shadow: 0 16px 34px rgba(33, 93, 168, 0.09);
                padding: 12px;
                min-width: 0;
            }
            .result-table-wrap {
                margin-top: 10px;
                border-radius: 14px;
                overflow: hidden;
                border: 1px solid #d8e8fb;
                background: #ffffff;
            }
            .result-table {
                width: 100%;
                border-collapse: collapse;
                font-size: 12px;
                table-layout: fixed;
            }
            .result-table th,
            .result-table td {
                padding: 9px 10px;
                border-bottom: 1px solid #e7f0fb;
            }
            .result-table th {
                background: linear-gradient(180deg, #e8f3ff 0%, #dcebff 100%);
                text-align: left;
                font-weight: 600;
                white-space: normal;
                color: #29558d;
                font-size: 11px;
                text-transform: uppercase;
                letter-spacing: 0.03em;
            }
            .result-table td {
                background: #ffffff;
                vertical-align: top;
                word-break: break-word;
            }
            .result-table th:nth-child(1),
            .result-table td:nth-child(1) { width: 40%; }
            .result-table th:nth-child(2),
            .result-table td:nth-child(2) { width: 15%; }
            .result-table th:nth-child(3),
            .result-table td:nth-child(3) { width: 22.5%; }
            .result-table th:nth-child(4),
            .result-table td:nth-child(4) { width: 22.5%; }
            .result-table tr:nth-child(even) td {
                background: #f7fbff;
            }
            .result-table tr:last-child td {
                border-bottom: none;
            }
            .result-table tr.row-max td {
                background: #e7f1ff !important;
                font-weight: 600;
            }
            .result-section-title {
                margin-top: 0;
                margin-bottom: 6px;
                font-size: 14px;
                font-weight: 700;
                color: #123c73;
            }
            .result-section-subtitle {
                color: #6580a3;
                font-size: 12px;
                margin-bottom: 2px;
            }
            .result-badge {
                display: inline-flex;
                align-items: center;
                margin-left: 8px;
                padding: 2px 8px;
                border-radius: 999px;
                background: #dbeafe;
                color: #1d4ed8;
                font-size: 11px;
                font-weight: 700;
                white-space: nowrap;
            }
            .result-name {
                font-weight: 600;
                color: #10233f;
            }
            .result-num {
                text-align: center;
                vertical-align: middle;
                white-space: nowrap;
                font-variant-numeric: tabular-nums;
            }
            .future-box {
                margin-top: 20px;
                border-radius: 18px;
                border: 1px solid #e5e7eb;
                background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
                padding: 18px 20px;
                box-shadow: 0 10px 24px rgba(15, 23, 42, 0.05);
            }
            .future-title {
                font-size: 16px;
                font-weight: 700;
                margin-bottom: 6px;
                color: #0f172a;
            }
            .future-text {
                font-size: 13px;
                color: #64748b;
                max-width: 720px;
            }
            @media (max-width: 980px) {
                .page-layout {
                    grid-template-columns: 1fr;
                }
                .left-rail {
                    position: static;
                }
                .result-box {
                    min-width: 0;
                    margin-top: 0;
                }
                .result-tables {
                    grid-template-columns: 1fr;
                }
            }
            h1 {
                font-size: 18px;
                margin: 0 0 12px;
                font-weight: 600;
                color: #123c73;
            }
            .field {
                margin-bottom: 12px;
            }
            label {
                display: block;
                font-size: 12px;
                margin-bottom: 6px;
                color: #5a7696;
            }
            input[type="file"] {
                width: 100%;
                border-radius: 8px;
                border: 1px solid #c7dcf5;
                padding: 8px 10px;
                font-size: 14px;
                background: #ffffff;
            }
            input[type="file"]::file-selector-button {
                border: 1px solid #bfd9f8;
                border-radius: 6px;
                padding: 6px 10px;
                margin-right: 8px;
                background: #eaf4ff;
                color: #1d4ed8;
                font-size: 12px;
                cursor: pointer;
            }
            .btn {
                margin-top: 4px;
                width: 100%;
                border-radius: 8px;
                border: 1px solid #bfd9f8;
                padding: 8px 10px;
                font-weight: 500;
                font-size: 13px;
                color: #184a8b;
                background: #eaf4ff;
                cursor: pointer;
            }
            .btn-row {
                display: flex;
                gap: 8px;
                margin-top: 4px;
            }
            .btn-row .btn {
                margin-top: 0;
            }
            .hp-field {
                position: absolute;
                left: -9999px;
                opacity: 0;
                pointer-events: none;
            }
            .field select {
                width: 100%;
                border-radius: 8px;
                border: 1px solid #c7dcf5;
                padding: 8px 10px;
                font-size: 14px;
                background: #ffffff;
                color: #10233f;
            }
            .cfg-btn {
                position: absolute;
                top: 12px;
                right: 12px;
                width: 44px;
                height: 44px;
                border-radius: 12px;
                border: 1px solid #c7dcf5;
                background: #eff7ff;
                cursor: pointer;
                display: flex;
                align-items: center;
                justify-content: center;
                color: #1d4ed8;
                font-size: 22px;
                line-height: 1;
                z-index: 10;
            }
            .cfg-btn:hover {
                color: #123c73;
                background: #e3f0ff;
            }
            .cfg-panel {
                position: fixed;
                inset: 0;
                background: rgba(18, 60, 115, 0.32);
                display: none;
                align-items: center;
                justify-content: center;
                z-index: 20;
            }
            .cfg-panel-inner {
                width: 100%;
                max-width: 720px;
                max-height: 90vh;
                background: linear-gradient(180deg, #ffffff 0%, #f7fbff 100%);
                border-radius: 16px;
                padding: 16px 18px;
                box-shadow: 0 20px 40px rgba(33, 93, 168, 0.18);
                display: flex;
                flex-direction: column;
                gap: 12px;
                border: 1px solid #d6e6fb;
            }
            .cfg-header {
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 8px;
            }
            .cfg-title {
                font-size: 15px;
                font-weight: 600;
            }
            .cfg-close {
                border: none;
                background: transparent;
                cursor: pointer;
                font-size: 18px;
                line-height: 1;
                color: #6b7280;
            }
            .cfg-body {
                font-size: 12px;
                display: flex;
                flex-direction: column;
                gap: 8px;
            }
            .cfg-tabs {
                display: flex;
                gap: 2px;
                border-bottom: 1px solid #e5e7eb;
            }
            .cfg-tab {
                padding: 6px 12px;
                border: none;
                background: transparent;
                cursor: pointer;
                font-size: 12px;
                color: #6b7fa0;
                border-bottom: 2px solid transparent;
                margin-bottom: -1px;
            }
            .cfg-tab:hover { color: #123c73; }
            .cfg-tab.active {
                color: #1d4ed8;
                border-bottom-color: #2563eb;
                font-weight: 500;
            }
            .cfg-pane { display: none; overflow: auto; max-height: 50vh; }
            .cfg-pane.active { display: block; }
            .cfg-section label {
                display: block;
                margin-bottom: 4px;
                font-size: 11px;
                color: #5a7696;
            }
            .cfg-table-wrap { overflow-x: auto; }
            .cfg-table {
                width: 100%;
                border-collapse: collapse;
                font-size: 12px;
            }
            .cfg-table th, .cfg-table td {
                border: 1px solid #dbe8f7;
                padding: 4px 6px;
                text-align: left;
            }
            .cfg-table th { background: #eef6ff; font-weight: 500; color: #29558d; }
            .cfg-table input, .cfg-table select {
                width: 100%;
                border: 1px solid #c7dcf5;
                border-radius: 4px;
                padding: 4px 6px;
                font-size: 12px;
                color: #10233f;
                background: #ffffff;
            }
            .cfg-table .col-del { width: 28px; text-align: center; }
            .cfg-recipe-block {
                border: 1px solid #d6e6fb;
                border-radius: 8px;
                padding: 8px;
                margin-bottom: 8px;
                background: linear-gradient(180deg, #ffffff 0%, #f4f9ff 100%);
            }
            .cfg-recipe-block h4 { margin: 0 0 6px; font-size: 12px; }
            .cfg-recipe-name { margin-bottom: 6px; }
            .cfg-recipe-name input { width: 100%; max-width: 280px; padding: 4px 6px; font-size: 12px; border-radius: 4px; border: 1px solid #c7dcf5; color: #10233f; background: #ffffff; }
            .cfg-add-row { margin-top: 6px; }
            .cfg-btn-sm {
                border: none;
                background: transparent;
                cursor: pointer;
                padding: 2px 6px;
                font-size: 11px;
                color: #6480a1;
            }
            .cfg-btn-sm:hover { color: #dc2626; }
            .cfg-footer {
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 8px;
                margin-top: 6px;
            }
            .cfg-footer-left,
            .cfg-footer-right {
                display: flex;
                align-items: center;
                gap: 6px;
                flex-wrap: wrap;
            }
            .cfg-input {
                border-radius: 6px;
                border: 1px solid #c7dcf5;
                padding: 4px 6px;
                font-size: 12px;
                color: #10233f;
                background: #ffffff;
            }
            .cfg-select {
                border-radius: 6px;
                border: 1px solid #c7dcf5;
                padding: 4px 6px;
                font-size: 12px;
                color: #10233f;
                background: #ffffff;
            }
            .cfg-btn-prim,
            .cfg-btn-sec,
            .cfg-btn-danger {
                border-radius: 6px;
                border: none;
                padding: 5px 8px;
                font-size: 11px;
                cursor: pointer;
            }
            .cfg-btn-prim {
                background: #eaf4ff;
                color: #184a8b;
                border: 1px solid #bfd9f8;
            }
            .cfg-btn-sec {
                background: #f3f8fe;
                color: #123c73;
                border: 1px solid #d7e6f8;
            }
            .cfg-btn-danger {
                background: #fff1f2;
                color: #be123c;
                border: 1px solid #fecdd3;
            }
            .cfg-hint {
                font-size: 11px;
                color: #5a7696;
            }
        </style>
    </head>
    <body>
        <div class="wrap">
            <div class="page-layout">
                <div class="left-rail">
                    <div class="left-stack">
                        <div class="stack-section">
                            <div class="rail-section-title">Расчет бетона по остаткам</div>
                            <div class="box">
                                <button type="button" class="cfg-btn" id="cfgBtn" title="Настройки">
                                    ⚙
                                </button>
                                <h1>Загрузка файла</h1>
                                <form id="calcForm" method="post" action="/upload" enctype="multipart/form-data">
                                    <div class="field">
                                        <label for="mainProfileSelect">Считать по настройкам</label>
                                        <select id="mainProfileSelect" class="main-profile-select">
                                            <option value="__base__">По умолчанию</option>
                                        </select>
                                    </div>
                                    <div class="field">
                                        <label for="file">Файл .xlsx</label>
                                        <input id="file" name="file" type="file" accept=".xlsx" required />
                                    </div>
                                    <div class="hp-field">
                                        <label>Ваш сайт</label>
                                        <input type="text" name="website" autocomplete="off" />
                                    </div>
                                    <div class="btn-row">
                                        <button class="btn" type="button" id="calcOnlyBtn">Посчитать</button>
                                        <button class="btn" type="button" id="downloadBtn">Скачать</button>
                                    </div>
                                </form>
                            </div>
                        </div>
                        <div class="stack-section">
                            <div class="rail-section-title">Расчет ЖБИ</div>
                            <div class="box">
                                <button type="button" class="cfg-btn" id="jbiCfgBtn" title="Настройки ЖБИ">
                                    ⚙
                                </button>
                                <h1>Загрузка файла</h1>
                                <form id="jbiForm" action="#" enctype="multipart/form-data">
                                    <div class="field">
                                        <label for="jbiProfileSelect">Считать по настройкам</label>
                                        <select id="jbiProfileSelect" class="main-profile-select">
                                            <option value="__base__">По умолчанию</option>
                                        </select>
                                    </div>
                                    <div class="field">
                                        <label for="jbiFile">Файл .xlsx</label>
                                        <input id="jbiFile" name="file" type="file" accept=".xlsx" />
                                    </div>
                                    <div class="hp-field">
                                        <label>Ваш сайт</label>
                                        <input type="text" name="website" autocomplete="off" />
                                    </div>
                                    <div class="btn-row">
                                        <button class="btn" type="button" id="jbiCalcOnlyBtn">Посчитать</button>
                                        <button class="btn" type="button" id="jbiDownloadBtn">Скачать</button>
                                    </div>
                                </form>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="result-box result-empty" id="resultBox">
                    <div class="result-title">Результат расчета</div>
                    <div>Справа показывается результат того раздела, который вы посчитали последним: бетон или ЖБИ.</div>
                </div>
            </div>
        </div>
        <div class="cfg-panel" id="cfgPanel">
            <div class="cfg-panel-inner">
                <div class="cfg-header">
                    <div>
                        <div class="cfg-title" id="cfgTitle">Настройки конфигурации</div>
                        <div class="cfg-hint">Редактирование работает только для веб-сервиса, бот использует базовую конфигурацию.</div>
                    </div>
                    <button type="button" class="cfg-close" id="cfgClose" aria-label="Закрыть">×</button>
                </div>
                <div class="cfg-footer">
                    <div class="cfg-footer-left">
                        <select id="cfgProfileSelect" class="cfg-select">
                            <option value="__base__">Базовая конфигурация</option>
                        </select>
                        <button type="button" class="cfg-btn-sec" id="cfgProfileApply">Выбрать</button>
                        <button type="button" class="cfg-btn-danger" id="cfgProfileDelete">Удалить</button>
                    </div>
                    <div class="cfg-footer-right">
                        <input id="cfgProfileName" class="cfg-input" placeholder="Имя профиля" />
                        <button type="button" class="cfg-btn-prim" id="cfgProfileSave">Сохранить профиль</button>
                    </div>
                </div>
                <div class="cfg-body">
                    <div class="cfg-tabs">
                        <button type="button" class="cfg-tab active" data-tab="materials">Материалы</button>
                        <button type="button" class="cfg-tab" data-tab="recipes">Составы</button>
                        <button type="button" class="cfg-tab" data-tab="prices">Цены</button>
                    </div>
                    <div id="cfgPaneMaterials" class="cfg-pane active">
                        <div class="cfg-section">
                            <label>Наименование и варианты написания</label>
                            <div class="cfg-table-wrap">
                                <table class="cfg-table" id="cfgMaterialsTable">
                                    <thead><tr><th>Наименование</th><th>Варианты написания (через запятую)</th><th class="col-del"></th></tr></thead>
                                    <tbody id="cfgMaterialsBody"></tbody>
                                </table>
                            </div>
                            <button type="button" class="cfg-btn-sec cfg-add-row" id="cfgMaterialsAdd">+ Добавить материал</button>
                        </div>
                    </div>
                    <div id="cfgPaneRecipes" class="cfg-pane">
                        <div class="cfg-section">
                            <label>Позиции и их составляющие (кг на 1 м³)</label>
                            <div id="cfgRecipesList"></div>
                            <button type="button" class="cfg-btn-sec cfg-add-row" id="cfgRecipesAdd">+ Добавить состав</button>
                        </div>
                    </div>
                    <div id="cfgPanePrices" class="cfg-pane">
                        <div class="cfg-section">
                            <label>Цены по наименованиям</label>
                            <div class="cfg-table-wrap">
                                <table class="cfg-table" id="cfgPricesTable">
                                    <thead><tr><th>Наименование</th><th>Без доставки без НДС</th><th>Без доставки с НДС 22%</th><th>Самовывоз без НДС</th><th>Самовывоз с НДС 22%</th><th class="col-del"></th></tr></thead>
                                    <tbody id="cfgPricesBody"></tbody>
                                </table>
                            </div>
                            <button type="button" class="cfg-btn-sec cfg-add-row" id="cfgPricesAdd">+ Добавить цену</button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        <script>
        document.addEventListener('DOMContentLoaded', function() {
            var cfgBtn = document.getElementById('cfgBtn');
            var jbiCfgBtn = document.getElementById('jbiCfgBtn');
            var cfgPanel = document.getElementById('cfgPanel');

            var cfgClose = document.getElementById('cfgClose');
            var cfgTitle = document.getElementById('cfgTitle');
            var sel = document.getElementById('cfgProfileSelect');
            var saveBtn = document.getElementById('cfgProfileSave');
            var applyBtn = document.getElementById('cfgProfileApply');
            var delBtn = document.getElementById('cfgProfileDelete');
            var nameInput = document.getElementById('cfgProfileName');
            var mainProfileSelect = document.getElementById('mainProfileSelect');
            var cfgMaterialsBody = document.getElementById('cfgMaterialsBody');
            var cfgRecipesList = document.getElementById('cfgRecipesList');
            var cfgPricesBody = document.getElementById('cfgPricesBody');
            var calcForm = document.getElementById('calcForm');
            var resultBox = document.getElementById('resultBox');
            var calcOnlyBtn = document.getElementById('calcOnlyBtn');
            var downloadBtn = document.getElementById('downloadBtn');
            var jbiForm = document.getElementById('jbiForm');
            var jbiProfileSelect = document.getElementById('jbiProfileSelect');
            var jbiCalcOnlyBtn = document.getElementById('jbiCalcOnlyBtn');
            var jbiDownloadBtn = document.getElementById('jbiDownloadBtn');

            if (!cfgBtn || !cfgPanel) {
                return;
            }

            var currentMaterialNames = [];
            var currentExternalMaterialNames = [];
            var currentConfigScope = 'beton';
            var currentPanelPassword = null;

            function setActiveTab(tabKey) {
                var tabs = document.querySelectorAll('.cfg-tab');
                var panes = document.querySelectorAll('.cfg-pane');
                for (var i = 0; i < tabs.length; i++) tabs[i].classList.remove('active');
                for (var j = 0; j < panes.length; j++) panes[j].classList.remove('active');
                for (var k = 0; k < tabs.length; k++) {
                    if (tabs[k].getAttribute('data-tab') === tabKey) tabs[k].classList.add('active');
                }
                var paneId = 'cfgPane' + (tabKey ? tabKey.charAt(0).toUpperCase() + tabKey.slice(1) : '');
                var pane = document.getElementById(paneId);
                if (pane) pane.classList.add('active');
            }

            var tabButtons = document.querySelectorAll('.cfg-tab');
            for (var t = 0; t < tabButtons.length; t++) {
                tabButtons[t].addEventListener('click', function() {
                    setActiveTab(this.getAttribute('data-tab'));
                });
            }

            function getScopeTitle(scope) {
                return scope === 'jbi' ? 'Настройки конфигурации ЖБИ' : 'Настройки конфигурации бетона';
            }
            function getProfileLabel(scope) {
                return scope === 'jbi' ? 'Изделия и их составляющие (кг на 1 м³)' : 'Виды бетона и их составляющие (кг на 1 м³)';
            }
            function askConfigPassword() {
                var entered = window.prompt('Введите пароль конфигуратора');
                if (!entered) return null;
                return String(entered);
            }
            function configFetch(url, options, passwordOverride) {
                options = options || {};
                var password = passwordOverride != null ? passwordOverride : currentPanelPassword;
                if (!password) {
                    password = askConfigPassword();
                }
                if (!password) {
                    return Promise.reject(new Error('Пароль не введен'));
                }
                options.headers = options.headers || {};
                options.headers['X-Config-Password'] = password;
                return fetch(url, options).then(function(res) {
                    if (res.status === 401) {
                        currentPanelPassword = null;
                    }
                    return res;
                });
            }
            function openPanel(scope) {
                currentConfigScope = scope === 'jbi' ? 'jbi' : 'beton';
                if (cfgTitle) cfgTitle.textContent = getScopeTitle(currentConfigScope);
                var password = askConfigPassword();
                if (!password) return;
                currentPanelPassword = password;
                cfgPanel.style.display = 'flex';
                loadConfig(password);
            }
            function closePanel() {
                currentPanelPassword = null;
                cfgPanel.style.display = 'none';
            }

            function fillProfileSelect(selectEl, profiles, active) {
                if (!selectEl) return;
                selectEl.innerHTML = '<option value="__base__">По умолчанию</option>';
                for (var i = 0; i < profiles.length; i++) {
                    var opt = document.createElement('option');
                    opt.value = profiles[i].name;
                    opt.textContent = profiles[i].name;
                    selectEl.appendChild(opt);
                }
                selectEl.value = active || '__base__';
            }
            function loadProfileOptions(scope, selectEl) {
                if (!selectEl || !window.fetch) return;
                return fetch('/api/config/options?scope=' + encodeURIComponent(scope))
                    .then(function(res) {
                        if (!res.ok) return null;
                        return res.json();
                    })
                    .then(function(data) {
                        if (!data) return;
                        fillProfileSelect(selectEl, data.profiles || [], data.active_profile || '__base__');
                    })
                    .catch(function(e) {
                        console.error(e);
                    });
            }
            function loadMainProfileSelects() {
                loadProfileOptions('beton', mainProfileSelect);
                loadProfileOptions('jbi', jbiProfileSelect);
            }

            loadMainProfileSelects();

            cfgBtn.addEventListener('click', function() { openPanel('beton'); });
            if (jbiCfgBtn) jbiCfgBtn.addEventListener('click', function() { openPanel('jbi'); });

            if (cfgClose) cfgClose.addEventListener('click', closePanel);
            cfgPanel.addEventListener('click', function(e) {
                if (e.target === cfgPanel) closePanel();
            });

            function escapeAttr(s) {
                return String(s || '').replace(/"/g, '&quot;');
            }
            function escapeHtml(s) {
                return String(s || '').replace(/</g, '&lt;');
            }
            function formatNumber(value, digits) {
                if (value == null || isNaN(value)) return '—';
                var fixed = Number(value).toFixed(digits);
                var parts = fixed.split('.');
                parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ' ');
                return parts.join('.');
            }
            function setScopeTexts() {
                var lbl = document.querySelector('#cfgPaneRecipes .cfg-section label');
                var btn = document.getElementById('cfgRecipesAdd');
                if (lbl) lbl.textContent = getProfileLabel(currentConfigScope);
                if (btn) btn.textContent = currentConfigScope === 'jbi' ? '+ Добавить состав ЖБИ' : '+ Добавить состав';
            }
            function getAvailableMaterialNames() {
                var all = [];
                var seen = {};
                for (var i = 0; i < currentMaterialNames.length; i++) {
                    if (!seen[currentMaterialNames[i]]) {
                        seen[currentMaterialNames[i]] = true;
                        all.push(currentMaterialNames[i]);
                    }
                }
                for (var j = 0; j < currentExternalMaterialNames.length; j++) {
                    if (!seen[currentExternalMaterialNames[j]]) {
                        seen[currentExternalMaterialNames[j]] = true;
                        all.push(currentExternalMaterialNames[j]);
                    }
                }
                return all;
            }

            function renderMaterials(arr) {
                arr = arr || [];
                currentMaterialNames = [];
                for (var i = 0; i < arr.length; i++) currentMaterialNames.push(arr[i].name || '');
                if (!cfgMaterialsBody) return;
                cfgMaterialsBody.innerHTML = '';
                for (var j = 0; j < arr.length; j++) {
                    var m = arr[j] || {};
                    var tr = document.createElement('tr');
                    var aliases = Array.isArray(m.aliases) ? m.aliases.join(', ') : '';
                    tr.innerHTML =
                        '<td><input type="text" class="mat-name" value="' +
                        escapeAttr(m.name || '') +
                        '" /></td><td><input type="text" class="mat-aliases" value="' +
                        escapeAttr(aliases) +
                        '" placeholder="через запятую" /></td><td class="col-del"><button type="button" class="cfg-btn-sm cfg-del-mat" title="Удалить">✕</button></td>';
                    cfgMaterialsBody.appendChild(tr);
                }
            }

            function renderRecipes(arr, materialNames) {
                arr = arr || [];
                var names = materialNames || getAvailableMaterialNames();
                if (!cfgRecipesList) return;
                cfgRecipesList.innerHTML = '';
                for (var i = 0; i < arr.length; i++) {
                    var rec = arr[i] || {};
                    var block = document.createElement('div');
                    block.className = 'cfg-recipe-block';
                    block.setAttribute('data-idx', String(i));
                    var matsHtml = '';
                    var mats = rec.materials && typeof rec.materials === 'object' ? rec.materials : {};
                    for (var matName in mats) {
                        if (!Object.prototype.hasOwnProperty.call(mats, matName)) continue;
                        var kg = mats[matName];
                        var opts = '';
                        for (var n = 0; n < names.length; n++) {
                            opts +=
                                '<option value="' +
                                escapeAttr(names[n]) +
                                '"' +
                                (names[n] === matName ? ' selected' : '') +
                                '>' +
                                escapeHtml(names[n]) +
                                '</option>';
                        }
                        if (names.indexOf(matName) < 0) {
                            opts =
                                '<option value="' +
                                escapeAttr(matName) +
                                '" selected>' +
                                escapeHtml(matName) +
                                '</option>' +
                                opts;
                        }
                        matsHtml +=
                            '<tr><td><select class="rec-mat-name">' +
                            opts +
                            '</select></td><td><input type="number" step="any" class="rec-mat-kg" value="' +
                            Number(kg) +
                            '" /></td><td class="col-del"><button type="button" class="cfg-btn-sm cfg-del-rec-row" title="Удалить">✕</button></td></tr>';
                    }
                    block.innerHTML =
                        '<h4>Состав</h4><div class="cfg-recipe-name"><input type="text" class="rec-name" value="' +
                        escapeAttr(rec.name || '') +
                        '" placeholder="Наименование позиции" /></div><div class="cfg-table-wrap"><table class="cfg-table"><thead><tr><th>Материал</th><th>кг</th><th class="col-del"></th></tr></thead><tbody>' +
                        matsHtml +
                        '</tbody></table></div><button type="button" class="cfg-btn-sm cfg-add-rec-row">+ Строка</button> <button type="button" class="cfg-btn-sm cfg-del-recipe" title="Удалить состав">Удалить состав</button>';
                    cfgRecipesList.appendChild(block);
                }
            }

            function renderPrices(arr) {
                arr = arr || [];
                if (!cfgPricesBody) return;
                cfgPricesBody.innerHTML = '';
                for (var i = 0; i < arr.length; i++) {
                    var p = arr[i] || {};
                    var tr = document.createElement('tr');
                    tr.innerHTML =
                        '<td><input type="text" class="price-name" value="' +
                        escapeAttr(p.name || '') +
                        '" /></td><td><input type="number" step="any" class="price-nd-nv" value="' +
                        (p.no_delivery_no_vat != null ? p.no_delivery_no_vat : '') +
                        '" /></td><td><input type="number" step="any" class="price-nd-v" value="' +
                        (p.no_delivery_vat_22 != null ? p.no_delivery_vat_22 : '') +
                        '" /></td><td><input type="number" step="any" class="price-pick-nv" value="' +
                        (p.pickup_no_vat != null ? p.pickup_no_vat : '') +
                        '" /></td><td><input type="number" step="any" class="price-pick-v" value="' +
                        (p.pickup_vat_22 != null ? p.pickup_vat_22 : '') +
                        '" /></td><td class="col-del"><button type="button" class="cfg-btn-sm cfg-del-price" title="Удалить">✕</button></td>';
                    cfgPricesBody.appendChild(tr);
                }
            }

            function getMaterialsFromUI() {
                var out = [];
                if (!cfgMaterialsBody) return out;
                var rows = cfgMaterialsBody.querySelectorAll('tr');
                for (var i = 0; i < rows.length; i++) {
                    var nameEl = rows[i].querySelector('.mat-name');
                    var name = nameEl ? String(nameEl.value || '').trim() : '';
                    if (!name) continue;
                    var aliasesEl = rows[i].querySelector('.mat-aliases');
                    var aliasesStr = aliasesEl ? String(aliasesEl.value || '').trim() : '';
                    var aliases = [];
                    if (aliasesStr) {
                        var parts = aliasesStr.split(',');
                        for (var j = 0; j < parts.length; j++) {
                            var s = String(parts[j] || '').trim();
                            if (s) aliases.push(s);
                        }
                    }
                    out.push({ name: name, aliases: aliases });
                }
                return out;
            }

            function getRecipesFromUI() {
                var out = [];
                if (!cfgRecipesList) return out;
                var blocks = cfgRecipesList.querySelectorAll('.cfg-recipe-block');
                for (var i = 0; i < blocks.length; i++) {
                    var nmEl = blocks[i].querySelector('.rec-name');
                    var name = nmEl ? String(nmEl.value || '').trim() : '';
                    if (!name) continue;
                    var materials = {};
                    var rows = blocks[i].querySelectorAll('tbody tr');
                    for (var r = 0; r < rows.length; r++) {
                        var matEl = rows[r].querySelector('.rec-mat-name');
                        var kgEl = rows[r].querySelector('.rec-mat-kg');
                        var matName = matEl ? String(matEl.value || '').trim() : '';
                        var kg = kgEl ? parseFloat(kgEl.value) : NaN;
                        if (matName && !isNaN(kg)) materials[matName] = kg;
                    }
                    out.push({ name: name, materials: materials });
                }
                return out;
            }

            function getPricesFromUI() {
                var out = [];
                if (!cfgPricesBody) return out;
                var rows = cfgPricesBody.querySelectorAll('tr');
                for (var i = 0; i < rows.length; i++) {
                    var nmEl = rows[i].querySelector('.price-name');
                    var name = nmEl ? String(nmEl.value || '').trim() : '';
                    if (!name) continue;
                    function readNum(cls) {
                        var el = rows[i].querySelector(cls);
                        var v = el ? String(el.value || '').trim() : '';
                        return v === '' ? null : parseFloat(v);
                    }
                    var row = { name: name };
                    var v1 = readNum('.price-nd-nv');
                    var v2 = readNum('.price-nd-v');
                    var v3 = readNum('.price-pick-nv');
                    var v4 = readNum('.price-pick-v');
                    if (v1 != null && !isNaN(v1)) row.no_delivery_no_vat = v1;
                    if (v2 != null && !isNaN(v2)) row.no_delivery_vat_22 = v2;
                    if (v3 != null && !isNaN(v3)) row.pickup_no_vat = v3;
                    if (v4 != null && !isNaN(v4)) row.pickup_vat_22 = v4;
                    out.push(row);
                }
                return out;
            }

            function loadConfig(password) {
                if (!window.fetch) return;
                setScopeTexts();
                return configFetch('/api/config?scope=' + encodeURIComponent(currentConfigScope), {}, password)
                    .then(function(res) {
                        if (!res.ok) {
                            return res.text().then(function(t) {
                                throw new Error(t || 'Ошибка загрузки настроек');
                            });
                        }
                        return res.json();
                    })
                    .then(function(data) {
                        if (sel) {
                            sel.innerHTML = '<option value="__base__">Базовая конфигурация</option>';
                            var profiles = data.profiles || [];
                            for (var i = 0; i < profiles.length; i++) {
                                var opt = document.createElement('option');
                                opt.value = profiles[i].name;
                                opt.textContent = profiles[i].name;
                                sel.appendChild(opt);
                            }
                            sel.value = data.active_profile || '__base__';
                        }
                        renderMaterials(data.materials || []);
                        currentExternalMaterialNames = data.external_materials || [];
                        var mats = data.materials || [];
                        var names = [];
                        for (var i2 = 0; i2 < mats.length; i2++) names.push(mats[i2].name);
                        for (var i3 = 0; i3 < currentExternalMaterialNames.length; i3++) names.push(currentExternalMaterialNames[i3]);
                        renderRecipes(data.recipes || [], names);
                        renderPrices(data.prices || []);
                        loadMainProfileSelects();
                    })
                    .catch(function(e) {
                        alert('Ошибка доступа к конфигуратору: ' + (e && e.message ? e.message : e));
                        closePanel();
                    });
            }

            function safeClosest(el, selector) {
                if (!el) return null;
                if (el.closest) return el.closest(selector);
                return null;
            }

            if (saveBtn) {
                saveBtn.addEventListener('click', function() {
                    var name = nameInput ? String(nameInput.value || '').trim() : '';
                    if (!name) {
                        alert('Введите имя набора настроек');
                        return;
                    }
                    if (!window.fetch) return;
                    var body = {
                        name: name,
                        scope: currentConfigScope,
                        recipes: getRecipesFromUI(),
                        prices: getPricesFromUI(),
                        materials: getMaterialsFromUI(),
                    };
                    configFetch('/api/config/profile', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(body),
                    })
                        .then(function(res) {
                            if (res.ok) return null;
                            return res.text().then(function(t) {
                                alert('Ошибка сохранения: ' + t);
                            });
                        })
                        .then(function() {
                            var p = loadConfig();
                            if (p && p.then && sel) {
                                p.then(function() {
                                    sel.value = name;
                                });
                            } else if (sel) {
                                sel.value = name;
                            }
                        })
                        .catch(function(e) {
                            alert('Ошибка: ' + (e && e.message ? e.message : e));
                        });
                });
            }

            if (applyBtn) {
                applyBtn.addEventListener('click', function() {
                    if (!window.fetch) return;
                    var name = sel ? sel.value : '__base__';
                    configFetch('/api/config/profile/select', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ name: name, scope: currentConfigScope }),
                    })
                        .then(function(res) {
                            if (res.ok) return null;
                            return res.text().then(function(t) {
                                alert('Ошибка выбора профиля: ' + t);
                            });
                        })
                        .then(function() {
                            loadConfig();
                        })
                        .catch(function(e) {
                            console.error(e);
                        });
                });
            }

            if (delBtn) {
                delBtn.addEventListener('click', function() {
                    if (!window.fetch) return;
                    var name = sel ? sel.value : '__base__';
                    if (name === '__base__') {
                        alert('Базовую конфигурацию удалить нельзя');
                        return;
                    }
                    if (!confirm('Удалить набор настроек "' + name + '"?')) return;
                    configFetch('/api/config/profile/' + encodeURIComponent(name) + '?scope=' + encodeURIComponent(currentConfigScope), { method: 'DELETE' })
                        .then(function(res) {
                            if (res.ok) return null;
                            return res.text().then(function(t) {
                                alert('Ошибка удаления: ' + t);
                            });
                        })
                        .then(function() {
                            loadConfig();
                        })
                        .catch(function(e) {
                            console.error(e);
                        });
                });
            }

            var cfgMaterialsAdd = document.getElementById('cfgMaterialsAdd');
            var cfgPricesAdd = document.getElementById('cfgPricesAdd');
            var cfgRecipesAdd = document.getElementById('cfgRecipesAdd');
            if (cfgMaterialsAdd && cfgMaterialsBody) {
                cfgMaterialsAdd.addEventListener('click', function() {
                    var tr = document.createElement('tr');
                    tr.innerHTML = '<td><input type="text" class="mat-name" /></td><td><input type="text" class="mat-aliases" placeholder="через запятую" /></td><td class="col-del"><button type="button" class="cfg-btn-sm cfg-del-mat" title="Удалить">✕</button></td>';
                    cfgMaterialsBody.appendChild(tr);
                });
            }
            if (cfgPricesAdd && cfgPricesBody) {
                cfgPricesAdd.addEventListener('click', function() {
                    var tr = document.createElement('tr');
                    tr.innerHTML = '<td><input type="text" class="price-name" /></td><td><input type="number" step="any" class="price-nd-nv" /></td><td><input type="number" step="any" class="price-nd-v" /></td><td><input type="number" step="any" class="price-pick-nv" /></td><td><input type="number" step="any" class="price-pick-v" /></td><td class="col-del"><button type="button" class="cfg-btn-sm cfg-del-price" title="Удалить">✕</button></td>';
                    cfgPricesBody.appendChild(tr);
                });
            }
            if (cfgRecipesAdd && cfgRecipesList) {
                cfgRecipesAdd.addEventListener('click', function() {
                    var materialNames = getAvailableMaterialNames();
                    var opts = '';
                    if (materialNames.length) {
                        for (var j = 0; j < materialNames.length; j++) {
                            opts += '<option value="' + escapeAttr(materialNames[j]) + '">' + escapeHtml(materialNames[j]) + '</option>';
                        }
                    } else {
                        opts = '<option value="">— выберите материал —</option>';
                    }
                    var block = document.createElement('div');
                    block.className = 'cfg-recipe-block';
                    block.innerHTML = '<h4>Состав</h4><div class="cfg-recipe-name"><input type="text" class="rec-name" placeholder="Наименование позиции" /></div><div class="cfg-table-wrap"><table class="cfg-table"><thead><tr><th>Материал</th><th>кг</th><th class="col-del"></th></tr></thead><tbody><tr><td><select class="rec-mat-name">' + opts + '</select></td><td><input type="number" step="any" class="rec-mat-kg" /></td><td class="col-del"><button type="button" class="cfg-btn-sm cfg-del-rec-row" title="Удалить">✕</button></td></tr></tbody></table></div><button type="button" class="cfg-btn-sm cfg-add-rec-row">+ Строка</button> <button type="button" class="cfg-btn-sm cfg-del-recipe" title="Удалить состав">Удалить состав</button>';
                    cfgRecipesList.appendChild(block);
                });
            }

            if (cfgMaterialsBody) cfgMaterialsBody.addEventListener('click', function(e) {
                if (e.target && e.target.classList && e.target.classList.contains('cfg-del-mat')) {
                    var tr = safeClosest(e.target, 'tr');
                    if (tr && tr.parentNode) tr.parentNode.removeChild(tr);
                }
            });
            if (cfgPricesBody) cfgPricesBody.addEventListener('click', function(e) {
                if (e.target && e.target.classList && e.target.classList.contains('cfg-del-price')) {
                    var tr = safeClosest(e.target, 'tr');
                    if (tr && tr.parentNode) tr.parentNode.removeChild(tr);
                }
            });
            if (cfgRecipesList) cfgRecipesList.addEventListener('click', function(e) {
                if (!e.target || !e.target.classList) return;
                if (e.target.classList.contains('cfg-del-recipe')) {
                    var b = safeClosest(e.target, '.cfg-recipe-block');
                    if (b && b.parentNode) b.parentNode.removeChild(b);
                }
                if (e.target.classList.contains('cfg-del-rec-row')) {
                    var tr = safeClosest(e.target, 'tr');
                    if (tr && tr.parentNode) tr.parentNode.removeChild(tr);
                }
                if (e.target.classList.contains('cfg-add-rec-row')) {
                    var block = safeClosest(e.target, '.cfg-recipe-block');
                    var tbody = block ? block.querySelector('tbody') : null;
                    var materialNames2 = getAvailableMaterialNames();
                    var opts2 = '';
                    if (materialNames2.length) {
                        for (var j = 0; j < materialNames2.length; j++) {
                            opts2 += '<option value="' + escapeAttr(materialNames2[j]) + '">' + escapeHtml(materialNames2[j]) + '</option>';
                        }
                    } else {
                        opts2 = '<option value="">— выберите —</option>';
                    }
                    var row = document.createElement('tr');
                    row.innerHTML = '<td><select class="rec-mat-name">' + opts2 + '</select></td><td><input type="number" step="any" class="rec-mat-kg" /></td><td class="col-del"><button type="button" class="cfg-btn-sm cfg-del-rec-row" title="Удалить">✕</button></td>';
                    if (tbody) tbody.appendChild(row);
                }
            });

            // основная форма расчета: отдельные действия "Посчитать" и "Скачать"
            if (calcForm && window.fetch && resultBox && calcOnlyBtn && downloadBtn) {
                function runBetonCalculation(shouldDownload) {
                    var fileInput = document.getElementById('file');
                    if (!fileInput || !fileInput.files || !fileInput.files[0]) {
                        alert('Выберите файл .xlsx');
                        return;
                    }
                    calcOnlyBtn.disabled = true;
                    downloadBtn.disabled = true;
                    calcOnlyBtn.textContent = shouldDownload ? 'Посчитать' : 'Считаем...';
                    downloadBtn.textContent = shouldDownload ? 'Скачиваем...' : 'Скачать';
                    resultBox.classList.remove('result-ok');
                    resultBox.classList.remove('result-empty');
                    resultBox.classList.remove('has-result');
                    resultBox.innerHTML = '<div class="result-title">Результат расчета</div><div class="result-meta">Выполняется расчет, подождите...</div>';

                    var fdSummary = new FormData(calcForm);
                    fdSummary.set('mode', 'summary');
                    fdSummary.set('scope', 'beton');
                    fdSummary.set('profile_name', mainProfileSelect ? mainProfileSelect.value : '__base__');

                    fetch('/upload', { method: 'POST', body: fdSummary })
                        .then(function(res) {
                            if (!res.ok) {
                                return res.text().then(function(t) {
                                    throw new Error(t || 'Ошибка сервера: ' + t);
                                });
                            }
                            return res.json();
                        })
                        .then(function(result) {
                            var items = result.items || [];

                            var html = '';

                            function fmtVolume(v) {
                                return formatNumber(v, 3);
                            }
                            function fmtMoney(v) {
                                return v != null && !isNaN(v) ? formatNumber(v, 2) + ' ₽' : '—';
                            }
                            function getMaxIndex(sourceItems, key) {
                                var bestIdx = -1;
                                var bestVal = -1;
                                for (var ii = 0; ii < sourceItems.length; ii++) {
                                    var amounts = sourceItems[ii].amounts || {};
                                    var value = amounts[key];
                                    if (value != null && !isNaN(value) && value > bestVal) {
                                        bestVal = value;
                                        bestIdx = ii;
                                    }
                                }
                                return bestIdx;
                            }
                            function buildTable(title, colA, colB, maxKey, labelA, labelB) {
                                var maxIdx = getMaxIndex(items, maxKey);
                                var out = '<div class="result-table-card">';
                                out += '<div class="result-section-title">' + escapeHtml(title) + '</div>';
                                out += '<div class="result-section-subtitle">Объем и цены по каждому виду бетона</div>';
                                out += '<div class="result-table-wrap"><table class="result-table">';
                                out += '<thead><tr><th>Наименование бетона</th><th class="result-num">Объем, м³</th><th class="result-num">' + escapeHtml(labelA) + '</th><th class="result-num">' + escapeHtml(labelB) + '</th></tr></thead><tbody>';
                                for (var iii = 0; iii < items.length; iii++) {
                                    var item = items[iii];
                                    var amounts = item.amounts || {};
                                    var isMax = iii === maxIdx;
                                    out += '<tr' + (isMax ? ' class="row-max"' : '') + '>';
                                    out += '<td><span class="result-name">' + escapeHtml(item.name || '') + '</span>' + (isMax ? '<span class="result-badge">Макс. цена</span>' : '') + '</td>';
                                    out += '<td class="result-num">' + fmtVolume(item.max_m3) + '</td>';
                                    out += '<td class="result-num">' + fmtMoney(amounts[colA]) + '</td>';
                                    out += '<td class="result-num">' + fmtMoney(amounts[colB]) + '</td>';
                                    out += '</tr>';
                                }
                                out += '</tbody></table></div></div>';
                                return out;
                            }

                            if (items.length) {
                                html += '<div class="result-tables">';
                                html += buildTable(
                                    'Для организации А',
                                    'no_delivery_no_vat',
                                    'no_delivery_vat_22',
                                    'no_delivery_vat_22',
                                    'Без доставки без НДС',
                                    'Без доставки с НДС 22%'
                                );
                                html += buildTable(
                                    'Для иных организаций',
                                    'pickup_no_vat',
                                    'pickup_vat_22',
                                    'pickup_vat_22',
                                    'Самовывоз без НДС',
                                    'Самовывоз с НДС 22%'
                                );
                                html += '</div>';
                            } else {
                                html +=
                                    '<ul class="result-list"><li>Данные по бетонам отсутствуют. Проверьте исходный файл.</li></ul>';
                            }

                            resultBox.classList.add('result-ok');
                            resultBox.classList.add('has-result');
                            resultBox.innerHTML = html;

                            if (!shouldDownload) {
                                return null;
                            }

                            var fdExcel = new FormData(calcForm);
                            fdExcel.set('mode', 'excel');
                            fdExcel.set('scope', 'beton');
                            fdExcel.set('profile_name', mainProfileSelect ? mainProfileSelect.value : '__base__');
                            return fetch('/upload', { method: 'POST', body: fdExcel }).then(function(res) {
                                if (!res.ok) {
                                    return res.text().then(function(t) {
                                        throw new Error(t || 'Ошибка сервера при формировании Excel');
                                    });
                                }
                                return res.blob().then(function(blob) {
                                    var fileName = 'raschet_po_ostatkam.xlsx';
                                    var disp = null;
                                    try {
                                        disp = res.headers ? res.headers.get('Content-Disposition') : null;
                                    } catch (e) {}
                                    if (disp) {
                                        var m = /filename=\"?([^\";]+)\"?/i.exec(disp);
                                        if (m) fileName = m[1];
                                    }
                                    var url = window.URL.createObjectURL(blob);
                                    var a = document.createElement('a');
                                    a.href = url;
                                    a.download = fileName;
                                    document.body.appendChild(a);
                                    a.click();
                                    setTimeout(function() {
                                        document.body.removeChild(a);
                                        window.URL.revokeObjectURL(url);
                                    }, 0);
                                });
                            });
                        })
                        .catch(function(err) {
                            resultBox.classList.remove('result-ok');
                            resultBox.classList.remove('has-result');
                            resultBox.innerHTML =
                                '<div class="result-title">Ошибка расчета</div><div class="result-meta">' +
                                escapeHtml(err && err.message ? err.message : String(err)) +
                                '</div>';
                        })
                        .finally(function() {
                            calcOnlyBtn.disabled = false;
                            downloadBtn.disabled = false;
                            calcOnlyBtn.textContent = 'Посчитать';
                            downloadBtn.textContent = 'Скачать';
                        });
                }

                calcOnlyBtn.addEventListener('click', function() {
                    runBetonCalculation(false);
                });
                downloadBtn.addEventListener('click', function() {
                    runBetonCalculation(true);
                });
            }

            if (jbiForm && resultBox && jbiCalcOnlyBtn && jbiDownloadBtn) {
                function runJbiCalculation(shouldDownload) {
                    var jbiFileInput = document.getElementById('jbiFile');
                    if (!jbiFileInput || !jbiFileInput.files || !jbiFileInput.files[0]) {
                        alert('Выберите файл .xlsx');
                        return;
                    }
                    jbiCalcOnlyBtn.disabled = true;
                    jbiDownloadBtn.disabled = true;
                    jbiCalcOnlyBtn.textContent = shouldDownload ? 'Посчитать' : 'Считаем...';
                    jbiDownloadBtn.textContent = shouldDownload ? 'Скачиваем...' : 'Скачать';
                    resultBox.classList.remove('result-ok');
                    resultBox.classList.remove('result-empty');
                    resultBox.classList.remove('has-result');
                    resultBox.innerHTML = '<div class="result-title">Результат расчета</div><div class="result-meta">Выполняется расчет ЖБИ, подождите...</div>';

                    var fdJbi = new FormData(jbiForm);
                    fdJbi.set('mode', 'summary');
                    fdJbi.set('scope', 'jbi');
                    fdJbi.set('profile_name', jbiProfileSelect ? jbiProfileSelect.value : '__base__');

                    fetch('/upload', { method: 'POST', body: fdJbi })
                        .then(function(res) {
                            if (!res.ok) {
                                return res.text().then(function(t) {
                                    throw new Error(t || 'Ошибка расчета ЖБИ');
                                });
                            }
                            return res.json();
                        })
                        .then(function(summary) {
                            var items = summary.items || [];
                            var html = '';

                            function fmtMoney(v) {
                                return v != null && !isNaN(v) ? formatNumber(v, 2) + ' ₽' : '—';
                            }

                            if (items.length) {
                                html += '<div class="result-table-card">';
                                html += '<div class="result-section-title">Расчет ЖБИ</div>';
                                html += '<div class="result-section-subtitle">Максимум изделий и итоговая стоимость</div>';
                                html += '<div class="result-table-wrap"><table class="result-table">';
                                html += '<thead><tr><th>Наименование изделия</th><th class="result-num">Максимум, шт</th><th class="result-num">Цена за 1 шт</th><th class="result-num">Общая цена</th></tr></thead><tbody>';
                                for (var i = 0; i < items.length; i++) {
                                    html += '<tr><td><span class="result-name">' + escapeHtml(items[i].name || '') + '</span></td><td class="result-num">' + String(items[i].max_units != null ? items[i].max_units : 0) + '</td><td class="result-num">' + fmtMoney(items[i].unit_price) + '</td><td class="result-num">' + fmtMoney(items[i].total_price) + '</td></tr>';
                                }
                                html += '</tbody></table></div></div>';
                            } else {
                                html += '<div class="result-meta">Данные по ЖБИ отсутствуют.</div>';
                            }

                            resultBox.classList.add('result-ok');
                            resultBox.classList.add('has-result');
                            resultBox.innerHTML = html;
                            return null;
                        })
                        .catch(function(err) {
                            resultBox.classList.remove('result-ok');
                            resultBox.classList.remove('has-result');
                            resultBox.innerHTML =
                                '<div class="result-title">Ошибка расчета</div><div class="result-meta">' +
                                escapeHtml(err && err.message ? err.message : String(err)) +
                                '</div>';
                        })
                        .finally(function() {
                            jbiCalcOnlyBtn.disabled = false;
                            jbiDownloadBtn.disabled = false;
                            jbiCalcOnlyBtn.textContent = 'Посчитать';
                            jbiDownloadBtn.textContent = 'Скачать';
                        });
                }

                jbiCalcOnlyBtn.addEventListener('click', function() {
                    runJbiCalculation(false);
                });
                jbiDownloadBtn.addEventListener('click', function() {
                    runJbiCalculation(true);
                });
            }
        });
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html)


@app.post("/upload")
async def upload(
    request: Request,
    file: UploadFile = File(...),
    website: str = Form(""),
    mode: str = Form("excel"),
    scope: str = Form(CONFIG_SCOPE_BETON),
    profile_name: str = Form("__base__"),
):
    if website:
        raise HTTPException(status_code=400, detail="Spam detected")

    scope = _validate_scope(scope)
    selected_profile = None if profile_name == "__base__" else profile_name

    ip = _client_ip(request)
    # лимитируем только "тяжелую" выдачу Excel, чтобы не блокировать
    # вспомогательный запрос сводки с тем же файлом
    if mode == "excel" and _is_rate_limited(ip):
        raise HTTPException(status_code=429, detail="Слишком много запросов, попробуйте позже.")

    if not file.filename or not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Поддерживаются только файлы Excel .xlsx.")

    materials = _load_materials(scope=scope, profile_name=selected_profile)
    beton_materials = _load_materials(scope=CONFIG_SCOPE_BETON)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        input_path = tmp_path / (file.filename or "остатки.xlsx")
        content = await file.read()
        input_path.write_bytes(content)

        try:
            balances = extract_balances(str(input_path), materials)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Ошибка чтения файла: {exc}") from exc

        beton_balances: dict[str, float] = {}
        if scope == CONFIG_SCOPE_JBI:
            try:
                beton_balances = extract_balances(str(input_path), beton_materials)
            except Exception:
                beton_balances = {}

    if mode == "summary":
        if scope == CONFIG_SCOPE_JBI:
            summary = _build_jbi_summary({**balances, **beton_balances}, profile_name=selected_profile)
            return JSONResponse(content=summary)
        summary = _build_summary(balances, scope=scope, profile_name=selected_profile)
        return JSONResponse(content=summary)

    if scope == CONFIG_SCOPE_JBI:
        raise HTTPException(status_code=400, detail="Excel для ЖБИ пока не реализован")

    excel_bytes = _build_excel(balances, scope=scope, profile_name=selected_profile)

    return StreamingResponse(
        io.BytesIO(excel_bytes),
        media_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
        headers={
            # заголовок должен быть только ASCII, чтобы не было UnicodeEncodeError
            "Content-Disposition": 'attachment; filename="raschet_po_ostatkam.xlsx"'
        },
    )


@app.get("/api/config/options")
async def api_get_config_options(scope: str = CONFIG_SCOPE_BETON) -> Dict[str, Any]:
    scope = _validate_scope(scope)
    profiles = _load_profiles(scope)
    active_name: Optional[str] = profiles.get("active")
    return {
        "profiles": [{"name": p.get("name", "")} for p in profiles.get("profiles", [])],
        "active_profile": active_name or "__base__",
        "scope": scope,
    }


@app.get("/api/config")
async def api_get_config(request: Request, scope: str = CONFIG_SCOPE_BETON) -> Dict[str, Any]:
    _require_config_password(request)
    scope = _validate_scope(scope)
    profiles = _load_profiles(scope)
    active_name: Optional[str] = profiles.get("active")
    active_profile = _get_profile(profiles, active_name)

    base = _default_config_payload(scope)
    materials = active_profile.get("materials") if active_profile and "materials" in active_profile else base["materials"]
    recipes = active_profile.get("recipes") if active_profile and "recipes" in active_profile else base["recipes"]
    prices = active_profile.get("prices") if active_profile and "prices" in active_profile else base["prices"]

    return {
        "materials": materials,
        "recipes": recipes,
        "prices": prices,
        "profiles": [{"name": p.get("name", "")} for p in profiles.get("profiles", [])],
        "active_profile": active_name or "__base__",
        "scope": scope,
        "external_materials": [r.name for r in _load_recipes(scope=CONFIG_SCOPE_BETON)] if scope == CONFIG_SCOPE_JBI else [],
    }


@app.post("/api/config/profile")
async def api_save_profile(
    request: Request, payload: Dict[str, Any] = Body(...)
) -> Dict[str, str]:
    _require_config_password(request)
    scope = _validate_scope(payload.get("scope"))
    name = (payload.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Имя профиля обязательно")

    profiles = _load_profiles(scope)
    prof_list = profiles.get("profiles", [])

    new_profile = {
        "name": name,
        "materials": payload.get("materials") or [],
        "recipes": payload.get("recipes") or [],
        "prices": payload.get("prices") or [],
    }

    replaced = False
    for idx, p in enumerate(prof_list):
        if p.get("name") == name:
            prof_list[idx] = new_profile
            replaced = True
            break
    if not replaced:
        prof_list.append(new_profile)

    profiles["profiles"] = prof_list
    profiles["active"] = name
    _save_profiles(profiles, scope)
    return {"status": "ok"}


@app.post("/api/config/profile/select")
async def api_select_profile(
    request: Request, payload: Dict[str, Any] = Body(...)
) -> Dict[str, str]:
    _require_config_password(request)
    scope = _validate_scope(payload.get("scope"))
    name = (payload.get("name") or "").strip()

    profiles = _load_profiles(scope)
    if name == "__base__" or not name:
        profiles["active"] = None
        _save_profiles(profiles, scope)
        return {"status": "ok"}

    if not _get_profile(profiles, name):
        raise HTTPException(status_code=404, detail="Профиль не найден")

    profiles["active"] = name
    _save_profiles(profiles, scope)
    return {"status": "ok"}


@app.delete("/api/config/profile/{name}")
async def api_delete_profile(
    name: str, request: Request, scope: str = CONFIG_SCOPE_BETON
) -> Dict[str, str]:
    _require_config_password(request)
    scope = _validate_scope(scope)
    if name == "__base__":
        raise HTTPException(status_code=400, detail="Базовый профиль удалить нельзя")
    profiles = _load_profiles(scope)
    prof_list = profiles.get("profiles", [])
    prof_list = [p for p in prof_list if p.get("name") != name]
    profiles["profiles"] = prof_list
    if profiles.get("active") == name:
        profiles["active"] = None
    _save_profiles(profiles, scope)
    return {"status": "ok"}

