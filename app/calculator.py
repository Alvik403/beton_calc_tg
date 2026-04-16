from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class Recipe:
    name: str
    materials: dict[str, float]


def calculate_recipe_diagnostics(
    recipe: Recipe, balances: dict[str, float]
) -> tuple[Decimal, dict[str, Decimal], list[dict[str, Any]]]:
    diagnostics: list[dict[str, Any]] = []
    limits = []
    for material, per_m3 in recipe.materials.items():
        per_m3_d = Decimal(str(per_m3))
        if per_m3_d <= 0:
            continue
        available_d = Decimal(str(balances.get(material, 0.0)))
        possible_output = available_d / per_m3_d
        diagnostics.append(
            {
                "material": material,
                "available": available_d,
                "required_per_unit": per_m3_d,
                "possible_output": possible_output,
            }
        )
        limits.append(possible_output)
    if not limits:
        return Decimal("0"), {}, []
    max_m3 = min(limits)
    required = {m: max_m3 * Decimal(str(v)) for m, v in recipe.materials.items()}
    limiters = [
        item for item in diagnostics if item.get("possible_output") == max_m3
    ]
    return max_m3, required, limiters


def calculate_max_cubic_meters(
    recipe: Recipe, balances: dict[str, float]
) -> tuple[Decimal, dict[str, Decimal]]:
    max_m3, required, _ = calculate_recipe_diagnostics(recipe, balances)
    return max_m3, required


def format_recipe_materials_kg(required: dict[str, Decimal], recipe: Recipe) -> str:
    """Многострочный текст расхода материалов (кг) для компактного Excel."""
    lines: list[str] = []
    for material in sorted(recipe.materials.keys()):
        val = required.get(material, Decimal("0"))
        if val == 0:
            continue
        num = float(val)
        s = f"{num:,.2f}".replace(",", " ").replace(".", ",")
        lines.append(f"{material}: {s} кг")
    return "\n".join(lines) if lines else "—"
