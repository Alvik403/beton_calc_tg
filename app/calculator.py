from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class Recipe:
    name: str
    materials: dict[str, float]


def calculate_max_cubic_meters(
    recipe: Recipe, balances: dict[str, float]
) -> tuple[Decimal, dict[str, Decimal]]:
    limits = []
    for material, per_m3 in recipe.materials.items():
        per_m3_d = Decimal(str(per_m3))
        if per_m3_d <= 0:
            continue
        available_d = Decimal(str(balances.get(material, 0.0)))
        limits.append(available_d / per_m3_d)
    if not limits:
        return Decimal("0"), {}
    max_m3 = min(limits)
    required = {m: max_m3 * Decimal(str(v)) for m, v in recipe.materials.items()}
    return max_m3, required
