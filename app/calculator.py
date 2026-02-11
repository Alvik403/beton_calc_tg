from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Recipe:
    name: str
    materials: dict[str, float]


def calculate_max_cubic_meters(
    recipe: Recipe, balances: dict[str, float]
) -> tuple[float, dict[str, float]]:
    limits = []
    for material, per_m3 in recipe.materials.items():
        if per_m3 <= 0:
            continue
        available = balances.get(material, 0.0)
        limits.append(available / per_m3 if per_m3 > 0 else 0.0)
    if not limits:
        return 0.0, {}
    max_m3 = min(limits)
    required = {m: max_m3 * v for m, v in recipe.materials.items()}
    return max_m3, required
