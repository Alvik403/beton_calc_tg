from decimal import Decimal

from app.calculator import Recipe, calculate_max_cubic_meters, calculate_recipe_diagnostics


def test_calculate_max_cubic_meters_uses_limiting_material_and_returns_required_amounts():
    recipe = Recipe(
        name="БСТ B20",
        materials={
            "cement": 250,
            "sand": 900,
            "gravel": 1100,
        },
    )

    max_m3, required = calculate_max_cubic_meters(
        recipe,
        {
            "cement": 1000,
            "sand": 3000,
            "gravel": 5000,
        },
    )

    assert max_m3 == Decimal("3.333333333333333333333333333")
    assert required["cement"] == Decimal("833.3333333333333333333333332")
    assert required["sand"] == Decimal("3000.000000000000000000000000")
    assert required["gravel"] == Decimal("3666.666666666666666666666666")


def test_calculate_max_cubic_meters_ignores_zero_and_negative_recipe_values():
    recipe = Recipe(
        name="Тест",
        materials={
            "cement": 100,
            "water": 0,
            "bad": -5,
        },
    )

    max_m3, required = calculate_max_cubic_meters(recipe, {"cement": 250})

    assert max_m3 == Decimal("2.5")
    assert required["cement"] == Decimal("250.0")
    assert required["water"] == Decimal("0.0")
    assert required["bad"] == Decimal("-12.5")


def test_calculate_max_cubic_meters_returns_zero_when_no_positive_materials_present():
    recipe = Recipe(name="Пустой", materials={"water": 0, "bad": -1})

    max_m3, required = calculate_max_cubic_meters(recipe, {"water": 100})

    assert max_m3 == Decimal("0")
    assert required == {}


def test_calculate_recipe_diagnostics_returns_limiting_materials():
    recipe = Recipe(
        name="БСТ B20",
        materials={
            "cement": 250,
            "sand": 900,
            "gravel": 1100,
        },
    )

    max_m3, required, limiters = calculate_recipe_diagnostics(
        recipe,
        {
            "cement": 1000,
            "sand": 3000,
            "gravel": 5000,
        },
    )

    assert max_m3 == Decimal("3.333333333333333333333333333")
    assert required["sand"] == Decimal("3000.000000000000000000000000")
    assert len(limiters) == 1
    assert limiters[0]["material"] == "sand"
    assert limiters[0]["available"] == Decimal("3000.0")
    assert limiters[0]["required_per_unit"] == Decimal("900")

