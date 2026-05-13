from pathlib import Path

import yaml


BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = BASE_DIR / "config"


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_materials_config() -> list[dict]:
    data = load_yaml(CONFIG_DIR / "materials.yaml")
    return data.get("materials", [])


def load_jbi_materials_config() -> list[dict]:
    data = load_yaml(CONFIG_DIR / "jbi_materials.yaml")
    return data.get("materials", [])


def load_recipes_config() -> list[dict]:
    data = load_yaml(CONFIG_DIR / "recipes.yaml")
    return data.get("recipes", [])


def load_jbi_recipes_config() -> list[dict]:
    data = load_yaml(CONFIG_DIR / "jbi_recipes.yaml")
    return data.get("recipes", [])


def load_prices_config() -> list[dict]:
    data = load_yaml(CONFIG_DIR / "prices.yaml")
    return data.get("prices", [])


def load_jbi_prices_config() -> list[dict]:
    data = load_yaml(CONFIG_DIR / "jbi_prices.yaml")
    return data.get("prices", [])
