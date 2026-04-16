from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
import re

from aiogram.types import FSInputFile, Message

from app.calculator import Recipe
from app.config import (
    get_bot_token,
    load_materials_config,
    load_prices_config,
    load_recipes_config,
)
from app.excel_parser import MaterialConfig, extract_balances
from app.web import _build_excel_first_table_beton, _build_prices_dataframe, _workbook_bytes_from_tables


def _load_materials() -> list[MaterialConfig]:
    raw = load_materials_config()
    materials: list[MaterialConfig] = []
    for item in raw:
        name = item.get("name", "").strip()
        aliases = [a for a in item.get("aliases", []) if a]
        if name and aliases:
            materials.append(MaterialConfig(name=name, aliases=aliases))
    return materials


def _load_recipes() -> list[Recipe]:
    raw = load_recipes_config()
    recipes: list[Recipe] = []
    for item in raw:
        name = item.get("name", "").strip()
        materials = item.get("materials", {})
        if name and materials:
            recipes.append(Recipe(name=name, materials=materials))
    return recipes


def _normalize_name(text: str) -> str:
    text = text.strip().lower().replace("ё", "е")
    text = text.replace("в", "b").replace("з", "3")
    text = re.sub(r"\s+", " ", text)
    return text


def _load_prices() -> dict[str, dict[str, float]]:
    raw = load_prices_config()
    prices: dict[str, dict[str, float]] = {}
    for item in raw:
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


async def handle_start(message: Message) -> None:
    await message.answer(
        "Пришлите Excel с остатками. Я верну Excel с максимумом м3 по каждому типу."
    )


async def handle_document(message: Message, bot: Bot) -> None:
    if not message.document:
        return
    filename = message.document.file_name or "остатки.xlsx"
    if not filename.lower().endswith(".xlsx"):
        await message.answer("Поддерживаются только файлы Excel .xlsx.")
        return

    await message.answer("Файл получен, считаю...")

    materials = _load_materials()
    recipes = _load_recipes()
    prices = _load_prices()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        input_path = tmp_path / filename
        tg_file = await bot.get_file(message.document.file_id)
        await bot.download_file(tg_file.file_path, destination=input_path)

        try:
            balances = extract_balances(str(input_path), materials)
        except Exception as exc:
            await message.answer(f"Ошибка чтения файла: {exc}")
            return

        output_df, merge_ranges = _build_excel_first_table_beton(recipes, balances)
        prices_df = _build_prices_dataframe(recipes, balances, prices)
        excel_bytes = _workbook_bytes_from_tables(
            output_df, prices_df, merge_first_section=merge_ranges
        )
        output_path = tmp_path / "результат.xlsx"
        output_path.write_bytes(excel_bytes)

        document = FSInputFile(output_path)
        await message.answer_document(document=document, filename="результат.xlsx")


async def main() -> None:
    bot = Bot(token=get_bot_token())
    dp = Dispatcher()
    dp.message.register(handle_start, CommandStart())
    dp.message.register(handle_document, F.document)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
