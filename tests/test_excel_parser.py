import pytest

from app.excel_parser import MaterialConfig, extract_balances


def test_extract_balances_reads_exact_alias_and_quantity(make_workbook):
    workbook = make_workbook(
        [
            {"name": "Цемент ЦЕМ I 42,5Н", "quantity": 120},
            {"name": "Песок средний Мк=2,0-2,5 класс 1", "quantity": 500},
        ]
    )

    balances = extract_balances(
        str(workbook),
        [
            MaterialConfig(name="Цемент", aliases=["Цемент ЦЕМ I 42,5Н"]),
            MaterialConfig(name="Песок", aliases=["Песок средний Мк=2,0-2,5 класс 1"]),
        ],
    )

    assert balances == {"Цемент": 120.0, "Песок": 500.0}


def test_extract_balances_prefers_exact_match_over_partial_and_best_nonzero(make_workbook):
    workbook = make_workbook(
        [
            {"name": "Цемент ЦЕМ I 42,5Н мешки", "quantity": 999},
            {"name": "Цемент ЦЕМ I 42,5Н", "quantity": 80},
            {"name": "Цемент ЦЕМ I 42,5Н", "quantity": 125},
        ]
    )

    balances = extract_balances(
        str(workbook),
        [MaterialConfig(name="Цемент", aliases=["Цемент ЦЕМ I 42,5Н"])],
    )

    assert balances["Цемент"] == 125.0


def test_extract_balances_returns_zero_when_only_zero_matches_exist(make_workbook):
    workbook = make_workbook(
        [
            {"name": "Добавка в бетон SikaPlast PH 3554", "quantity": 0},
        ]
    )

    balances = extract_balances(
        str(workbook),
        [MaterialConfig(name="Добавка", aliases=["Добавка в бетон SikaPlast PH 3554"])],
    )

    assert balances["Добавка"] == 0.0


def test_extract_balances_raises_without_saldo_column(make_workbook):
    workbook = make_workbook(
        [{"name": "Цемент ЦЕМ I 42,5Н", "quantity": 120}],
        include_saldo_header=False,
        filename="missing_saldo.xlsx",
    )

    with pytest.raises(ValueError, match="Сальдо на конец периода"):
        extract_balances(
            str(workbook),
            [MaterialConfig(name="Цемент", aliases=["Цемент ЦЕМ I 42,5Н"])],
        )

