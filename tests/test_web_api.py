from __future__ import annotations

from app import web as web_module


def _sample_profile_payload(scope: str = "beton") -> dict:
    materials = [
        {"name": "Цемент", "aliases": ["Цемент ЦЕМ I 42,5Н"]},
        {"name": "Песок", "aliases": ["Песок средний Мк=2,0-2,5 класс 1"]},
    ]
    recipes = [
        {
            "name": "Тестовый бетон" if scope == "beton" else "Тестовое ЖБИ",
            "materials": {
                "Цемент": 100,
                "Песок": 200,
            },
        }
    ]
    prices = [
        {
            "name": "Тестовый бетон" if scope == "beton" else "Тестовое ЖБИ",
            "no_delivery_no_vat": 10,
            "no_delivery_vat_22": 12.2,
            "pickup_no_vat": 9,
            "pickup_vat_22": 10.98,
        }
    ]
    if scope == "jbi":
        recipes[0]["materials"]["БСТ B7,5 F50 W2"] = 1
    return {
        "scope": scope,
        "name": f"{scope}-profile",
        "materials": materials,
        "recipes": recipes,
        "prices": prices,
    }


def test_config_endpoints_require_password(client):
    response = client.get("/api/config")

    assert response.status_code == 401
    assert response.json()["detail"] == "Неверный пароль конфигуратора"


def test_save_profile_returns_structured_validation_errors(client, config_password_header):
    payload = {
        "scope": "beton",
        "name": "broken-profile",
        "materials": [
            {"name": "Цемент", "aliases": ["Общий алиас"]},
            {"name": "Песок", "aliases": ["Общий алиас"]},
            {"name": "Цемент", "aliases": ["Другой алиас"]},
        ],
        "recipes": [
            {
                "name": "Плохой состав",
                "materials": {
                    "Неизвестный материал": -1,
                },
            }
        ],
        "prices": [
            {
                "name": "Плохой состав",
                "no_delivery_no_vat": -10,
                "no_delivery_vat_22": 0,
                "pickup_no_vat": 0,
                "pickup_vat_22": 0,
            }
        ],
    }

    response = client.post(
        "/api/config/profile",
        json=payload,
        headers=config_password_header,
    )

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["message"] == "Ошибка валидации конфигурации"
    messages = [error["message"] for error in detail["errors"]]
    assert any("Дублируется материал" in message for message in messages)
    assert any("конфликтует" in message for message in messages)
    assert any("неизвестный материал" in message for message in messages)
    assert any("отрицательное значение" in message for message in messages)
    assert any("не может быть отрицательным" in message for message in messages)

def test_profiles_are_saved_independently_by_scope(client, config_password_header):
    jbi_payload = _sample_profile_payload("jbi")
    jbi_payload["recipes"][0]["materials"] = {
        "Цемент": 100,
        "Песок": 200,
        "Тестовый бетон": 1,
    }

    beton_response = client.post(
        "/api/config/profile",
        json=_sample_profile_payload("beton"),
        headers=config_password_header,
    )
    jbi_response = client.post(
        "/api/config/profile",
        json=jbi_payload,
        headers=config_password_header,
    )

    assert beton_response.status_code == 200
    assert jbi_response.status_code == 200

    beton_options = client.get("/api/config/options?scope=beton").json()
    jbi_options = client.get("/api/config/options?scope=jbi").json()

    assert [item["name"] for item in beton_options["profiles"]] == ["beton-profile"]
    assert [item["name"] for item in jbi_options["profiles"]] == ["jbi-profile"]

    jbi_config = client.get("/api/config?scope=jbi", headers=config_password_header).json()
    assert jbi_config["active_profile"] == "jbi-profile"
    assert "Тестовый бетон" in jbi_config["external_materials"]


def test_upload_supports_beton_summary_excel_and_jbi_summary(make_workbook, client):
    workbook = make_workbook(
        [
            {"name": "Цемент ЦЕМ I 42,5Б", "quantity": 1000},
            {"name": "Песок средний Мк=2,0-2,5 класс 1", "quantity": 3000},
            {"name": "Гравий М1000 фракции 5-20", "quantity": 5000},
            {"name": "Добавка в бетон SikaPlast PH 3554", "quantity": 20},
            {"name": "БСТ В40 F150 W8 (ЖБИ)", "quantity": 10},
        ]
    )
    file_bytes = workbook.read_bytes()

    summary_response = client.post(
        "/upload",
        files={"file": ("balances.xlsx", file_bytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        data={"mode": "summary", "scope": "beton", "profile_name": "__base__"},
    )
    assert summary_response.status_code == 200
    summary_body = summary_response.json()
    assert summary_body["kind"] == "beton"
    assert any(item["name"] == "БСТ B7,5 F50 W2" for item in summary_body["items"])
    beton_item = next(item for item in summary_body["items"] if item["name"] == "БСТ B7,5 F50 W2")
    assert beton_item["limiters"]
    assert beton_item["limiters"][0]["material"] == "Песок средний Мк=2,0-2,5 класс 1"
    assert "recipe_materials" in beton_item
    assert len(beton_item["recipe_materials"]) >= len(beton_item["limiters"])
    mat_names = {row["material"] for row in beton_item["recipe_materials"]}
    assert "Песок средний Мк=2,0-2,5 класс 1" in mat_names

    web_module._last_request_per_ip.clear()
    excel_response = client.post(
        "/upload",
        files={"file": ("balances.xlsx", file_bytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        data={"mode": "excel", "scope": "beton", "profile_name": "__base__"},
    )
    assert excel_response.status_code == 200
    assert (
        excel_response.headers["content-type"]
        == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    jbi_response = client.post(
        "/upload",
        files={"file": ("balances.xlsx", file_bytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        data={"mode": "summary", "scope": "jbi", "profile_name": "__base__"},
    )
    assert jbi_response.status_code == 200
    jbi_body = jbi_response.json()
    assert jbi_body["kind"] == "jbi"
    assert len(jbi_body["items"]) == 1
    assert jbi_body["items"][0]["limiters"]

    web_module._last_request_per_ip.clear()
    jbi_excel = client.post(
        "/upload",
        files={"file": ("balances.xlsx", file_bytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        data={"mode": "excel", "scope": "jbi", "profile_name": "__base__"},
    )
    assert jbi_excel.status_code == 200
    assert (
        jbi_excel.headers["content-type"]
        == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    assert len(jbi_excel.content) > 100


def test_upload_jbi_excel_minimal_workbook(make_workbook, client):
    workbook = make_workbook(
        [{"name": "Цемент ЦЕМ I 42,5Б", "quantity": 1000}],
        filename="jbi_balances.xlsx",
    )

    response = client.post(
        "/upload",
        files={"file": ("jbi_balances.xlsx", workbook.read_bytes(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        data={"mode": "excel", "scope": "jbi", "profile_name": "__base__"},
    )

    assert response.status_code == 200
    assert (
        response.headers["content-type"]
        == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
