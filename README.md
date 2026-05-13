# Beton — калькулятор бетона и ЖБИ по остаткам

Веб-сервис для расчёта максимального объёма бетона и количества ЖБИ по остаткам материалов из Excel.

## Реализовано

### Веб (FastAPI, в Docker порт хоста 8081 → 8000 в контейнере)

- **Расчёт бетона** — по файлу .xlsx с остатками считает максимум м³ по каждому рецепту; выдаёт объёмы, стоимость (4 варианта цен), лимитирующие материалы
- **Расчёт ЖБИ** — считает максимум изделий с учётом ограничения по бетону (остатки из того же Excel)
- **Конфигуратор** — редактирование материалов, рецептов, цен через UI; поддержка профилей (сохранённых наборов настроек)
- **Загрузка Excel** — фоновые задачи (Celery + Redis) или синхронный расчёт при недоступности очереди
- **Справка** — всплывающее окно с описанием логики и форматов Excel
- **Динамический UI** — левая панель строится из реестра направлений; добавление направления = новая запись в реестр

### Направления

| id    | Отображение               | calc_type | supports_excel | concrete_source |
|-------|---------------------------|-----------|----------------|-----------------|
| beton | Расчёт бетона по остаткам | m3        | ✓              | —               |
| jbi   | Расчёт ЖБИ                | units     | ✓              | beton           |

---

## Быстрый старт

1. Скопируйте `.env.example` в `.env` и укажите **`CONFIG_PASSWORD`** (случайная строка для доступа к конфигуратору веба).
2. При необходимости задайте порт веба на хосте: в `.env` строка `BETON_WEB_PORT=8090` (если заняты 8080/8081 — например другой контейнер слушает `8080`).
3. Запуск:

```bash
docker compose up --build
```

- Веб: по умолчанию http://localhost:8081 (или порт из `BETON_WEB_PORT`)
Если контейнер **`web` постоянно перезапускается** с ошибкой `CONFIG_PASSWORD must be set`: в `.env` добавьте непустую строку **`CONFIG_PASSWORD=...`** (любой надёжный секрет для доступа к конфигуратору). Без этого приложение намеренно не стартует. При `docker compose up` переменная также проверяется при разборе compose — при отсутствии пароля вы увидите сообщение до запуска контейнеров.

---

## Архитектура

### Структура проекта

```
beton/
├── app/
│   ├── calculator.py    # Логика расчёта max м³ / единиц
│   ├── celery_app.py    # Celery + Redis
│   ├── config.py        # Загрузка YAML (materials, recipes, prices)
│   ├── directions.py    # Реестр направлений (бетон, ЖБИ, …)
│   ├── excel_parser.py  # Парсинг остатков из Excel
│   ├── tasks.py         # Celery-задачи обработки Excel
│   └── web.py           # FastAPI: UI, API, конфигуратор
├── config/
│   ├── materials.yaml   # Материалы и алиасы бетона
│   ├── recipes.yaml     # Рецепты бетона
│   ├── prices.yaml      # Цены бетона
│   ├── jbi_materials.yaml # Материалы и алиасы ЖБИ
│   ├── jbi_recipes.yaml   # Составы ЖБИ
│   ├── jbi_prices.yaml    # Цены ЖБИ
│   ├── web_profiles.json     # Профили веб (бетон)
│   └── web_profiles_jbi.json # Профили веб (ЖБИ)
├── tests/
├── docker-compose.yml
├── requirements.txt
└── FORMULAS.md          # Формулы расчёта
```

### Реестр направлений (`app/directions.py`)

Направление — сущность с id, заголовком, путём к профилям, типом расчёта и источником бетона.

```python
Direction(
    id="beton",
    display_name="Расчет бетона по остаткам",
    profiles_path=CONFIG_DIR / "web_profiles.json",
    calc_type="m3",
    supports_excel=True,
    concrete_source=None,
)
```

Добавление направления — вызов `_register(...)` и опционально обновление `_build_left_stack_html()` для новых ID элементов.

### Поток данных

1. Excel → `extract_balances()` (excel_parser) → `{материал: кг}`
2. Материалы, рецепты, цены → из YAML или профиля
3. Расчёт:
   - **m3**: `calculate_max_cubic_meters()` для каждого рецепта
   - **units**: `_build_jbi_summary()` — max изделий с учётом бетона из `concrete_source`
4. Результат — JSON или Excel

### API

| Метод | Путь | Назначение |
|-------|------|------------|
| GET   | `/` | Главная с UI |
| GET   | `/health` | Проверка живости сервиса |
| POST  | `/upload` | Загрузка Excel (file, scope, mode, profile_name) |
| GET   | `/upload/result/{job_id}` | Статус/результат фоновой задачи |
| GET   | `/upload/file/{job_id}` | Скачать Excel-результат |
| GET   | `/api/directions` | Список направлений |
| GET   | `/api/config/options?scope=` | Профили для scope |
| GET   | `/api/config?scope=` | Материалы, рецепты, цены (требует X-Config-Password) |
| POST  | `/api/config/profile` | Сохранить профиль |
| POST  | `/api/config/profile/select` | Выбрать активный профиль |
| DELETE| `/api/config/profile/{name}?scope=` | Удалить профиль |

### Excel

- Ищется столбец «Сальдо на конец периода»
- По алиасам из материалов — строки с остатками (кг)
- Поддерживается .xlsx

### Docker

- `redis` — очередь для Celery (порт **не** проброшен на хост; доступ только между контейнерами). Для отладки: `docker compose exec redis redis-cli`
- `worker` — воркер Celery
- `web` — FastAPI (uvicorn)
- volume `beton-jobs` — временные Excel-файлы результатов
- volume `beton-profiles` — `web_profiles.json` / `web_profiles_jbi.json` (переменная `WEB_PROFILES_DIR=/app/var/profiles` у сервисов `web` и `worker`)

---

## Настройка

### Рецепты и материалы

Редактируйте `config/materials.yaml`, `config/recipes.yaml`, `config/prices.yaml`.

### Конфигуратор (веб)

Задайте **`CONFIG_PASSWORD`** в `.env` (обязательно в проде). Запросы к `/api/config*` передают заголовок `X-Config-Password`. При пустом пароле приложение не стартует (кроме режима тестов с `TESTING=1`).

### Тесты

При `TESTING=1` используется синхронный режим (без Celery):

```bash
pytest tests/ -v
```
