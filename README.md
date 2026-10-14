# FastAPI Template

Пустой шаблон FastAPI с Clean Architecture, PostgreSQL и эндпоинтом проверки состояния.

## Стек

- Python 3.14.6
- FastAPI
- SQLAlchemy Core + asyncpg
- PostgreSQL 17
- Alembic
- Sentry (опционально)

## Архитектура

```text
src/
├── api/             # HTTP-контракты
├── core/            # сущности, схемы, протоколы и бизнес-логика
├── depends/         # сборка зависимостей FastAPI
├── models/          # SQLAlchemy Core tables
├── repositories/    # реализации repository protocols
├── migration/       # Alembic
├── utils/           # База данных и инфраструктурные утилиты
├── main.py
└── settings.py
```

## Запуск в Docker

```bash
cp .env.example .env
docker compose up --build
```

Compose дождётся PostgreSQL, применит миграции и запустит API на `http://localhost:8000`.
Swagger доступен на `http://localhost:8000/docs`.
Контейнеры объединяются в Compose-проект `fastapi-template`. Каталог `src` подключён как bind mount,
а Uvicorn автоматически перезапускает приложение при изменении исходного кода.

## Локальная разработка

```bash
cp .env.example .env
uv sync
docker compose up -d db
uv run alembic -c src/alembic.ini upgrade head
uv run uvicorn main:app --app-dir src --reload
```

```bash
make format
make lint
make test
```

## API

| Метод | Путь | Назначение |
|---|---|---|
| GET | `/health` | Healthcheck |

Пакеты `api`, `core`, `depends`, `models` и `repositories` оставлены как точки расширения для новых бизнес-фич.

Sentry включается через `SENTRY_ENABLED=true` и `SENTRY_DSN`. Prometheus в шаблон не входит.

## Тесты

```bash
uv sync --group dev
make test
make test-cov
```

Автономные тесты проверяют HTTP API, шифрование, обработку ответов Яндекса,
DI-контейнеры, настройки, транзакции, Sentry и запуск миграций. Внешние клиенты
и соединения БД заменены тестовыми объектами: PostgreSQL, Docker, доступ к сети
и `.env` не требуются. Тестовое окружение задаётся в `tests/conftest.py` до импорта
приложения; реальные dotenv-файлы не читаются.

`make test-cov` проверяет покрытие строк и ветвей всего `src` с порогом 100%.
Исключены только декларативные Protocol-интерфейсы и начальная пустая миграция.
`make test-infra` предназначен для тестов с маркером `infrastructure`.

Четыре регрессионных сценария отсутствующего, `null`, списка и словаря в `rows`
помечены `xfail(strict=True, raises=TypeError)`: сервис пока выпускает `TypeError`
вместо ожидаемого `ClientError`. После исправления тесты дадут `XPASS` и потребуют
удалить отметку `xfail`; другая неожиданная ошибка завершит тесты с ошибкой.
