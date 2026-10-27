# Ya Metrica Service

Внутренний адаптер Яндекс.Метрики: список счётчиков и асинхронная загрузка
статистики страниц и поисковых запросов конюшни. Python/FastAPI, SQLAlchemy Core,
PostgreSQL, Alembic, Celery/Redis; Sentry опционален.

## API и доступ

| ID | method | path | access class / caller | без auth | с Cookie/Bearer |
|---|---|---|---|---|---|
| M1 | GET | `/api/metrica/counters` | Sensitive Private Peer Read; доверенный внутренний caller | 200 / ошибка 400 | то же |
| M2 | POST | `/api/metrica/stats/parse` | Private Peer Write exception; внутренний сервис | 200 / ошибка 400 | то же |
| M3 | GET | `/api/metrica/page-stats/table` | Private Peer Read exception; seo-service | 200 / ошибка 400 | то же |
| M4 | GET | `/api/metrica/page-stats/ranking` | Private Peer Read exception; seo-service | 200 / ошибка 400 | то же |
| H1 | GET | `/health` | Public Read; любой | 200 | то же |

M1 получает зашифрованный `access_token` в query, расшифровывает в памяти и
запрашивает список счётчиков. Не передавайте открытый токен и не сохраняйте query
в логах прокси. M2 не принимает credentials. Cookie/Authorization игнорируются,
CMS-роли не проверяются; foreign resource ID имеет тот же peer-контракт.
`equestrian_id` — идентификатор ресурса, не секретный tenant selector.
Ошибки ввода, маршрутизации и интеграции возвращают локальный `400`, не `401/422/500`.
M1/M2 не возвращают `204` (этот статус относится к DELETE соседнего yandex-service).

Это явные dev service-to-service исключения, **не публичный production API**.
Production выпуск блокируется до подтверждения внешней сетевой защиты
чувствительных peers. Данный change не доказывает production isolation.

```http
POST /api/metrica/stats/parse
Content-Type: application/json

{"equestrian_id":"<UUID>","date_from":"2026-10-21","date_to":"2026-10-25"}
```

Обе даты опциональны и допускают null, лишние поля запрещены. Ответ `200`:
`run_id`, `equestrian_id`, `date_mode` (`explicit`/`auto`), `clamped`,
`ranges` (`pages`, `search_queries`, каждый с `date_from/date_to`),
`tasks` (`id`, `kind`, `date_from`, `date_to`, `sequence`, `status=queued`).
Без account/counter/profile/credentials. Окно одного задания — не больше пяти дней.

- «Сегодня» вычисляется по UTC + `YA_METRICA_UTC_OFFSET_HOURS` (по умолчанию +3).
- Явные даты обрезаются до `[сегодня − 2 календарных года; сегодня]`;
  результат отражается в `clamped`. Одна дата, перевёрнутый или пустой после
  обрезки диапазон → `400 «Некорректный диапазон дат»`.
- Без дат каждый вид начинается с последней сохранённой даты включительно,
  но не раньше двухлетней границы; без данных — с этой границы.
- Нет привязки → `400 «Счётчик Яндекс.Метрики не привязан к конюшне»`.
- Ошибка yandex-service → `400 «Не удалось получить данные интеграции Яндекс»`.
- Уже есть queued/running → `400 «Парсинг конюшни уже выполняется»`.

## Чтение статистики страниц (M3/M4)

M3/M4 — приватные peer GET внутри доверенной сети: без credentials и с
произвольными Cookie/Bearer ответ одинаков. `equestrian_id` — UUID ресурса,
не авторизация. Успех — `200`; ошибки валидации, БД, обработки, метода или
trailing slash — безопасный `400`, без отражения входа, SQL и traceback.
Production требует подтверждённой внешней недоступности этих маршрутов.

Все query-параметры обязательны. `metric` — один из `visits`, `users`,
`pageviews`, `bounces`, `visit_duration_seconds`, `bounce_rate`, `page_depth`,
`avg_visit_duration_seconds`.

```http
GET /api/metrica/page-stats/table?equestrian_id=00000000-0000-0000-0000-000000000001&metric=visits&bucket=2026-10-19..2026-10-25&bucket=2026-10-26..2026-10-26&page_ids=00000000-0000-0000-0000-000000000002
```

```json
{"metric":"visits","values":[{"page_id":"00000000-0000-0000-0000-000000000002","bucket":"2026-10-19","value":12}]}
```

- `bucket` повторяется 1..31 раз: строго `YYYY-MM-DD..YYYY-MM-DD`, начало
  не позже конца, по возрастанию без пересечений; разрывы допустимы.
- `page_ids` повторяется: 1..100 различных UUID, дубликаты схлопываются.
- `bucket` ответа — дата начала колонки. Возвращаются только пары с данными;
  отсутствующие пары не заменяются нулями, пустой результат — `values: []`.

```http
GET /api/metrica/page-stats/ranking?equestrian_id=00000000-0000-0000-0000-000000000001&metric=visits&date_from=2026-10-19&date_to=2026-10-25&direction=desc
```

```json
{"metric":"visits","direction":"desc","page_ids":["00000000-0000-0000-0000-000000000002"]}
```

M4 принимает `date_from/date_to` строго `YYYY-MM-DD`: от 1 до 366 дней
включительно, `direction=asc|desc`. Возвращаются все страницы конюшни с данными
в диапазоне, упорядоченные по агрегату; при равенстве — UUID по возрастанию.
Нет данных — `page_ids: []`.

Каждый маршрут выполняет один SQL, фильтруя по конюшне. Аддитивные показатели
суммируются и возвращаются целыми JSON-числами (включая `users`, не distinct).
Производные показатели считаются из сумм по формулам раздела ниже, округляются
до 8 знаков и возвращаются JSON-числами, не строками; при нуле визитов — `0`.
Сессия отдельная на запрос, без commit. Границы от SEO передаются без изменений:
M3/M4 не вычисляют календарные периоды, умолчания или «сегодня», не обрезают
будущие даты. Указанные выше лимиты — только валидация запроса.

## Хранение и агрегация

`metrica_parse_runs` хранит запуск, режим дат и `clamped`;
`metrica_parse_tasks` — устойчивую очередь, окна, checkpoint, lease и попытки.
`metrica_page_stats` и `metrica_search_query_stats` хранят строки по
`(equestrian_id, stat_date, page_id/search_query_id)`; UUID сущностей резолвит
seo-service. Привязку счётчика/аккаунта и ciphertext получает yandex-service:
Y7 — `/api/connections/metrica/{equestrian_id}`,
Y5 — `/api/servicing/access-token/{account_id}` → `enc_access_token`.
Y5 — Sensitive Private Peer Read exception: anonymous и Cookie/Bearer дают
одинаковые `200`, malformed/missing resource — `400`. Расшифровка только в памяти
ya-metrica общим ключом; ни ключ, ни ciphertext не входят в результат M2 и логи.

Аддитивные колонки: `visits`, `users`, `pageviews`, `bounces`,
`visit_duration_seconds`. Нормализованные дубли (например, `/tours?utm` и `/tours`)
внутри страницы API суммируются, следующие страницы того же задания добавляются.
Другое задание **заменяет**, а не накапливает старую строку; checkpoint и upsert
коммитятся вместе. После успеха удаляются не встреченные в новом задании строки
его окна. Повторный запуск идемпотентен; незавершённая загрузка может быть частичной.

**`users` неаддитивен семантически:** сохранённая сумма исходных строк не является
числом уникальных пользователей после нормализации или за несколько дат/сущностей.
Для корректного distinct-users нужен отдельный источник/расчёт, не SUM этих строк.

Generated stored средние рассчитываются из сумм, не усредняются напрямую:

- `bounce_rate = 100 * SUM(bounces) / SUM(visits)`;
- `page_depth = SUM(pageviews) / SUM(visits)`;
- `avg_visit_duration_seconds = SUM(visit_duration_seconds) / SUM(visits)`.

При нуле визитов результат равен нулю. Округление исходного процента отказов и
средней длительности выполняется при переводе в аддитивные целые колонки.

## Окружение и запуск

Настройки задаются оператором **в собственном `.env` этого сервиса** (образец
`.env.example`) или environment. Runtime не читает env других сервисов/оркестратора.
Не публикуйте значения credentials и вывод интерполированного Compose config.

- `POSTGRES_USER/PASSWORD/HOST/PORT/DB` — PostgreSQL сервиса.
- `ENCRYPTION_SERVICE_KEY` — согласованный с yandex-service ключ.
- `YANDEX_SERVICE_URL`, `SEO_SERVICE_URL` — адреса peers в сети контейнеров.
- `YA_METRICA_UTC_OFFSET_HOURS=3` — целое −12..14.
- `YA_METRICA_STAT_PAGE_LIMIT=100000`, `YA_METRICA_STAT_TIMEOUT_SECONDS=120`.
- `SEO_RESOLVE_BATCH_SIZE=1000` (максимум 1000).
- `PARSE_DISPATCH_INTERVAL_SECONDS=30`, `TASK_MAX_ATTEMPTS=3`,
  `TASK_RETRY_BASE_SECONDS=60`, `TASK_LEASE_SECONDS=600`.
- `CELERY_APP_MAIN=ya-metrica-service`,
  `CELERY_APP_BROKER=redis://eqsitecms-redis:6379/5`,
  `CELERY_APP_BACKEND=redis://eqsitecms-redis:6379/6`.
  `REDIS_PASSWORD` задаёт оператор в env этого сервиса; настройки безопасно
  подставляют пароль в Redis URL. Пустой пароль для защищённого Redis недопустим.
- `WORKER_CONCURRENCY=1` — Compose concurrency; задачи одного аккаунта всегда
  сериализуются PostgreSQL claim/lease независимо от числа workers.
- `ENVIRONMENT`, `DEBUG`, `APP_TITLE`, `SENTRY_ENABLED/DSN/ENVIRONMENT/TRACES_SAMPLE_RATE/RELEASE`
  — общие настройки приложения/телеметрии; API не раскрывает traceback даже при DEBUG.

Из корня монорепозитория: `make ya-metrica-build`, `make ya-metrica`.
БД и Redis предварительно запускаются через infra; миграция сервиса должна дойти
до head (`20261026_0002`). Service Compose:
`.docker-compose/docker-compose.ya_metrica.yml` (обязателен service `.env`).
Подробные границы root-команд — в `SERVICES.md`.

Compose запускает `ya-metrica-celery-worker` и **один экземпляр**
`ya-metrica-celery-beat`. Очередь `ya_metrica`, задачи
`metrica.process_account`, `metrica.dispatch_ready`; beat периодически возвращает
готовые задания и просроченные lease. Недоступность брокера после commit не
отменяет постановку: beat доставит её позже.

Для локальной разработки из каталога сервиса:

```bash
uv sync
uv run alembic -c src/alembic.ini upgrade head
uv run uvicorn main:app --app-dir src --reload
uv run celery -A workers.celery_app worker -Q ya_metrica --concurrency=1 --hostname 'ya-metrica-worker@%h'
uv run celery -A workers.celery_app beat
```

Локально переопределите container hostnames доступными адресами БД/Redis/peers.
Worker health проверяется адресным `celery inspect ping -d <имя-worker>`.
PostgreSQL — источник истины; задачи JSON не содержат токенов, ciphertext или паролей.

## Тесты

```bash
make format
make lint
make test
TEST_DATABASE_URL='<операторский DSN postgresql+asyncpg для eqsitecmsyametrica_test>' make test-cov
```

Автономные unit/API тесты подменяют внешние системы и не читают dotenv.
Для полной проверки нужен реальный PostgreSQL и **отдельная** БД
`eqsitecmsyametrica_test`; не используйте application DB. Integration fixtures
создают случайные схемы `test_metrica_<uuid>` и удаляют их в `finally`.
Без `TEST_DATABASE_URL` PostgreSQL тесты пропускаются, такой запуск не доказывает
полноту suite/coverage. Точечный IT: `TEST_DATABASE_URL=… uv run pytest tests/integration`.
`make test-cov` требует 100% строк и ветвей `src` (кроме декларативных Protocol
и пустой начальной миграции). Live smoke выполняется отдельно, не unit-тестами.
