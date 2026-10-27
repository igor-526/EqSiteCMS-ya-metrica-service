# Ya Metrica Service

Внутренний адаптер Яндекс.Метрики: список счётчиков и асинхронная загрузка
статистики страниц и поисковых запросов конюшни. Python/FastAPI, SQLAlchemy Core,
PostgreSQL, Alembic, NATS JetStream, Celery/Redis; Sentry опционален.

## API и доступ

| ID | method | path | access class / caller | без auth | с Cookie/Bearer |
|---|---|---|---|---|---|
| M1 | GET | `/api/metrica/counters` | Sensitive Private Peer Read; доверенный внутренний caller | 200 / ошибка 400 | то же |
| M3 | GET | `/api/metrica/page-stats/table` | Private Peer Read exception; seo-service | 200 / ошибка 400 | то же |
| M4 | GET | `/api/metrica/page-stats/ranking` | Private Peer Read exception; seo-service | 200 / ошибка 400 | то же |
| M5 | GET | `/api/metrica/search-query-stats/table` | Private Peer Read exception; доверенный seo-service | 200 / ошибка 400 | то же |
| M6 | GET | `/api/metrica/search-query-stats/ranking` | Private Peer Read exception; доверенный seo-service | 200 / ошибка 400 | то же |
| H1 | GET | `/health` | Public Read; любой | 200 | то же |

M1 получает зашифрованный `access_token` в query, расшифровывает в памяти и
запрашивает список счётчиков. Не передавайте открытый токен и не сохраняйте query
в логах прокси. Cookie/Authorization игнорируются,
CMS-роли не проверяются; foreign resource ID имеет тот же peer-контракт.
`equestrian_id` — идентификатор ресурса, не секретный tenant selector.
Ошибки ввода, маршрутизации и интеграции возвращают локальный `400`, не `401/422/500`.
M1 не возвращает `204` (этот статус относится к DELETE соседнего yandex-service).

Это явные dev service-to-service исключения, **не публичный production API**.
Production выпуск блокируется до подтверждения внешней сетевой защиты
чувствительных peers. Данный change не доказывает production isolation.

## NATS JetStream

| Stream | Subject | Durable / filter | Назначение | Роль |
|---|---|---|---|---|
| `PARSE_COMMANDS` | `commands.parse.ya_metrica.run` | `ya-metrica-parse-run` / тот же subject | Загрузка статистики конюшни | входящий (Consumer) |

Канонический контракт: [`docs/asyncapi.yaml`](docs/asyncapi.yaml). seo-service
(API и scheduler) владеет stream: `commands.parse.>`, FILE, LIMITS,
`max_age=604800s` по умолчанию. ya-metrica **не создаёт stream**: регистрирует
pull durable (`deliver=all`, `ack=explicit`, `ack_wait=30s`, `max_deliver=5`).
Команды других провайдеров отсекаются filter subject.

Envelope: `schema_version=1`, `occurred_at` с часовым поясом, `job_id`,
`provider=ya_metrica`, `equestrian_id`, `trigger=schedule|manual`,
`window={"mode":"auto"}` либо
`{"mode":"explicit","date_from":"YYYY-MM-DD","date_to":"YYYY-MM-DD"}`,
`params={}`. Лишние поля запрещены. Токены, ciphertext, account ID и профиль
Яндекса не передаются. Header `Nats-Msg-Id` равен
`uuid5(NAMESPACE_PARSE_COMMANDS, "<provider>:<job_id>")`, где namespace —
`uuid5(NAMESPACE_DNS, "parse-commands.eqcms")`. Для schedule `job_id` равен
`uuid5(NAMESPACE_PARSE_COMMANDS, "schedule:<provider>:<equestrian_id>:<локальная дата слота>")`,
для manual — `uuid4()`. Handler не валидирует транспортные headers и не использует
их для выбора конюшни. БД исключает повторный запуск по `job_id`
независимо от окна дедупликации брокера; старые запуски имеют `job_id=NULL`.

| Результат handler | Действие consumer |
|---|---|
| Успех, повтор `job_id`, уже активный запуск, нет привязки, невалидный диапазон дат | `ack`, без повторной доставки |
| Невалидный envelope, несовпадение provider/subject, непустые params | `term`, без повторной доставки |
| Недоступен yandex-service, ошибка хранилища или другая временная ошибка | `nak`, повтор до `max_deliver` |

`ack` означает устойчивую постановку заданий, а не завершение загрузки.
Граф сервисов и сессия БД создаются отдельно на каждую доставку.
Регистрация durable повторяется ограниченно только при отсутствии stream;
`BadRequestError` не повторяется на этапе setup. Фоновый supervisor
восстанавливает транспорт с backoff 1–30s при временной недоступности NATS.
Статический `/health` не доказывает готовность consumer.

Доступ — **Internal Trusted Network Write**: seo-service → ya-metrica,
без NATS auth и без проверки CMS-ролей (её выполняет backend до публикации).
Production gate: отдельно подтвердить внешнюю недоступность брокера и peer
HTTP, доступ только из доверенной сети. Тесты не доказывают изоляцию;
сетевые настройки change не меняет.

Удалённый `POST /api/metrica/stats/parse` не является peer endpoint:
без credentials и с Cookie/Bearer возвращает `400` unknown-route, обработчик
не вызывается, маршрута нет в OpenAPI. Ручной запуск — через backend
`POST /api/seo/parse-jobs` (ADMIN), далее seo → NATS.

### Окно загрузки

- «Сегодня» — UTC + `YA_METRICA_UTC_OFFSET_HOURS` (по умолчанию +3).
  **Оператор обязан согласовать его с `SEO_UTC_OFFSET_HOURS`**: иначе локальная
  дата слота scheduler и границы автоматического окна могут расходиться.
- Явные даты обрезаются до `[сегодня − 2 календарных года; сегодня]`;
  внутренний результат отражает это в `clamped`. Перевёрнутый или пустой
  после обрезки диапазон считается невалидным (команда завершается `ack`).
- В auto каждый вид начинается с последней сохранённой даты включительно,
  но не раньше двухлетней границы; без данных — с этой границы.
- Окно одного задания — не больше пяти дней; исполнение — Celery/Redis.

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

## Чтение статистики поисковых запросов (M5/M6)

M5/M6 — новые private trusted-network peer GET для SEO, без credentials.
Anonymous, Cookie и Bearer дают одинаковые ответы, роли не проверяются.
`equestrian_id` — обязательный query UUID ресурса, не selector:
`X-Equestrian-Id` его не заменяет и не переопределяет. Missing/invalid UUID,
metric, buckets/IDs/dates/direction → `400`, не `401`. Ошибки БД и обработки
также дают безопасный `400` без входных значений, SQL, credentials и traceback.
Успех, включая отсутствие данных, — `200`.

```http
GET /api/metrica/search-query-stats/table?equestrian_id=00000000-0000-0000-0000-000000000001&metric=page_depth&bucket=2026-10-19..2026-10-25&bucket=2026-10-26..2026-10-26&search_query_ids=00000000-0000-0000-0000-000000000002
```

```json
{"metric":"page_depth","values":[{"search_query_id":"00000000-0000-0000-0000-000000000002","bucket":"2026-10-19","value":2.08}]}
```

M5 принимает те же восемь `metric`, что M3. Повторяемый `bucket`:
1..31 интервалов строго `YYYY-MM-DD..YYYY-MM-DD`, начало ≤ конца,
по возрастанию без пересечений; разрывы допустимы. Повторяемый
`search_query_ids`: 1..100 различных UUID, дубликаты нормализуются и
схлопываются **до** ограничения. В ответе bucket — начало интервала;
нет данных — пара опущена (либо `values: []`), реальные нули включены.

```http
GET /api/metrica/search-query-stats/ranking?equestrian_id=00000000-0000-0000-0000-000000000001&metric=visits&date_from=2026-10-19&date_to=2026-10-25&direction=desc
```

```json
{"metric":"visits","direction":"desc","search_query_ids":["00000000-0000-0000-0000-000000000002"]}
```

M6: все параметры обязательны, `date_from/date_to` строго `YYYY-MM-DD`,
1..366 дней включительно, `direction=asc|desc`. Возвращаются все UUID
конюшни с данными диапазона, по числовому агрегату в указанном направлении,
при равенстве — UUID asc; без данных — `search_query_ids: []`.

Источник обоих маршрутов — только `metrica_search_query_stats`, без JOIN
parse tasks/pages/counters. Каждый вызов выполняет один SQL независимо от
размера пакета (в том числе 100×31); Python только отображает готовые rows.
Суммы visits/users/pageviews/bounces/visit_duration_seconds — целые JSON-числа;
users означает сумму дневных счётчиков, не уникальных пользователей периода.
Производные: `SUM(bounces)*100/SUM(visits)`, `SUM(pageviews)/SUM(visits)`,
`SUM(visit_duration_seconds)/SUM(visits)` соответственно bounce_rate,
page_depth, avg_visit_duration_seconds; округление до 8 знаков, при нуле
визитов — 0. Это JSON-числа, не строки; дневные средние не усредняются.

Сессия отдельная на запрос, без commit; завершение read dependency — до
отправки ответа. Границы используются ровно как передал SEO: нет defaults,
вычисления календаря/сегодня, поиска, пагинации или in-memory aggregation.
Парсинг, нормализация и page API не меняются, миграций нет.

**Production isolation gate:** до выпуска отдельно подтвердить внешнюю
недоступность M5/M6 и БД, доступ только из доверенной сети. Новые порты не
публикуются. Успешные тесты/live smoke не доказывают изоляцию; существующая
внешняя доступность блокирует production выпуск, а не исправляется этим unit.

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
ya-metrica общим ключом; ни ключ, ни ciphertext не входят в команды NATS и логи.

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
БД, Redis и NATS предварительно запускаются через infra; миграция сервиса должна дойти
до head (`20261028_0003`). Service Compose:
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
полноту suite/coverage. Точечный PostgreSQL IT: `TEST_DATABASE_URL=… uv run pytest tests/integration -m "not infrastructure"`.
`make test-cov` требует 100% строк и ветвей `src` (кроме декларативных Protocol
и пустой начальной миграции). Live smoke выполняется отдельно, не unit-тестами.

### Real-broker acceptance (IT-NATS-M1)

```bash
TEST_NATS_URL=nats://127.0.0.1:4222 uv run pytest -m infrastructure tests/integration/test_nats_broker.py -v
```

Нужен реальный NATS с JetStream. Отсутствие `TEST_NATS_URL`, брокера или
JetStream — **fail**, не skip. Тест создаёт stream с конфигурацией seo в
уникальном subject namespace и durable, проверяет filter, explicit ack,
сохранение durable при переподписке, nak/redelivery, max_deliver и term.
Удаляются только собственные случайно именованные ресурсы теста. Production
`PARSE_COMMANDS` и `ya-metrica-parse-run` не затрагиваются.
`make test` исключает marker `infrastructure`; PostgreSQL и real-broker
проверки — разные gates. Валидация обоих AsyncAPI из корня:
`make asyncapi-validate-parse`.
