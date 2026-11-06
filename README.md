# Ya Metrica Service

Внутренний адаптер Яндекс.Метрики: список счётчиков и асинхронная загрузка
статистики страниц и поисковых запросов конюшни. Python/FastAPI, SQLAlchemy Core,
PostgreSQL, Alembic, NATS JetStream, Celery/Redis; Sentry опционален.

**Текущий checkpoint — M-3d (ниже): wiring реализован, runtime OFF.**
Секции M-1…M-3c2 описывают исторические этапы, а не текущую полноту wiring.
PG/NATS acceptance остаётся открытым; merge algorithm — только M-4.

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
до head (`20261102_0004`). Service Compose:
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


## Maintenance storage (106–107, M-1 schema checkpoint)

Ревизия `20261102_0004`, parent `20261028_0003`, только добавляет storage.
**Арбитраж ещё не подключён к старым repositories/workers.** Не включать producers,
новые очереди и destructive UI до завершения следующих M units. HTTP не меняется.

- `metrica_tenant_gates`: постоянная строка tenant, generation/requested/completed;
  `requested > completed` — барьер, но равенство не снимает незавершённые merge.
- `metrica_maintenance_operations`: UUID операции, tenant, cleanup seq либо merge
  revision (unique на tenant), state, seal `part_count`, JSON checkpoint, attempts,
  next_attempt_at, безопасный error_code, корреляция cleanup/result.
  Cleanup: `pending → draining → deleting → completed`, ошибка `failed_blocked`.
  Merge: `collecting → waiting → running → awaiting_seo → completed`, либо
  `superseded_by_delete → awaiting_seo → completed`; ошибка `failed_blocked`.
  CHECK ограничивает допустимые состояния; переходы CAS и приоритет — задача repositories.
- `metrica_maintenance_chunks`: PK `(operation_id, part_index)`, tenant-qualified
  FK, SHA-256 payload_hash, 1–500 mappings, status/checkpoint. Seal может прийти
  раньше частей: `part_count IS NULL` означает отсутствие seal, 0 — пустой seal.
- `metrica_pending_parse_intents`: PK job_id, tenant, hash, authoritative generation
  и observed requested seq, исходный date_mode. Auto хранит NULL dates, без раннего
  расчёта окна, credentials и task decomposition. `pending/admitted/cancelled/missing_binding`;
  admitted ссылается на run того же tenant. Отменённые intents сохраняют аудит.
- Inbox PK `(consumer,message_id)`, outbox PK message_id, tenant-qualified operation
  FK и payload_hash. Inbox фиксируется с эффектом в одной транзакции; outbox
  `pending/published`, attempts/next_attempt_at, publish вне транзакции.
- Lease storage операций/outbox: UUID `lease_owner`, `lease_expires_at`,
  bigint `fencing_revision >= 0`. Owner/expires одновременно NULL либо заполнены.
  Следующий repository unit должен обеспечить token+revision+неистёкший lease CAS
  и общий порядок коротких tenant locks → account locks; schema сама этого не делает.

Run/task получили generation и observed cleanup seq; task дополнительно
fencing_revision и terminal `cancelled`. Старые строки, counters и ошибки не
переписываются: additive default 0 — **legacy/unknown snapshot**, не доказательство
актуального binding. Перед новым dispatcher остановить старые admissions/workers,
дождаться завершения старых run либо сверить generation по authoritative private
peer snapshot под остановленным dispatcher. Нельзя автоматически считать 0 равным
текущему поколению 1 и продолжать старую задачу с новыми credentials.

Индексы operations/intents/outbox `(status,next_attempt_at)` и tenant/status;
operations/outbox lease expiry; chunks operation/status; обе stats-таблицы
`(equestrian_id,id)` для bounded keyset cleanup. Сами статистики миграция не
переписывает. EXPLAIN на выделенной PostgreSQL и оценка времени обычного
CREATE INDEX остаются обязательными перед rollout; создание индексов выполнять
в maintenance window без parse.

Миграция frozen, runtime models не импортирует. `downgrade` явно запрещён, чтобы
не удалить барьеры, outbox и аудит: до включения producers откатить приложение,
оставив additive schema; после destructive эффекта — backup/forward-fix.

Проверка schema без инфраструктуры:
`uv run pytest tests/unit/test_maintenance_schema.py tests/unit/test_migrations.py`.
Реальная PostgreSQL (только TEST_DATABASE_URL, database с суффиксом `_test`):
`uv run pytest tests/integration/test_maintenance_schema_postgres.py -m infrastructure`.
Нет URL — **BLOCKED**, не skip=PASS. Тесты проверяют metadata/Alembic, legacy
backfill, FK isolation, dedup и auto-окна; не заменяют IT-D-02/IT-N-02 батчей,
конкурентных claims и stale-worker fencing следующего repository/worker unit.

### M-1b: primitives арбитража (частичный checkpoint)

`MaintenanceArbitrationRepository` реализует
`core.protocols.repositories.metrica_maintenance.MaintenanceArbitrationProtocol`.
Это **не интеграция** старых task claims/ingestion: до M-2 запускать новые
maintenance workers рядом со старыми workers небезопасно.

- `lock_scope(equestrian_ids, account_ids=())`: сначала все уникальные tenant UUID
  по возрастанию (`metrica-parse:<uuid>`), затем все account UUID по возрастанию
  (`metrica-account:<uuid>`). Namespace tenant общий с `ParseRunRepository`.
  Заранее собрать полный набор locks; не брать новый tenant после account.
  Advisory xact locks живут до commit/rollback. Транзакции короткие, без HTTP/NATS;
  peer snapshot получить **до** транзакции, не считать локальный gate заменой peer.
- `selected_kind(tenant) -> WorkKind`: `cleanup > merge > parse`. Requested watermark
  блокирует parse ещё до команды cleanup. Любая незавершённая операция, включая
  delayed, failed_blocked, collecting, awaiting_seo и superseded, удерживает барьер.
  Это приоритет, а не разрешение claim parse: authoritative snapshot и account
  exclusivity проверяет M-2 под теми же locks.
- `claim(tenant, owner: UUID, lease_seconds) -> MaintenanceLease | None`:
  выбирается только первая незавершённая операция по cleanup seq / merge revision,
  без обхода blocked/delayed head. Cleanup seq обязан быть `local completed + 1`;
  gaps и уже completed seq не исполняются. M-3 должен атомарно продвигать local
  completed при реальном завершении cleanup. Пока существует любой running task
  (даже expired или generation=0), claim обслуживания невозможен. Queued не дренируются.
  Cleanup pending/draining/deleting получает deleting; merge waiting/running — running.
  Проверяется DB clock, live lease не перехватывается. Новый claim увеличивает
  fencing revision, даже если owner UUID повторён.
- `MaintenanceLease` содержит tenant, operation, owner, revision, kind, status.
  `assert_owned(lease)` повторно проверяет приоритет/drain/token/revision/status/expiry;
  `checkpoint(lease, checkpoint: dict, lease_seconds)` повторяет проверку и CAS
  с renew. Вызывать до эффекта и в конце **одной транзакции** batch+checkpoint.
  `MaintenanceLeaseLostError` требует rollback всей транзакции, не commit частичного
  эффекта. Никаких внутренних commit. Guard не валидирует произвольный SQL вызывающего.

Для M-2: generation=0 не повышается автоматически; expired running сначала
терминализировать/reconcile с fencing под tenant→account locks, только затем
обслуживание сможет claim. Старые ParseTaskRepository.claim/retry/reap/owned writes
пока не используют общий guard: их интеграция и token+revision CAS обязательны
в M-2. Storage transitions и intent admission/dedup реализованы следующими
checkpoint ниже; terminal transitions/outbox workers остаются M-3/M-4. Публичные endpoints/access policy не изменены.

Проверки primitives: `uv run pytest tests/unit/test_maintenance_arbitration.py`;
реальная PG: `uv run pytest tests/integration/test_maintenance_arbitration_postgres.py -m infrastructure`.
Последние требуют выделенный `TEST_DATABASE_URL` (`*_test`), иначе явно BLOCKED.
Это предпосылки IT-D-02/IT-N-02, не доказательство batch effects или полного fencing
старых parse workers. Изолированные schema fixtures переиспользуются без доступа
к application DB. Evidence M-1b: `.qa/106-107-metrica-cleanup-page-merge/M-1b/`.

### M-1b2: idempotent ledger storage (checkpoint)

`MaintenanceStorageRepository` расширяет arbitration и реализует
`MaintenanceStorageProtocol`. Методы не делают commit; при любом conflict/lease loss
откатить **всю** транзакцию. Порядок при приёме: validated envelope → tenant lock →
`save_operation` → `save_chunk`/`seal` → `save_inbox` → commit → ACK.
Inbox нельзя коммитить отдельно от ledger. HTTP/NATS внутри транзакции запрещены.

- `save_operation(tenant, operation, kind, cleanup_seq?, binding_generation?,
  normalization_revision?) -> bool` сравнивает неизменяемую identity при дубле;
  unique tenant seq/revision не позволяет заменить операцию новым UUID.
  Cleanup монотонно повышает requested/generation, **не** completed.
- `save_inbox(..., consumer, message_id, payload) -> bool`: SHA-256 canonical JSON
  (ключи отсортированы; порядок массивов значим). Повтор сверяет tenant/operation/hash,
  независимо от broker duplicate window. Сериализация UUID/UTC и wire validation
  принадлежат M-3. Безопасный `MaintenanceConflictError` не содержит payload.
- `save_chunk(..., part_index, mappings)`, `seal(..., part_count)` идемпотентны;
  другой hash/count — conflict. Seal до частей допустим; `collecting → waiting`
  только при полном наборе индексов `[0, part_count)`, включая пустой seal.
  Late parts superseded merge сохраняются для корреляции, но не возвращают waiting.
  Membership и уникальность source между разными частями гарантирует SEO producer;
  локально проверяются UUID/source!=target/дубликаты внутри части и лимит 500.
- `checkpoint_chunk(lease, part_index, checkpoint, completed, lease_seconds)`
  меняет pending/running → running/completed и renew/checkpoint операции в той же
  транзакции с эффектом. Completed chunk повторно не исполнять; stale lease — rollback.
- `finish_work(lease, status, result_message_id?)`: CAS по tenant/token/revision/
  status/expiry. Cleanup → completed либо failed_blocked; merge → awaiting_seo либо
  failed_blocked. Awaiting SEO требует seal и все completed chunks. Lease отзывается,
  revision увеличивается. Cleanup completed продвигает **только следующий** local seq.
  Проверка отсутствия статистик и result outbox в этой же транзакции — M-3/M-4.
- `supersede(tenant, operation, cleanup_operation)` отзывает merge lease, сохраняет
  первую cleanup correlation; `record_superseded_result(..., result_message_id)`
  переводит superseded_by_delete → awaiting_seo. Оба перехода вызывающий коммитит
  только вместе с durable result outbox. Ни один из них не снимает parse barrier.
- `finalize(tenant, operation, normalization_revision, result_message_id)` допускает
  awaiting_seo → completed только с точной корреляцией; повтор — no-op. Для superseded
  дополнительно нужны completed cleanup и seal. Проверки scan/source deletion в SEO
  и transport retry остаются M-3/M-4, как и recovery failed_blocked.

Pending intent admission/dedup реализован в M-1b2b ниже. Старые parse claims
и ingestion по-прежнему требуют M-2; новые workers пока включать нельзя.

Проверки: `uv run pytest tests -k "maintenance or gate or lease" -m "not infrastructure"`;
PG fixtures: `uv run pytest tests/integration/test_maintenance_{arbitration,storage}_postgres.py -m infrastructure`.
Без выделенного TEST_DATABASE_URL (`*_test`) PostgreSQL **BLOCKED**, не PASS.
Storage fixtures — лишь prerequisites IT-D-02/IT-N-02, не acceptance stats batches.
Evidence: `.qa/106-107-metrica-cleanup-page-merge/M-1b2/`.


### M-1b2b: durable pending intent admission (контракт для M-2)

`PendingParseIntentProtocol` реализован `PendingParseIntentRepository`, использующим
общие tenant→account advisory locks и ту же сессию, что run/tasks. Схема и Alembic
head `20261102_0004` не менялись. Новых endpoints нет; tenant берётся из команды,
не из глобального job lookup. Все методы **без commit**; exception после начала
consumption требует rollback всей транзакции. Legacy `create_run_with_tasks`
сохранён; старые services/workers ещё НЕ подключены к admission.

Контракты:
- `PendingParseIntent(job_id, equestrian_id, date_mode, date_from=None, date_to=None)`
  хранит исходное auto-намерение с NULL датами либо explicit диапазон. Hash включает
  tenant/mode/dates, но не меняющийся lifecycle, credentials или рассчитанное окно.
- `BindingLifecycle(binding_generation, cleanup_requested_seq, cleanup_completed_seq,
  account_id=None, counter_id=None)` — проверенный authoritative snapshot без секрета,
  загруженный **до** открытия короткой транзакции. Неполные binding identity и
  некорректные watermark отклоняются. При недоступности peer не подставлять нули:
  retry/nak до enqueue, либо оставить уже сохранённый intent pending.
- `enqueue_intent(intent, lifecycle=...)` под tenant lock сохраняет исходный snapshot
  generation/observed requested seq и NULL auto-даты. Возвращает `ready` либо
  `deferred`; это ещё не run. При maintenance (включая authoritative requested>
  completed до доставки NATS) даже существующий active run не теряет новый intent.
  Вне maintenance `active_run` сохраняет прежний rejection без вставки intent.
  Тот же job+hash — `duplicate` независимо от нового snapshot; foreign tenant или
  другой hash — безопасный conflict. Для legacy job без intent действует прежний
  ParseJobAlreadyExistsError, поскольку его полный command hash восстановить нельзя.
- `ready_intents(limit=1..1000)` — bounded discovery pending по next_attempt_at;
  НЕ claim. После rollback и рестарта данные остаются. `defer_intent(intent,
  next_attempt_at=<aware datetime>)` планирует повтор, не возрождает terminal строки.
- `prepare_admission(intent, lifecycle=...)` под tenant→account locks возвращает
  `ready` только после сверки свежего snapshot, обоих gate, next_attempt_at и active
  queued/running tasks tenant. До `ready` auto-окно/credentials не разворачивать.
  Local generation/requested только возрастают; local completed принадлежит ledger
  и НЕ повышается по peer snapshot. Старый snapshot fail-closed; local merge и
  awaiting_seo также удерживают gate даже при завершённом upstream cleanup.
- Если observed requested сохранённого intent ниже local requested, он получает
  terminal `cancelled`. `cancel_before_cleanup(tenant, cleanup_seq=N)` делает то же
  для pending observed<N, НЕ трогая новые observed=N, admitted и чужие tenant.
  Поздний повтор enqueue не переписывает observation и не воскрешает cancelled.
- Уже сохранённый intent при active run возвращает `deferred`, а не active-run
  rejection; после его окончания повтор допускается. Если gate открыт, но binding
  отсутствует, состояние — terminal `missing_binding`, error_code=binding_missing.
- После `ready` M-2 вычисляет auto-окно локальными DB reads в **той же транзакции**,
  строит run/tasks с исходным job_id, но актуальными generation/requested/binding,
  вызывает `consume_intent(intent, lifecycle=..., run=..., tasks=...)`. Метод повторно
  проверяет admission и identity всех tasks, затем атомарно insert run/tasks + CAS
  pending→admitted/run_id; успех — `admitted`. При изменении gate возвращает deferred
  без вставки; при нарушении CAS — rollback всего effect. На duplicate получить
  ранее созданный run можно через tenant/hash-qualified `admitted_run(intent)`.

Account lock здесь сериализует admission; account exclusivity running tasks,
parse task claim/lease fencing, retry/reap и generation reconciliation остаются
**M-2**, effects/outbox/worker lifecycle — M-3/M-4. Не держать locks при HTTP/NATS.
Свежую authoritative проверку перед parse claim M-2 обязан выполнить отдельно:
transaction-local admission не является worker lease или разрешением поздней записи.

Проверки: unit `tests/unit/test_maintenance_intents.py` покрывает admission UT-D-04..06;
PG `tests/integration/test_maintenance_intents_postgres.py` содержит restart/dedup,
rollback run+tasks+intent, local/peer barrier, missing binding и tenant fixtures.
Это prerequisites IT-D-02, не acceptance удаления/суммирования статистик.
M-1b2b targeted suite: **120 passed, 2 skipped** (не PostgreSQL acceptance).
Ruff/format и scoped basedpyright проверяются отдельно. Real-PG acceptance
**BLOCKED: TEST_DATABASE_URL отсутствует**, повторный infrastructure запуск не делался.
Evidence: `.qa/106-107-metrica-cleanup-page-merge/M-1b2b/`.

### M-2a: lifecycle-aware enqueue (интеграция admission)

`MetricaBinding` требует `binding_generation`, `cleanup_requested_seq` и
`cleanup_completed_seq` даже при `counter=null`: только неотрицательные целые,
completed ≤ requested; существующая привязка требует generation > 0. Отсутствующие,
некорректные поля и peer outage не заменяются нулями: безопасная ошибка интеграции
до открытия транзакции, consumer повторяет доставку. Проверка tenant у counter
сохраняется. HTTP endpoints и peer auth не изменены.

`ParseEnqueueService(..., intent_repository: PendingParseIntentProtocol, ...)`
использует одну с репозиторием сессию. `enqueue(data=..., job_id=...)` возвращает
`ParseEnqueueOutDto | None`; `None` — устойчиво сохранённое ожидание либо терминальный
исход admission, а не транспортная ошибка. Команда с job_id сохраняет его в intent
и run; прямой внутренний вызов без job_id получает UUID. Старые строки run с NULL
job_id не изменяются. `ParseRunHandler` по-прежнему ACK только после возврата enqueue.

Порядок: peer snapshot **до транзакции** → `enqueue_intent` →
`prepare_admission` → только при ready чтение последних дат и вычисление auto-окна →
`consume_intent` (повторная проверка) → общий commit intent/run/tasks → Celery signal.
Run/tasks получают generation и observed requested из authoritative snapshot.
При maintenance сохраняются исходные auto без дат и post-delete observed seq;
`counter=null` не приводит к преждевременному пропуску команды. Active run вне
maintenance и duplicate сохраняют прежние ACK outcomes. Missing binding становится
терминальным intent и коммитится **до** соответствующего ACK outcome. Ошибка commit
распространяется: не ACK, не dispatch. Брокерный сбой после commit не теряет run.

Это ограниченный admission slice, не готовность всего maintenance runtime к запуску.
Dispatcher/claim реализованы в M-2b (контракт ниже). M-2c должен подключить running drain,
транзакционные lease/token/generation fences для записи/reconcile, terminal cancellation
expired running (включая generation=0) и периодический recovery pending intent с новым
snapshot/auto-окном. До этих частей нельзя запускать maintenance workers рядом с
неограждёнными parse workers. Новые post-delete intents нельзя отменять по времени
доставки NATS: критерий — сохранённый observed requested < cleanup seq.


### M-2b: authoritative dispatcher/claim и отмена queued

`ParseDispatchService(..., yandex_client=...)` сначала завершает discovery-транзакцию,
затем получает authenticated lifecycle без кэша для каждого tenant. Peer outage/401/403
и чужой counter fail closed, не блокируя обработку следующего tenant. Claim повторяет
HTTP-проверку независимо от предшествующего dispatch. HTTP не выполняется под lock.
`TaskRunner` получает `dispatch_service` из DI; прямого старого claim без snapshot нет.

Repository API (общая session, методы **не коммитят**):
- `list_queued_tenants(account_id=None)` — только discovery, включая будущие retries,
  с порядком по старейшему run; не разрешение на запуск.
- `list_dispatchable_accounts(*, equestrian_id, lifecycle)` и
  `claim(account_id, *, equestrian_id, lifecycle, locked_by, lease_seconds)` — короткий
  tenant→account lock, монотонная observation generation/requested, проверка peer
  barrier и локального selector cleanup>merge>parse. Claim атомарно сверяет tenant,
  generation, observed requested, account/counter и account running exclusivity.
- `cancel_queued_before_cleanup(equestrian_id, *, cleanup_seq)` — для cleanup orchestration:
  только queued с observed requested < seq, включая delayed retry; terminal cancelled,
  finished_at, безопасный binding_changed. Run, checkpoints, attempts и counters сохраняются.
  Этот метод не трогает intents: их отдельный `cancel_before_cleanup` использует тот же
  строгий observed<seq, поэтому новые post-delete intents остаются pending.

Перед dispatch/claim queued старее локальных generation/requested отменяются. Merge
только приостанавливает queued, не отменяет их. Старый snapshot, пересёкшийся с локальным
barrier, не запускает работу. Если DELETE ещё не наблюдался локально и snapshot получен
до DELETE, такой claim считается старой работой: M-2c обязан её дренировать/fence.
Общие `_observe`/`_blocked` вынесены из admission в `MaintenanceGateRepository`;
локальный completed продвигается только ledger, не authoritative snapshot.

**Maintenance runtime всё ещё не включать.** `schedule_retry`, expired-running reaper,
ingestion/reconcile и deferred replay остаются M-2c: старый running retry нельзя вернуть
в исполнимый queued, expired/gen0 нужно terminalize с fencing, разрешённую running
попытку дренировать до начала удаления. Текущий discovery `has_more` — только подсказка
для сигнала, следующий claim обязательно получает новый snapshot. M-3 должен вызвать
queued/intent cancellation в общей cleanup transaction. HTTP/access policy неизменна.

M-2b evidence: `.qa/106-107-metrica-cleanup-page-merge/M-2b/`; unit/worker regressions
и scoped Ruff/format/basedpyright. PG fixtures расширены UT-D-04/05/06, но реальная
PG acceptance остаётся BLOCKED без TEST_DATABASE_URL; skip не считается доказательством.

### M-2c1: deferred replay (ограниченный slice M-2c)

`ParseEnqueueService.replay_intent(intent)` повторно читает authoritative binding
**до транзакции**, затем вызывает prepare→локальный свежий расчёт окна→consume→commit.
Никакого повторного `enqueue_intent` нет: исходный job_id/tenant/hash сохраняется,
старые admitted/cancelled/missing-binding intents не возрождаются. При active run,
cleanup, merge или ожидании SEO-finalized окно ещё не вычисляется. Текущий binding,
а не сохранённый до обслуживания account/counter, используется для новых tasks.
Пустая после cleanup статистика даёт новое двухлетнее auto-окно; explicit даты
сохраняют прежние правила. Missing binding коммитит терминальный исход; peer outage
не меняет сохранённый intent. Dispatch происходит только после commit, его потеря
восстанавливается обычным queued dispatcher.

`services_container.parse_intent_replay_service()` — per-session DI factory;
`replay_ready(limit=100)` (1..1000) — интерфейс для M-3 periodic recovery после
финализации обслуживания. Discovery коммитится до HTTP; каждый intent проходит
отдельную admission-транзакцию. Ожидающие/ошибочные candidates откладываются на
60 секунд, чтобы ограниченная выборка не голодала из-за одного tenant. Это delay
polling, не worker lease; два recoverer безопасны благодаря prepare/consume под
tenant/account locks. Сбой discovery/defer распространяется; crash после admission
не теряет run, повтор не создаёт второй. Ошибки логируются без текста исключения.

Runtime/beat/maintenance workers **не активированы**. M-3 подключает recovery к
периодическому dispatch и может вызывать его после финализации (отложенные candidates
доступны в следующем polling). **Drain/fencing readiness ещё НЕ достигнута**:
M-2.5 (ingestion/reconcile tenant+lease fences, retry/reap cancellation и expired/gen0
recovery) и полный M-2.7 остаются обязательными до запуска cleanup/merge workers.
Текущие проверки replay не являются acceptance IT-D-02; PostgreSQL acceptance
BLOCKED без TEST_DATABASE_URL, отсутствие URL не заменяется skipped=PASS.


### M-2c2: running write/reconcile guards (частичная readiness)

`ParseTaskRepository.assert_writable(task_id, locked_by=...)` — обязательный guard
в **той же session/transaction**, что stats effect и checkpoint/terminal CAS.
Он получает tenant→account advisory locks, затем task row lock, проверяет
`running`, owner token и неистёкший lease через DB clock. Ingestion берёт guard
перед upsert каждой страницы, TaskRunner — перед reconcile окна. Повторная
проверка в owned update/CAS ловит expiry после stats effect; `LeaseLostError`
должен откатить всю страницу/reconcile, без продвижения counters/checkpoint.
Нельзя коммитить guard отдельно от эффекта или держать его во время peer HTTP.

Pending cleanup/merge и pending authoritative watermark разрешают **только уже
running попытке** дописать оставшиеся страницы и завершить reconcile. Не требуется
совпадение с новым binding generation: это дренирование старой попытки, не новый
claim и не право использовать новые credentials. Исполняемое/неизвестное/failed
maintenance состояние и более новый completed cleanup seq запрещают запись.
Maintenance arbitration всё ещё ждёт терминализации всех running, включая expired
и generation=0; один факт expiry не разрешает очистку. Generation=0 не повышается.

**Cleanup/merge workers включать НЕЛЬЗЯ.** M-2c2 выполнен только в части атомарных
write/reconcile guards. Старые `schedule_retry`/`reap_expired_leases` ещё требуют
terminal cancellation при cleanup, authoritative lifecycle recheck, tenant→account
locking и bounded expired/gen0 recovery без resurrection. Factory пока сверяет
только tenant/account/counter: generation guard до credentials также остаётся
блокером rebind с теми же identity. Предложен следующий
slice M-2c2b после готового M-2c2a write/reconcile; M-2.5/7 остаются открытыми.
`replay_ready` M-2c1 не менялся; runtime M-3 не подключался.

UT-D-05/06 guards и rollback проверены targeted тестами. Новые реальные PG fixtures
`tests/integration/test_parse_write_gate_postgres.py` проверяют fencing/drain,
обе stats-таблицы и транзакционный rollback, но не удаление батчами или recovery.
В M-2c2 TEST_DATABASE_URL отсутствует: PostgreSQL acceptance **BLOCKED**, skips
обычной regression-команды не являются acceptance. Evidence:
`.qa/106-107-metrica-cleanup-page-merge/M-2c2/`.

### Retry/reap recovery (M-2c2b)

Текущая running попытка дренируется при pending cleanup/merge; новая попытка — нет.
Перед retry runner получает authoritative lifecycle вне транзакции. В той же
transaction `schedule_retry(..., lifecycle=...)` под tenant→account→task locks
наблюдает монотонный generation/requested watermark и переводит старую работу в
`cancelled`, даже если ещё не доставлена cleanup-команда. Причина исходной ошибки,
attempts, checkpoint и счётчики сохраняются. Pending merge сам по себе не отменяет
retry: queued ждёт gate. Generation=0 никогда автоматически не повышается.

Dispatcher использует `list_recovery_tenants(limit=100)` (допустимо 1..1000): только
running с expired/NULL lease; discovery без row locks. Для каждого tenant получает
свежий lifecycle вне transaction, затем отдельно коммитит
`reap_expired_leases(equestrian_id=..., lifecycle=...)`. Peer outage оставляет
expired running fenced до следующего recovery, без выдуманного lifecycle и новых
попыток. Reap повторно проверяет статус/DB-clock expiry под tenant→account locks:
старое поколение/cleanup seq и legacy generation=0 становятся cancelled; обычный
expired task — queued либо failed при исчерпании attempts. Живая lease, другие
tenant и post-delete intents не затрагиваются. Повтор reap идемпотентен; observation
и terminal transition откатываются вместе. Вызов reap без параметров сохранён как
локальный compatibility recovery (bounded, использует только сохранённый gate),
production dispatcher всегда передаёт tenant-qualified authoritative snapshot.

ParseJobFactory проверяет positive/equal generation и отсутствие завершённой
очистки новее task **до** запроса credentials. Pending requested watermark не
прерывает текущую попытку сам по себе; rebind даже с теми же counter/account UUID
не может подменить старые credentials. Lease fencing ingestion/reconcile остаётся
обязательным независимо от этой проверки. Старые ungated workers недопустимы.

Это подготовка interfaces для M-3, не включение runtime: maintenance workers,
queues и periodic replay hook ещё не подключены. Real-PG recovery/rollback acceptance
требует выделенную TEST_DATABASE_URL; collection/unit SQL tests её не заменяют.
Retryable ошибка, включая последнюю попытку, всегда проходит через fenced
`schedule_retry`: cancellation имеет приоритет над exhausted→failed.
Лог runner `status=retry_resolved` означает repository-решение queued/failed/cancelled,
а не обещание нового запуска; окончательный статус хранится в task ledger.

### M-3a: bounded cleanup storage (код готов, acceptance не выполнен)

`services_container.cleanup_batch_service().run_batch(lease)` — ленивый per-session
interface **одного батча**. Передавать только сохранённую cleanup lease в `deleting`;
service открывает короткую transaction на той же session, что repository. Никакой
HTTP/NATS I/O под locks. Claim/drain/recovery, supersession merge, result outbox,
relay и periodic dispatch в этот slice не входят. Новых HTTP endpoints нет.

Настройки: `CLEANUP_BATCH_SIZE=1000` (1..10000), `MAINTENANCE_LEASE_SECONDS=60`
(положительное число секунд). Один вызов удаляет не более batch-size строк из
**одной** таблицы, сначала pages, затем search queries. Выборка первых оставшихся
PK по `(equestrian_id,id)` без OFFSET использует существующие tenant/PK indexes.
Удалённые ключи — устойчивый keyset cursor; UUID lower bound не сохраняется, чтобы
повтор не мог пропустить оставшиеся строки. `DELETE ... RETURNING` агрегируется
`count(*)` на стороне PostgreSQL, UUID не загружаются в Python. Ledger checkpoint
`{"table":"pages|search_queries|exhausted"}` продвигает таблицу при неполном батче;
при точном кратном лимиту требуется следующий пустой батч.

Guard до effect и checkpoint CAS после effect проверяют tenant, owner, fencing
revision, DB-clock expiry, приоритет и отсутствие **любых** running parse, включая
expired/gen0. Effect, checkpoint и lease renewal коммитятся вместе; исключение
или потеря lease откатывают весь батч. После restart продолжать с актуальной lease
и ledger; при expiry новый owner делает обычный claim, старая revision fenced.
`exhausted=True` возвращается после commit и проверки пустоты **обеих** таблиц под
tenant lock; это **НЕ completed**, не PubAck и не право запускать parse. Ledger
остаётся `deleting`, cleanup_completed_seq не меняется. Будущий finalizer обязан
снова проверить lease/пустоту и атомарно записать completed + result outbox, а не
доверять результату ранее завершённого вызова.

`tests/integration/test_cleanup_batch_postgres.py` — выделенные IT-D-02 storage
fixtures: обе таблицы >2 батчей, exact limit/empty, другой tenant, crash до commit,
restart после commit, stale owner, expiry между DELETE и checkpoint, running drain.
Запуск только с выделенной `TEST_DATABASE_URL` (`postgresql+asyncpg`, БД `*_test`):
`uv run pytest tests/integration/test_cleanup_batch_postgres.py -m infrastructure`.
Отсутствие URL — BLOCKED, collection/unit checks/skips не являются acceptance.

**Operational readiness НЕ подтверждена.** В M-3a TEST_DATABASE_URL и TEST_NATS_URL
отсутствуют. M-1.4/M-2.5/real-PG IT-D-02 остаются открытыми. Maintenance consumers,
Celery queues/beat/compose workers и deferred replay hook пока не подключены и не
активированы; перед активацией нужны реальные PG/NATS checks и подтверждение
отсутствия старых ungated workers. Нельзя считать этот storage interface готовым
cleanup pipeline. AsyncAPI/transport — следующий отдельный bounded slice, merge
algorithm остаётся M-4.

### M-3b: maintenance transport (пока без runtime activation)

`NatsJetstreamClient.setup_maintenance()` — явный opt-in: создаёт отсутствующий
**только** принадлежащий этому сервису `METRICA_MAINTENANCE_RESULTS` с subjects
`events.metrica.cleanup.completed`, `events.metrica.pages.merge.completed`.
FILE/LIMITS, max_age=0, max_msgs/max_bytes=-1, discard=new, duplicate_window=120s.
До регистрации consumers проверяются stream_info всех maintenance streams по конфигу
владельца: точные subjects, FILE/LIMITS, отсутствие TTL/лимитов сообщений и байтов,
discard=new и окно 120s. Существующие streams/durables не актуализируются молча.
Durable: pull/ALL/explicit/30s/-1, точный filter, без backoff/push/headers-only.
Чужие streams никогда не создаются. NotFound и transient network/503 получают
bounded retry (10 попыток, линейный backoff 2s); несовместимость —
`MaintenanceTopologyError` (BadRequestError), fail-fast существующего supervisor.
Исправление существующего несовместимого ресурса требует отдельного решения оператора.
Прежний `setup()`/parse durable не изменяет maintenance topology.

| Чужой stream / owner | Subject | Durable этого сервиса |
|---|---|---|
| YANDEX_LIFECYCLE / yandex-service | events.yandex.metrica.cleanup.requested | ya-metrica-cleanup-requested |
| SEO_MAINTENANCE / seo-service | commands.seo.pages.merge | ya-metrica-pages-merge |
| SEO_MAINTENANCE / seo-service | events.seo.pages.merge.finalized | ya-metrica-pages-merge-finalized |

Все три — pull/ALL/explicit, ack_wait=30s, max_deliver=-1. Result stream читают
`yandex-metrica-cleanup-completed`, `seo-metrica-cleanup-completed` и
`seo-metrica-pages-merge-completed`; **их durables создают соответствующие consumers,
не Метрика**. AsyncAPI содержит все пять maintenance направлений и обе формы merge.
Envelope, UTC timestamp, bigint ranges, UUID, subject и `Nats-Msg-Id` проверяются
до storage. Лимит полного body 256 KiB; 1..500 mappings, source != target, уникальный
source внутри части. Tenant берётся только из payload, не из hint headers.
Membership/source uniqueness по всей ревизии гарантирует SEO producer.

`MaintenanceHandler` → `MaintenanceDeliveryService.receive` → существующий
`MaintenanceStorageProtocol`: одна короткая transaction inbox + operation/chunk/seal
либо correlated finalized. Payload hash сравнивается и после broker duplicate window;
seal может прийти до частей. Неизвестный/чужой finalized не создаёт operation и не
снимает gate. Commit завершается **до ACK**; storage/commit failure → NAK 1,2,4…60s,
эскалация с пятой delivery без retry cap. Invalid/schema/hash/correlation → TERM;
payload, headers, exception text и secrets в диагностике не выводятся.

Identity suffix: requested; part_index; seal; completed (cleanup result);
completed|superseded_by_delete (merge result); finalized:<result_message_id>.
ID = uuid5(NAMESPACE_URL, `eqcms:maintenance:v1:<subject>:<operation_id>:<suffix>`),
равен payload.message_id и Nats-Msg-Id. Producer сохраняет полный envelope один раз;
новый occurred_at при повторе того же ID — hash conflict, а не новая операция.

**Readiness не достигнута.** `create_maintenance_consumers(client, settings)` — lazy
DI factory, новая session на delivery. API lifespan её не запускает. Никакой stats
работы, dispatch, Celery queues, beat, result finalizer/outbox relay в consumers нет.
M-3c1/M-3c2 обеспечивают cleanup execution/preemption + transactional result/outbox;
M-3d — supervision всех трёх consumers, durable ledger replay и runtime wiring.
Не запускать workers до M-1.4/M-2.5/IT-D-02 и последующих acceptance gates.
HTTP endpoints/access/peer outcomes не меняются; production требует сетевой изоляции.

Targeted regression: `uv run pytest tests -k "cleanup or maintenance or nats" -m
"not infrastructure"`. Это не acceptance: реальные TEST_DATABASE_URL/TEST_NATS_URL
обязательны для PostgreSQL/JetStream gates, отсутствие URL/skip не считается PASS.
После потери broker storage восстановление — контролируемый replay producer ledger,
а не ручное снятие tenant gate. Мониторинг disk budget обязателен для retained streams.

### M-3c: cleanup finalizer и result relay (runtime не активирован)

`CleanupFinalizerService.finalize(lease) -> bool` начинает **новую** короткую
transaction после committed exhausted batch. `CleanupBatchRepository.finalize_cleanup`
повторно проверяет tenant priority, отсутствие running parse и актуальные
owner/revision/expiry, затем пустоту **обеих** stats-таблиц. `False` означает наличие
строк: продолжить bounded cleanup, не публиковать результат. При успехе один commit
сохраняет cleanup `completed`, непрерывный локальный completed seq и pending result
outbox. Последний ownership CAS после вставки outbox откатывает всё при потере lease.
Повторный вызов со старой terminal lease отклоняется; повторную публикацию обеспечивает
outbox, а не повторная финализация. Другие cleanup/merge barriers не снимаются.

Result envelope сохраняется один раз с UTC timestamp и deterministic Msg-Id
`maintenance_message_id(CLEANUP_COMPLETED, operation_id, "completed")`. Повтор с тем
же ID и изменённым payload/hash — conflict, даже вне broker duplicate window.
`MaintenanceResultOutboxRepository.save_result` также подготовлен для merge result;
superseded merge создаётся admission-time repository (M-3c2); completed merge реализован M-4b.
Никакие SEO-страницы finalizer/relay не удаляют.

`MaintenanceResultRelay.relay_once(owner=UUID) -> bool` обрабатывает **одну** запись:
короткий `FOR UPDATE SKIP LOCKED` claim, commit, публикация **без DB transaction**,
затем отдельный fenced mark после PubAck. `False` — нет ready записей, `True` —
была попытка (включая publish failure). `MaintenanceResultPublisher` adapter использует
существующий connected `NatsJetstreamClient`, timeout 5s, сохранённые payload и
`Nats-Msg-Id`; проверяет positive sequence и owned result stream. Duplicate PubAck
считается доставкой и явно логируется warning. Отсутствие/ошибка PubAck оставляет
pending с retry 1..60s без terminal cap; с пятой ошибки выдаётся безопасная эскалация.

После crash до/после publish или PubAck до mark запись reclaimable по истечении
`MAINTENANCE_LEASE_SECONDS` (default 60). Новый claim увеличивает fencing revision;
старый worker не может отметить доставку или изменить retry. Ошибка mark/commit
пробрасывается: не создавать новый envelope, дождаться expiry и повторить тот же ID.
После потери broker storage published outbox не переоткрывается автоматически:
оператор сверяет producer ledger/consumer inbox; gate нельзя разрешать вручную лишь
по истечению lease. Поля ошибки не содержат payload или exception text.

DI factories `cleanup_finalizer_service` и `maintenance_result_relay` ленивые и
используют одну injected session. Для relay требуется явная dependency
`maintenance_result_publisher=MaintenanceResultPublisher(client=..., settings=...)`.
M-3d подключает supervision/ticks только после readiness gates. На данном этапе
cleanup worker/drain и immediate merge supersession реализованы M-3c2 (см. ниже);
queues/beat/deferred replay не подключены. Runtime **не включать**.

Targeted tests: `uv run pytest tests -k "cleanup or maintenance or nats" -m "not infrastructure"`.
Real-PG fixtures: `tests/integration/test_maintenance_result_postgres.py` (atomic
rollback, stats recheck, restart/lost PubAck + stale-owner fence). Они требуют
выделенную `TEST_DATABASE_URL`; отсутствие URL/skip — **не acceptance**.


### M-3c2: bounded worker/drain/preemption (runtime выключен)

Ленивая DI `cleanup_worker_service().tick(tenant, owner=UUID)` выполняет **не более
одного** stats-батча. Claim/drain commit предшествует batch; exhausted вызывает
готовый finalizer в новой transaction. Результаты tick:
- `waiting`: нет исполнимого head (running drain, live lease, missing seq/command,
  delayed/failed-blocked head или уже completed); это не разрешение parse;
- `progress`: один батч committed, lease fenced/released для следующего claim;
- `completed`: fresh finalizer атомарно сохранил completed/prefix/result outbox;
- `lease_lost`: transaction откатилась, чужой/новый owner продолжает по ledger.

Иные storage/commit ошибки пробрасываются, runtime должен повторить сигнал.
После crash/cancellation lease сохраняется до expiry; новый claim повышает fence,
committed checkpoint не теряется. Нет автоматического сброса failed-blocked,
terminal result не создаётся на ошибке. Старый worker не может yield/finalize.

Claim отменяет queued старого observed seq и старые pending intents, не новые
post-delete intents. Live running попытка остаётся running, cleanup — draining;
expired attempts terminalize через существующий reaper (tenant → account locks).
Только отсутствие running позволяет deleting. Seq выполняются непрерывным префиксом,
между cleanup нет parse; failed/delayed head нельзя обойти. Каждый батч повторяет
priority/lease checks, никакая очередь не заменяет PostgreSQL fences.

`maintenance_storage_repository` в DI теперь создаёт
`MaintenanceDeliveryRepository`: при сохранении cleanup немедленно отменяются
collecting/waiting/running/failed-blocked merge; при новом merge во время cleanup
результат создаётся до chunks/seal. Всё в той же inbox/ledger transaction, без
stats SQL или dispatch в NATS consumer. Отмена отзывает lease и сохраняет
`superseded_by_delete → awaiting_seo` + immutable correlated pending outbox.
Дубликаты/late parts не выполняют merge и не пересоздают report, следующий cleanup
не переписывает исходную correlation. Уже `awaiting_seo` normal completed merge
сохраняет исходный результат. Null part_count допустим для раннего report;
он **не разрешает SEO delete** до correlated cleanup completed и scan seal.

M-3d должен подключить bounded dispatch/повторные сигналы по durable ledger,
queues/beat, consumer supervision и result relay; `progress` требует следующего tick,
`waiting` — последующей переоценки, не busy-loop. Runtime/queues/compose этим unit
не включены. Merge algorithm и completed merge — M-4. Новых HTTP endpoints нет.

Targeted worker tests: `uv run pytest tests/unit/test_cleanup_worker.py -q`.
Real-PG fixture: `tests/integration/test_cleanup_worker_postgres.py` (drain/expiry,
claim restart, rollback correlated outbox, immediate supersession, late parts,
bounded обе таблицы/два seq/чужой tenant). Для исполнения нужен выделенный
`TEST_DATABASE_URL`; отсутствие URL — blocked, collection/skip не acceptance.

### M-3d: runtime, очереди и восстановление (по умолчанию OFF)

`MAINTENANCE_RUNTIME_ENABLED=false` не создаёт maintenance consumers/relay в API
и отключает maintenance dispatch/ticks/replay в Celery. Это deployment gate,
**не доказательство readiness**. `/health` и Celery ping также не доказывают
наличие миграций, корректность streams/durables и работу fenced транзакций.

Перед включением необходимы реальные PostgreSQL acceptance M-1.4/M-2.5/M-3.3
(включая IT-D-02), NATS topology/consumer/PubAck/restart acceptance и проверка
финальных 105 peer lifecycle guards. До этого не запускать maintenance workers.
В этом unit стек не запускался, operational readiness НЕ подтверждена.

Service Compose использует opt-in профиль `metrica-runtime`: parse worker слушает
только `ya_metrica`, cleanup worker — только `ya_metrica_cleanup`, единственный beat
восстанавливает сигналы. Отдельный профиль `metrica-merge` зарезервирован для M-4:
worker слушает только `ya_metrica_merge`, но запускать его до реального handler и
его acceptance запрещено. Cleanup/merge commands требуют флаг `true`.
При rollout остановить/пересоздать старые workers/beat со старым образом/подписками;
профиль не останавливает уже работающий старый контейнер. Не оставлять старые
ungated workers параллельно. Volumes не сбрасывать.

| Интерфейс | Семантика |
|---|---|
| `metrica.cleanup_tick(tenant: str)` | `ya_metrica_cleanup`, fresh owner UUID, максимум один bounded batch |
| `metrica.merge_tick(tenant: str)` | `ya_metrica_merge`, без handler возвращает `unavailable`, не claim/completed |
| `metrica.dispatch_maintenance()` | `ya_metrica`, bounded discovery в PG, commit до отправки сигналов |
| `metrica.replay_intents()` | `ya_metrica`, готовый `replay_ready(limit=100)`, свежие binding/auto-window |
| `ServicesContainer.merge_worker_service` | M-4b: реальная ленивая Factory `tick(tenant, *, owner: UUID) -> str`, fenced claim/один батч; runtime по умолчанию OFF |

Все три periodic dispatch/replay записи используют существующий
`PARSE_DISPATCH_INTERVAL_SECONDS` (30s). `MAINTENANCE_DISPATCH_LIMIT` (100, 1..1000)
ограничивает число tenants на kind за проход. Merge discovery отключён, пока DI
handler отсутствует; незавершённые merge продолжают блокировать parse. Cleanup
всегда выше merge, failed/delayed head не обходится. Redis содержит только сигналы:
никакой отметки «доставлено» в ledger. Bounded discovery под tenant locks
сдвигает due `next_attempt_at` к текущему времени (не в будущее), чтобы ожидающий
первый limit tenants не голодал остальные; claim не откладывается. Потерянный сигнал/ошибка Redis/падение worker
восстанавливаются следующим beat; live lease не dispatch-ится, expired получает
новый fenced claim. `waiting` дренирования повторяется через beat, не busy-loop.
После завершения обслуживания отдельный periodic replay подбирает durable intents,
включая потерю сигнала после финализации и ожидание окончания active run.

API запускает отдельное maintenance NATS-соединение: `supervise_maintenance(client,
consumers, relay)` выполняет setup owned result stream и lookup foreign streams,
затем три pull consumers и relay. Любой выход/ошибка дочерней задачи перезапускает
всю группу с backoff 1..30s; incompatible config (`BadRequest`) останавливает группу
с безопасной диагностикой. Частичный startup и shutdown останавливают все consumers,
отменяют/дожидаются relay, закрывают connection. Parse transport независим.
`MAINTENANCE_RELAY_INTERVAL_SECONDS` (1s) задаёт интервал, один outbox claim на tick,
свежая session/owner; DB ошибка перезапускает группу, publish ошибка использует
устойчивый outbox retry 1..60s. PubAck до DB mark обязателен. Crash после PubAck
восстанавливается через expiry claim и идемпотентный result message ID; не очищать
inbox/outbox вручную. Секреты, URLs, payload и exception text не логируются.

### Historical merge storage: checkpoint M-4a (runtime остаётся OFF)

Общие исходные метрики описаны в `core/entities/metrica_metrics.py`:
`visits/users/pageviews/bounces/visit_duration_seconds` складываются без обрезания;
`users` — аддитивная оценка, не межстраничный distinct. Provider rates/average
переводятся в целые totals с прежним округлением ingestion. Generated ratios
БД пересчитываются из totals, проценты и средние не складываются.

`MergeBatchRepository.move_batch(lease, *, batch_size, lease_seconds)` реализует
`MergeBatchProtocol`; репозиторий и вызывающая транзакция используют **одну сессию**.
Вызов обязательно оборачивается в `async with session.begin()`; метод не коммитит.
`batch_size` = 1..1000. Только page stats, никогда search-query stats. За вызов
читается одна часть (максимум 500 mappings) и до batch_size дат одного source.
Tenant fence берётся до статистик, source блокируется, target конфликт по
`(tenant,date,page)` разрешается отдельным additive upsert (НЕ parse upsert).
Target `parse_task_id` сбрасывается в NULL даже при collision. Addition, source
DELETE, chunk checkpoint и lease renewal составляют одну транзакцию.

Chunk checkpoint `{"mapping_index": N}` указывает текущую mapping; удалённые даты
source служат durable keyset (первый оставшийся source/date, без OFFSET). При
неполном батче индекс продвигается; при полном нужен следующий bounded вызов,
включая пустой, чтобы доказать исчерпание. Результат `MergeBatch` содержит
part/mapping index, moved_rows и part_completed; `None` означает отсутствие
незавершённых частей, **не** completed операции и **не** снятие parse gate.
После crash до commit откатываются суммы/source/checkpoint; после commit новая
сессия продолжает оставшиеся даты/части. При lease loss/preemption или overflow
весь вызов должен откатиться; запрещены commit частичного эффекта, clamp и skip
ошибочной строки. Будущий worker обязан после rollback fenced-переходом оставить
`failed_blocked` для неисправимого overflow; этот storage slice не делает такой
переход и не публикует результат самостоятельно.

Worker lifecycle (claim/drain/yield/retry/overflow-block), result outbox,
correlated SEO finalized и реальный handler в DI реализованы следующим slice M-4b (ниже).
Ручной сброс checkpoint/барьера или выдача storage exhaustion за SEO finalized
недопустимы. Fresh parse после настоящей финализации заменяет merge aggregate и
выполняет обычный reconcile, не прибавляется к старому `parse_task_id=NULL`.
PG fixtures `test_merge_batch_postgres.py` требуют выделенную TEST_DATABASE_URL;
collection/unit SQL shape не являются PG acceptance. До PG/NATS readiness gates
`MAINTENANCE_RUNTIME_ENABLED=false`; на checkpoint M-4a handler ещё не был подключён.
M-4b подключает ленивый handler в коде, но НЕ включает стек/runtime.


### M-4b: bounded merge worker и finalized barrier (runtime OFF)

`MergeWorkerService.tick(tenant, *, owner)` теперь подключён к prepared
`metrica.merge_tick` и maintenance discovery через DI. Создание графа не выполняет
SQL/HTTP/NATS. Это готовность handler в коде, НЕ production readiness: перед
запуском профиля `metrica-merge` обязательны реальные PG/NATS acceptance M-4c/QG.
Новых HTTP endpoints и изменений tenant/peer access policy нет.

Один tick: короткая транзакция claim → отдельная транзакция одного `move_batch`
→ короткая транзакция finalizer/yield. Default batch_size=1000 (1..1000),
lease — `MAINTENANCE_LEASE_SECONDS`. Загружается одна часть <=500 mappings,
один source и <=batch_size дат; полной карты операции в памяти нет. Seal и все
части обязательны перед claim; число завершённых частей проверяется SQL COUNT.
Queued parse не отменяется ради merge; live running попытка дренируется, expired
попытка сначала reconcile/fence под tenant→account locks. Cleanup имеет приоритет
при claim и каждом batch/finalizer/yield: stale owner возвращает `lease_lost`.

Последний batch НЕ открывает parse gate. Finalizer атомарно сохраняет immutable
`MergeCompleted(status=completed)` в result outbox и переводит операцию в
`awaiting_seo`, освобождая lease. Существующий relay публикует с PubAck/retry.
Только доставка `MergeFinalized` с точными tenant/operation/revision/result_message_id
завершает конкретную операцию; дубликат идемпотентен, чужой/неизвестный результат
отклоняется. Остальные merge/cleanup barriers сохраняются. `awaiting_seo` не
исполняется повторно: при потере сообщения восстанавливают pending outbox/producer
ledger, а не вручную открывают gate.

Admission cleanup немедленно сохраняет `superseded_by_delete` + outbox в одной
транзакции, даже до seal; активный merge останавливается между батчами. Correlation
первой cleanup неизменна, late parts сохраняются без исполнения. Уже normal
`awaiting_seo` продолжает ждать исходный finalized; новая cleanup не заменяет
его report. Для superseded finalized обязательны seal и completed связанной
cleanup; SEO дополнительно отвечает за полный scan и безопасное удаление sources.

Сбой до commit откатывает source addition/delete/checkpoint вместе. Падение после
commit или cancellation оставляет durable lease: после expiry beat повторяет tick,
claim выдаёт новую fencing revision, source delete/checkpoint исключают двойные
суммы. SQLSTATE 22003 превращается в безопасный `merge_aggregate_overflow`: после
rollback отдельная fenced транзакция ставит `failed_blocked`, без completed result.
Другие DBAPI ошибки дают `merge_storage_failed`, durable exponential retry 1..60s;
после 5 последовательных ошибок — `failed_blocked`. Успешный yield сбрасывает счётчик.
Если lease уже потерян или принята cleanup, прежний owner не может записать даже
blocked/retry. При недоступной БД запись диагностики тоже может не состояться —
lease expiry сохраняет путь recovery. Unexpected errors не скрываются.
`failed_blocked` требует диагностики и контролируемого операторского решения;
автоматического clamp, удаления ledger или снятия gate нет.

Targeted unit tests: `tests/unit/test_merge_worker.py`, существующие merge batch,
maintenance storage/transport/arbitration и runtime tests. Они НЕ заменяют real-PG
atomicity/overflow/restart и real-NATS delivery. Полный suite — M-4c, runtime OFF.

### M-4c: recovery runbook и границы acceptance (runtime OFF)

Сначала различайте **кодовую готовность** и **проверку на инфраструктуре**.
`make lint` проверяет mypy/flake8, `make test` — non-infrastructure suite.
Даже зелёный `test` с PostgreSQL skips не доказывает atomicity. `make test-infra`
допустим только с выделенными `TEST_DATABASE_URL` (`postgresql+asyncpg`, имя БД
заканчивается `_test`) и `TEST_NATS_URL`. Значения/credentials не публиковать.
Отсутствующие URL — **BLOCKED**, не успешный skip. Не подставлять DATABASE_URL,
не включать рабочий стек и не сбрасывать volumes ради теста.

Порядок восстановления без обхода fencing:

1. **Crash/retry батча.** Сохранить ledger, source и target. Проверить состояние
   операции, lease expiry и checkpoint в одной tenant-корреляции. После исправления
   временной причины штатный dispatcher переотправляет сигнал; новый owner продолжает
   оставшиеся source/date с новой revision. До commit эффект отсутствует; после
   commit удалённые sources не суммируются второй раз. Не обнулять checkpoint.
2. **Overflow / failed_blocked.** Сохранить безопасный error_code и evidence,
   диагностировать диапазон исходных метрик. Нужен отдельный согласованный forward-fix
   и контролируемое возобновление, готового endpoint/operator reset здесь нет.
   Не обрезать totals, не пропускать строку, не выставлять completed вручную.
3. **Delete preemption/cancel.** Сохранить первую cleanup_operation_id/seq и
   immutable superseded result. Cleanup должна закончить обе stats-таблицы;
   late parts только сохраняются для протокола, не исполняются. SEO ждёт seal и
   коррелированный cleanup completed, удаляет только sources, затем шлёт finalized.
   Сам superseded report не даёт разрешения открыть parse gate.
4. **awaiting_seo / потеря finalized.** Сверить operation/tenant/revision и
   result_message_id с ledger/outbox обеих сторон. Pending result повторяет relay;
   при потере PubAck повторяется тот же envelope/Msg-Id. Если result уже published,
   восстановление из producer ledger — согласованная операционная процедура,
   не изменение gate в SQL. SEO finalized должен ссылаться на исходный result;
   неизвестный/чужой result отвергается, повтор корректного идемпотентен.
   Потеря брокерного storage не разрешает удалить inbox/outbox/ledger.
5. **Fresh parse.** Только после завершения всех локальных barriers и сверки
   authoritative binding разрешается обычный claim/replay. Canonical resolve
   возвращает surviving UUID. Первая запись нового task **заменяет** агрегат с
   parse_task_id=NULL; следующие API pages того же task складываются. Финальный
   reconcile удаляет в своём tenant/окне строки другого task, включая NULL;
   даты вне окна, search-query stats и чужой tenant не затрагиваются page reconcile.

Покрытие M-4c: `test_merge_parse_contract.py` проверяет SQL-ветки replacement/addition
и NULL-aware reconcile, ingestion regression — canonical resolve на нескольких API
pages; это unit-контракты, не PostgreSQL execution. `test_merge_batch_postgres.py`
покрывает storage batches/crash/restart/lease/ratios. Новые
`test_merge_cross_flow_postgres.py` запускаются на **Alembic head**, покрывают несколько
parts, worker→awaiting_seo→коррелированный finalized/redelivery→fresh task/reconcile,
overflow rollback→durable blocked и cleanup после committed merge batch с повтором
части. Collection этих fixtures не означает их исполнения. Full-service evidence:
`.qa/106-107-metrica-cleanup-page-merge/M-4c/`.

На checkpoint M-4c выделенные test URLs отсутствуют: реальные PG/NATS acceptance
остаются открыты, `MAINTENANCE_RUNTIME_ENABLED=false`. Unit/migration DDL consistency
не заменяет upgrade/compare_metadata и cross-flow транзакции на настоящем PostgreSQL.
