# Payment Service

Микросервис асинхронной обработки платежей. Принимает запросы на оплату, обрабатывает их через эмулятор платёжного шлюза и уведомляет клиента о результате через webhook.

**Стек:** FastAPI · Pydantic v2 · SQLAlchemy 2.0 (async) · PostgreSQL 18 · RabbitMQ 4.3 (FastStream) · Alembic · Docker · uv · Python 3.14

## Как это работает

```
 клиент ──POST /api/v1/payments──►  api ──одна транзакция──►  payments + outbox
                                                                   │
                         relay: FOR UPDATE SKIP LOCKED, publisher confirms
                                                                   ▼
              exchange payments ──payment.created──► очередь payments.new (quorum)
                                                                   │
                                                               consumer
                    эмуляция 2–5 с (90% успех) → статус в БД → webhook клиенту
                                                                   │ ошибка
                        ┌──────────────── временная ───────────────┴──── постоянная ──┐
                        ▼                                                             ▼
   payments.retry → payments.new.retry.{1,2,3} (TTL 1/2/4 с) → назад      payments.dlx → payments.new.dlq
                        └── после 3 ретраев ─────────────────────────────────────────┘
```

1. **API** в одной транзакции сохраняет платёж (`pending`) и событие `payment.created` в таблицу `outbox` и отвечает `202 Accepted`. С RabbitMQ API не работает вообще.
2. **Relay** — отдельный процесс. Он забирает неопубликованные события пачкой (`FOR UPDATE SKIP LOCKED`), публикует их и ждёт подтверждения брокера (publisher confirms), только после этого отмечает событие опубликованным.
3. **Consumer** читает `payments.new`, эмулирует обработку, условным `UPDATE` сохраняет статус и отправляет webhook на `webhook_url`.
4. **Ошибки:**
   - временные (БД, сеть, webhook 5xx/408/429) уходят в retry-очереди с задержкой 1 → 2 → 4 с;
   - после трёх ретраев сообщение попадает в DLQ;
   - постоянные ошибки (webhook 4xx, битое сообщение, платёж не найден) уходят в DLQ сразу.

   В DLQ сообщение приходит с историей ошибок в заголовках.

## Быстрый старт

Нужен Docker с Compose v2.

```bash
docker compose up --build
```

Ничего настраивать вручную не нужно: все значения по умолчанию заданы в `docker-compose.yml`. Миграции применяет одноразовый сервис `migrations`.

| Что | Адрес |
|---|---|
| Swagger UI | http://localhost:8000/docs |
| RabbitMQ management | http://localhost:15672 (guest / guest) |
| Webhook-mock: полученные уведомления | http://localhost:8001/received |
| API-ключ по умолчанию | `change-me` (переменная `API_KEY`) |

Сервисы compose: `postgres`, `rabbitmq`, `migrations` (одноразовый), `api`, `relay`, `consumer`, `webhook-mock` (демо-приёмник webhook). В ТЗ перечислены только `postgres`, `rabbitmq`, `api` и `consumer`; зачем нужны остальные, объяснено в разделе [Решения](#решения-и-компромиссы).

Остановить и удалить данные:

```bash
docker compose down -v
```

## Примеры запросов

Создать платёж. `webhook-mock` — адрес демо-приёмника внутри сети compose:

```bash
curl -i -X POST http://localhost:8000/api/v1/payments -H "X-API-Key: change-me" -H "Idempotency-Key: order-1042" -H "Content-Type: application/json" -d '{"amount": "1500.00", "currency": "RUB", "description": "Order #1042", "metadata": {"order_id": 1042}, "webhook_url": "http://webhook-mock:8080/hook"}'
```

Тело должно быть в UTF-8. Терминал Windows может отправить кириллицу в другой кодировке; тогда API ответит `422`.

```
HTTP/1.1 202 Accepted
{"payment_id":"01a1...","status":"pending","created_at":"2026-10-10T18:53:42.585933Z"}
```

Повторить тот же запрос с тем же ключом. Ответ будет тот же `202` с текущим состоянием платежа и заголовком `Idempotent-Replayed: true`; второй платёж не создаётся. Если отправить с этим ключом другое тело, придёт `409`:

```json
{"error": {"code": "idempotency_conflict", "message": "Idempotency-Key already used with a different request body"}}
```

Получить платёж. Через 2–5 секунд статус станет `succeeded` или `failed`:

```bash
curl http://localhost:8000/api/v1/payments/<payment_id> -H "X-API-Key: change-me"
```

```json
{"payment_id": "01a1...", "amount": "1500.00", "currency": "RUB", "description": "Order #1042",
 "metadata": {"order_id": 1042}, "status": "succeeded", "idempotency_key": "order-1042",
 "webhook_url": "http://webhook-mock:8080/hook", "created_at": "...", "processed_at": "..."}
```

Тело webhook, которое получает клиент (с заголовком `Idempotency-Key: <payment_id>`):

```json
{"event": "payment.processed", "payment_id": "01a1...", "status": "succeeded",
 "amount": "1500.00", "currency": "RUB", "processed_at": "..."}
```

### Ошибки

У всех ошибок одинаковый формат: `{"error": {"code": ..., "message": ...}}`. У `422` есть ещё поле `details: [{"field", "message"}]`.

| Код | `code` | Когда |
|---|---|---|
| 401 | `unauthorized` | нет `X-API-Key` или ключ неверный |
| 404 | `payment_not_found` | платёж не найден |
| 409 | `idempotency_conflict` | `Idempotency-Key` уже использован с другим телом |
| 422 | `validation_error` | невалидное тело, заголовок или `payment_id` |
| 503 | `service_unavailable` | БД временно недоступна (с `Retry-After`) |
| 500 | `internal_error` | всё остальное; детали только в логе сервера |

## Как увидеть ретраи и DLQ

У webhook-mock есть параметр `?fail=<код>`: с ним мок всегда отвечает этим кодом.

- `"webhook_url": "http://webhook-mock:8080/hook?fail=500"` — первая попытка, затем 3 ретрая с паузами примерно 1, 2 и 4 с, затем DLQ (`x-error-kind: retries_exhausted`). Статус платежа при этом сохраняется, повторяется только отправка webhook.
- `"webhook_url": "http://webhook-mock:8080/hook?fail=404"` — сразу в DLQ (`x-error-kind: permanent`).
- `WEBHOOK_MOCK_FAIL_RATE=0.5 docker compose up` — мок случайно отвечает 500 на половину запросов: так видно, что после ретрая большинство webhook всё-таки доставляется.

Где смотреть:

- `docker compose logs -f consumer`: строки `message scheduled for retry ... retry=1/3` и `message moved to DLQ`;
- http://localhost:8001/received: все попытки доставки и коды ответов;
- RabbitMQ UI → Queues → `payments.new.dlq` → *Get messages*: заголовки `x-errors` (история «attempt N: Тип: сообщение»), `x-attempts`, `x-error-kind`, `x-failed-at`, `x-original-routing-key`.

Масштабирование: `SKIP LOCKED` в relay и идемпотентность consumer позволяют запускать несколько экземпляров.

```bash
docker compose up --build --scale consumer=3 --scale relay=2
```

## Локальный запуск без Docker (для отладки)

Postgres и RabbitMQ поднимаются в Docker, приложение запускается на хосте.

```bash
docker compose up -d postgres rabbitmq
```

```bash
cp .env.example .env
```

```bash
uv sync
```

```bash
uv run alembic upgrade head
```

Затем в отдельных терминалах:

```bash
uv run uvicorn app.api.app:create_app --factory --reload
```

```bash
uv run python -m app.entrypoints.relay
```

```bash
uv run python -m app.entrypoints.consumer
```

```bash
uv run uvicorn app.entrypoints.webhook_mock:app --port 8001
```

Postgres на хосте слушает порт **5433**, потому что 5432 часто занят локально установленным PostgreSQL. При запуске на хосте `webhook_url` указывайте как `http://localhost:8001/hook`.

## Конфигурация

Все значения берутся из переменных окружения. Для запуска на хосте их также читает `.env` (шаблон — `.env.example`).

| Переменная | По умолчанию (compose) | Назначение |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://app:app@postgres:5432/payments` | подключение к БД |
| `RABBITMQ_URL` | `amqp://guest:guest@rabbitmq:5672/` | подключение к брокеру |
| `API_KEY` | `change-me` | статический ключ для `X-API-Key`: только печатные ASCII-символы без пробелов |
| `RELAY_BATCH_SIZE` | `100` | размер пачки relay |
| `RELAY_POLL_INTERVAL` | `0.5` | пауза relay, когда outbox пуст, в секундах |
| `CONSUMER_PREFETCH` | `10` | сколько платежей consumer обрабатывает параллельно |
| `WEBHOOK_TIMEOUT` | `5` | общий таймаут отправки webhook, в секундах |
| `GATEWAY_MIN_DELAY` / `GATEWAY_MAX_DELAY` | `2` / `5` | длительность эмуляции обработки |
| `GATEWAY_SUCCESS_RATE` | `0.9` | доля успешных платежей |
| `WEBHOOK_MOCK_FAIL_RATE` | `0` | доля случайных ответов 500 у webhook-mock |
| `LOG_LEVEL` | `INFO` | уровень логов |
| `API_HOST_PORT`, `POSTGRES_HOST_PORT`, `RABBITMQ_HOST_PORT`, `RABBITMQ_UI_HOST_PORT`, `WEBHOOK_MOCK_HOST_PORT` | `8000`, `5433`, `5672`, `15672`, `8001` | порты на хосте |

Число ретраев (3) и задержки (1/2/4 с) — константы в `app/infrastructure/broker/topology.py`, а не переменные окружения. TTL зашит в аргументы очередей, а аргументы существующей очереди изменить нельзя.

Все порты публикуются только на `127.0.0.1`. Чтобы открыть API наружу, используйте `docker-compose.override.yml` с `ports: ["8000:8000"]` или `ssh -L 8000:localhost:8000`. Для любого не локального запуска задайте свой `API_KEY`.

## Тесты

```bash
uv run pytest
```

Всего 275 тестов: 214 unit и 61 интеграционный. Интеграционным тестам нужен запущенный Docker: Postgres 18 и RabbitMQ 4.3 поднимаются через testcontainers, на случайных портах, и не мешают запущенному compose.

- Только unit-тесты: `uv run pytest tests/unit`
- Только интеграционные: `uv run pytest -m integration`
- Unit-тесты в Docker: `docker build --target test .` (сборка падает, если упал тест)
- Линтеры: `uv run ruff check .` и `uv run mypy` (strict, включая тесты)

Что покрыто:

- домен: хэш запроса, нормализация суммы;
- репозитории и UoW на настоящем Postgres: гонка двух транзакций с одним ключом, `SKIP LOCKED`, ограничения, соответствие моделей миграциям;
- `PaymentService`;
- HTTP API: коды, формат ошибок, валидация, точность сумм;
- топология RabbitMQ: маршрутизация, TTL-ретрай, DLX, delivery-limit;
- relay;
- consumer: идемпотентность шагов, гонки;
- классификация ответов webhook;
- маршрутизация ретраев и DLQ, включая мусорные заголовки;
- настройки и webhook-mock.

## Решения и компромиссы

**Outbox.** Платёж и событие пишутся в одной транзакции, поэтому платёж без события и событие без платежа невозможны. Relay отмечает событие опубликованным только после подтверждения от брокера (publisher confirms). Публикация идёт с `mandatory`, а `on_return_raises` превращает недоставляемое сообщение в ошибку, иначе оно молча пропало бы. Гарантия доставки — **at-least-once**: если публикация прошла, а коммит отметки нет, событие уйдёт повторно. Поэтому consumer идемпотентен.

**API не зависит от брокера.** Если RabbitMQ недоступен, платежи всё равно принимаются, а события уходят, когда брокер вернётся. `/ready` проверяет только БД.

**Аутентификация.** `X-API-Key` обязателен для всех эндпоинтов API (`/api/v1/*`). Без ключа намеренно доступны только служебные: `/health` и `/ready` (их вызывают healthcheck Docker и оркестратор, у которых ключа нет) и документация `/docs`, `/openapi.json`. Данных платежей они не отдают. Если документацию нужно закрыть, её отключают через `docs_url=None` или прячут за reverse proxy.

**Relay — отдельный сервис**, а не фоновая задача в API или consumer. Так API не зависит от брокера, а relay масштабируется независимо: `SKIP LOCKED` не даёт двум экземплярам взять одно событие. Частичный индекс `(id) WHERE published_at IS NULL` остаётся маленьким, сколько бы опубликованных событий ни накопилось в таблице.

**Идемпотентность создания.**
- `UNIQUE (idempotency_key)` + `INSERT … ON CONFLICT DO NOTHING RETURNING`.
- Повтор сравнивается по `request_hash` — sha256 нормализованного тела (`100` и `100.00`, порядок ключей metadata не важен).
- Тот же ключ с тем же телом — `202` и `Idempotent-Replayed: true`; с другим телом — `409`.
- При гонке двух одинаковых запросов второй INSERT ждёт на уникальном индексе, а при READ COMMITTED последующий SELECT видит закоммиченную строку. Это проверено детерминированным интеграционным тестом.
- Второй слой защиты — `UNIQUE (event_type, aggregate_id)` в outbox.

**Идемпотентность обработки.** Consumer работает в три коротких транзакции. Эмуляция (2–5 с) и HTTP-запрос идут вне транзакций, чтобы не держать соединение и блокировки. Каждый шаг проверяет, не сделан ли он раньше:
- статус сохраняется условным `UPDATE … WHERE status = 'pending'`;
- `webhook_sent_at` ставится только один раз.

Повторная доставка не обрабатывает платёж заново, а досылает только неотправленный webhook. Отказ шлюза (10%) — это бизнес-результат (`failed` + webhook), а не ошибка для ретрая.

**«3 попытки».** Трактуется как первая попытка плюс 3 ретрая с экспоненциальной задержкой 1 → 2 → 4 с, всего 4 попытки, затем DLQ.

**Retry-очереди с TTL, по одной на каждую задержку.** RabbitMQ удаляет просроченные сообщения только с головы очереди: в одной очереди с разными TTL короткая задержка ждала бы длинную. Сообщения без потребителей истекают и через DLX возвращаются в `payments.new`. TTL задан в коде. Для продакшена его стоит вынести в policy, но если policy не применилась, retry-очередь становится «вечной» без единой ошибки.

**Свой счётчик `x-attempt` вместо `x-death`.** Семантика `x-death` менялась между версиями RabbitMQ, и он считает проходы через очереди, а не попытки обработки.

Два счётчика независимы:
- `x-attempt` считает логические ретраи;
- `x-delivery-limit` (5) у quorum-очереди — страховка от «ядовитых» сообщений, которые роняют consumer. Он считает возвраты в очередь после падений. Consumer никогда не делает `nack(requeue=True)`: RabbitMQ 4.x не учитывает его в лимите.

Заголовки: `x-attempt` — сколько ретраев уже сделано, едет вместе с ретраями; `x-attempts` — сколько попыток было всего, есть только у копии в DLQ.

**Webhook 4xx → сразу в DLQ.** ТЗ требует повторов «при ошибках отправки». Повторяются временные сбои: 5xx, 408, 429, таймауты, сетевые ошибки. Ответ 4xx (404, 400, 401…) означает ошибку на стороне клиента — неверный URL или отказ в доступе, — и повтор через 1–7 с ничего не изменит, а только отложит разбор. Редиректы (3xx) не выполняются и тоже считаются постоянной ошибкой.

**Webhook — at-least-once.** Уведомление может прийти повторно (ретрай, параллельная доставка). Клиент дедуплицирует по `payment_id`, он же приходит в заголовке `Idempotency-Key`. В webhook-mock повтор помечается флагом `duplicate`.

**Деньги без float.** Сумма передаётся как `Decimal` и хранится в `NUMERIC(18,2)`. В JSON ответов она строка. JSON-тела запросов разбираются с `parse_float=Decimal`, поэтому сумма, переданная числом, тоже не теряет копейки. Рекомендуется всё же передавать строку.

**Технологические мелочи:**
- один Docker-образ на все сервисы, отличаются только команды;
- runtime-образ без uv, не от root, код принадлежит root;
- миграции применяет отдельный сервис, а не entrypoint API, иначе несколько реплик применяли бы их одновременно;
- RabbitMQ с фиксированным hostname и томом, поэтому очереди переживают `docker compose down` без `-v`.

## Известные ограничения

- **Платёж в `pending` после исчерпания ретраев.** Если все попытки сорвались до сохранения статуса (например, БД была недоступна всё время), платёж остаётся `pending`, а сообщение лежит в DLQ. Его можно переотправить вручную из DLQ в exchange `payments` с ключом из `x-original-routing-key`, предварительно убрав заголовок `x-attempt`.
- **Повтор эмуляции.** Если эмуляция прошла, а сохранить статус не удалось, при ретрае эмуляция повторится. Реальному шлюзу нужно передавать `payment_id` как ключ идемпотентности, тогда списание не задвоится. Параллельные дубли сообщения тоже могут дважды вызвать шлюз и дважды отправить webhook.
- **Копии в DLQ, пришедшие по страховочному пути** (reject или delivery-limit), отличаются от явных. У них нет `x-error-kind` и `x-attempts`, зато есть `x-death`. Если брокер недоступен дольше 5 с в момент публикации ретрая, сообщение может попасть в DLQ без истории ошибок.
- **Порядок ответов при ошибках.** Невалидный JSON без `X-API-Key` даёт `422`, а не `401`: FastAPI разбирает тело до проверки зависимостей. Валидное тело без ключа даёт `401`.
- **Удержание соединения relay.** Relay держит транзакцию и соединение из пула, пока публикует пачку. Публикации последовательные, каждая с ожиданием confirm и таймаутом 5 с.
- **Медленная публикация после outage.** Если брокер долго недоступен, `attempts` растёт у первого события пачки.
- **Неограниченные повторы в outbox.** Событие, которое брокер отвергает постоянно, будет пробоваться бесконечно с растущей паузой.
- **Нет healthcheck у relay и consumer.**
- **SSRF.** `webhook_url` задаёт клиент, а запрос отправляет сервер, поэтому через него можно обратиться во внутреннюю сеть. Для демо защита не включена, потому что webhook-mock живёт внутри сети compose. Для продакшена нужно: запрещать приватные диапазоны адресов после резолва DNS, вести allowlist доменов, выпускать исходящий трафик через egress-прокси. Логин и пароль внутри URL уже запрещены. Подписи webhook (HMAC) нет.

## Что улучшить для продакшена

- Метрики (лаг outbox, глубина очередей, размер DLQ) и трассировка через API → outbox → consumer.
- HMAC-подпись webhook и защита от SSRF.
- Очистка опубликованных записей outbox.
- TTL ретраев через policy, `max_attempts` для outbox.
- At-least-once dead-lettering у quorum-очереди (`x-dead-letter-strategy`).
- Healthcheck для relay и consumer.

## Структура проекта

```
src/app/
├── core/            # настройки (pydantic-settings), логирование
├── domain/          # сущности, перечисления, хэш запроса, доменные исключения
├── application/     # use cases (PaymentService, OutboxRelay, ProcessPayment), порты, ошибки портов
├── infrastructure/  # БД (модели, репозитории, UoW), брокер (топология, публикация, ретраи), шлюз, webhook
├── api/             # FastAPI: схемы, роутеры, обработчики ошибок, health
└── entrypoints/     # процессы: relay, consumer, declare_topology, webhook_mock
migrations/          # Alembic (async)
tests/unit/          # без внешних зависимостей (in-memory фейки портов)
tests/integration/   # Postgres и RabbitMQ через testcontainers
```

Зависимости направлены внутрь: domain ни от чего не зависит, application зависит только от domain и своих портов (Protocol), infrastructure реализует порты, а `api` и `entrypoints` собирают всё вместе.
