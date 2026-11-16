# Наблюдаемость конвейера

Конвейер: источник (`producer`) и API → ограниченная очередь RabbitMQ → FastStream-воркер с моделью из MLflow → PostgreSQL.

## Стек и обоснование выбора

| Сигнал | Путь | Почему |
| --- | --- | --- |
| Логи | stdout (JSON, structlog) → Vector → Loki → Grafana | Loki индексирует только лейблы, а не текст: дёшево по памяти и диску, нативная связка с Grafana. Vector читает Docker API, фильтрует по label проекта, приложение про хранилище ничего не знает. |
| Трейсы | OTel SDK → OTel Collector → Tempo → Grafana | Tempo хранит трейсы на локальном диске без отдельной БД и индексов, есть trace to logs. Collector нужен как буфер и точка смены бэкенда (memory_limiter, batch, retry). |
| Метрики | OTel SDK (push, OTLP) → Collector → VictoriaMetrics; RabbitMQ (pull) через prometheus receiver Collector'а → VictoriaMetrics | VictoriaMetrics заметно легче Prometheus по памяти, понимает PromQL, принимает remote write. |
| Алерты | Grafana Alerting, правила в YAML | Один инструмент вместо vmalert и Alertmanager, состояние Firing видно в UI. |

Выбор push для метрик приложения. Источник — короткоживущий пакетный процесс: он может завершиться между двумя scrape'ами Prometheus, и его метрики не будут собраны. Push по OTLP отправляет данные каждые 5 секунд, а при завершении делается `shutdown()` с финальной отправкой. API и воркеры идут тем же путём ради единого конвейера. RabbitMQ уже отдаёт `/metrics` встроенным плагином на порту 15692, поэтому его Collector опрашивает сам (pull).

Как каждая модель узнаёт, что цель умерла. Pull: scrape не удался, метрика `up == 0`. Push: тишина. Серия перестаёт обновляться, в VictoriaMetrics она становится stale, а детектировать это нужно через `absent_over_time(metric[2m])`. Для пакетного процесса отсутствие данных после завершения — норма, поэтому на него алертят по результату (`scoring_finished`, счётчики), а не по `up`.

## Запуск

```
docker compose up -d                          # только приложение
docker compose --profile logs up -d           # + Vector и Loki
docker compose --profile traces up -d         # + Collector и Tempo
docker compose --profile metrics up -d        # + Collector и VictoriaMetrics
docker compose --profile observability up -d  # всё, включая Grafana (http://localhost:3000)
```

Приложение не зависит от стека: нет `depends_on` на сервисы наблюдаемости. Если Collector не запущен, экспорт молча теряет данные, сервис работает. Полностью отключить SDK: `OTEL_SDK_DISABLED=true`.

Переменные: `LOG_LEVEL`, `OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_TRACES_SAMPLER`, `OTEL_RESOURCE_ATTRIBUTES` (service.version, deployment.environment.name), `OTEL_METRIC_EXPORT_INTERVAL`. `service.instance.id` берётся из hostname контейнера, поэтому два воркера различаются в трейсах и метриках.

## Логи

- Формат: JSON в stdout, единый для API, источника, воркера и библиотек (uvicorn, aio_pika, mlflow, faststream идут через тот же handler). Поля: `timestamp`, `level`, `logger`, `event`, `service`, `env`, `task_id`, `trace_id`, `span_id`, `exception`.
- `task_id` попадает во все записи обработчика через `structlog.contextvars.bound_contextvars`; он же передаётся в `correlation_id` сообщения RabbitMQ и в теле. `trace_id` и `span_id` добавляются процессором из активного спана.
- Секреты: процессор `redact_secrets` маскирует пароль в DSN (`amqp://rabbit:***@...`) и значения ключей password, secret, token и т. п. Содержимое записей файла не логируется (в DLQ логируется только `task_id` и размер).
- Health-check'и не логируются на INFO: `uvicorn.access` и `aiohttp.access` на WARNING, спаны health-эндпоинтов исключены из трассировки.
- Ключевые события: `model_loaded_successfully` (alias и version), `backpressure_nack` (warning), `queue_full_503` (warning), `task_failed` (error, стектрейс, `attempt`), `task_dead_lettered` и `dead_letter_received` (error), `shutdown_signal_received` и `worker_stopped_gracefully` (начало и конец graceful shutdown).
- Лейблы Loki: только `service`, `level`, `env`. `task_id` и `trace_id` остаются полями записи. Каждая уникальная комбинация лейблов — отдельный поток, у каждого потока свой индекс и чанки. Для 6000 задач было бы 6000 потоков с одной строкой: индекс раздувается, чанки мелкие, запросы медленные. Поиск по полю идёт как `{env="dev"} |= "<task_id>"`, это быстрый перебор небольших чанков.

Примеры запросов LogQL:

```
{env="dev"} |= "score-8123345"
{env="dev", level=~"error|critical"}
{service="worker"} |= "dead_letter_received"
{service="worker"} |= "graceful_shutdown"
```

## Трейсинг

Один трейс на одну задачу. API: `POST /api/v1/tasks` → publish → `dota_inference_queue process` (воркер) → `inference` → `db.save_result` (+ SQL-спан от SQLAlchemy). Источник: каждая задача — отдельный корневой спан `source.publish_task` (контекст пустой, поэтому пакет не сливается в один трейс на тысячи спанов), внутри события `broker_nack` и дочерние спаны `source.backoff`.

- Контекст через RabbitMQ: middleware FastStream кладёт заголовок `traceparent` (`version-traceid-spanid-flags`) в сообщение и достаёт его на стороне воркера, `process` становится дочерним к publish.
- Через пул потоков контекст передаётся явно: `contextvars.copy_context().run` внутри `run_in_executor`, поэтому `inference` — дочерний к `process`, а не корневой. Тот же контекст несёт `task_id` в логи потока.
- Ошибки: `record_exception`, статус ERROR, атрибут `task.attempt`, события `retry_scheduled` и `dead_lettered`. Повторная публикация идёт внутри текущего спана, поэтому все попытки ядовитого сообщения лежат в одном трейсе. 503 — ERROR-спан с исключением и логом `queue_full_503`.
- Время в очереди видно как разрыв между концом publish-спана и началом process-спана; время inference — длительность спана `inference`.

Семплирование. Стенд: `parentbased_always_on` (100%, нагрузка маленькая, нужны все трейсы для защиты). Прод при сотнях задач в секунду: head-семплер в SDK (`parentbased_traceidratio`, 1–5%) дешёвый, но случайный: ошибочные трейсы с вероятностью 95–99% потеряются. Поэтому в проде SDK оставляют на `always_on`, а решение принимает tail-sampling в Collector (политики: статус ERROR — 100%, длительность выше порога — 100%, остальное — 1%). Цена: все спаны трейса должны попасть в один Collector (load-balancing exporter по trace_id) и буферизуются до решения.

## Метрики

| Метрика | Тип | Лейблы |
| --- | --- | --- |
| `api_requests_total` | counter | route, method, status |
| `api_request_duration_seconds` | histogram | route, method |
| `source_published_total`, `source_broker_nacks_total`, `source_wait_seconds_total` | counter | — |
| `worker_tasks_processed_total` | counter | outcome (ok, error, dlq) |
| `worker_inference_duration_seconds` | histogram | — |
| `worker_tasks_in_flight` | gauge (up-down counter) | — |
| `worker_redeliveries_total`, `worker_dead_letters_total` | counter | — |
| `process_memory_rss_bytes` | gauge | — |
| `rabbitmq_detailed_queue_messages_ready`, `..._unacked`, `..._messages`, `..._consumers` | gauge | queue (из плагина) |

К каждой серии автоматически добавляются `job` (service.name) и `instance` (service.instance.id).

Бакеты histogram inference считаются от `INFERENCE_DELAY_SEC` (множители 0.2…10 от него, при 0,5 с это 0.1, 0.25, 0.4, 0.45, 0.5, 0.525, 0.55, 0.625, 0.75, 1, 2, 5), чтобы реальная латентность не попадала в один бакет. Бакеты API — 5 мс … 5 с.

Оценка числа рядов (произведение значений лейблов), 2 воркера:

- `api_requests_total`: 6 маршрутов × 1 метод × 4 статуса = 24
- `api_request_duration_seconds` (13 серий на гистограмму): 6 × 13 = 78
- `worker_tasks_processed_total`: 3 outcome × 2 воркера = 6
- `worker_inference_duration_seconds`: 15 × 2 = 30
- in_flight, redeliveries, dead_letters: 3 × 2 = 6
- источник: 3 серии на запуск
- память: 4 процесса
- RabbitMQ: 2 очереди × 4 метрики = 8

Итого порядка 160 рядов. Если добавить label `task_id` к `worker_tasks_processed_total` и к гистограмме inference, то после прогона файла из 88 задач будет 88 серий на счётчик и 88 × 15 = 1320 серий на гистограмму, а при пакете в 6000 задач — 90 тысяч серий гистограммы. Поэтому идентификаторы в лейблы не кладут: трассу конкретной задачи ищут в логах и трейсах.

PromQL для защиты:

```
sum(rate(source_published_total[30s]))                                   # скорость источника
sum(rate(worker_tasks_processed_total{outcome="ok"}[30s]))               # пропускная способность воркеров
sum by (instance) (rate(worker_tasks_processed_total{outcome="ok"}[30s]))
histogram_quantile(0.95, sum by (le) (rate(worker_inference_duration_seconds_bucket[1m])))
```

Перцентили нельзя усреднять по воркерам: p95 от p95 не равен общему p95. Поэтому суммируют бакеты (`sum by (le)`), и `histogram_quantile` интерполирует внутри бакета; точность ограничена шириной бакета, отсюда подбор границ под реальное время. SLO «99% задач быстрее N секунд»: `histogram_quantile(0.99, ...) < N`, или доля быстрых: `sum(rate(..._bucket{le="N"}[5m])) / sum(rate(..._count[5m])) >= 0.99` (N должно совпадать с границей бакета). Error budget = допустимые 1% нарушений за окно; алерт срабатывает по скорости его сжигания.

## Grafana

Datasources, дашборд и алерты лежат в `observability/grafana/provisioning/` и `observability/grafana/dashboards/`, всё поднимается без кликов после `docker compose down -v`. Дашборд «Конвейер» открывается главной страницей; переменная `worker` выбирает воркера, линия лимита очереди — переменная `queue_limit` (по умолчанию 20, как `x-max-length`).

Корреляция: в Loki настроен derived field `trace_id` (переход в Tempo одним кликом), в Tempo настроен trace to logs (поиск строк логов по `traceId` ±1 минута).

Алерты:

- **Queue is over 90% full**: `ready / 20 > 0.9`, `for: 1m`. Порог 90% оставляет запас до `reject-publish`, `for: 1m` отсекает кратковременные всплески (в штатном режиме очередь касается лимита и откатывается за секунды). Чтобы он сработал на защите, держите очередь заполненной больше минуты: остановите воркеров (`docker compose stop worker`) и запустите источник, он будет получать nack.
- **Messages reached DLQ**: `increase(worker_dead_letters_total[5m]) > 0`, `for: 30s`. Любое сообщение в DLQ — инцидент, поэтому порог 0; окно 5 минут держит алерт в Firing достаточно долго, чтобы его заметили, а `for: 30s` не даёт ему дребезжать на одиночном сэмпле. DLQ читает `dead_letter_handler`, поэтому глубина очереди почти всегда 0, и алерт строится на счётчике.

Алертить лучше на симптомы (очередь не успевает, пользователи получают 503), а не на причины (CPU воркера): причин много, а симптом один и он точно значит, что есть проблема.

## Бюджет памяти

Лимиты (`mem_limit`) и retention 72 часа (Loki, Tempo, VictoriaMetrics):

| Сервис | Лимит |
| --- | --- |
| otel-collector | 192 MiB |
| tempo | 256 MiB |
| loki | 320 MiB |
| vector | 96 MiB |
| victoria-metrics | 192 MiB |
| grafana | 256 MiB |
| Итого | 1312 MiB (≈1,3 ГБ при бюджете 1,5 ГБ) |

Фактическое потребление под нагрузкой (`docker stats --no-stream`, 2 воркера, после прогона источника, остановки воркеров и DLQ-сценария):

| Сервис | Факт | Лимит |
| --- | --- | --- |
| tempo | 222 MiB | 256 MiB |
| otel-collector | 127 MiB | 192 MiB |
| grafana | 162 MiB | 256 MiB |
| victoria-metrics | 125 MiB | 192 MiB |
| loki | 126 MiB | 320 MiB |
| vector | 36 MiB | 96 MiB |
| **Итого стек наблюдаемости** | **≈ 798 MiB (0,78 ГиБ)** | 1312 MiB |

Стек укладывается в бюджет 1,5 ГБ с запасом. Самый нагруженный сервис — Tempo (87% лимита); при росте потока трейсов лимит поднимают, ограничения `mem_limit` защищают остальной стенд от его утечек.

## Проверенные сценарии

- **Алерт очереди.** Воркеры остановлены, источник получает nack, очередь на лимите 20. История правила: Pending 01:09:15 → Alerting 01:10:15 (ровно `for: 1m`) → Normal 01:14:25 после запуска воркеров. Короткие всплески (01:00:35, 01:01:35) остались в Pending и в Alerting не перешли: так `for: 1m` отсекает шум.
- **DLQ.** `send_poison.py`: попытки 0, 1, 2 на разных воркерах, затем `task_dead_lettered` и `dead_letter_received`. Все попытки лежат в одном трейсе (`create → process → inference → create → publish → process → … → dlq publish → dlq process`, 14 спанов, ошибки красные). Алерт «Messages reached DLQ» перешёл в Firing (значение 1).
- **Два воркера.** `--scale worker=2`: на дашборде две серии, пропускная способность по воркерам 1,6–1,8 ops/s на каждом.
- **Идемпотентность.** Повторный прогон тех же `match_id` даёт `inserted=false` (`ON CONFLICT DO NOTHING`).

## Что произойдёт, если стек недоступен

- Collector остановлен: SDK копит спаны в `BatchSpanProcessor` (очередь 2048, затем сбрасывает новые), метрики теряются до следующего успешного экспорта. Приложение не замедляется: экспорт идёт в фоновом потоке. Это сброс нагрузки (load shedding) и противодавление на стороне телеметрии: она никогда не должна блокировать полезную работу. Collector, в свою очередь, при недоступном Tempo или VictoriaMetrics ограничен `memory_limiter` и `retry_on_failure` (30 с), после чего отбрасывает данные.
- Loki остановлен: Vector повторяет отправку, источник логов — Docker `json-file` (10 МБ × 3 файла), так что при коротком простое логи не теряются.
- Sentry отличается от лога тем, что группирует одинаковые ошибки в задачу (issue) с владельцем, статусом и счётчиком, а строка лога остаётся событием без жизненного цикла.

## Отладочный хук

Для демонстрации DLQ воркер считает сообщение «ядовитым», если `task_id` начинается с `poison-` (переменная `POISON_PREFIX`): inference на нём падает после задержки. Отправка: `uv run python scripts/send_poison.py --count 1`.