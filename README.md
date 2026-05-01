# BigDataFlink

Лабораторная работа №3 по дисциплине "Анализ больших данных".

Выполнил: Агафонов Андрей Сергеевич, М8О-315Б-23.

Тема: потоковая обработка данных с помощью Apache Flink. Producer читает CSV, отправляет строки в Kafka в формате JSON, Flink читает поток и записывает данные в PostgreSQL по схеме "звезда".

## Что лежит в проекте

- `docker-compose.yml` - PostgreSQL, Kafka, Flink JobManager/TaskManager, producer и служебные контейнеры.
- `docker/postgres/init/01_init.sql` - создание таблиц `dim_*` и `fact_sales`.
- `producer/producer.py` - отправка CSV-строк в Kafka.
- `flink-job/streaming_star_schema_job.py` - PyFlink job: `Kafka -> MapFunction -> PostgreSQL`.
- `flink-job/submit_job.sh` - отправка Flink job в кластер.
- `sql/check_results.sql` - проверочные SQL-запросы.
- `scripts/*` - короткие команды запуска, проверки и остановки.
- `report.md` - отчёт по лабораторной работе.

## Как работает

1. `kafka-init` создаёт топик `pet-shop-sales`.
2. `flink-submit` запускает streaming job в Flink.
3. `kafka-producer` читает 10 CSV-файлов из папки `исходные данные` и отправляет 10000 JSON-сообщений.
4. Flink читает топик с earliest offset, для каждого сообщения вставляет измерения и факт в PostgreSQL.
5. При вставке используется `INSERT ... ON CONFLICT DO NOTHING`, поэтому записи с уже существующим ключом пропускаются.

Producer формирует уникальные ключи для всех 10000 строк и передаёт их вместе с остальными полями в Kafka.

Для магазинов и поставщиков в CSV нет готового ID. Flink создаёт детерминированный `INTEGER` ID как MD5-хеш от набора атрибутов магазина или поставщика.

## Запуск лабораторной

Нужен Docker Desktop или Docker Engine с поддержкой `docker compose`.

В PowerShell:

```powershell
.\scripts\run-lab.ps1
```

Команда Docker Compose:

```bash
docker compose up --build -d
```

Проверить контейнеры:

```powershell
.\scripts\show-status.ps1
```

После загрузки должно быть так:

- `postgres`, `kafka`, `flink-jobmanager`, `flink-taskmanager` - `Up`
- `kafka-init`, `flink-submit`, `kafka-producer` - `Exited (0)`

Посмотреть логи producer:

```bash
docker compose logs kafka-producer
```

В конце должно быть:

```text
All done. Sent 10000 messages to topic 'pet-shop-sales'
```

## Проверка

Выполнить проверочные SQL-запросы:

```powershell
.\scripts\check-results.ps1
```

Та же проверка командой PowerShell:

```powershell
$sql = Get-Content -Raw -Encoding UTF8 ".\sql\check_results.sql"
$sql | docker compose exec -T postgres psql -U postgres -d pet_shop
```

Для bash:

```bash
docker compose exec -T postgres psql -U postgres -d pet_shop < ./sql/check_results.sql
```

Количество строк после загрузки:

| Таблица | Строк |
|---|---:|
| `fact_sales` | 10000 |
| `dim_customer` | 10000 |
| `dim_seller` | 10000 |
| `dim_product` | 10000 |
| `dim_store` | 10000 |
| `dim_supplier` | 10000 |

`sql/check_results.sql` также выводит распределение по исходным файлам и несколько аналитических запросов:

- топ стран по выручке;
- средний рейтинг товаров по категориям;
- продажи по месяцам.

## Подключение из DataGrip или DBeaver

PostgreSQL:

- Host: `localhost`
- Port: `5432`
- Database: `pet_shop`
- User: `postgres`
- Password: `postgres`

Kafka с хоста:

- Bootstrap server: `localhost:29092`
- Topic: `pet-shop-sales`

Flink Dashboard:

- `http://localhost:8081`

## Остановка

Остановить и удалить контейнеры вместе с томом PostgreSQL:

```powershell
.\scripts\stop-lab.ps1
```

Команда Docker Compose:

```bash
docker compose down -v
```

Ключ `-v` удаляет том PostgreSQL вместе с загруженными данными.
