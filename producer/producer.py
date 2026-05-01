import csv
import json
import os
import re
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "pet-shop-sales")
DELAY_SECONDS = float(os.getenv("PRODUCER_DELAY_SECONDS", "0.002"))
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = os.getenv("DATA_DIR")
SOURCE_FOLDER_NAME = "исходные данные"
SHIFTED_ID_FIELDS = ("id", "sale_customer_id", "sale_seller_id", "sale_product_id")


def data_directory() -> Path:
    if DATA_DIR:
        data_dir = Path(DATA_DIR)
        if data_dir.exists():
            return data_dir
        raise FileNotFoundError(f"DATA_DIR does not exist: {data_dir}")

    candidates = [
        PROJECT_ROOT / "data",
        PROJECT_ROOT / SOURCE_FOLDER_NAME,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError("Source CSV directory was not found")


def file_sort_key(path: Path) -> tuple[int, str]:
    match = re.search(r"\((\d+)\)", path.stem)
    if match:
        return int(match.group(1)) + 1, path.name
    return 0, path.name


def wait_for_kafka() -> KafkaAdminClient:
    last_error = None
    for attempt in range(1, 31):
        try:
            admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS, client_id="csv-producer")
            admin.list_topics()
            print(f"Kafka is available, attempt {attempt}")
            return admin
        except Exception as error:
            last_error = error
            print(f"Kafka is not ready yet, attempt {attempt}/30")
            time.sleep(5)
    raise RuntimeError("Kafka did not become available in time") from last_error


def ensure_topic(admin_client: KafkaAdminClient) -> None:
    topic = NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)
    try:
        admin_client.create_topics([topic])
        print(f"Topic {TOPIC_NAME} created")
    except TopicAlreadyExistsError:
        print(f"Topic {TOPIC_NAME} already exists")


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        acks="all",
        retries=5,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
    )


def shifted_int(row: dict[str, str], field_name: str, offset: int) -> int | None:
    value = row.get(field_name, "").strip()
    if not value:
        return None
    return int(value) + offset


def send_files() -> None:
    admin = wait_for_kafka()
    ensure_topic(admin)
    admin.close()

    producer = build_producer()
    total_messages = 0

    files = sorted(data_directory().glob("*.csv"), key=file_sort_key)
    for file_index, path in enumerate(files):
        source_file = f"mock_data_{file_index + 1:02d}"
        id_offset = file_index * 1000
        sent_from_file = 0

        with path.open("r", encoding="utf-8-sig", newline="") as file:
            reader = csv.DictReader(file)
            for row_number, row in enumerate(reader, start=1):
                message = {key: (value.strip() if isinstance(value, str) else value) for key, value in row.items()}
                source_row_id = shifted_int(row, "id", 0)
                for field_name in SHIFTED_ID_FIELDS:
                    message[field_name] = shifted_int(row, field_name, id_offset)
                message["source_file"] = source_file
                message["source_filename"] = path.name
                message["source_row_number"] = row_number
                message["source_row_id"] = source_row_id
                message["id_offset"] = id_offset

                producer.send(TOPIC_NAME, key=str(message["id"]), value=message)
                sent_from_file += 1
                total_messages += 1

                if DELAY_SECONDS > 0:
                    time.sleep(DELAY_SECONDS)

        producer.flush()
        print(f"{path.name}: sent {sent_from_file} messages, id offset {id_offset}")

    producer.flush()
    producer.close()
    print(f"All done. Sent {total_messages} messages to topic '{TOPIC_NAME}'")


if __name__ == "__main__":
    send_files()
