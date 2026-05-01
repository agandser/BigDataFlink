import hashlib
import json
import os
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import psycopg2
from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import MapFunction, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "pet-shop-sales")
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/pet_shop")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")


STORE_KEY_FIELDS = (
    "store_name",
    "store_location",
    "store_city",
    "store_state",
    "store_country",
    "store_phone",
    "store_email",
)

SUPPLIER_KEY_FIELDS = (
    "supplier_name",
    "supplier_contact",
    "supplier_email",
    "supplier_phone",
    "supplier_address",
    "supplier_city",
    "supplier_country",
)


def parse_jdbc_url(jdbc_url: str) -> tuple[str, int, str]:
    match = re.match(r"jdbc:postgresql://([^:/]+)(?::(\d+))?/([^?]+)", jdbc_url)
    if not match:
        raise ValueError(f"Unsupported JDBC URL: {jdbc_url}")

    host = match.group(1)
    port = int(match.group(2) or 5432)
    database = match.group(3)
    return host, port, database


def hash_id(*values: Any) -> int:
    joined = "|".join("" if value is None else str(value).strip() for value in values)
    return int(hashlib.md5(joined.encode("utf-8")).hexdigest(), 16) % 2_147_483_647


class PostgresStarSchemaSink(MapFunction):
    def __init__(self, jdbc_url: str, user: str, password: str) -> None:
        self.host, self.port, self.database = parse_jdbc_url(jdbc_url)
        self.user = user
        self.password = password
        self.connection = None
        self.cursor = None

    def open(self, runtime_context) -> None:
        self.connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
        )
        self.connection.autocommit = False
        self.cursor = self.connection.cursor()

    def close(self) -> None:
        if self.cursor is not None:
            self.cursor.close()
        if self.connection is not None:
            self.connection.close()

    def map(self, value: str) -> str:
        row = json.loads(value)

        try:
            sale_id = self._int(row.get("id"))
            customer_id = self._int(row.get("sale_customer_id"))
            seller_id = self._int(row.get("sale_seller_id"))
            product_id = self._int(row.get("sale_product_id"))
            store_id = hash_id(*(row.get(field) for field in STORE_KEY_FIELDS))
            supplier_id = hash_id(*(row.get(field) for field in SUPPLIER_KEY_FIELDS))

            self._insert_customer(row, customer_id)
            self._insert_seller(row, seller_id)
            self._insert_product(row, product_id)
            self._insert_store(row, store_id)
            self._insert_supplier(row, supplier_id)
            self._insert_fact(row, sale_id, customer_id, seller_id, product_id, store_id, supplier_id)

            self.connection.commit()
            return f"loaded sale_id={sale_id}"
        except Exception:
            self.connection.rollback()
            raise

    def _insert_customer(self, row: dict[str, Any], customer_id: int | None) -> None:
        self.cursor.execute(
            """
            INSERT INTO dim_customer (
                customer_id, first_name, last_name, age, email, country,
                postal_code, pet_type, pet_name, pet_breed
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (customer_id) DO NOTHING
            """,
            (
                customer_id,
                self._str(row.get("customer_first_name")),
                self._str(row.get("customer_last_name")),
                self._int(row.get("customer_age")),
                self._str(row.get("customer_email")),
                self._str(row.get("customer_country")),
                self._str(row.get("customer_postal_code")),
                self._str(row.get("customer_pet_type")),
                self._str(row.get("customer_pet_name")),
                self._str(row.get("customer_pet_breed")),
            ),
        )

    def _insert_seller(self, row: dict[str, Any], seller_id: int | None) -> None:
        self.cursor.execute(
            """
            INSERT INTO dim_seller (
                seller_id, first_name, last_name, email, country, postal_code
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (seller_id) DO NOTHING
            """,
            (
                seller_id,
                self._str(row.get("seller_first_name")),
                self._str(row.get("seller_last_name")),
                self._str(row.get("seller_email")),
                self._str(row.get("seller_country")),
                self._str(row.get("seller_postal_code")),
            ),
        )

    def _insert_product(self, row: dict[str, Any], product_id: int | None) -> None:
        self.cursor.execute(
            """
            INSERT INTO dim_product (
                product_id, product_name, category, price, quantity, pet_category,
                weight, color, size, brand, material, description, rating, reviews,
                release_date, expiry_date
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO NOTHING
            """,
            (
                product_id,
                self._str(row.get("product_name")),
                self._str(row.get("product_category")),
                self._decimal(row.get("product_price")),
                self._int(row.get("product_quantity")),
                self._str(row.get("pet_category")),
                self._decimal(row.get("product_weight")),
                self._str(row.get("product_color")),
                self._str(row.get("product_size")),
                self._str(row.get("product_brand")),
                self._str(row.get("product_material")),
                self._str(row.get("product_description")),
                self._decimal(row.get("product_rating")),
                self._int(row.get("product_reviews")),
                self._date(row.get("product_release_date")),
                self._date(row.get("product_expiry_date")),
            ),
        )

    def _insert_store(self, row: dict[str, Any], store_id: int) -> None:
        self.cursor.execute(
            """
            INSERT INTO dim_store (
                store_id, store_name, location, city, state, country, phone, email
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (store_id) DO NOTHING
            """,
            (
                store_id,
                self._str(row.get("store_name")),
                self._str(row.get("store_location")),
                self._str(row.get("store_city")),
                self._str(row.get("store_state")),
                self._str(row.get("store_country")),
                self._str(row.get("store_phone")),
                self._str(row.get("store_email")),
            ),
        )

    def _insert_supplier(self, row: dict[str, Any], supplier_id: int) -> None:
        self.cursor.execute(
            """
            INSERT INTO dim_supplier (
                supplier_id, supplier_name, contact, email, phone, address, city, country
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (supplier_id) DO NOTHING
            """,
            (
                supplier_id,
                self._str(row.get("supplier_name")),
                self._str(row.get("supplier_contact")),
                self._str(row.get("supplier_email")),
                self._str(row.get("supplier_phone")),
                self._str(row.get("supplier_address")),
                self._str(row.get("supplier_city")),
                self._str(row.get("supplier_country")),
            ),
        )

    def _insert_fact(
        self,
        row: dict[str, Any],
        sale_id: int | None,
        customer_id: int | None,
        seller_id: int | None,
        product_id: int | None,
        store_id: int,
        supplier_id: int,
    ) -> None:
        self.cursor.execute(
            """
            INSERT INTO fact_sales (
                sale_id, source_file, source_filename, source_row_number, source_row_id,
                sale_date, customer_id, seller_id, product_id, store_id, supplier_id,
                quantity, total_price, product_unit_price, product_stock_quantity
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (sale_id) DO NOTHING
            """,
            (
                sale_id,
                self._str(row.get("source_file")),
                self._str(row.get("source_filename")),
                self._int(row.get("source_row_number")),
                self._int(row.get("source_row_id")),
                self._date(row.get("sale_date")),
                customer_id,
                seller_id,
                product_id,
                store_id,
                supplier_id,
                self._int(row.get("sale_quantity")),
                self._decimal(row.get("sale_total_price")),
                self._decimal(row.get("product_price")),
                self._int(row.get("product_quantity")),
            ),
        )

    @staticmethod
    def _str(value: Any) -> str | None:
        if value is None:
            return None
        value = str(value).strip()
        return value or None

    @staticmethod
    def _int(value: Any) -> int | None:
        if value is None or str(value).strip() == "":
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _decimal(value: Any) -> Decimal | None:
        if value is None or str(value).strip() == "":
            return None
        try:
            return Decimal(str(value).strip())
        except (InvalidOperation, ValueError):
            return None

    @staticmethod
    def _date(value: Any):
        if value is None or str(value).strip() == "":
            return None

        value = str(value).strip()
        for date_format in ("%m/%d/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(value, date_format).date()
            except ValueError:
                continue
        return None


def build_job() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    consumer = FlinkKafkaConsumer(
        topics=KAFKA_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties={
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "pet-shop-sales-consumer",
        },
    )
    consumer.set_start_from_earliest()

    env.add_source(consumer).map(
        PostgresStarSchemaSink(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD),
        output_type=Types.STRING(),
    ).print()

    env.execute("Star Schema ETL Job")


if __name__ == "__main__":
    build_job()
