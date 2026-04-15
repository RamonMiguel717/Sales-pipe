import importlib
import os
import re

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL


DRIVER_MODULES = {
    "pymysql": "pymysql",
    "mysqlconnector": "mysql.connector",
}


def _to_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default

    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def get_mysql_config() -> dict[str, str]:
    load_dotenv()

    return {
        "host": os.getenv("MYSQL_HOST", ""),
        "port": os.getenv("MYSQL_PORT", "3306"),
        "database": os.getenv("MYSQL_DATABASE", ""),
        "user": os.getenv("MYSQL_USER", ""),
        "password": os.getenv("MYSQL_PASSWORD", ""),
        "driver": os.getenv("MYSQL_DRIVER", "pymysql"),
        "echo": os.getenv("MYSQL_ECHO", "false"),
    }


def _validate_database_name(database_name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", database_name):
        raise ValueError(
            "MYSQL_DATABASE deve conter apenas letras, números e underscore."
        )

    return database_name


def _get_mysql_url(database: str | None = None) -> URL:
    config = get_mysql_config()
    required = ["host", "user", "password"]
    if database is not None:
        required.append("database")

    missing = [key for key in required if not config[key]]
    if missing:
        raise ValueError(
            f"Variáveis obrigatórias do MySQL não preenchidas no .env: {missing}"
        )

    driver = config["driver"]
    module_name = DRIVER_MODULES.get(driver, driver)

    try:
        importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            f"Driver MySQL '{driver}' não instalado. "
            f"Instale o pacote correspondente para usar a carga no banco."
        ) from exc

    database_name = config["database"] if database is None else database
    if database_name:
        database_name = _validate_database_name(database_name)

    return URL.create(
        f"mysql+{driver}",
        username=config["user"],
        password=config["password"],
        host=config["host"],
        port=int(config["port"]),
        database=database_name or None,
    )


def get_mysql_server_engine() -> Engine:
    config = get_mysql_config()
    url = _get_mysql_url(database="")

    return create_engine(url, echo=_to_bool(config["echo"]), future=True)


def create_mysql_database_if_not_exists() -> None:
    config = get_mysql_config()
    database_name = _validate_database_name(config["database"])

    engine = get_mysql_server_engine()
    statement = text(
        f"CREATE DATABASE IF NOT EXISTS `{database_name}` "
        "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
    )

    with engine.begin() as connection:
        connection.execute(statement)


def get_mysql_engine() -> Engine:
    config = get_mysql_config()
    create_mysql_database_if_not_exists()
    url = _get_mysql_url(database=config["database"])

    return create_engine(url, echo=_to_bool(config["echo"]), future=True)
