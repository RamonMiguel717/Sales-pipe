import importlib
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
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


def get_mysql_engine() -> Engine:
    config = get_mysql_config()
    missing = [key for key in ["host", "database", "user", "password"] if not config[key]]

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

    url = URL.create(
        f"mysql+{driver}",
        username=config["user"],
        password=config["password"],
        host=config["host"],
        port=int(config["port"]),
        database=config["database"],
    )

    return create_engine(url, echo=_to_bool(config["echo"]), future=True)
