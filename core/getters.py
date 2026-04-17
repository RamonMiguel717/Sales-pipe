from pathlib import Path
import shutil

import pandas as pd
import requests


def get_excel(file_path: str | Path) -> pd.DataFrame:
    """Load an Excel file into a DataFrame."""
    return pd.read_excel(file_path)


def get_csv(file_path: str | Path) -> pd.DataFrame:
    """Load a CSV file into a DataFrame using the Olist default delimiter."""
    return pd.read_csv(file_path, sep=",", encoding="utf-8-sig")


def get_json(file_path: str | Path) -> pd.DataFrame:
    """Load a JSON file into a DataFrame."""
    return pd.read_json(file_path)


def get_parquet(file_path: str | Path) -> pd.DataFrame:
    """Load a parquet file into a DataFrame."""
    return pd.read_parquet(file_path)


def get_by_api(path: str) -> pd.DataFrame:
    """Load tabular data from an HTTP endpoint."""
    response = requests.get(path, timeout=30)
    response.raise_for_status()

    data = response.json()
    if isinstance(data, dict) and "value" in data:
        data = data["value"]

    return pd.DataFrame(data)


LOADERS = {
    ".csv": get_csv,
    ".xlsx": get_excel,
    ".xls": get_excel,
    ".json": get_json,
    ".parquet": get_parquet,
}


def load_data(file_path: str | Path) -> pd.DataFrame:
    """Load a supported local file into a DataFrame."""
    path = Path(file_path)
    extension = path.suffix.lower()
    loader = LOADERS.get(extension)

    if not loader:
        raise ValueError(f"Formato de arquivo '{extension}' nao e suportado pelo DataLoader.")

    if not path.exists():
        raise FileNotFoundError(f"O arquivo nao foi encontrado: {path}")

    return loader(path)


def descompactar_arquivo(file_path: str | Path, output_path: str | Path) -> None:
    """Extract a compressed file into the target directory."""
    shutil.unpack_archive(str(file_path), str(output_path))


class Getters:
    """Compatibility wrapper around the module-level loader functions."""

    @staticmethod
    def get_excel(file_path: str | Path) -> pd.DataFrame:
        return get_excel(file_path)

    @staticmethod
    def get_csv(file_path: str | Path) -> pd.DataFrame:
        return get_csv(file_path)

    @staticmethod
    def get_json(file_path: str | Path) -> pd.DataFrame:
        return get_json(file_path)

    @staticmethod
    def get_parquet(file_path: str | Path) -> pd.DataFrame:
        return get_parquet(file_path)

    @staticmethod
    def get_by_api(path: str) -> pd.DataFrame:
        return get_by_api(path)

    def load_data(self, file_path: str | Path) -> pd.DataFrame:
        return load_data(file_path)

    @staticmethod
    def descompactar_arquivo(file_path: str | Path, output_path: str | Path) -> None:
        descompactar_arquivo(file_path, output_path)
