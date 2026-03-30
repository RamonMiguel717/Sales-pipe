import pandas as pd
from pathlib import Path
from core import paths, filters as ft
from core.getters import Getters
from core.input_resolver import InputResolver


def principal(input_file: str | None = None, source: str = "local") -> pd.DataFrame:
    getters = Getters()
    resolver = InputResolver()

    if source.lower() == "api":
        api_path = input_file or paths.API
        return getters.get_by_api(api_path)

    if input_file is None:
        input_file = resolver.discover(paths.ENTRADA, ignore_dirs=[paths.BASE_TRATAMENTO])
    else:
        input_file = Path(input_file)
        if input_file.is_dir():
            input_file = resolver.discover(str(input_file), ignore_dirs=[paths.BASE_TRATAMENTO])

    while input_file.suffix.lower() == ".zip":
        getters.descompactar_arquivo(str(input_file), paths.BASE_TRATAMENTO)
        input_file = resolver.discover(
            paths.BASE_TRATAMENTO,
            prefer_zip=False,
            allowed_suffixes={".csv", ".xlsx", ".xls", ".json", ".parquet", ".zip"}
        )

    df = getters.load_data(str(input_file))
    return df
