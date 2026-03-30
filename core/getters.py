import pandas as pd
import shutil
import requests
from pathlib import Path

class Getters:
    def __init__(self):
        # Mapeamento direto de extensões para os métodos internos
        self._methods_map = {
            '.csv': self.get_csv,
            '.xlsx': self.get_excel,
            '.xls': self.get_excel,
            '.json': self.get_json,
            '.parquet': self.get_parquet
        }

    @staticmethod
    def get_excel(file_path: str) -> pd.DataFrame:
        return pd.read_excel(file_path)
        
    @staticmethod
    def get_csv(file_path: str) -> pd.DataFrame:
        return pd.read_csv(file_path, sep=None, engine='python', encoding='utf-8-sig')

    @staticmethod
    def get_json(file_path: str) -> pd.DataFrame:
        return pd.read_json(file_path)

    @staticmethod
    def get_parquet(file_path: str) -> pd.DataFrame:
        return pd.read_parquet(file_path)
    
    @staticmethod
    def get_by_api(path: str) -> pd.DataFrame:
        response = requests.get(path)
        response.raise_for_status()

        data = response.json()
        if isinstance(data, dict) and "value" in data:
            data = data["value"]

        return pd.DataFrame(data)


    def load_data(self, file_path: str) -> pd.DataFrame:
        """
        Identifica a extensão e carrega os dados. 
        Lança ValueError se a extensão não for suportada.
        """
        extension = Path(file_path).suffix.lower()
        
        method = self._methods_map.get(extension)
        
        if not method:
            raise ValueError(f"Formato de arquivo '{extension}' não é suportado pelo DataLoader.")
            
        if not Path(file_path).exists():
            raise FileNotFoundError(f"O arquivo não foi encontrado: {file_path}")

        # Executa a leitura. Se o pandas der erro, ele vai "estourar" aqui.
        return method(file_path)

    @staticmethod
    def descompactar_arquivo(file_path: str, output_path: str) -> None:
        shutil.unpack_archive(file_path, output_path)
