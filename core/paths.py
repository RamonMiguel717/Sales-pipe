from pathlib import Path


BASE = Path(__file__).resolve().parent.parent
ENTRADA = BASE / "data" / "entry"
BASE_TRATAMENTO = ENTRADA / "intermediario"
FINAL = BASE / "data" / "exit"

API = "https://olinda.bcb.gov.br/olinda/servico/TaxaJuros/versao/v1/odata/TaxasJurosMensalPorMes"
