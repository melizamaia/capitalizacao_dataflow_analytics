from __future__ import annotations
from typing import List, Dict, Optional
import pandas as pd
import requests

BASE = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{sid}/dados"

DEFAULT_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Brasilcap-Analytics/1.0 (contato: seuemail@exemplo.com)"
}

def _safe_get(url: str, params: Optional[dict] = None) -> List[Dict]:
    """
    Faz GET com headers/timeout e valida o JSON retornado.
    A API retorna lista de objetos com chaves 'data' e 'valor'.
    """
    try:
        resp = requests.get(url, headers=DEFAULT_HEADERS, params=params or {}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"[api_bcb] Erro ao consultar {url}: {e}")
        return []

def _get_series(series_id: int,
                formato: str = "json",
                dataInicial: Optional[str] = None,
                dataFinal: Optional[str] = None) -> pd.DataFrame:
    params = {"formato": formato}
    if dataInicial:
        params["dataInicial"] = dataInicial
    if dataFinal:
        params["dataFinal"] = dataFinal

    url = BASE.format(sid=series_id)
    raw = _safe_get(url, params=params)
    if not raw:
        return pd.DataFrame(columns=["data", "valor"])

    df = pd.DataFrame(raw)
    if "data" not in df or "valor" not in df:
        return pd.DataFrame(columns=["data", "valor"])

    df["data"] = pd.to_datetime(df["data"], dayfirst=True, errors="coerce")
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    df = df.dropna(subset=["data", "valor"]).sort_values("data").reset_index(drop=True)
    return df

def get_cdi(dataInicial: Optional[str] = None, dataFinal: Optional[str] = None) -> pd.DataFrame:
    """
    CDI anual (% a.a.) — série 12.
    Retorna colunas: [data, cdi_aa]
    """
    df = _get_series(12, dataInicial=dataInicial, dataFinal=dataFinal)
    return df.rename(columns={"valor": "cdi_aa"})

def get_ipca(dataInicial: Optional[str] = None, dataFinal: Optional[str] = None) -> pd.DataFrame:
    """
    IPCA mensal (% a.m.) — série 433.
    Retorna colunas: [data, ipca_am]
    """
    df = _get_series(433, dataInicial=dataInicial, dataFinal=dataFinal)
    return df.rename(columns={"valor": "ipca_am"})

if __name__ == "__main__":
    print("Últimos 5 valores do CDI:")
    print(get_cdi().tail())
    print("\nÚltimos 5 valores do IPCA:")
    print(get_ipca().tail())
