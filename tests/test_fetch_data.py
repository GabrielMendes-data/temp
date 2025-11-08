import yaml
import random
import requests
import pandas as pd
from typing import List, Dict

from datetime import datetime, timedelta
from src.infra.fetch_data import (
    SelicFetchData,
    FocusFetchData,
    MoedasFetchData,
    IbgeFetchData,
    TesouroFetchData,
)

# ============== Carregar arquivo de endpoints ==============
with open("api_endpoints.yaml", "r") as file:
    file_yaml = yaml.safe_load(file)


class DataAleatoria:

    FERIADOS_URL = file_yaml["endpoints"]["feriados"]["url"]

    def __init__(self, ano: int):
        self.ano = ano
        self.base_url = self.FERIADOS_URL

    def build_url(self) -> str:
        return f"{self.base_url}/{self.ano}/BR"

    def fetch_data(self) -> List[Dict]:
        url = self.build_url()
        response = requests.get(url, timeout=30, verify=False)
        response.raise_for_status()
        return response.json()

    def feriados(self) -> Dict[pd.Timestamp, str]:
        data = self.fetch_data()
        feriados = {
            pd.Timestamp(h["date"]): h["localName"]
            for h in data
            if (
                ("Public" in h.get("types", []))
                or ("Bank" in h.get("types", []))
            )
            and h.get("global", False)
        }
        return feriados

    def gerar_data_aleatoria(self) -> pd.Timestamp:
        """
        Retorna a data ajustada para o último dia útil
        (considera finais de semana e feriados nacionais).
        """
        inicio = datetime(self.ano, 1, 2)
        fim = datetime.now()

        dias_aleatorios = random.randint(0, (fim - inicio).days)
        data_aleatoria = inicio + timedelta(days=dias_aleatorios)
        data_util = pd.Timestamp(data_aleatoria.date())

        feriados = self.feriados()
        while data_util.weekday() >= 5 or data_util in feriados:
            data_util -= timedelta(days=1)
        return data_util.strftime("%d/%m/%Y")


def gerar_indicador_aleatorio():
    return random.choice(["Selic", "IPCA", "Câmbio"])


def gerar_serie_temporal_aleatoria():
    return random.choice(["anual", "mensal"])


# ========== Testes Funcionais ==========


def test_selic_build_url_and_fetch_data():
    selic = SelicFetchData()
    data = DataAleatoria(datetime.now().year).gerar_data_aleatoria()
    print(f"Data: {data}")
    url = selic.build_url(data)
    print(f"URL: {url}")

    r = requests.get(url, timeout=30)
    assert r.status_code == 200


def test_focus_build_url_and_fetch_data():
    focus = FocusFetchData()
    data = DataAleatoria(datetime.now().year).gerar_data_aleatoria()
    print(f"Data: {data}")
    url = focus.build_url(
        gerar_indicador_aleatorio(), data, gerar_serie_temporal_aleatoria()
    )
    print(f"URL: {url}")

    r = requests.get(url, timeout=30)
    assert r.status_code == 200


def test_moedas_build_url_and_fetch_data():
    moedas = MoedasFetchData()
    data = DataAleatoria(datetime.now().year).gerar_data_aleatoria()
    print(f"Data: {data}")
    url = moedas.build_url(data)
    print(f"URL: {url}")

    r = requests.get(url, timeout=30)
    assert r.status_code == 200


def test_ibge_build_url_and_fetch_data():
    ibge = IbgeFetchData()
    data = DataAleatoria(datetime.now().year).gerar_data_aleatoria()
    print(f"Data: {data}")
    url = ibge.build_url(data)
    print(f"URL: {url}")

    r = requests.get(url, timeout=30)
    assert r.status_code == 200


def test_tesouro_build_url_and_fetch_data():
    tesouro = TesouroFetchData()
    data = DataAleatoria(datetime.now().year).gerar_data_aleatoria()
    print(f"Data: {data}")
    url = tesouro.build_url(data)
    print(f"URL: {url}")

    r = requests.get(url, timeout=30)
    assert r.status_code == 200


# NOTE: solucionar warnings devido API de feriados
