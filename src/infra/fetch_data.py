import logging
from abc import ABC, abstractmethod
import yaml
import requests
from io import StringIO
import csv
import json
from datetime import datetime
from urllib.parse import urlencode


# ============== Configuração de Log ==============
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ============== Carregar arquivo de endpoints ==============
with open("api_endpoints.yaml", "r") as file:
    file_yaml = yaml.safe_load(file)


# ============== Classe Base ==============
class IFetchData(ABC):

    def __init__(self, endpoint: str):
        self.endpoint = endpoint

    @abstractmethod
    def parse_date(self, date_str: str) -> datetime: ...

    @abstractmethod
    def build_url(self) -> str: ...

    @abstractmethod
    def fetch_data(self) -> dict: ...


# ============== API Selic ==============
class SelicFetchData(IFetchData):
    def __init__(self):
        super().__init__(file_yaml["endpoints"]["bcb"]["selic"]["url"])

    def parse_date(self, date_str: str) -> datetime:
        """
        Tenta interpretar a data em diferentes formatos possíveis.
        Suporta os formatos: dd/mm/yyyy e yyyy-mm-dd.
        """
        for date_format in ("%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str, date_format)
            except ValueError:
                pass
        raise ValueError(f"Formato de data inválido: {date_str}")

    def build_url(self, date: str) -> str:
        """
        Constroi a URL com os parâmetros necessários.
        """
        parsed_date = self.parse_date(date).strftime("%d/%m/%Y")
        params = {
            "formato": "json",
            "dataInicial": parsed_date,
            "dataFinal": parsed_date,
        }

        return f"{self.endpoint}?{urlencode(params)}"

    def fetch_data(self, date: str) -> dict:
        """ "
        Busca os dados da API Selic.
        """
        try:
            url = self.build_url(date)
            logging.debug(f"URL Selic: {url}")
            # Faz a requisição HTTP
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            return r.json()

        except Exception as e:
            logging.error(f"Erro ao buscar dados: {e}")
            return None


# ============== API Expectativa Mercado ==============
class FocusFetchData(IFetchData):
    """Classe para coletar dados de expectativas de mercado
    da API Focus (BCB)."""

    def __init__(self) -> None:
        super().__init__(file_yaml["endpoints"]["bcb"]["focus"]["url"])

    def parse_date(self, date_str: str) -> datetime:
        """
        Tenta interpretar a data em diferentes formatos possíveis.
        Suporta os formatos: dd/mm/yyyy e yyyy-mm-dd.
        """
        for date_format in ("%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str, date_format)
            except ValueError:
                pass
        raise ValueError(f"Formato de data inválido: {date_str}")

    def build_url(
        self, indicator: str, date: str, temporal_series: str
    ) -> str:
        """Constrói URL completa para requisição da API Olinda."""

        if not date or not indicator or not temporal_series:
            raise ValueError(
                "`dt_referencia`, `indicador` e `serie_temporal` "
                "são obrigatórios para construir a URL."
            )

        if temporal_series == "anual":
            date_str = self.parse_date(date).strftime("%Y")

            url = (
                f"{self.endpoint}ExpectativasMercadoAnuais?$format=json&"
                f"$filter=Indicador eq '{indicator}' and "
                f"DataReferencia eq '{date_str}'"
            )

        elif temporal_series == "mensal":
            date_str = self.parse_date(date).strftime("%m/%Y")

            url = (
                f"{self.endpoint}ExpectativaMercadoMensais?$format=json&"
                f"$filter=Indicador eq '{indicator}' and "
                f"DataReferencia eq '{date_str}'"
            )
        else:
            raise ValueError(
                "`serie_temporal` inválido. Deve ser 'anual' ou 'mensal'."
            )

        return url

    def fetch_data(
        self, indicator: str, date: str, temporal_series: str
    ) -> dict:
        """
        Realiza a requisição HTTP e retorna os dados como um DataFrame.
        """
        try:
            url = self.build_url(indicator, date, temporal_series)
            logging.debug(f"URL construída: {url}")

            # Faz a requisição HTTP
            r = requests.get(url, timeout=30)
            r.raise_for_status()

            data = r.json().get("value", [])

            # Aplicar filtro de baseCalculo
            data = [item for item in data if item["baseCalculo"] == 0]
            return data

        except Exception as e:
            logging.error(f"Erro na API Boletim Focus: {e}")
            return None


# ============== API Moedas ==============
class MoedasFetchData(IFetchData):
    """Classe para coletar dados de moedas da API Moedas (BCB)."""

    def __init__(self) -> None:
        super().__init__(file_yaml["endpoints"]["bcb"]["moedas"]["url"])

    def parse_date(self, date_str: str) -> datetime:
        """
        Tenta interpretar a data em diferentes formatos possíveis.
        Suporta os formatos: dd/mm/yyyy e yyyy-mm-dd.
        """
        for date_format in ("%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str, date_format)
            except ValueError:
                pass
        raise ValueError(f"Formato de data inválido: {date_str}")

    def build_url(self, date: str = None) -> str:
        """
        Constrói a URL para acessar a API Moedas.
        """

        # Verifica se a data de referência foi fornecida
        if not date:
            raise ValueError("`date` é obrigatório para construir a URL.")

        try:
            date_str = self.parse_date(date).strftime("%Y%m%d")
        except ValueError as e:
            logging.error(f"Erro ao processar as datas: {e}")
            raise

        # Retorna a URL completa
        return f"{self.endpoint}/{date_str}.csv"

    def fetch_data(self, date: str) -> dict:
        """
        Realiza a requisição HTTP e retorna os dados como um DataFrame.
        """
        try:
            url = self.build_url(date)
            logging.debug(f"URL construída: {url}")

            # Faz a requisição HTTP ignorando a verificação de certificados
            r = requests.get(url, timeout=30)
            r.raise_for_status()

            # Usar módulo csv para ler o arquivo CSV
            csv_file = StringIO(r.text)
            reader = csv.reader(csv_file, delimiter=";")

            # Desempacortar o arquivo CSV em lista
            headers = [
                "data",
                "cod",
                "tipo",
                "moeda",
                "taxa_compra",
                "taxa_venda",
                "paridade_compra",
                "paridade_venda",
            ]
            list_of_dicts = []
            for row in reader:
                list_of_dicts.append(dict(zip(headers, row)))

            # Converter para json
            json_str = json.dumps(list_of_dicts)

            return json_str

        except Exception as e:
            logging.error(f"Erro na API Moedas: {e}")
            return None


# ============== API IBGE ==============
class IbgeFetchData(IFetchData):
    def __init__(self):
        super().__init__(file_yaml["endpoints"]["ibge"]["ipca"]["url"])

    def parse_date(self, date_str: str) -> datetime:
        """
        Tenta interpretar a data em diferentes formatos possíveis.
        Suporta os formatos: dd/mm/yyyy e yyyy-mm-dd.
        """
        for date_format in ("%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str, date_format)
            except ValueError:
                pass
        raise ValueError(f"Formato de data inválido: {date_str}")

    def build_url(self, date: str) -> str:
        """
        Constroi a URL com os parâmetros necessários.
        """
        parsed_date = self.parse_date(date).strftime("%Y%m")
        params = {"formato": "json", "p": parsed_date}

        return f"{self.endpoint}?{urlencode(params)}"

    def fetch_data(self, date: str) -> dict:
        """ "
        Busca os dados da API IBGE.
        """
        try:
            url = self.build_url(date)
            logging.debug(f"URL IBGE: {url}")

            # Faz a requisição HTTP
            r = requests.get(url, timeout=30)
            r.raise_for_status()

            return r.json()

        except Exception as e:
            logging.error(f"Erro ao buscar dados: {e}")
            return None


# ============== API Tesouro ==============
class TesouroFetchData(IFetchData):
    """Classe para coletar dados de taxas do Tesouro Direto."""

    def __init__(self) -> None:
        super().__init__(
            file_yaml["endpoints"]["tesouro"]["taxas_tesouro"]["base_url"]
        )

    def parse_date(self, date_str: str) -> datetime:
        """Tenta interpretar a data em diferentes formatos possíveis."""
        for date_format in ("%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str, date_format)
            except ValueError:
                pass
        raise ValueError(f"Formato de data inválido: {date_str}")

    def build_url(self, date: str) -> str:
        """Constrói a URL para acessar a API do Tesouro Direto."""
        parsed_date = self.parse_date(date).strftime("%Y%m%d")
        return f"{self.endpoint}/{parsed_date}.csv"

    def fetch_data(self, date: str) -> dict:
        """Realiza a requisição HTTP e retorna os dados como um dicionário."""
        try:
            url = self.build_url(date)
            logging.debug(f"URL construída: {url}")

            r = requests.get(url, timeout=30)
            r.raise_for_status()

            csv_file = StringIO(r.text)
            reader = csv.reader(csv_file, delimiter=";")

            headers = [
                "tp_titulo",
                "dt_vencimento",
                "dt_base",
                "taxa_compra_manha",
                "taxa_venda_manha",
                "pu_compra_manha",
                "pu_venda_manha",
                "pu_base_manha",
            ]

            target_date = self.parse_date(date).strftime("%d/%m/%Y")
            list_of_dicts = [
                dict(zip(headers, row))
                for row in reader
                if len(row) >= len(headers) and row[2] == target_date
            ]

            return list_of_dicts

        except Exception as e:
            logging.error(f"Erro na API Tesouro Direto: {e}")
            return None
