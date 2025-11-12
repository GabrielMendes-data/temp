from datetime import datetime

from src.infra.fetch_data import (
    SelicFetchData,
    FocusFetchData,
    DolarFetchData,
    IbgeFetchData,
    TesouroFetchData,
)


class ColDtReferencia:

    @staticmethod
    def add_dt_referencia(data: list[dict], date: str) -> list[dict]:
        for item in data:
            item["dt_execucao"] = datetime.strptime(date, "%d/%m/%Y").strftime(
                "%Y-%m-%d"
            )
        return data


class TransformFocusData:
    def __init__(self, indicators: list[str], date: str, temporal_series: str):
        self.indicators = indicators

        # Adicionar proximo ano na data
        dt_date = datetime.strptime(date, "%d/%m/%Y").date()
        self.date = [date, dt_date.replace(year=dt_date.year + 1).strftime("%d/%m/%Y")]

        self.temporal_series = temporal_series

    def transform(self) -> list[dict]:
        """
        Transforma os dados da API Focus em um dicionário.
        """
        result_focus = []

        for indicator in self.indicators:
            for date in self.date:

                fetch_data = FocusFetchData().fetch_data(
                    indicator, date, self.temporal_series
                )

                dict_filtered = fetch_data[-1]

                keys = ["Indicador", "Data", "DataReferencia", "Mediana"]

                dict_filtered = {key: dict_filtered[key] for key in keys}

                result_focus.append(dict_filtered)

        result_focus = ColDtReferencia.add_dt_referencia(result_focus, self.date[0])

        return result_focus


class TransformIbgeData:
    def __init__(self, date: str):
        self.date = date

    def transform(self) -> list[dict]:
        """
        Transforma os dados da API IBGE em um dicionário.
        """

        fetch_data = IbgeFetchData().fetch_data(self.date)

        # Arrays de mapeamento para substituir os nomes das keys
        keys = ["V", "D2C", "D3C", "D3N"]
        new_keys = ["valor_ipca", "ano_mes", "variavel_codigo", "variavel_nome"]

        # Converter nomes das keys
        if isinstance(fetch_data, list):
            # Itera pela lista e transforma cada item (dicionário)
            data_transform = [
                {new_keys[keys.index(k)]: v for k, v in item.items() if k in keys}
                for item in fetch_data
            ]
        else:
            raise TypeError("fetch_data precisa ser uma lista!")

        variaveis = ["63", "2265"]
        result_ibge = [
            {key: item[key] for key in new_keys}
            for item in data_transform[1:]
            if item.get("variavel_codigo") in variaveis
        ]

        result_ibge = ColDtReferencia.add_dt_referencia(result_ibge, self.date)

        return result_ibge


class TransformSelicData:
    def __init__(self, date: str):
        self.date = date

    def transform(self) -> list[dict]:
        """
        Transforma os dados da API Selic em um dicionário.
        """
        fetch_data = SelicFetchData().fetch_data(self.date)

        result_selic = ColDtReferencia.add_dt_referencia(fetch_data, self.date)
        return result_selic


class TransformDolarData:
    def __init__(self, date: str):
        self.date = date

    def transform(self) -> list[dict]:
        """
        Transforma os dados da API Selic em um dicionário.
        """
        fetch_data = DolarFetchData().fetch_data(self.date)

        result_dolar = ColDtReferencia.add_dt_referencia(fetch_data, self.date)
        return result_dolar


class TransformTesouroData:
    def __init__(self, date: str):
        self.date = date

    def transform(self) -> list[dict]:
        """
        Transforma os dados da API Selic em um dicionário.
        """
        fetch_data = TesouroFetchData().fetch_data(self.date)

        result_tesouro = ColDtReferencia.add_dt_referencia(fetch_data, self.date)
        return result_tesouro


class FactoryAPIs:
    @staticmethod
    def execute_api(api_name: str, params: dict) -> list[dict]:

        if api_name == "selic":
            return TransformSelicData(params["date"]).transform()
        elif api_name == "focus":
            return TransformFocusData(
                params["indicators"], params["date"], params["temporal_series"]
            ).transform()
        elif api_name == "dolar":
            return TransformDolarData(params["date"]).transform()
        elif api_name == "ibge":
            return TransformIbgeData(params["date"]).transform()
        elif api_name == "tesouro":
            return TransformTesouroData(params["date"]).transform()
        else:
            raise ValueError(f"API {api_name} não encontrada")
