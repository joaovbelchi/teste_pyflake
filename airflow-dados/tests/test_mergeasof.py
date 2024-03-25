import pytest
from unittest.mock import patch, Mock
import pandas as pd

import sys

sys.path.append("../dags")

import os

os.environ["AMBIENTE"] = "dev"

from utils.mergeasof import mergeasof_socios


@patch("utils.mergeasof.pd.read_parquet")
def test_mergeasof_socios(mock_read_parquet):

    dados_socios_homologacao = {
        "signature_name": [
            "nome_1",
            "nome_1",
            "nome_2",
            "One Investimentos",
            "Não contatar",
            "One Corporate",
        ],
        "start_date": [
            "2024-01-01",
            "2024-01-01",
            "2024-01-01",
            "2024-01-01",
            "2024-01-01",
            "2024-01-01",
        ],
        "business_unit": [
            "NOVA LIMA",
            "SÃO PAULO",
            None,
            "NOVA LIMA",
            "NOVA LIMA",
            "NOVA LIMA",
        ],
        "department": ["GROWTH ", "CORPORATE", None, None, None, None],
        "area": ["COMERCIAL", "COMERCIAL CORPORATE", None, None, None, None],
        "team_leader": [
            "nome_team_leader_1",
            "nome_team_leader_2",
            "nome_team_leader_3",
            "One Investimentos",
            "Não contatar",
            "One Corporate",
        ],
        "business_leader": [
            "nome_business_leader_1",
            "nome_business_leader_2",
            "nome_business_leader_3",
            "One Investimentos",
            "Não contatar",
            "One Corporate",
        ],
        "register": ["2023-01-01", "2023-02-10", "2023-03-20", None, None, None],
    }

    df_socios_homologacao = pd.DataFrame(dados_socios_homologacao).astype(
        {"register": "datetime64[ns]"}
    )

    df_socios_homologacao

    dados_homologacao = {
        "name_column": [
            "nome_1",
            "nome_1",
            "nome_1",
            "nome_1",
            "nome_2",
            "nome_2",
            "One Investimentos",
            "Não contatar",
            "One Corporate",
        ],
        "date_column": [
            "2022-12-01",
            "2023-01-02",
            "2023-02-09",
            "2023-02-15",
            "2023-03-01",
            "2023-03-02",
            "2023-03-02",
            "2023-03-02",
            "2023-03-02",
        ],
    }

    df_homologacao = pd.DataFrame(dados_homologacao).astype(
        {"date_column": "datetime64[ns]"}
    )

    df_homologacao

    resultado_homologacao = {
        "name_column": [
            "nome_1",
            "nome_1",
            "nome_1",
            "nome_1",
            "nome_2",
            "nome_2",
            "One Investimentos",
            "Não contatar",
            "One Corporate",
        ],
        "date_column": [
            "2022-12-01",
            "2023-01-02",
            "2023-02-09",
            "2023-02-15",
            "2023-03-01",
            "2023-03-02",
            "2023-03-02",
            "2023-03-02",
            "2023-03-02",
        ],
        "Departamento": [
            "GROWTH",
            "GROWTH",
            "GROWTH",
            "CORPORATE",
            "Não identificado",
            "Não identificado",
            "GROWTH",
            "GROWTH",
            "CORPORATE",
        ],
        "Área": [
            "COMERCIAL",
            "COMERCIAL",
            "COMERCIAL",
            "COMERCIAL CORPORATE",
            "Não identificado",
            "Não identificado",
            "Não identificado",
            "Não identificado",
            "Não identificado",
        ],
        "Unidade de Negócios": [
            "NOVA LIMA",
            "NOVA LIMA",
            "NOVA LIMA",
            "SÃO PAULO",
            "Não identificado",
            "Não identificado",
            "NOVA LIMA",
            "NOVA LIMA",
            "NOVA LIMA",
        ],
        "Business Leader": [
            "nome_business_leader_1",
            "nome_business_leader_1",
            "nome_business_leader_1",
            "nome_business_leader_2",
            "nome_business_leader_3",
            "nome_business_leader_3",
            "One Investimentos",
            "Não contatar",
            "One Corporate",
        ],
        "Team Leader": [
            "nome_team_leader_1",
            "nome_team_leader_1",
            "nome_team_leader_1",
            "nome_team_leader_2",
            "nome_team_leader_3",
            "nome_team_leader_3",
            "One Investimentos",
            "Não contatar",
            "One Corporate",
        ],
    }

    df_resultado_homologacao = pd.DataFrame(resultado_homologacao).astype(
        {"date_column": "datetime64[ns]"}
    )

    mock_read_parquet.return_value = df_socios_homologacao

    resultado = mergeasof_socios(df_homologacao, "name_column", "date_column")

    assert resultado.equals(df_resultado_homologacao)
