SCHEMA_BTG_ACCES_RELATORIO_NET_NEW_MONEY = {
    "cod_carteira": {
        "nomes_origem": [
            "'DL_F_MovimentaçãoConsolidada'[COD_CARTEIRA]",
            "CONTA",
            "Conta",
        ],
        "data_type": "int64",
    },
    "nome": {"nomes_origem": ["NOME", "Nome"], "data_type": "string"},
    "mercado": {"nomes_origem": ["MERCADO", "Mercado"], "data_type": "string"},
    "descricao": {"nomes_origem": ["DESCRIÇÃO", "Descrição"], "data_type": "string"},
    "ativo": {"nomes_origem": ["ATIVO", "Ativo"], "data_type": "string"},
    "quantidade": {"nomes_origem": ["QUANTIDADE", "Quantidade"], "data_type": "double"},
    "captacao": {"nomes_origem": ["CAPTAÇÃO", "Captação"], "data_type": "double"},
    "data": {"nomes_origem": ["DATA", "Data"], "data_type": "timestamp[us]"},
    "tipo": {"nomes_origem": ["TIPO", "Tipo"], "data_type": "string"},
    "assessor": {"nomes_origem": ["ASSESSOR", "Assessor"], "data_type": "string"},
    "escritorio": {"nomes_origem": ["ESCRITÓRIO", "Escritório"], "data_type": "string"},
}

SCHEMA_BTG_ACCES_RELATORIO_MOVIMENTACAO = {
    "conta": {"nomes_origem": ["CONTA", "Conta"], "data_type": "int64"},
    "nome": {"nomes_origem": ["NOME", "Nome"], "data_type": "string"},
    "mercado": {"nomes_origem": ["MERCADO", "Mercado"], "data_type": "string"},
    "sub_mercado": {"nomes_origem": ["Sub Mercado"], "data_type": "string"},
    "produto": {"nomes_origem": ["Produto"], "data_type": "string"},
    "movimentacao": {
        "nomes_origem": ["MOVIMENTAÇÃO", "Movimentação"],
        "data_type": "string",
    },
    "lancamento": {
        "nomes_origem": ["LANCAMENTO", "LANÇAMENTO", "Lançamento"],
        "data_type": "string",
    },
    "emissor": {"nomes_origem": ["EMISSOR", "Emissor"], "data_type": "string"},
    "ativo": {"nomes_origem": ["ATIVO", "Ativo"], "data_type": "string"},
    "tipo_previdencia": {
        "nomes_origem": ["TIPO PREVIDÊNCIA", "TIPO PREVIDENCIA", "Tipo Previdência"],
        "data_type": "string",
    },
    "indexador": {"nomes_origem": ["INDEXADOR", "Indexador"], "data_type": "string"},
    "taxa": {"nomes_origem": ["TAXA", "Taxa Emissão"], "data_type": "double"},
    "tx_compra": {
        "nomes_origem": ["TX COMPRA", "TAXA COMPRA", "Taxa Compra"],
        "data_type": "double",
    },
    "dt_compra": {
        "nomes_origem": ["DT COMPRA", "DATA COMPRA", "Data Compra"],
        "data_type": "timestamp[us]",
    },
    "vencimento": {
        "nomes_origem": ["VENCIMENTO", "Vencimento"],
        "data_type": "timestamp[us]",
    },
    "soma_de_qtd": {
        "nomes_origem": ["Soma de QTD", "QUANTIDADE", "Quantidade"],
        "data_type": "double",
    },
    "valor_bruto": {
        "nomes_origem": ["VALOR BRUTO", "Valor Bruto"],
        "data_type": "double",
    },
    "ir": {"nomes_origem": ["IR"], "data_type": "double"},
    "iof": {"nomes_origem": ["IOF"], "data_type": "double"},
    "valor_liquido": {
        "nomes_origem": ["VALOR LIQUIDO", "Valor Líquido"],
        "data_type": "double",
    },
    "dt_cotizacao": {
        "nomes_origem": ["DT COTIZAÇÃO", "DATA COTIZAÇÃO", "Data Cotização"],
        "data_type": "timestamp[us]",
    },
    "dt_liquidacao": {
        "nomes_origem": [
            "DT LIQUIDACAO",
            "DATA LIQUIDAÇÃO",
            "DATA LIQUIDACAO",
            "Data Liquidação",
        ],
        "data_type": "timestamp[us]",
    },
}
