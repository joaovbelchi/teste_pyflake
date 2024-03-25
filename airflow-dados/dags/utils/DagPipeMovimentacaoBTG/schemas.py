SCHEMA_BTG_RELATORIO_MOVIMENTACAO = {
    "nr_conta": {"nomes_origem": ["nr_conta"], "data_type": "int64"},
    "dt_interface": {
        "nomes_origem": ["dt_interface"],
        "data_type": "timestamp[us]",
    },
    "dt_movimentacao": {
        "nomes_origem": ["dt_movimentacao"],
        "data_type": "timestamp[us]",
    },
    "mercado": {"nomes_origem": ["mercado"], "data_type": "string"},
    "sub_mercado": {"nomes_origem": ["sub_mercado"], "data_type": "string"},
    "historico_movimentacao": {
        "nomes_origem": ["historico_movimentacao"],
        "data_type": "string",
    },
    "tipo_lancamento": {"nomes_origem": ["tipo_lancamento"], "data_type": "string"},
    "tipo_operacao": {"nomes_origem": ["tipo_operacao"], "data_type": "string"},
    "tipo_opcao": {"nomes_origem": ["tipo_opcao"], "data_type": "string"},
    "tipo": {"nomes_origem": ["tipo"], "data_type": "string"},
    "ativo": {"nomes_origem": ["ativo"], "data_type": "string"},
    "emissor": {"nomes_origem": ["emissor"], "data_type": "string"},
    "grupo_contabil": {"nomes_origem": ["grupo_contabil"], "data_type": "string"},
    "indexador": {"nomes_origem": ["indexador"], "data_type": "string"},
    "cge_fundo": {"nomes_origem": ["cge_fundo"], "data_type": "string"},
    "quantidade": {
        "nomes_origem": ["quantidade"],
        "data_type": "double",
    },
    "vl_preco": {
        "nomes_origem": ["vl_preco"],
        "data_type": "double",
    },
    "vl_bruto": {
        "nomes_origem": ["vl_bruto"],
        "data_type": "double",
    },
    "vl_ir": {
        "nomes_origem": ["vl_ir"],
        "data_type": "double",
    },
    "vl_iof": {
        "nomes_origem": ["vl_iof"],
        "data_type": "double",
    },
    "vl_taxa": {
        "nomes_origem": ["vl_taxa"],
        "data_type": "double",
    },
    "vl_taxa_compra": {
        "nomes_origem": ["vl_taxa_compra"],
        "data_type": "double",
    },
    "vl_liquido": {
        "nomes_origem": ["vl_liquido"],
        "data_type": "double",
    },
    "vl_captacao": {
        "nomes_origem": ["vl_captacao"],
        "data_type": "double",
    },
    "flag_nnm": {"nomes_origem": ["flag_nnm"], "data_type": "string"},
    "dt_emissao": {
        "nomes_origem": ["dt_emissao"],
        "data_type": "timestamp[us]",
    },
    "dt_exercicio": {
        "nomes_origem": ["dt_exercicio"],
        "data_type": "timestamp[us]",
    },
    "dt_vencimento": {
        "nomes_origem": ["dt_vencimento"],
        "data_type": "timestamp[us]",
    },
    "dt_liquidacao": {
        "nomes_origem": ["dt_liquidacao"],
        "data_type": "timestamp[us]",
    },
}
