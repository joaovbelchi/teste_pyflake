SCHEMA_SUGESTAO_FUNDOS = {
    "fundo": {"nomes_origem": ["Fundo"], "data_type": "string"},
    "cnpj": {"nomes_origem": ["CNPJ"], "data_type": "string"},
    "pl_one": {"nomes_origem": ["PL One"], "data_type": "double"},
    "categoria": {"nomes_origem": ["Categoria"], "data_type": "string"},
    "margem_adm": {"nomes_origem": ["Margem Adm"], "data_type": "double"},
    "performance": {"nomes_origem": ["Performance"], "data_type": "double"},
    "cnpj_novo": {"nomes_origem": ["CNPJ Novo"], "data_type": "string"},
    "fundo_nome": {"nomes_origem": ["Fundo Novo"], "data_type": "string"},
    "margem_adm_novo": {"nomes_origem": ["Margem Adm Novo"], "data_type": "double"},
    "performance_novo": {"nomes_origem": ["Perfomance Novo"], "data_type": "double"},
    "dif_adm": {"nomes_origem": ["Dif Adm"], "data_type": "string"},
    "status": {"nomes_origem": ["Status"], "data_type": "string"},
    "maximo_por_fundo": {"nomes_origem": ["Máximo por fundo "], "data_type": "double"},
}

SCHEMA_SUGESTAO_CP = {
    "emissor": {"nomes_origem": ["Emissor"], "data_type": "string"},
    "setor": {"nomes_origem": ["Setor"], "data_type": "string"},
    "codigo": {"nomes_origem": ["Código"], "data_type": "string"},
    "vencimento": {"nomes_origem": ["Vencimento"], "data_type": "timestamp[us]"},
    "produto": {"nomes_origem": ["Produto"], "data_type": "string"},
}

SCHEMA_TERMOS_NEGATIVOS = {
    "emissor": {"nomes_origem": ["Emissor"], "data_type": "string"},
    "produto": {"nomes_origem": ["Produto"], "data_type": "string"},
    "vencimento": {"nomes_origem": ["Vencimento"], "data_type": "timestamp[us]"},
}

SCHEMA_OFERTAS_PUBLICAS = {
    "emissor": {"nomes_origem": ["Emissor"], "data_type": "string"},
    "produto": {"nomes_origem": ["Produto"], "data_type": "string"},
    "indexador": {"nomes_origem": ["Indexador"], "data_type": "string"},
    "vencimento": {"nomes_origem": ["Vencimento"], "data_type": "timestamp[us]"},
    "inicio_reserva": {
        "nomes_origem": ["Inicio reserva"],
        "data_type": "timestamp[us]",
    },
    "fim_reserva": {"nomes_origem": ["Fim reserva"], "data_type": "timestamp[us]"},
    "roa_bruto": {"nomes_origem": ["ROA Bruto"], "data_type": "double"},
}

SCHEMA_ASSET_ALLOCATION = {
    "perfil": {"nomes_origem": ["Perfil"], "data_type": "string"},
    "categoria": {"nomes_origem": ["Categoria"], "data_type": "string"},
    "maximo": {"nomes_origem": ["Máximo"], "data_type": "double"},
    "minimo": {"nomes_origem": ["Mínimo"], "data_type": "double"},
    "maximo_por_ativo": {"nomes_origem": ["Máximo por ativo"], "data_type": "double"},
}

SCHEMA_DEBENTURES_ISENTAS = {
    "emissor": {"nomes_origem": ["EMISSOR"], "data_type": "string"},
    "setor": {"nomes_origem": ["SETOR"], "data_type": "string"},
    "codigo": {"nomes_origem": ["CÓDIGO"], "data_type": "string"},
    "vencimento": {"nomes_origem": ["VENCIMENTO"], "data_type": "timestamp[us]"},
    "rating": {"nomes_origem": ["RATING"], "data_type": "string"},
    "agencia": {"nomes_origem": ["AGÊNCIA"], "data_type": "string"},
    "indexador": {"nomes_origem": ["INDEXADOR"], "data_type": "string"},
    "taxa_maxima": {"nomes_origem": ["TAXA MÁXIMA"], "data_type": "double"},
    "taxa_sugerida": {"nomes_origem": ["TAXA SUGERIDA"], "data_type": "double"},
    "taxa_bruta_equivalente": {
        "nomes_origem": ["TAXA BRUTA EQUIVALENTE"],
        "data_type": "double",
    },
    "receita_estimada": {"nomes_origem": ["RECEITA ESTIMADA"], "data_type": "double"},
    "duration": {"nomes_origem": ["DURATION (em anos)"], "data_type": "double"},
    "investidor_qualificado": {
        "nomes_origem": ["INVESTIDOR QUALIFICADO"],
        "data_type": "string",
    },
    "amortizacao": {"nomes_origem": ["AMORTIZAÇÃO"], "data_type": "string"},
    "juros": {"nomes_origem": ["JUROS"], "data_type": "string"},
    "quantidade_indicativa": {
        "nomes_origem": ["QUANTIDADE INDICATIVA"],
        "data_type": "double",
    },
    "relatorio": {"nomes_origem": ["RELATÓRIO"], "data_type": "string"},
}

SCHEMA_DEBENTURES_NAO_ISENTAS = {
    "emissor": {"nomes_origem": ["EMISSOR"], "data_type": "string"},
    "setor": {"nomes_origem": ["SETOR"], "data_type": "string"},
    "codigo": {"nomes_origem": ["CÓDIGO"], "data_type": "string"},
    "vencimento": {"nomes_origem": ["VENCIMENTO"], "data_type": "timestamp[us]"},
    "rating": {"nomes_origem": ["RATING"], "data_type": "string"},
    "agencia": {"nomes_origem": ["AGÊNCIA"], "data_type": "string"},
    "indexador": {"nomes_origem": ["INDEXADOR"], "data_type": "string"},
    "taxa_maxima": {"nomes_origem": ["TAXA MÁXIMA"], "data_type": "double"},
    "taxa_sugerida": {"nomes_origem": ["TAXA SUGERIDA"], "data_type": "double"},
    "taxa_bruta_equivalente": {
        "nomes_origem": ["TAXA BRUTA EQUIVALENTE"],
        "data_type": "string",
    },
    "receita_estimada": {"nomes_origem": ["RECEITA ESTIMADA"], "data_type": "double"},
    "duration": {"nomes_origem": ["DURATION (em anos)"], "data_type": "double"},
    "investidor_qualificado": {
        "nomes_origem": ["INVESTIDOR QUALIFICADO"],
        "data_type": "string",
    },
    "amortizacao": {"nomes_origem": ["AMORTIZAÇÃO"], "data_type": "string"},
    "juros": {"nomes_origem": ["JUROS"], "data_type": "string"},
    "quantidade_indicativa": {
        "nomes_origem": ["QUANTIDADE INDICATIVA"],
        "data_type": "double",
    },
    "relatorio": {"nomes_origem": ["RELATÓRIO"], "data_type": "string"},
}

SCHEMA_CRI_CRA = {
    "emissor": {"nomes_origem": ["EMISSOR"], "data_type": "string"},
    "setor": {"nomes_origem": ["SETOR"], "data_type": "string"},
    "codigo": {"nomes_origem": ["CÓDIGO"], "data_type": "string"},
    "vencimento": {"nomes_origem": ["VENCIMENTO"], "data_type": "timestamp[us]"},
    "rating": {"nomes_origem": ["RATING"], "data_type": "string"},
    "agencia": {"nomes_origem": ["AGÊNCIA"], "data_type": "string"},
    "indexador": {"nomes_origem": ["INDEXADOR"], "data_type": "string"},
    "taxa_maxima": {"nomes_origem": ["TAXA MÁXIMA"], "data_type": "double"},
    "taxa_sugerida": {"nomes_origem": ["TAXA SUGERIDA"], "data_type": "double"},
    "taxa_bruta_equivalente": {
        "nomes_origem": ["TAXA BRUTA EQUIVALENTE"],
        "data_type": "double",
    },
    "receita_estimada": {"nomes_origem": ["RECEITA ESTIMADA"], "data_type": "double"},
    "duration": {"nomes_origem": ["DURATION (em anos)"], "data_type": "double"},
    "investidor_qualificado": {
        "nomes_origem": ["INVESTIDOR QUALIFICADO"],
        "data_type": "string",
    },
    "amortizacao": {"nomes_origem": ["AMORTIZAÇÃO"], "data_type": "string"},
    "juros": {"nomes_origem": ["JUROS"], "data_type": "string"},
    "quantidade_indicativa": {
        "nomes_origem": ["QUANTIDADE INDICATIVA"],
        "data_type": "double",
    },
    "relatorio": {"nomes_origem": ["RELATÓRIO"], "data_type": "string"},
}

SCHEMA_CREDITO_BANCARIO = {
    "emissor": {"nomes_origem": ["EMISSOR"], "data_type": "string"},
    "produto": {"nomes_origem": ["PRODUTO"], "data_type": "string"},
    "prazo": {"nomes_origem": ["PRAZO"], "data_type": "double"},
    "vencimento": {"nomes_origem": ["VENCIMENTO"], "data_type": "timestamp[us]"},
    "indexador": {"nomes_origem": ["INDEXADOR"], "data_type": "string"},
    "taxa_maxima": {"nomes_origem": ["TX. MÁXIMA"], "data_type": "double"},
    "taxa_sugerida": {"nomes_origem": ["TX. PORTAL"], "data_type": "double"},
    "spread_portal": {"nomes_origem": ["SPREAD PORTAL"], "data_type": "double"},
    "receita_estimada": {"nomes_origem": ["RECEITA ESTIMADA"], "data_type": "double"},
    "aplicacao_minima": {"nomes_origem": ["APLICAÇÃO MÍNIMA"], "data_type": "double"},
    "rating": {"nomes_origem": ["RATING"], "data_type": "string"},
    "juros": {"nomes_origem": ["JUROS"], "data_type": "string"},
}

SCHEMA_TITULO_PUBLICO = {
    "vencimento": {"nomes_origem": ["VENCIMENTO"], "data_type": "timestamp[us]"},
    "taxa_sugerida": {"nomes_origem": ["TAXA PORTAL DAS 10hs"], "data_type": "double"},
    "spread_portal": {"nomes_origem": ["SPREAD PADRÃO"], "data_type": "double"},
    "receita_estimada": {"nomes_origem": ["RECEITA ESTIMADA"], "data_type": "double"},
    "produto": {"nomes_origem": ["Produto"], "data_type": "string"},
}
