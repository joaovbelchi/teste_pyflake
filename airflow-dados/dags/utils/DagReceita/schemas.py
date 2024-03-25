SCHEMA_AAI = {
    "Data Receita": {"nomes_origem": ["Data Receita"], "data_type": "string"},
    "Conta": {"nomes_origem": ["Conta"], "data_type": "string"},
    "Cliente": {"nomes_origem": ["Cliente"], "data_type": "string"},
    "Assessor Principal": {
        "nomes_origem": ["Assessor Principal"],
        "data_type": "string",
    },
    "Categoria": {"nomes_origem": ["Categoria"], "data_type": "string"},
    "Produto": {"nomes_origem": ["Produto"], "data_type": "string"},
    "Ativo": {"nomes_origem": ["Ativo"], "data_type": "string"},
    "Código/CNPJ": {"nomes_origem": ["Código/CNPJ"], "data_type": "string"},
    "Tipo Receita": {"nomes_origem": ["Tipo Receita"], "data_type": "string"},
    "Receita Bruta": {"nomes_origem": ["Receita Bruta"], "data_type": "double"},
    "Receita Líquida": {"nomes_origem": ["Receita Líquida"], "data_type": "double"},
    "Comissão bruta": {"nomes_origem": ["Comissão bruta"], "data_type": "double"},
    "Comissão": {"nomes_origem": ["Comissão"], "data_type": "double"},
    "Ajuste data": {"nomes_origem": ["Ajuste data"], "data_type": "string"},
    "Ajuste data2": {"nomes_origem": ["Ajuste data2"], "data_type": "timestamp[us]"},
    "Tipo da receita": {"nomes_origem": ["Tipo da receita"], "data_type": "string"},
}

SCHEMA_AVENUE = {
    "Email": {"nomes_origem": ["Email"], "data_type": "string"},
    "CPF": {"nomes_origem": ["CPF"], "data_type": "string"},
    "Produto": {"nomes_origem": ["Produto"], "data_type": "string"},
    "Tipo": {"nomes_origem": ["Tipo"], "data_type": "string"},
    "Operação": {"nomes_origem": ["Operação"], "data_type": "string"},
    "Moeda": {"nomes_origem": ["Moeda"], "data_type": "string"},
    "Volume": {"nomes_origem": ["Volume"], "data_type": "double"},
    "Receita Bruta": {"nomes_origem": ["Receita Bruta"], "data_type": "double"},
    "Custo Total": {"nomes_origem": ["Custo Total"], "data_type": "double"},
    "Receita Liquida": {"nomes_origem": ["Receita Liquida"], "data_type": "double"},
    "Repasse (%)": {"nomes_origem": ["Repasse (%)"], "data_type": "int64"},
    "Comissão": {"nomes_origem": ["Comissão"], "data_type": "double"},
    "Mes": {"nomes_origem": ["Mes"], "data_type": "string"},
    "Data": {"nomes_origem": ["Data"], "data_type": "timestamp[us]"},
    "Conta": {"nomes_origem": ["Conta"], "data_type": "string"},
}

SCHEMA_CORBAN = {
    "Data Receita": {"nomes_origem": ["Data Receita"], "data_type": "string"},
    "Conta": {"nomes_origem": ["Conta"], "data_type": "string"},
    "Cliente": {"nomes_origem": ["Cliente"], "data_type": "string"},
    "Código Assessor": {"nomes_origem": ["Código Assessor"], "data_type": "double"},
    "Assessor Principal": {
        "nomes_origem": ["Assessor Principal"],
        "data_type": "string",
    },
    "Categoria": {"nomes_origem": ["Categoria"], "data_type": "string"},
    "Produto": {"nomes_origem": ["Produto"], "data_type": "string"},
    "Ativo": {"nomes_origem": ["Ativo"], "data_type": "string"},
    "Código/CNPJ": {"nomes_origem": ["Código/CNPJ"], "data_type": "string"},
    "Tipo Receita": {"nomes_origem": ["Tipo Receita"], "data_type": "string"},
    "Receita Bruta": {"nomes_origem": ["Receita Bruta"], "data_type": "double"},
    "Receita Líquida": {"nomes_origem": ["Receita Líquida"], "data_type": "double"},
    "Comissão": {"nomes_origem": ["Comissão"], "data_type": "double"},
}

SCHEMA_EXTRAS = {
    "Conta": {"nomes_origem": ["Conta"], "data_type": "string"},
    "Tipo": {"nomes_origem": ["Tipo"], "data_type": "string"},
    "Categoria": {"nomes_origem": ["Categoria"], "data_type": "string"},
    "Produto_1": {"nomes_origem": ["Produto_1"], "data_type": "string"},
    "Produto_2": {
        "nomes_origem": ["Produto_2"],
        "data_type": "string",
    },
    "Comissão": {"nomes_origem": ["Comissão"], "data_type": "double"},
    "Data": {"nomes_origem": ["Data"], "data_type": "timestamp[us]"},
    "Fornecedor": {"nomes_origem": ["Fornecedor"], "data_type": "string"},
    "Intermediador": {"nomes_origem": ["Intermediador"], "data_type": "string"},
    "Financeira": {"nomes_origem": ["Financeira"], "data_type": "string"},
}

SCHEMA_ICATU = {
    "Data da Posicao": {
        "nomes_origem": ["Data da Posicao"],
        "data_type": "timestamp[us]",
    },
    "Certificado": {"nomes_origem": ["Certificado"], "data_type": "string"},
    "Fundo": {"nomes_origem": ["Fundo"], "data_type": "string"},
    "Documento Fundo": {"nomes_origem": ["Documento Fundo"], "data_type": "string"},
    "Saldo em Cotas": {
        "nomes_origem": ["Saldo em Cotas"],
        "data_type": "double",
    },
    "Conta": {"nomes_origem": ["Conta", "Conta Corrente"], "data_type": "string"},
    "CPF": {"nomes_origem": ["CPF"], "data_type": "string"},
    "Cliente": {"nomes_origem": ["Cliente"], "data_type": "string"},
    "Saldo Financeiro": {"nomes_origem": ["Saldo Financeiro"], "data_type": "double"},
    "Taxa de Administracao": {
        "nomes_origem": ["Taxa de Administracao"],
        "data_type": "double",
    },
    "Percentual de Comissao": {
        "nomes_origem": ["Percentual de Comissao"],
        "data_type": "double",
    },
    "Comissao Bruta": {"nomes_origem": ["Comissao Bruta"], "data_type": "double"},
    "Comissao Liquida": {"nomes_origem": ["Comissao Liquida"], "data_type": "double"},
    "Assessor": {"nomes_origem": ["Assessor"], "data_type": "string"},
}

SCHEMA_OFFSHORE_US = {
    "Data Receita": {
        "nomes_origem": ["Data Receita"],
        "data_type": "timestamp[us]",
    },
    "Conta": {"nomes_origem": ["Conta"], "data_type": "string"},
    "Cliente": {"nomes_origem": ["Cliente"], "data_type": "string"},
    "Categoria": {"nomes_origem": ["Categoria"], "data_type": "string"},
    "Produto": {
        "nomes_origem": ["Produto"],
        "data_type": "string",
    },
    "Ativo": {"nomes_origem": ["Ativo"], "data_type": "string"},
    "Código Produto": {"nomes_origem": ["Código Produto"], "data_type": "string"},
    "Tipo Receita": {"nomes_origem": ["Tipo Receita"], "data_type": "string"},
    "Receita": {"nomes_origem": ["Receita"], "data_type": "double"},
    "Comissão": {
        "nomes_origem": ["Comissão"],
        "data_type": "double",
    },
    "Comissão real": {
        "nomes_origem": ["Comissão real"],
        "data_type": "double",
    },
}

SCHEMA_OFFSHORE_CY = {
    "Data Receita": {
        "nomes_origem": ["Data Receita"],
        "data_type": "timestamp[us]",
    },
    "Conta": {"nomes_origem": ["Conta Corrente", "Conta"], "data_type": "string"},
    "Cliente": {"nomes_origem": ["Cliente"], "data_type": "string"},
    "Categoria": {"nomes_origem": ["Categoria"], "data_type": "string"},
    "Produto": {
        "nomes_origem": ["Produto"],
        "data_type": "string",
    },
    "Ativo": {"nomes_origem": ["Ativo"], "data_type": "string"},
    "Código Produto": {"nomes_origem": ["Código Produto"], "data_type": "string"},
    "Tipo Receita": {"nomes_origem": ["Tipo Receita"], "data_type": "string"},
    "Receita": {"nomes_origem": ["Receita"], "data_type": "double"},
    "Comissão": {
        "nomes_origem": ["Comissão"],
        "data_type": "double",
    },
    "Comissão real": {
        "nomes_origem": ["Comissão real"],
        "data_type": "double",
    },
}

SCHEMA_ONEWM = {
    "Conta": {
        "nomes_origem": ["Conta"],
        "data_type": "string",
    },
    "Tipo": {"nomes_origem": ["Tipo"], "data_type": "string"},
    "Categoria": {"nomes_origem": ["Categoria"], "data_type": "string"},
    "Produto_1": {"nomes_origem": ["Produto_1"], "data_type": "string"},
    "Produto_2": {
        "nomes_origem": ["Produto_2"],
        "data_type": "string",
    },
    "Comissão": {"nomes_origem": ["Comissão"], "data_type": "double"},
    "Data": {"nomes_origem": ["Data"], "data_type": "timestamp[us]"},
}

SCHEMA_OURIBANK = {
    "Data_liquidacao": {
        "nomes_origem": ["Data_liquidacao"],
        "data_type": "timestamp[us]",
    },
    "Boletado": {"nomes_origem": ["Boletado"], "data_type": "string"},
    "n_ficha": {"nomes_origem": ["n_ficha"], "data_type": "int64"},
    "Tipo_cliente": {"nomes_origem": ["Tipo_cliente"], "data_type": "string"},
    "Moeda": {
        "nomes_origem": ["Moeda"],
        "data_type": "string",
    },
    "Operação": {"nomes_origem": ["Operação"], "data_type": "string"},
    "Valor_moeda": {"nomes_origem": ["Valor_moeda"], "data_type": "double"},
    "Taxa_cliente": {"nomes_origem": ["Taxa_cliente"], "data_type": "double"},
    "Spot": {"nomes_origem": ["Spot"], "data_type": "double"},
    "Spread": {"nomes_origem": ["Spread"], "data_type": "double"},
    "Tarifa_cliente": {
        "nomes_origem": ["Tarifa_cliente"],
        "data_type": "double",
    },
    "Tarifa_one": {
        "nomes_origem": ["Tarifa_one"],
        "data_type": "double",
    },
    "Valor_resultado": {
        "nomes_origem": ["Valor_resultado"],
        "data_type": "double",
    },
    "Comissionamento": {
        "nomes_origem": ["Comissionamento"],
        "data_type": "double",
    },
    "Valor_reais": {
        "nomes_origem": ["Valor_reais"],
        "data_type": "double",
    },
    "Comissao": {
        "nomes_origem": ["Comissao"],
        "data_type": "double",
    },
    "Conta": {
        "nomes_origem": ["Conta"],
        "data_type": "string",
    },
}

SCHEMA_PREV_BTG = {
    "Data_liquidacao": {
        "nomes_origem": ["Data_liquidacao"],
        "data_type": "timestamp[us]",
    },
    "Boletado": {"nomes_origem": ["Boletado"], "data_type": "timestamp[us]"},
    "n_ficha": {"nomes_origem": ["n_ficha"], "data_type": "int64"},
    "Tipo_cliente": {"nomes_origem": ["Tipo_cliente"], "data_type": "string"},
    "Moeda": {
        "nomes_origem": ["Moeda"],
        "data_type": "string",
    },
    "Operação": {"nomes_origem": ["Operação"], "data_type": "string"},
    "Valor_moeda": {"nomes_origem": ["Valor_moeda"], "data_type": "double"},
    "Taxa_cliente": {"nomes_origem": ["Taxa_cliente"], "data_type": "double"},
    "Spot": {"nomes_origem": ["Spot"], "data_type": "double"},
    "Spread": {"nomes_origem": ["Spread"], "data_type": "double"},
    "Tarifa_cliente": {
        "nomes_origem": ["Tarifa_cliente"],
        "data_type": "double",
    },
    "Tarifa_one": {
        "nomes_origem": ["Tarifa_one"],
        "data_type": "double",
    },
    "Valor_resultado": {
        "nomes_origem": ["Valor_resultado"],
        "data_type": "double",
    },
    "Comissionamento": {
        "nomes_origem": ["Comissionamento"],
        "data_type": "double",
    },
    "Valor_reais": {
        "nomes_origem": ["Valor_reais"],
        "data_type": "double",
    },
    "Comissao": {
        "nomes_origem": ["Comissao"],
        "data_type": "double",
    },
    "Conta": {
        "nomes_origem": ["Conta"],
        "data_type": "string",
    },
}

SCHEMA_PREV_BTG = {
    "Data da Posicao": {
        "nomes_origem": ["Data da Posicao"],
        "data_type": "timestamp[us]",
    },
    "Certificado": {"nomes_origem": ["Certificado"], "data_type": "string"},
    "Fundo": {"nomes_origem": ["Fundo"], "data_type": "string"},
    "Documento Fundo": {"nomes_origem": ["Documento Fundo"], "data_type": "string"},
    "Saldo em Cotas": {
        "nomes_origem": ["Saldo em Cotas"],
        "data_type": "double",
    },
    "Conta Corrente": {"nomes_origem": ["Conta Corrente"], "data_type": "string"},
    "CPF": {"nomes_origem": ["CPF"], "data_type": "string"},
    "Cliente": {"nomes_origem": ["Cliente"], "data_type": "string"},
    "Saldo Financeiro": {"nomes_origem": ["Saldo Financeiro"], "data_type": "double"},
    "Taxa de Administracao": {
        "nomes_origem": ["Taxa de Administracao"],
        "data_type": "double",
    },
    "Percentual de Comissao": {
        "nomes_origem": ["Percentual de Comissao"],
        "data_type": "double",
    },
    "Comissao Bruta": {
        "nomes_origem": ["Comissao Bruta"],
        "data_type": "double",
    },
    "Comissao Liquida": {
        "nomes_origem": ["Comissao Liquida"],
        "data_type": "double",
    },
    "Assessor": {
        "nomes_origem": ["Assessor"],
        "data_type": "string",
    },
}

SCHEMA_SEMANAL = {
    "Conta": {
        "nomes_origem": ["Conta"],
        "data_type": "string",
    },
    "Categoria": {"nomes_origem": ["Categoria"], "data_type": "string"},
    "Produto": {"nomes_origem": ["Produto"], "data_type": "string"},
    "Código/CNPJ": {"nomes_origem": ["Código/CNPJ"], "data_type": "string"},
    "Comissão Líquida": {
        "nomes_origem": ["Comissão Líquida"],
        "data_type": "double",
    },
    "Data ajustada": {"nomes_origem": ["Data ajustada"], "data_type": "timestamp[us]"},
    "Data Receita": {"nomes_origem": ["Data Receita"], "data_type": "timestamp[us]"},
    "Cliente": {"nomes_origem": ["Cliente"], "data_type": "string"},
    "Assessor Principal": {
        "nomes_origem": ["Assessor Principal"],
        "data_type": "string",
    },
    "Ativo": {
        "nomes_origem": ["Ativo"],
        "data_type": "string",
    },
    "Tipo Receita": {
        "nomes_origem": ["Tipo Receita"],
        "data_type": "string",
    },
    "Receita Bruta": {
        "nomes_origem": ["Receita Bruta"],
        "data_type": "double",
    },
    "Receita Líquida": {
        "nomes_origem": ["Receita Líquida"],
        "data_type": "double",
    },
    "Comissão Bruta": {
        "nomes_origem": ["Comissão Bruta"],
        "data_type": "double",
    },
    "Tipo da receita": {
        "nomes_origem": ["Tipo da receita"],
        "data_type": "string",
    },
    "Código Produto": {
        "nomes_origem": ["Código Produto"],
        "data_type": "string",
    },
    "Receita": {
        "nomes_origem": ["Receita"],
        "data_type": "double",
    },
    "CGE AA": {
        "nomes_origem": ["CGE AA"],
        "data_type": "string",
    },
    "Comissão": {
        "nomes_origem": ["Comissão"],
        "data_type": "double",
    },
    "Código Assessor": {
        "nomes_origem": ["Código Assessor"],
        "data_type": "string",
    },
    "Tipo": {
        "nomes_origem": ["Tipo"],
        "data_type": "string",
    },
    "Fornecedor": {
        "nomes_origem": ["Fornecedor"],
        "data_type": "string",
    },
    "Intermediador": {
        "nomes_origem": ["Intermediador"],
        "data_type": "string",
    },
    "Financeira": {
        "nomes_origem": ["Financeira"],
        "data_type": "string",
    },
    "Mes": {
        "nomes_origem": ["Mes"],
        "data_type": "string",
    },
    "BASE": {
        "nomes_origem": ["BASE"],
        "data_type": "string",
    },
}

SCHEMA_PRUDENTIAL = {
    "Conta": {
        "nomes_origem": ["Conta"],
        "data_type": "string",
    },
    "Data": {"nomes_origem": ["Data"], "data_type": "timestamp[us]"},
    "Apolice": {"nomes_origem": ["Apolice"], "data_type": "string"},
    "Segurado": {
        "nomes_origem": ["Segurado"],
        "data_type": "string",
    },
    "Premio_liquido": {"nomes_origem": ["Premio_liquido"], "data_type": "double"},
    "Taxa_adm": {
        "nomes_origem": ["Taxa_adm"],
        "data_type": "double",
    },
    "Comissao_nova": {"nomes_origem": ["Comissao_nova"], "data_type": "double"},
    "Comissao_renovacao": {
        "nomes_origem": ["Comissao_renovacao"],
        "data_type": "double",
    },
    "Descricao": {
        "nomes_origem": ["Descricao"],
        "data_type": "string",
    },
    "Produto": {
        "nomes_origem": ["Produto"],
        "data_type": "string",
    },
    "Capital_segurado": {"nomes_origem": ["Capital_segurado"], "data_type": "double"},
    "Pagamento_ano": {"nomes_origem": ["Pagamento_ano"], "data_type": "string"},
    "Pagamento_mes": {"nomes_origem": ["Pagamento_mes"], "data_type": "string"},
    "Periodicidade": {"nomes_origem": ["Periodicidade"], "data_type": "string"},
    "Data_assinatura": {
        "nomes_origem": ["Data_assinatura"],
        "data_type": "timestamp[us]",
    },
    "Data_emissao": {"nomes_origem": ["Data_emissao"], "data_type": "timestamp[us]"},
}

SCHEMA_SILVER_PROCESSADA = {
    "Conta": {
        "nomes_origem": ["Conta"],
        "data_type": "string",
    },
    "Comissão": {"nomes_origem": ["Comissão"], "data_type": "double"},
    "Data": {"nomes_origem": ["Data"], "data_type": "timestamp[us]"},
    "Produto_2": {"nomes_origem": ["Produto_2"], "data_type": "string"},
    "ano_particao": {
        "nomes_origem": ["ano_particao"],
        "data_type": "int64",
    },
    "mes_particao": {"nomes_origem": ["mes_particao"], "data_type": "int64"},
    "atualizacao_incremental": {
        "nomes_origem": ["atualizacao_incremental"],
        "data_type": "string",
    },
    "Categoria": {"nomes_origem": ["Categoria"], "data_type": "string"},
    "Tipo": {"nomes_origem": ["Tipo"], "data_type": "string"},
    "Intermediador": {
        "nomes_origem": ["Intermediador"],
        "data_type": "string",
    },
    "Produto_1": {
        "nomes_origem": ["Produto_1"],
        "data_type": "string",
    },
    "Fornecedor": {"nomes_origem": ["Fornecedor"], "data_type": "string"},
    "Financeira": {"nomes_origem": ["Financeira"], "data_type": "string"},
}
