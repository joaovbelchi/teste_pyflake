from typing import List, Dict, Union
import unicodedata

import re


def formata_numero_para_texto_monetario(number: Union[int, float]) -> str:
    """Converte um número para uma string com formato visualmente adequado à visualização de valores financeiros.

    Args:
        number (int, float): String a ser normalizada.

    Returns:
        str: String após normalização.
    """
    a = "{:,.2f}".format(float(number))
    b = a.replace(",", "v")
    c = b.replace(".", ",")
    return c.replace("v", ".")


def normaliza_caracteres_unicode(string: str) -> str:
    """Normaliza caracteres unicode para garantir que operações de junção por campos de texto não falhem.
    Uma boa referência para o tema pode ser encontrada aqui: https://learn.microsoft.com/pt-br/windows/win32/intl/using-unicode-normalization-to-represent-strings

    Args:
        string (str): String a ser normalizada.

    Returns:
        str: String após normalização.
    """
    return (
        unicodedata.normalize("NFKD", string).encode("ASCII", "ignore").decode().strip()
    )


def sneak_case_ascii(str: str) -> str:
    """Transforma um string em sneak_case com caracters ASCII.

    Args:
        str (str): String a ser transformada.

    Returns:
        str_final(str): String em ASCII sneak-cased.
    """
    str_ascii = "".join(
        c for c in unicodedata.normalize("NFKD", str) if unicodedata.category(c) != "Mn"
    )
    str_ascii = str_ascii.lower().replace("-", "").replace("/", "")
    str_final = re.sub(r"\s+", "_", str_ascii)

    return str_final
