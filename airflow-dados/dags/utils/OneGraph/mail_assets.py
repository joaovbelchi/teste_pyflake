from datetime import datetime, time
import pytz


class MailAssets:
    """Classe estática para centralizar imagens e links de uso geral em e-mails."""

    logo_one = "https://mcusercontent.com/8f5ac152e0221cbbe6722b174/images/d55142d1-8621-bc5c-a4b8-23012f582906.png"

    class Greetings:
        """Cumprimentos que dependem de condições."""

        @staticmethod
        def time_of_day() -> str:
            """Dá bom dia, boa tarde ou boa noite (sempre em letras minúsculas e sem pontuação).

            Returns:
                str: Saudação do dia.
            """
            fuso_horario = pytz.timezone("America/Sao_Paulo")
            data_hora_utc = datetime.utcnow()
            data_hora_local = data_hora_utc.replace(tzinfo=pytz.utc).astimezone(
                fuso_horario
            )

            time_brasil_agora = data_hora_local.time()
            if time(4, 30, 0, 0) <= time_brasil_agora < time(6, 0, 0, 0):
                return "perdão pelo incômodo tão cedo"
            elif time(6, 0, 0, 0) <= time_brasil_agora < time(12, 00, 0, 0):
                return "bom dia"
            elif time(12, 00, 0, 0) <= time_brasil_agora <= time(18, 00, 0, 0):
                return "boa tarde"
            elif time(18, 00, 0, 0) < time_brasil_agora <= time(22, 00, 0, 0):
                return "boa noite"
            else:
                return "perdão pelo incômodo tão tarde"
