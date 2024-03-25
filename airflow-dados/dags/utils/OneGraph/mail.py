import pandas as pd
import base64
from typing import Union, Optional
from requests import Response
from utils.OneGraph.base import OneGraphBase, refresh_token_decorator
from utils.OneGraph.mail_assets import MailAssets
import io


class Mail(OneGraphBase, MailAssets):
    """Classe para permitir a criação de e-mails sem usar HTML, CSS e JS."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        tenant_id: str,
        subject: str,
        recipients: Union[list, str],
        sender_id: str,
    ) -> None:
        """Args:
        client_id (str): ID do registro de aplicativo.
        client_secret (str): Chave secretaa do registro de aplicativo.
        tenant_id (str): ID do Tenant no qual o aplicativo reside.
        subject (str): Assunto do e-mails.
        recipients (list | str): Endereços de e-mails dos destinatários.
        sender_id (str): ID do e-mail do remetente.
        """

        self.subject = subject

        if type(recipients) == str:
            self.recipients = [recipients]
        elif type(recipients) == list:
            self.recipients = recipients
        else:
            raise ("Informe os e-mails dos destinatários no formato adequado!")

        self.sender_id = sender_id

        self.attachments = []

        self.table_counter = 1
        self.paragraph_counter = 1
        self.title_counter = 1
        self.image_counter = 1
        self.attachment_counter = 1

        self.body = "<body>"
        self.head = """<head>
                            <meta charset="UTF-8">
                            <style>
                                body {
                                    text-align: left;
                                    font-size: large;
                                }
                                table {
                                    border-collapse: collapse;
                                    width: 100%;
                                }
                                td {
                                    border: 1px solid orange;
                                    text-align: center;
                                    font-size: small;
                                }
                                tr:nth-child(even) {
                                    background-color: white;
                                }
                                tr:not(:first-child):nth-child(odd) {
                                    background-color: gray;
                                }
                                th{
                                    background-color: orange;
                                    text-align: center;
                                    font-size: medium;
                                }                                                                                    
                            """

        super().__init__(
            client_id=client_id, client_secret=client_secret, tenant_id=tenant_id
        )

    def paragraph(self, paragraph: str) -> None:
        """Escreve um parágrafo.

        Args:
            paragraph (str): Texto do parágrafo.
        """
        self.body += (
            f'<div class="paragraph_{self.paragraph_counter}"><p>{paragraph}</p></div>'
        )
        self.paragraph_counter += 1

    def attachment(self, file_name: str):
        ## definir função geral de envio de arquivos em anexo
        pass

    def table_as_xlsx(self, file_name: str, df: pd.DataFrame) -> None:
        """Transforma um DataFrame em um arquivo Excel anexo ao e-mail.

        Args:
            file_name (str): Nome no arquivos xlsx anexado.
            df (pd.DataFrame): DataFrame que será convertido em xlsx.
        """

        with io.BytesIO() as df_to_excel_buffer:
            df.to_excel(df_to_excel_buffer, index=False)
            df_to_excel_bytes = df_to_excel_buffer.getvalue()
            df_to_excel_bytes_64 = base64.b64encode(df_to_excel_bytes).decode("utf-8")

        self.attachments.append(
            {
                "@odata.type": "#microsoft.graph.fileAttachment",
                "name": file_name,
                "contentBytes": df_to_excel_bytes_64,
                "contentType": "text/plain",
            }
        )

        self.table_counter += 1
        self.attachment_counter += 1

    def table(
        self,
        df_table: pd.DataFrame,
        pgnr_number: Optional[list] = False,
        pgnr_textual_number: Optional[list] = False,
        highlight_last_row: Optional[bool] = False,
    ) -> None:
        """Inclui tabela no e-mail.

        Args:
            df_table (pd.DataFrame): DataFrame que será transformado em tabela e estilizado.
            pgnr_number (Optional[list], optional): Lista com os nomes das colunas em que os números são tipos numéricos. Defaults to False.
            pgnr_textual_number (Optional[list], optional): Lista com os nomes das colunas em que os números são strings. Defaults to False.
            highlight_last_row (Optional[bool], optional): Opção de destacar se a última linha de uma tabela será destacada. Defaults to False.
        """
        if not pgnr_number:
            df_table.map(
                lambda x: 0
                if pd.isna(x)
                else int(x)
                if isinstance(x, (int, float))
                else x
            )
            df_table.style.map(self.__pgnr_number, subset=pgnr_number)
        if not pgnr_textual_number:
            df_table.map(
                lambda x: 0
                if pd.isna(x)
                else int(x)
                if isinstance(x, (int, float))
                else x
            )
            df_table.style.map(self.__pgnr_textual_number, subset=pgnr_textual_number)

        if not highlight_last_row:
            df_table.style.set_table_styles(
                [
                    {
                        "selector": "tr:last-child",
                        "props": [("background-color", "yellow")],
                    }
                ]
            )

        html_table = df_table.to_html(
            classes=f"table_{self.table_counter}", escape=False, index=False
        )
        self.body += f'<div class="table_{self.table_counter}">{html_table}</div>'
        self.table_counter += 1

    def title(self, title: str) -> None:
        """Escreve título do e-mail.

        Args:
            title (str): Título do e-mail
        """
        self.body += f'<div class="title_{self.title_counter}"><h3>{title}</h3></div>'
        self.title_counter += 1

    def image(self, path_to_image: str, mode: str = "attachement") -> None:
        """Adiciona imagem ao e-mail com anexo.

        Args:
            path_to_image (str): Caminho onde se encontra a imagem a ser enviada.
        """

        posibles_modes = ["link", "attachment"]
        image_id_class = f"image_{self.image_counter}"

        if mode not in posibles_modes:
            raise ValueError(f"Os modos pemitidos são os seguintes: {posibles_modes}")
        elif mode == "link":
            self.body += f'<div class="{image_id_class}"><img src="{path_to_image}" class="{image_id_class}" width="400"></div>'
        elif mode == "attachment":
            with open(path_to_image, "rb") as image_file:
                image_content = base64.b64encode(image_file.read()).decode("utf-8")

            self.attachments.append(
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": path_to_image,
                    "contentBytes": image_content,
                    "isInline": True,
                    "contentId": image_id_class,
                }
            )

            self.body += f'<div class="{image_id_class}"><img src="data:image/png;base64,{image_content}" class="{image_id_class}"></div>'
            self.image_counter += 1
            self.attachment_counter += 1

    def __pgnr_textual_number(text: str) -> str:
        """Aplica cores verde e vermelhas para as células nos DataFrames para gerar HTML que diferencia visualmente números negativos de positivos.

        Args:
            text (str): Valor de entrada

        Returns:
            inline_css (str): CSS inline que coloca número negativos em vermelho e números positivos em verde.
        """
        return "color: red" if "-" in text else "color: blue"

    def __pgnr_number(number: Union[int, float]) -> str:
        """Aplica cores verde e vermelhas para as células nos DataFrames para gerar HTML que diferencia visualmente números negativos de positivos.

        Args:
            number (int|float): Valor de entrada

        Returns:
            inline_css (str): CSS inline que coloca número negativos em vermelho e números positivos em verde.
        """
        return "color: red" if number < 0 else "color: blue"

    def line_break(self) -> None:
        """Adiciona uma quebra de linha entre parágrafos."""
        self.body += "<br>"

    def __end_mail(self) -> None:
        """Finaliza HTML do e-mail"""
        self.body += "</body>"
        self.head += "</style></head>"
        self.template_email = self.head + self.body

        self.json_graph_api = {
            "message": {
                "subject": self.subject,
                "body": {
                    "contentType": "HTML",
                    "content": self.template_email,
                },
                "toRecipients": [
                    {"emailAddress": {"address": mail}} for mail in self.recipients
                ],
            }
        }
        if self.attachments:
            self.json_graph_api["message"]["attachments"] = self.attachments

    @refresh_token_decorator
    def send_mail(self) -> Response:
        """Fecha o HTML do e-mail e o envia.

        Returns:
            Response: Resposta HTTP do envio do e-mail.
        """
        self.__end_mail()
        endpoint = f"users/{self.sender_id}/sendMail"
        return self._api_call(
            method="POST", endpoint=endpoint, json=self.json_graph_api
        )

    def mail_signature():
        ## na primeira versao nao preencherei esta funcao porque precisamos decidir a forma de gerar a assinatura (gerar a partir de bases de dados ou salvar brutos em um bucket ou no drive)
        ...
