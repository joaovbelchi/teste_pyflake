import json
from requests import Response
from requests.exceptions import HTTPError
import pandas as pd
import io
from typing import List, Dict, Union
from utils.OneGraph.base import OneGraphBase, refresh_token_decorator
from utils.OneGraph.exceptions import DriveItemNotFoundException
from pytz import timezone


class OneGraphDrive(OneGraphBase):
    """Classe para interagir com um drive do OneDrive via
    API do Microsoft Graph
    """

    def __init__(
        self, client_id: str, client_secret: str, tenant_id: str, drive_id: str
    ):
        """
        Args:
            client_id (str): ID do registro de aplicativo
            client_secret (str): Chave secretaa do registro de aplicativo
            tenant_id (str): ID do Tenant no qual o aplicativo reside
            drive_id (str): ID do drive no Onedrive com o qual se deseja interagir
        """
        super().__init__(
            client_id=client_id, client_secret=client_secret, tenant_id=tenant_id
        )

        self.drive_id = drive_id

    @refresh_token_decorator
    def list_children_endpoint(self, item_id: str) -> Response:
        endpoint = f"drives/{self.drive_id}/items/{item_id}/children"
        return self._api_call(method="GET", endpoint=endpoint)

    @refresh_token_decorator
    def download_file_endpoint(self, item_id: str) -> Response:
        endpoint = f"drives/{self.drive_id}/items/{item_id}/content"
        return self._api_call(method="GET", endpoint=endpoint)

    @refresh_token_decorator
    def get_drive_item_endpoint(self, item_id: str) -> Response:
        endpoint = f"drives/{self.drive_id}/items/{item_id}"
        return self._api_call(method="GET", endpoint=endpoint)

    @refresh_token_decorator
    def list_email_endpoint(self, item_id: str = None, **kwargs) -> Response:
        endpoint = (
            f"users/{item_id}/messages" if item_id else kwargs.pop("endpoint", "")
        )

        return self._api_call(method="GET", endpoint=endpoint, **kwargs)

    @refresh_token_decorator
    def send_email_endpoint(self, item_id: str, json) -> Response:
        endpoint = f"users/{item_id}/sendMail"
        return self._api_call(method="POST", endpoint=endpoint, json=json)

    @refresh_token_decorator
    def create_upload_session_endpoint(self, item_id: str, file_name: str) -> Response:
        try:
            self.get_drive_item_endpoint(item_id)
        except HTTPError as e:
            if e.response.status_code == 404:
                raise Exception(f"Não existe nenhuma pasta com esse id: '{item_id}'")
            raise e

        endpoint = (
            f"drives/{self.drive_id}/items/{item_id}:/{file_name}:/createUploadSession"
        )
        headers = {"Content-Type": "application/json"}
        data = {
            "Item": {"@microsoft.graph.conflictBehavior": "replace", "name": file_name}
        }
        return self._api_call(
            method="POST", endpoint=endpoint, data=json.dumps(data), headers=headers
        )

    def _get_ids_from_path(self, path: List[str]) -> List[Dict[str, str]]:
        """Mapeia os IDs de todos os componentes de um path
        especificado para um drive do Onedrive

        Args:
            path (List[str]): Lista contendo nomes de pastas e nome
            do arquivo no último item, formando o path. Exemplo:
            ['pasta_1', 'pasta_2', 'arquivo.csv']

        Raises:
            DriveItemNotFoundException: Se algum Drive Item passado
            no path não for encontrado

        Returns:
            List[Dict[str, str]]: Lista com mapeamentos de Drive Items
            com seus IDs. Exemplo: path=['pasta_1', 'pasta_2', 'arquivo'],
            retorno=[{'name': 'pasta_1', 'id': '1'},{'name': 'pasta_2', 'id': '2'},
            {'name': 'arquivo', 'id': '3'}]
        """
        ids = []
        item_id = parent_item_name = "root"

        for item_name in path:
            items = self.list_children_endpoint(item_id=item_id).json()["value"]
            filtered = [i["id"] for i in items if i["name"] == item_name]
            if not filtered:
                raise DriveItemNotFoundException(
                    f"Nenhum item com nome '{item_name}' encontrado em '{parent_item_name}' para o path {path}"
                )
            item_id = filtered[0]

            ids.append({"name": item_name, "id": item_id})
            parent_item_name = item_name

        return ids

    def download_file(
        self, path: List[str], to_pandas: bool = False, **kwargs
    ) -> Union[Response, pd.DataFrame]:
        """Faz download de arquivo do Onedrive. Retorna o objeto
        da Response ou um DataFrame com o conteúdo

        Args:
            path (List[str]): Lista contendo nomes de pastas e nome
            do arquivo no último item, formando o path. Exemplo:
            ['pasta_1', 'pasta_2', 'arquivo.csv']
            to_pandas (bool, optional): Se atribuído para verdadeiro,
            transforma o conteúdo baixado em um DataFrame do pandas. O
            arquivo deve ter extensão e formato suportados por esse método

        Raises:
            Exception: Se o arquivo tiver uma extensão não
            suportada para carregamento em DataFrame

        Returns:
            Union[Response, pd.DataFrame]: Por padrão, retorna um objeto Response
            do módulo 'requests'. Se to_pandas=True, retorna um DataFrame.
        """
        item_id = self._get_ids_from_path(path=path)[-1]["id"]

        print(f"Download iniciado de {path}")
        response = self.download_file_endpoint(item_id=item_id)
        print("Download_finalizado")

        if to_pandas:
            print("Leitura de DataFrame iniciada")
            file_name = path[-1]

            if (
                file_name.endswith(".xlsx")
                or file_name.endswith(".xls")
                or file_name.endswith(".xlsm")
            ):
                df = pd.read_excel(io.BytesIO(response.content), **kwargs)

            elif file_name.endswith(".csv"):
                df = pd.read_csv(io.BytesIO(response.content), **kwargs)

            elif file_name.endswith(".parquet"):
                df = pd.read_parquet(io.BytesIO(response.content), **kwargs)

            elif file_name.endswith(".pkl"):
                df = pd.read_pickle(io.BytesIO(response.content), **kwargs)

            else:
                raise Exception(
                    f"O arquivo precisa ter uma das extensões a seguir para ser carregado em um DataFrame: {self.CONTENT_TYPES.keys()}"
                )

            print("Leitura de DataFrame finalizada")
            return df
        return response

    def upload_file(
        self, path: List[str], bytes: bytes, partition_size: int = 25000000
    ) -> List[Response]:
        """Realiza um upload de bytes para um arquivo no Onedrive. O upload
        é feito via sessão de upload, portanto é possível realizar uploads
        de arquivos grandes

        Args:
            path (List[str]): Lista contendo nomes de pastas e nome
            do arquivo no último item, formando o path. Exemplo:
            ['pasta_1', 'pasta_2', 'arquivo.csv']
            bytes (bytes): Conteúdo de arquivo em formato 'bytes'. A biblioteca
            'io' do Python é útil para escrever arquivos temporários na memória
            e coletar seu valor em 'bytes'
            partition_size (int, optional): Tamanho das partições (em bytes)
            que serão enviadas a cada requisição do upload particionado

        Raises:
            Exception: Se a extensão e formato de arquivo não são suportadas
            por esse método

        Returns:
            List[Response]: Lista contendo os objetos de Response para
            cada requisição dos uploads particionados
        """
        file_name = path[-1]

        if not any(file_name.endswith(ext) for ext in self.CONTENT_TYPES.keys()):
            raise Exception(
                f"O parâmetro 'file_name' deve terminar com uma das extensões suportadas: {self.CONTENT_TYPES.keys()}"
            )

        item_id = self._get_ids_from_path(path=path[:-1])[-1]["id"]

        upload_url = self.create_upload_session_endpoint(
            item_id=item_id, file_name=file_name
        ).json()["uploadUrl"]

        print(f"Sessão de upload criada para {path}")

        extension = f".{file_name.split('.')[-1]}"
        content_type = self.CONTENT_TYPES[extension]

        num_bytes = len(bytes)
        part_start_byte = 0
        end_byte = num_bytes - 1

        response_list = []
        while part_start_byte <= end_byte:
            part_end_byte = min(end_byte, part_start_byte + partition_size - 1)

            content_range = f"bytes {part_start_byte}-{part_end_byte}/{num_bytes}"
            content_length = end_byte - part_start_byte + 1
            part_bytes = bytes[part_start_byte : part_end_byte + 1]

            headers = {
                "Content-Length": str(content_length),
                "Content-Range": content_range,
                "Content-Type": content_type,
            }

            rsp = self._api_call(
                method="PUT", custom_url=upload_url, headers=headers, data=part_bytes
            )

            if rsp.status_code == 202:
                print(f"Parte {part_start_byte}-{part_end_byte} enviada com sucesso.")
            elif rsp.status_code in (200, 201):
                print(
                    f"Última parte {part_start_byte}-{part_end_byte} enviada com sucesso."
                )

            response_list.append(rsp)

            part_start_byte = part_end_byte + 1

        return response_list

    def get_drive_item_metadata(self, path: List[str]) -> Response:
        """Retorna os metadados de um arquivo no Onedrive

        Args:
            path (List[str]): Lista contendo nomes de pastas e nome
            do arquivo no último item, formando o path. Exemplo:
            ['pasta_1', 'pasta_2', 'arquivo.csv']

        Returns:
            Response: Objeto Response, contendo os metadados
            do arquivo indicado
        """
        item_id = self._get_ids_from_path(path=path)[-1]["id"]
        return self.get_drive_item_endpoint(item_id=item_id)

    def list_drive_dir(
        self, path: List[str], mode: str = "df"
    ) -> Union[pd.DataFrame, list, dict]:
        """Retorna o conteúdo de uma pasta no OneDrive

        Args:
            path (List[str]): Lista contendo nomes de pastas na ordem,
            formando o path. Exemplo:
            ['pasta_1', 'pasta_2', 'pasta_3']
            mode (str): Formato de retorno dos dados
        Returns:
            Response: Retorna os informações arquivos/subdiretórios de acordo com o mode:
                1. 'df' -> retorna a lista de arquivos/subdiretórios em um DataFrame
                2. 'list' -> retorna a lista de arquivos/subdiretórios em uma lista
                3. 'dict' -> retorna um dicionário com os metadados completos de cada arquivo/subdiretório do arquivo indicado
                4. None -> a pasta está vazia
        """
        metadata_dir = self.get_drive_item_metadata(path).content
        id_dir = json.loads(metadata_dir.decode("utf-8"))["id"]
        raw_metada_childs_dir = self.list_children_endpoint(id_dir).content
        dict_metada_childs_dir = json.loads(raw_metada_childs_dir.decode("utf-8"))[
            "value"
        ]
        if dict_metada_childs_dir == []:
            return None
        if mode == "df":
            df = pd.DataFrame(dict_metada_childs_dir, dtype=str)
            df = df[
                ["id", "name", "@microsoft.graph.downloadUrl", "lastModifiedDateTime"]
            ]
            df = df.rename(
                columns={
                    "id": "id_arquivo",
                    "name": "nome_arquivo",
                    "@microsoft.graph.downloadUrl": "link_download",
                    "lastModifiedDateTime": "datetime_ultima_alteracao",
                }
            )

            df["datetime_ultima_alteracao"] = pd.to_datetime(
                df["datetime_ultima_alteracao"], utc=True
            ).dt.tz_convert(timezone("America/Sao_Paulo"))

            return df
        elif mode == "dict":
            return dict_metada_childs_dir
        elif mode == "list":
            return [
                [
                    file_data["id"],
                    file_data["name"],
                    file_data["@microsoft.graph.downloadUrl"],
                    pd.to_datetime(
                        file_data["lastModifiedDateTime"], utc=True
                    ).tz_convert(timezone("America/Sao_Paulo")),
                ]
                for file_data in dict_metada_childs_dir
            ]
        else:
            raise Exception("Os modos permitidos atualmente são 'df', 'dict' e 'list'")
