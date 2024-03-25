from datetime import date
import boto3
import botocore


def verifica_objeto_modificado_s3(s3_object_key: str, s3_bucket_name: str) -> bool:
    """Verifica se um arquivo do s3 foi modificado na mesmo dia
    da execução desta função

    Args:
        s3_bucket_name (str): nome do bucket onde está armazenado
        o objeto desejado.
        s3_object_key (str): chave do objeto desejado (path completo
        após '<nome_do_bucket>/').
    Returns:
        bool: Verdadeiro se o objeto existe e foi modificado no
        mesmo dia de execução desta função. Falso, caso contrário
    """
    s3 = boto3.client("s3")
    try:
        rsp = s3.head_object(Bucket=s3_bucket_name, Key=s3_object_key)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print(f"Key: '{s3_object_key}' does not exist!")
            return False
        else:
            raise Exception("Something else went wrong")

    last_modified = rsp.get("LastModified")
    today = date.today()

    if last_modified.date() >= today:
        print(f"Objeto atualizado encontrado. LastModified: {last_modified}")
        return True

    print(f"Objeto encontrado, mas desatualizado. LastModified: {last_modified}")
    return False


def move_objeto_s3(s3_object_key_src: str, s3_object_key_dst: str, s3_bucket_name: str):
    s3 = boto3.client("s3")
    s3.copy_object(
        Bucket=s3_bucket_name,
        CopySource=f"{s3_bucket_name}/{s3_object_key_src}",
        Key=s3_object_key_dst,
    )
    s3.delete_object(Bucket=s3_bucket_name, Key=s3_object_key_src)
