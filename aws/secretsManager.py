from .imds import boto3_session_from_access_key_id

def get_secret_from_secrets_manager(
    access_key_id: str,
    secret_id: str,
    region_name: str = "us-east-1"
) -> str:
    """
    Fetch a plaintext secret from AWS Secrets Manager.

    Parameters
    ----------
    access_key_id : str
        The AccessKeyId to select from EC2 instance metadata credentials.
    secret_id : str
        The secret name or ARN in Secrets Manager.
    region_name : str, default "us-east-1"
        AWS region.

    Returns
    -------
    str
        The secret value (string). Raises KeyError if not found.
    """
    session = boto3_session_from_access_key_id(access_key_id, region_name=region_name)
    sm = session.client("secretsmanager")
    resp = sm.get_secret_value(SecretId=secret_id)

    if "SecretString" in resp:
        return resp["SecretString"]
    return base64.b64decode(resp["SecretBinary"]).decode("utf-8")