import requests
import json
import boto3
from typing import Optional, Dict, Any
from botocore.session import Session as BotoCoreSession

IMDS_BASE = "http://169.254.169.254"
IMDS_TIMEOUT = (0.2, 1.5)

"""
Essentially this code fetches temporary creds tied to the ec2 instance that's running the code. 

You can then create a boto3 session and access resources.
"""


def get_imds_token() -> Optional[str]:
    """
    Get an IMDSv2 session token for querying EC2 instance metadata.

    Returns
    -------
    Optional[str]
        The token string if successful, otherwise None (fall back to IMDSv1).
    """
    try:
        r = requests.put(
            f"{IMDS_BASE}/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
            timeout=IMDS_TIMEOUT,
        )
        if r.ok:
            return r.text
    except requests.RequestException:
        pass
    return None


def get_instance_metadata(path: str, token: Optional[str] = None) -> str:
    """
    Fetch a specific metadata value from the EC2 Instance Metadata Service.

    Parameters
    ----------
    path : str
        Relative metadata path under `latest/meta-data/`.
        Examples:
          - "instance-id"
          - "ami-id"
          - "iam/security-credentials/"
          - "iam/security-credentials/{role_name}"
    token : Optional[str], default None
        IMDSv2 token. If not provided, attempts IMDSv1.

    Returns
    -------
    str
        The metadata value as plain text.
    """
    headers = {}
    if token:
        headers["X-aws-ec2-metadata-token"] = token

    url = f"{IMDS_BASE}/latest/meta-data/{path.lstrip('/')}"
    r = requests.get(url, headers=headers, timeout=IMDS_TIMEOUT)
    r.raise_for_status()
    return r.text


def get_role_names(token: Optional[str] = None) -> List[str]:
    """
    Get IAM role names attached to the EC2 instance.

    Parameters
    ----------
    token : Optional[str], default None
        IMDSv2 token. If not provided, attempts IMDSv1.

    Returns
    -------
    List[str]
        List of role names (usually just one).
    """
    raw = get_instance_metadata("iam/security-credentials/", token)
    return [line.strip() for line in raw.splitlines() if line.strip()]


def get_role_credentials(role_name: str, token: Optional[str] = None) -> Dict[str, Any]:
    """
    Get IAM role credentials (temporary keys) for a given role.

    Parameters
    ----------
    role_name : str
        The IAM role name to fetch credentials for.
    token : Optional[str], default None
        IMDSv2 token. If not provided, attempts IMDSv1.

    Returns
    -------
    Dict[str, Any]
        Dictionary of credentials including:
        - AccessKeyId
        - SecretAccessKey
        - Token
        - Expiration
        - Code
        - Type
        - LastUpdated
    """
    raw = get_instance_metadata(f"iam/security-credentials/{role_name}", token)
    return json.loads(raw)


def boto3_session_from_access_key_id(
    access_key_id: str, region_name: Optional[str] = None
) -> boto3.Session:
    """
    Create a boto3.Session from instance metadata credentials
    that match a specific AccessKeyId.

    Parameters
    ----------
    access_key_id : str
        The AWS AccessKeyId to look up in instance metadata.
    region_name : Optional[str], default None
        AWS region for the boto3 session. If None, boto3's normal
        region resolution applies.

    Returns
    -------
    boto3.Session
        A boto3 session authenticated with the matching temporary credentials.

    Raises
    ------
    RuntimeError
        If no role credentials with the given AccessKeyId are found.
    """
    token = get_imds_token()
    roles = get_role_names(token)

    for role in roles:
        creds = get_role_credentials(role, token)
        if creds.get("AccessKeyId") == access_key_id and creds.get("Code") == "Success":
            return boto3.Session(
                aws_access_key_id=creds["AccessKeyId"],
                aws_secret_access_key=creds["SecretAccessKey"],
                aws_session_token=creds.get("Token"),
                region_name=region_name,
            )

    raise RuntimeError(f"No credentials found with AccessKeyId={access_key_id!r}")
