"""
Secret Manager helpers for GCE

THIS CODE IS UNTESTED, NO IDEA IF IT'LL WORK.

Requires:
    pip install google-cloud-secret-manager

IAM:
    - Attach a service account to the VM with
      roles/secretmanager.secretAccessor on the target secret (or project).
"""

from typing import Optional
from google.cloud import secretmanager


def get_secret_str(
    project_id: str,
    secret_id: str,
    version: str = "latest",
    *,
    client: Optional[secretmanager.SecretManagerServiceClient] = None,
) -> str:
    """
    Fetch and return a secret value as UTF-8 text from Google Secret Manager.

    Parameters
    ----------
    project_id : str
        GCP project ID that owns the secret.
    secret_id : str
        Secret name (resource short name, not the full path).
    version : str, default "latest"
        Secret version to access (e.g., "latest", "1", "5").
    client : Optional[SecretManagerServiceClient], default None
        An existing client to reuse. If None, a new one is created using
        Application Default Credentials on GCE.

    Returns
    -------
    str
        The secret payload decoded as UTF-8 text.

    Raises
    ------
    google.api_core.exceptions.GoogleAPICallError
        On API errors (permission denied, not found, etc.).
    """
    c = client or secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"
    resp = c.access_secret_version(request={"name": name})
    return resp.payload.data.decode("utf-8")


