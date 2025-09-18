"""
BigQuery loading helpers for GCE using pandas_gbq.

Requires:
    pip install pandas pandas-gbq google-cloud-bigquery pyarrow

Auth on GCE:
    - Attach a service account to the VM with BigQuery permissions (e.g.,
      roles/bigquery.dataEditor on the target dataset and roles/bigquery.jobUser).
    - No key files needed; uses ADC automatically.

Usage:
    df = pd.DataFrame([...])
    load_dataframe_to_bq(
        df,
        project_id="my-project",
        dataset_id="analytics",
        table_id="events",
        if_exists="append",   # "fail" | "replace" | "append"
        location="US",
    )
"""

from typing import Optional, Sequence
import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from pandas_gbq import to_gbq


def ensure_dataset(
    project_id: str,
    dataset_id: str,
    *,
    location: str = "US",
    labels: Optional[dict] = None,
) -> None:
    """
    Ensure a BigQuery dataset exists; create if missing.

    Parameters
    ----------
    project_id : str
        GCP project ID.
    dataset_id : str
        BigQuery dataset name (without project prefix).
    location : str, default "US"
        Dataset location ("US", "EU", or region like "us-central1").
    labels : Optional[dict], default None
        Dataset labels to set on creation.
    """
    client = bigquery.Client(project=project_id, location=location)
    ds_ref = bigquery.DatasetReference(project_id, dataset_id)
    try:
        client.get_dataset(ds_ref)
    except NotFound:
        ds = bigquery.Dataset(ds_ref)
        ds.location = location
        if labels:
            ds.labels = labels
        client.create_dataset(ds, exists_ok=True)


def _build_bq_load_job_config(
    *,
    partition_field: Optional[str] = None,
    clustering_fields: Optional[Sequence[str]] = None,
    create_disposition: str = "CREATE_IF_NEEDED",
    write_disposition: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Construct a BigQuery load job configuration dict for use with pandas_gbq/to_gbq.

    Parameters
    ----------
    partition_field : Optional[str], default None
        Column name (DATE/TIMESTAMP/DATETIME) used for DAY-based time partitioning.
        If provided, job_config['timePartitioning'] = {"type": "DAY", "field": partition_field}.
    clustering_fields : Optional[Sequence[str]], default None
        Up to four column names to cluster by within each partition. Improves pruning/grouping.
        If provided, job_config['clustering'] = {"fields": list(clustering_fields)}.
    create_disposition : str, default "CREATE_IF_NEEDED"
        Table creation behavior:
          - "CREATE_IF_NEEDED": auto-create if missing (default).
          - "CREATE_NEVER": fail if table does not exist.
        Sets job_config['createDisposition'].
    write_disposition : Optional[str], default None
        Write behavior when table exists:
          - "WRITE_APPEND": append rows.
          - "WRITE_TRUNCATE": overwrite table data.
          - "WRITE_EMPTY": fail if not empty.
        If provided, sets job_config['writeDisposition'].

    Returns
    -------
    Optional[Dict[str, Any]]
        A dict suitable for the BigQuery LoadJob configuration (i.e., the value for
        configuration['load']) or None if no advanced options were requested.

    Notes
    -----
    - Any keys whose values would be None are removed to avoid API validation errors.
    - Example result when all options are used:
        {
          "timePartitioning": {"type": "DAY", "field": "event_time"},
          "clustering": {"fields": ["user_id"]},
          "createDisposition": "CREATE_IF_NEEDED",
          "writeDisposition": "WRITE_APPEND"
        }
    """
    if not any([partition_field, clustering_fields, write_disposition]) and create_disposition == "CREATE_IF_NEEDED":
        return None

    cfg: Dict[str, Any] = {}
    if partition_field:
        cfg["timePartitioning"] = {"type": "DAY", "field": partition_field}
    if clustering_fields:
        cfg["clustering"] = {"fields": list(clustering_fields)}
    if create_disposition:
        cfg["createDisposition"] = create_disposition
    if write_disposition:
        cfg["writeDisposition"] = write_disposition

    return cfg


def load_dataframe_to_bq(
    df: pd.DataFrame,
    *,
    project_id: str,
    dataset_id: str,
    table_id: str,
    if_exists: str = "append",
    location: str = "US",
    # Optional advanced config:
    schema: Optional[Sequence[dict]] = None,
    partition_field: Optional[str] = None,           # e.g., "event_date"
    clustering_fields: Optional[Sequence[str]] = None,  # e.g., ["user_id", "type"]
    create_disposition: str = "CREATE_IF_NEEDED",    # or "CREATE_NEVER"
    write_disposition: Optional[str] = None,         # overrides if_exists if set
) -> None:
    """
    Load a pandas DataFrame to BigQuery using pandas_gbq, creating the dataset if needed.

    Parameters
    ----------
    df : pandas.DataFrame
        Data to load.
    project_id : str
        GCP project ID.
    dataset_id : str
        BigQuery dataset name.
    table_id : str
        BigQuery table name (no project/dataset prefix).
    if_exists : {"fail","replace","append"}, default "append"
        Behavior when table exists (pandas_gbq semantics).
    location : str, default "US"
        Job location / dataset location.
    schema : Optional[Sequence[dict]], default None
        Explicit schema (list of {"name": ..., "type": ..., "mode": ...}).
        If None, pandas_gbq infers types (good with pyarrow).
    partition_field : Optional[str], default None
        Field to use for time-partitioning (DATE/TIMESTAMP/DATETIME).
    clustering_fields : Optional[Sequence[str]], default None
        Up to 4 clustering columns.
    create_disposition : str, default "CREATE_IF_NEEDED"
        BigQuery create disposition.
    write_disposition : Optional[str], default None
        BigQuery write disposition; if provided, overrides `if_exists`.
    """
    # Make sure the dataset exists
    ensure_dataset(project_id, dataset_id, location=location)

    table_fqdn = f"{dataset_id}.{table_id}"


 
    job_config = _build_bq_load_job_config(
            partition_field=partition_field,
            clustering_fields=clustering_fields,
            create_disposition=create_disposition,
            write_disposition=write_disposition,
        )

    to_gbq(
        dataframe=df,
        destination_table=table_fqdn,
        project_id=project_id,
        if_exists=if_exists,
        location=location,
        table_schema=list(schema) if schema else None,
        api_method="load_csv",   # fastest for most DataFrames; uses load job
        # Pass through job config if we built one
        configuration={"load": job_config} if job_config else None,
    )


# Example usage....
load_dataframe_to_bq(
    df,
    project_id="my-project",
    dataset_id="analytics",
    table_id="events",
    if_exists="append",
    location="US",
    partition_field="event_time",
    clustering_fields=["user_id", "action"],
)

