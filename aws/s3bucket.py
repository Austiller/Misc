from imds import boto3_session_from_role_name


"""
To set up an S3 bucket to be accessible by your ec2 instance...in AWS, under IAM, create a role with trusted entity = EC2 (so EC2 instances can assume it).
and attach a policy that gives it access to the S3 bucket. When you launch the  ec2 instance, under IAM Role, 
choose the role you just created.
"""
def get_s3_client(
    *,
    role_name: Optional[str] = None,
    region_name: Optional[str] = None,
) -> BaseClient:
    """
    Return an S3 client.

    - If role_name is provided, fetch credentials for that role via IMDS.
    - If no role_name, fall back to boto3 default resolution (which already
      uses the instance role automatically).
    """
    if role_name:
        session = boto3_session_from_access_key_id(role_name, region_name=region_name)
        return session.client("s3")
    else:
        return boto3.client("s3", region_name=region_name)
