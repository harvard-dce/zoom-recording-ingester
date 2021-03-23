import boto3
import jmespath
from os import getenv as _getenv
from functions.common.common import zoom_api_request

AWS_PROFILE = _getenv("AWS_PROFILE")
if AWS_PROFILE:
    boto3.setup_default_session(profile_name=AWS_PROFILE)


# wrap the default getenv so we can enforce required vars
def getenv(param_name, required=True):
    val = _getenv(param_name)
    if required and (val is None or val.strip() == ""):
        raise Exception(f"Missing environment variable {param_name}")
    return val


def zoom_admin_id():
    # get admin user id from admin email
    r = zoom_api_request(f"users/{getenv('ZOOM_ADMIN_EMAIL')}")
    return r.json()["id"]


def vpc_components():

    oc_cluster_name = getenv("OC_CLUSTER_NAME")
    opsworks = boto3.client("opsworks")

    stacks = opsworks.describe_stacks()
    vpc_id = jmespath.search(
        f"Stacks[?Name=='{oc_cluster_name}'].VpcId", stacks
    )[0]

    ec2_boto = boto3.client("ec2")
    security_groups = ec2_boto.describe_security_groups(
        Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {
                "Name": "tag:aws:cloudformation:logical-id",
                "Values": ["OpsworksLayerSecurityGroupCommon"],
            },
        ]
    )
    sg_id = jmespath.search("SecurityGroups[0].GroupId", security_groups)

    return vpc_id, sg_id


def oc_base_url():

    ec2 = boto3.client("ec2")
    oc_cluster_name = getenv("OC_CLUSTER_NAME")

    result = ec2.describe_instances(
        Filters=[
            {"Name": "tag:opsworks:stack", "Values": [oc_cluster_name]},
            {"Name": "tag:opsworks:layer:admin", "Values": ["Admin"]},
        ]
    )
    if "Reservations" not in result or not result["Reservations"]:
        raise Exception(
            f"No dns name found for OC_CLUSTER_NAME {oc_cluster_name}"
        )
    dns_name = result["Reservations"][0]["Instances"][0]["PublicDnsName"]
    return "http://" + dns_name.strip()


def oc_db_url():

    rds = boto3.client("rds")
    oc_cluster_name = getenv("OC_CLUSTER_NAME")
    db_password = getenv("OC_DB_PASSWORD")

    result = rds.describe_db_clusters(
        DBClusterIdentifier=f"{oc_cluster_name}-cluster"
    )

    if "DBClusters" not in result or not result["DBClusters"]:
        raise Exception(
            f"Failed RDS cluster lookup for OC_CLUSTER_NAME {oc_cluster_name}"
        )
    reader_endpoint = result["DBClusters"][0]["ReaderEndpoint"]
    return f"mysql://root:{db_password}@{reader_endpoint}:3306/opencast"


def aws_account_id():
    return boto3.client("sts").get_caller_identity()["Account"]


def stack_tags():
    """
    STACK_TAGS looks like 'Key=foo,Value=1 Key=bar,Value=baz'
    """
    env_tags = getenv("STACK_TAGS", False)

    if not env_tags:
        return {}

    return dict(
        [[x.split("=")[1] for x in y.split(",")] for y in env_tags.split()]
    )
