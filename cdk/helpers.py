import boto3
import jmespath
from os import getenv as _getenv
from functools import lru_cache
from functions.common import zoom_api_request

AWS_PROFILE = _getenv("AWS_PROFILE")
if AWS_PROFILE is not None:
    boto3.setup_default_session(profile_name=AWS_PROFILE)

# wrap the default getenv so we can enforce required vars
def getenv(param_name, required=True):
    val = _getenv(param_name)
    if required and (val is None or val.strip() == ''):
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
        f"Stacks[?Name=='{oc_cluster_name}'].VpcId",
        stacks
    )[0]

    ec2_boto = boto3.client("ec2")
    security_groups = ec2_boto.describe_security_groups(
        Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "tag:aws:cloudformation:logical-id",
             "Values": ["OpsworksLayerSecurityGroupCommon"]}
        ]
    )
    sg_id = jmespath.search("SecurityGroups[0].GroupId", security_groups)

    return vpc_id, sg_id

def oc_base_url():

    ec2 = boto3.client('ec2')
    oc_cluster_name = getenv("OC_CLUSTER_NAME")

    result = ec2.describe_instances(
        Filters=[
            {
                "Name": "tag:opsworks:stack",
                "Values": [oc_cluster_name]
            },
            {
                "Name": "tag:opsworks:layer:admin",
                "Values": ["Admin"]
            }
        ]
    )
    if "Reservations" not in result or len(result["Reservations"]) == 0:
        raise Exception(
            f"No dns name found for OC_CLUSTER_NAME {oc_cluster_name}"
        )
    dns_name = result["Reservations"][0]["Instances"][0]["PublicDnsName"]
    return "http://" + dns_name.strip()


@lru_cache()
def aws_identity():
    return boto3.client('sts').get_caller_identity()

def aws_account_id():
    return aws_identity()['Account']

def aws_region():
    return aws_identity()['Region']
