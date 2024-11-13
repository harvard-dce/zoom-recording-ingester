import boto3
import jmespath
from os import getenv as env
from functions.utils import getenv

AWS_PROFILE = env("AWS_PROFILE")
if AWS_PROFILE:
    boto3.setup_default_session(profile_name=AWS_PROFILE)


def vpc_components():
    stack_type = getenv("STACK_TYPE", False)
    oc_cluster_name = getenv("OC_CLUSTER_NAME")
    if not stack_type or stack_type != "ecs":
        # Opsworks
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
    else:
        # ECS deployment
        # Get the VPC id and the deployment's "private" security group id
        # from the stack exports
        vpc_id = _get_stack_export(f"{oc_cluster_name}-vpc-id")
        sg_id = _get_stack_export(
            f"{oc_cluster_name}-private-security-group-id"
        )
        return vpc_id, sg_id


def oc_base_url():
    stack_type = getenv("STACK_TYPE", False)
    oc_cluster_name = getenv("OC_CLUSTER_NAME")
    if not stack_type or stack_type != "ecs":
        # Opsworks
        ec2 = boto3.client("ec2")

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
    else:
        # ECS deployment
        host = _get_stack_export(f"{oc_cluster_name}-admin-public-hostname")
        if not host:
            raise Exception(
                f"No admin host name found for OC_CLUSTER_NAME {oc_cluster_name}"
            )
        return "https://" + host


def _get_stack_export(export_name):
    oc_cluster_name = getenv("OC_CLUSTER_NAME")
    stack_name = f"opencast-ecs-{oc_cluster_name}-database"
    cloudformation = boto3.client("cloudformation")

    result = cloudformation.describe_stacks(StackName=stack_name)
    value = jmespath.search(
        f"Stacks[0].Outputs[?ExportName=='{export_name}'].OutputValue",
        result,
    )
    return value[0] if value else None


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
