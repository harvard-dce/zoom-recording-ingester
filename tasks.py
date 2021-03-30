import sys
import json
import boto3
import requests
from requests.auth import HTTPDigestAuth
import time
import shutil
from datetime import datetime, timedelta, date as datetime_date
from invoke import task, Collection
from invoke.exceptions import Exit
from os import symlink, mkdir, listdir, getenv as env
from dotenv import load_dotenv
from os.path import join, dirname, exists, relpath
from tabulate import tabulate
from pprint import pprint
from functions.utils import (
    zoom_api_request,
    GSheetsAuth,
    schedule_json_to_dynamo,
    schedule_csv_to_dynamo,
)
from multiprocessing import Process
from urllib.parse import urlparse, quote
from cdk import names
from functools import lru_cache
import logging

# suppress warnings for cases where we want to ignore dev
# cluster dummy certificates
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

dotenv_path = join(dirname(__file__), ".env")
load_dotenv(dotenv_path, override=True)

AWS_PROFILE = env("AWS_PROFILE")
AWS_DEFAULT_REGION = env("AWS_DEFAULT_REGION", "us-east-1")
STACK_NAME = env("STACK_NAME")
OC_CLUSTER_NAME = env("OC_CLUSTER_NAME")
PROD_IDENTIFIER = "prod"
NONINTERACTIVE = env("NONINTERACTIVE")
INGEST_ALLOWED_IPS = env("INGEST_ALLOWED_IPS", "")

LAMBDA_CODE_BUCKET = env("LAMBDA_CODE_BUCKET")
LAMBDA_CODE_URI = f"s3://{LAMBDA_CODE_BUCKET}/{STACK_NAME}"
RECORDINGS_URI = f"s3://{STACK_NAME}-{names.RECORDINGS_BUCKET}"

if AWS_PROFILE is not None:
    boto3.setup_default_session(profile_name=AWS_PROFILE)


@task
def production_failsafe(ctx):
    """
    This is not a standalone task and should not be added to any of the task
    collections. It is meant to be prepended to the execution of other tasks
    to force a confirmation when a task is being executed that could have an
    impact on a production stack
    """
    if not STACK_NAME:
        raise Exit("No STACK_NAME specified")

    if not NONINTERACTIVE and PROD_IDENTIFIER in STACK_NAME.lower():
        print("You are about to run this task on a production system")
        ok = input("are you sure? [y/N] ").lower().strip().startswith("y")
        if not ok:
            raise Exit("Aborting")


@task(
    pre=[production_failsafe],
    help={"revision": "tag or branch name to build and release (required)"},
)
def codebuild(ctx, revision):
    """
    Execute a codebuild run. Optional: --revision=[tag or branch]
    """
    project_name = f"{STACK_NAME}-{names.CODEBUILD_PROJECT}"
    cmd = (
        "aws {} codebuild start-build "
        "--project-name {} --source-version {} "
        " --environment-variables-override"
        " name='STACK_NAME',value={},type=PLAINTEXT"
        " name='NONINTERACTIVE',value=1"
    ).format(profile_arg(), project_name, revision, STACK_NAME)

    res = ctx.run(cmd, hide="out")
    build_id = json.loads(res.stdout)["build"]["id"]

    cmd = "aws {} codebuild batch-get-builds --ids={}".format(
        profile_arg(), build_id
    )
    current_phase = "IN_PROGRESS"
    print("Waiting for codebuild to finish...")
    while True:
        time.sleep(5)
        res = ctx.run(cmd, hide="out")
        build = json.loads(res.stdout)["builds"][0]

        new_phase = build["currentPhase"]
        if new_phase != current_phase:
            print(current_phase)
            current_phase = new_phase

        build_complete = build["buildComplete"]
        if build_complete:
            build_status = build["buildStatus"]
            print("Build finished with status {}".format(build_status))
            break


@task(pre=[production_failsafe])
def stack_create(ctx):
    """
    Package & upload the lambda function code
    and build the CloudFormation stack
    """
    for func in names.FUNCTIONS:
        __build_function(ctx, func, upload_to_s3=True)

    ctx.run(f"cdk deploy -c VIA_INVOKE=true {profile_arg()}", pty=True)


@task(pre=[production_failsafe])
def stack_update(ctx):
    """
    Updates the CloudFormation stack
    (use the deploy.* tasks to update functions)
    """
    ctx.run(f"cdk deploy -c VIA_INVOKE=true {profile_arg()}", pty=True)


@task
def stack_diff(ctx):
    """
    Output a cdk diff of the Cloudformation stack
    """
    ctx.run(f"cdk -c VIA_INVOKE=true diff {profile_arg()}")


@task
def stack_synth(ctx):
    """
    Output the cdk-generated CloudFormation template
    """
    ctx.run(f"cdk synth -c VIA_INVOKE=true {profile_arg()}")


@task
def stack_list(ctx):
    """
    Outputs the name of the cdk CloudFormation stack
    """
    ctx.run(f"cdk list -c VIA_INVOKE=true {profile_arg()}")


@task
def stack_delete(ctx):
    """
    Deletes the cdk CloudFormation stack
    """
    empty_bucket_cmd = (
        f"aws {profile_arg()} s3 rm " f"--recursive {RECORDINGS_URI}"
    )
    lambda_code_cmd = (
        f"aws {profile_arg()} s3 rm " f"--recursive {LAMBDA_CODE_URI}"
    )
    delete_cmd = f"cdk destroy --force -c VIA_INVOKE=true {profile_arg()}"

    confirm = (
        f"\nAre you sure you want to delete stack '{STACK_NAME}'?\n"
        "WARNING: This will also delete all recording files "
        f"in '{RECORDINGS_URI}' and all lambda function code "
        f"in '{LAMBDA_CODE_URI}'.\n"
        "Type the stack name to confirm deletion: "
    )

    if input(confirm).strip() == STACK_NAME:
        ctx.run(empty_bucket_cmd)
        ctx.run(lambda_code_cmd)
        ctx.run(delete_cmd)


@task(help={"function": "name of a specific function"})
def package(ctx, function=None, upload_to_s3=False):
    """
    Package function(s) + deps into a zip file.
    """
    functions = resolve_function_arg(function)
    for func in functions:
        __build_function(ctx, func, upload_to_s3)


@task(
    pre=[production_failsafe], help={"function": "name of specific function"}
)
def deploy(ctx, function=None, do_release=False):
    """
    Package, upload and register new code for all lambda functions
    """
    functions = resolve_function_arg(function)
    for func in functions:
        __build_function(ctx, func)
        __update_function(ctx, func)
        if do_release:
            release(ctx, func)


@task(pre=[production_failsafe])
def deploy_schedule_update(ctx, do_release=False):
    deploy(ctx, names.SCHEDULE_UPDATE_FUNCTION, do_release)


@task(pre=[production_failsafe])
def deploy_status(ctx, do_release=False):
    deploy(ctx, names.STATUS_FUNCTION, do_release)


@task(pre=[production_failsafe])
def deploy_slack(ctx, do_release=False):
    deploy(ctx, names.SLACK_FUNCTION, do_release)


@task(pre=[production_failsafe])
def deploy_on_demand(ctx, do_release=False):
    deploy(ctx, names.ON_DEMAND_FUNCTION, do_release)


@task(pre=[production_failsafe])
def deploy_opencast_op_counts(ctx, do_release=False):
    deploy(ctx, names.OP_COUNTS_FUNCTION, do_release)


@task(pre=[production_failsafe])
def deploy_webhook(ctx, do_release=False):
    deploy(ctx, names.WEBHOOK_FUNCTION, do_release)


@task(pre=[production_failsafe])
def deploy_downloader(ctx, do_release=False):
    deploy(ctx, names.DOWNLOAD_FUNCTION, do_release)


@task(pre=[production_failsafe])
def deploy_uploader(ctx, do_release=False):
    deploy(ctx, names.UPLOAD_FUNCTION, do_release)


@task(
    pre=[production_failsafe], help={"function": "name of specific function"}
)
def release(ctx, function=None, description=None):
    """
    Publish a new version of the function(s)
    and update the release alias to point to it
    """
    functions = resolve_function_arg(function)
    for func in functions:
        new_version = __publish_version(ctx, func, description)
        __update_release_alias(ctx, func, new_version, description)


@task
def update_requirements(ctx):
    """
    Run a `pip-compile -U` on all requirements files
    """
    req_file = relpath(join(dirname(__file__), "requirements.in"))
    ctx.run(
        "pip install -U pip-tools && pip-compile -r -U {}".format(req_file)
    )
    for func in names.FUNCTIONS:
        req_file = relpath(
            join(dirname(__file__), "function_requirements/{}.in".format(func))
        )
        ctx.run("pip-compile -r -U {}".format(req_file))


@task(
    help={
        "uuid": "meeting instance uuid",
        "oc_series_id": "opencast series id",
        "allow_multiple_ingests": (
            "whether to allow this recording to " "be ingested multiple times"
        ),
    }
)
def exec_on_demand(ctx, uuid, oc_series_id=None, allow_multiple_ingests=False):
    """
    Manually trigger an on demand ingest.
    """

    event_body = {"uuid": uuid.strip()}

    if oc_series_id:
        event_body["oc_series_id"] = oc_series_id.strip()

    if allow_multiple_ingests:
        event_body["allow_multiple_ingests"] = allow_multiple_ingests

    print(event_body)

    resp = __invoke_api(on_demand_resource_id(), event_body)

    print(
        "Returned with status code: {}. {}".format(
            resp["status"], resp["body"]
        )
    )


@task(
    help={
        "uuid": "meeting instance uuid",
        "ignore_schedule": (
            "ignore schedule, use default series if " "available"
        ),
        "oc_series_id": (
            "opencast series id to use regardless of " "schedule"
        ),
    }
)
def exec_pipeline(ctx, uuid, ignore_schedule=False, oc_series_id=None):
    """
    Manually trigger the webhook handler, downloader, and uploader.
    """

    print("\nTriggering webhook...\n")
    exec_webhook(ctx, uuid, oc_series_id)

    # Keep retrying downloader until some messages are processed
    # or it fails.
    print("\nTriggering downloader...\n")
    resp = exec_downloader(ctx, ignore_schedule=ignore_schedule)
    wait = 1
    while not resp:
        resp = exec_downloader(ctx)
        print(" waiting {} seconds to retry".format(wait))
        time.sleep(wait)
        wait *= 2

    if "FunctionError" in resp:
        print("Downloader failed!")
        return

    # Keep retrying uploader until some messages are processed
    # or it fails.
    print("\nTriggering uploader...\n")
    resp = exec_uploader(ctx)

    if resp and "FunctionError" in resp:
        print("Uploader failed!")
        return


@task(
    help={
        "uuid": "meeting instance uuid",
        "oc_series_id": "opencast series id",
    }
)
def exec_webhook(ctx, uuid, oc_series_id=None):
    """
    Manually trigger the webhook endpoint. uuid, optional: --oc-series-id
    """

    if not uuid:
        raise Exit("You must provide a recording uuid")

    double_urlencoded_uuid = quote(quote(uuid, safe=""), safe="")
    data = zoom_api_request(
        "meetings/{}/recordings".format(double_urlencoded_uuid)
    ).json()

    required_fields = ["host_id", "recording_files"]
    for field in required_fields:
        if field not in data:
            pprint(data)
            raise Exception("No {} found in response.\n".format(field))

    for file in data["recording_files"]:
        if "status" in file and file["status"] != "completed":
            raise Exception("Not all recordings have completed processing.")
        if "id" not in file and file["file_type"].lower() != "mp4":
            data["recording_files"].remove(file)

    if oc_series_id:
        event_body = {
            "event": "on.demand.ingest",
            "payload": {
                "on_demand_series_id": oc_series_id.strip(),
                "object": data,
            },
        }
    else:
        event_body = {
            "event": "recording.completed",
            "payload": {"object": data, "delay_seconds": 0},
        }

    resp = __invoke_api(webhook_resource_id(), event_body)

    print(
        "Returned with status code: {}. {}".format(
            resp["status"], resp["body"]
        )
    )


@task(
    help={
        "series_id": "override normal opencast series id lookup",
        "ignore_schedule": (
            "do opencast series id lookup but"
            " ignore if meeting times don't match",
        ),
    }
)
def exec_downloader(
    ctx, series_id=None, ignore_schedule=False, qualifier=None
):
    """
    Manually trigger downloader.
    """
    if queue_is_empty(ctx, names.DOWNLOAD_QUEUE):
        print("No downloads in queue")
        return

    payload = {"ignore_schedule": ignore_schedule}

    if series_id:
        payload["override_series_id"] = series_id

    if not qualifier:
        qualifier = names.LAMBDA_RELEASE_ALIAS

    cmd = (
        "aws lambda invoke --function-name='{}-zoom-downloader' "
        "--payload='{}' --qualifier {} outfile.txt"
    ).format(STACK_NAME, json.dumps(payload), qualifier)
    print(cmd)
    res = json.loads(ctx.run(cmd).stdout)

    return res


@task
def exec_uploader(ctx, qualifier=None):
    """
    Manually trigger uploader.
    """
    if queue_is_empty(ctx, "upload.fifo"):
        print("No uploads in queue")
        return

    if qualifier is None:
        qualifier = names.LAMBDA_RELEASE_ALIAS

    cmd = (
        "aws lambda invoke --function-name='{}-zoom-uploader' "
        "--qualifier {} outfile.txt"
    ).format(STACK_NAME, qualifier)

    print(cmd)
    res = json.loads(ctx.run(cmd).stdout)

    if "FunctionError" in res:
        ctx.run("cat outfile.txt && echo")

    return res


@task
def status(ctx):
    """
    Show table of CloudFormation stack details
    """
    __show_stack_status(ctx)
    __show_function_status(ctx)
    __show_sqs_status(ctx)


@task(pre=[production_failsafe])
def debug_on(ctx):
    """
    Enable debug logging in all lambda functions
    """
    __set_debug(ctx, 1)


@task(pre=[production_failsafe])
def debug_off(ctx):
    """
    Disable debug logging in all lambda functions
    """
    __set_debug(ctx, 0)


@task(pre=[production_failsafe])
def test(ctx):
    """
    Execute the pytest tests
    """
    ctx.run("py.test --cov-report term-missing --cov=functions tests -vv")


@task(pre=[production_failsafe])
def retry_downloads(ctx, limit=1, uuid=None):
    """
    Move SQS messages DLQ to source. Optional: --limit (default 1).
    """
    downloads_dlq = queue_url(names.DOWNLOAD_DLQ)
    downloads_queue = queue_url(names.DOWNLOAD_QUEUE)
    __move_messages(downloads_dlq, downloads_queue, limit=limit, uuid=uuid)


@task(pre=[production_failsafe])
def retry_uploads(ctx, limit=1, uuid=None):
    """
    Move SQS messages DLQ to source. Optional: --limit (default 1).
    """
    uploads_dql = queue_url(names.UPLOAD_DLQ)
    uploads_queue = queue_url(names.UPLOAD_QUEUE)
    __move_messages(uploads_dql, uploads_queue, limit=limit, uuid=uuid)


@task(pre=[production_failsafe])
def view_downloads(ctx, limit=20):
    """
    View items in download queues. Optional: --limit (default 20).
    """
    downloads_queue = queue_url(names.DOWNLOAD_QUEUE)
    downloads_dlq = queue_url(names.DOWNLOAD_DLQ)
    __view_messages(downloads_queue, limit=limit)
    __view_messages(downloads_dlq, limit=limit)


@task(pre=[production_failsafe])
def view_uploads(ctx, limit=20):
    """
    View items in upload queues. Optional: --limit (default 20).
    """
    uploads_queue = queue_url(names.UPLOAD_QUEUE)
    uploads_dql = queue_url(names.UPLOAD_DLQ)
    __view_messages(uploads_queue, limit=limit)
    __view_messages(uploads_dql, limit=limit)


@task(pre=[production_failsafe])
def import_schedule_from_file(ctx, filename):
    """
    Import schedule from file. Used for testing.
    """
    if not filename.endswith(".json"):
        print("Invalid file type {}. File must be .json".format(filename))

    schedule_json_to_dynamo(names.SCHEDULE_TABLE, filename)

    print("Schedule updated")


@task(pre=[production_failsafe])
def import_schedule_from_opencast(ctx, endpoint=None):
    """
    Fetch schedule data from Opencast series endpoint
    """
    if endpoint is None:
        engage_host = oc_host(ctx, "engage")
        endpoint = "https://{}/otherpubs/search/series.json".format(
            engage_host
        )

    session = requests.Session()
    api_user = getenv("OPENCAST_API_USER")
    api_pass = getenv("OPENCAST_API_PASSWORD")
    session.auth = HTTPDigestAuth(api_user, api_pass)
    session.headers.update(
        {
            "X-REQUESTED-AUTH": "Digest",
            "X-Opencast-Matterhorn-Authorization": "true",
        }
    )

    try:
        print("Fetching helixEvents from {}".format(endpoint))
        r = session.get(endpoint, verify=False)
        r.raise_for_status()
        helix_events = r.json()["helixEvents"]
    except requests.HTTPError as e:
        raise Exit(
            "Failed fetching schedule from {}: {}".format(endpoint, str(e))
        )
    except Exception as e:
        raise Exit("Got bad response from endpoint: {}".format(str(e)))

    if int(helix_events["total"]) == 0:
        print("No helix events returned")
        return

    # yes all these values really do show up in this column
    day_of_week_map = {
        "M": ("MON", "Mon"),
        "T": ("TUES", "Tu", "Tues"),
        "W": ("WED", "Wed"),
        "R": ("THURS", "Th", "Thurs"),
        "F": ("FRI"),
    }

    def get_day_of_week_code(day):
        if day in day_of_week_map:
            return day
        try:
            return next(k for k, v in day_of_week_map.items() if day in v)
        except StopIteration:
            raise Exit(
                "Unable to map day of week '{}' from schedule".format(day)
            )

    schedule_data = {}
    for event in helix_events["resultSet"]:
        try:
            zoom_link = urlparse(event["zoomLink"])
            zoom_series_id = zoom_link.path.split("/")[-1]
            schedule_data.setdefault(zoom_series_id, {})
            schedule_data[zoom_series_id]["zoom_series_id"] = zoom_series_id
            schedule_data[zoom_series_id]["opencast_series_id"] = event[
                "seriesId"
            ]
            schedule_data[zoom_series_id]["opencast_subject"] = event[
                "seriesNumber"
            ]

            day = get_day_of_week_code(event["day"])
            schedule_data[zoom_series_id].setdefault("Days", [])
            if day not in schedule_data[zoom_series_id]["Days"]:
                schedule_data[zoom_series_id]["Days"].append(day)

            time_object = datetime.strptime(event["time"], "%I:%M %p")
            schedule_data[zoom_series_id]["Time"] = [
                datetime.strftime(time_object, "%H:%M"),
                (time_object + timedelta(minutes=30)).strftime("%H:%M"),
                (time_object + timedelta(hours=1)).strftime("%H:%M"),
            ]
        except KeyError as e:
            raise Exit(
                "helix event missing data: {}\n{}".format(
                    str(e), json.dumps(event, indent=2)
                )
            )
        except Exception as e:
            raise Exit(
                "Failed converting to dynamo item format: {}\n{}".format(
                    str(e), json.dumps(event, indent=2)
                )
            )

    schedule_json_to_dynamo(names.SCHEDULE_TABLE, schedule_data=schedule_data)

    print("Schedule updated")


@task(pre=[production_failsafe])
def import_schedule_from_csv(ctx, filepath):
    schedule_csv_to_dynamo(names.SCHEDULE_TABLE, filepath)


@task
def logs(ctx, function=None, watch=False):

    functions = resolve_function_arg(function)

    def _awslogs(group, watch=False):
        watch_flag = watch and "--watch" or ""
        cmd = "awslogs get {} ALL {} {}".format(
            group, watch_flag, profile_arg()
        )
        ctx.run(cmd)

    procs = []
    for func in functions:
        group = "/aws/lambda/{}-{}".format(STACK_NAME, func)
        procs.append(Process(target=_awslogs, name=func, args=(group, watch)))

    for p in procs:
        p.start()

    for p in procs:
        p.join()


@task
def recording(ctx, uuid, function=None):
    functions = resolve_function_arg(function)
    for function in functions:
        __find_recording_log_events(ctx, function, uuid)


@task
def logs_on_demand(ctx, watch=False):
    logs(ctx, names.ON_DEMAND_FUNCTION, watch)


@task
def logs_webhook(ctx, watch=False):
    logs(ctx, names.WEBHOOK_FUNCTION, watch)


@task
def logs_downloader(ctx, watch=False):
    logs(ctx, names.DOWNLOAD_FUNCTION, watch)


@task
def logs_uploader(ctx, watch=False):
    logs(ctx, names.UPLOAD_FUNCTION, watch)


@task
def save_gsheets_creds(ctx, filename=None):
    """
    Save gsheets credentials (service_account.json) in SSM.
    """
    try:
        __save_gsheets_credentials(filename)
    except Exception as e:
        print(f"Error: {e}")


ns = Collection()
ns.add_task(test)
ns.add_task(codebuild)
ns.add_task(package)
ns.add_task(release)
ns.add_task(update_requirements)

stack_ns = Collection("stack")
stack_ns.add_task(status)
stack_ns.add_task(stack_list, "list")
stack_ns.add_task(stack_synth, "synth")
stack_ns.add_task(stack_diff, "diff")
stack_ns.add_task(stack_create, "create")
stack_ns.add_task(stack_update, "update")
stack_ns.add_task(stack_delete, "delete")
ns.add_collection(stack_ns)

deploy_ns = Collection("deploy")
deploy_ns.add_task(deploy, "all")
deploy_ns.add_task(deploy_schedule_update, "schedule-update")
deploy_ns.add_task(deploy_webhook, "webhook")
deploy_ns.add_task(deploy_downloader, "downloader")
deploy_ns.add_task(deploy_uploader, "uploader")
deploy_ns.add_task(deploy_opencast_op_counts, "opencast-op-counts")
deploy_ns.add_task(deploy_on_demand, "on-demand")
deploy_ns.add_task(deploy_status, "status-query")
deploy_ns.add_task(deploy_slack, "slack")
ns.add_collection(deploy_ns)

exec_ns = Collection("exec")
exec_ns.add_task(exec_on_demand, "on-demand")
exec_ns.add_task(exec_webhook, "webhook")
exec_ns.add_task(exec_downloader, "downloader")
exec_ns.add_task(exec_uploader, "uploader")
exec_ns.add_task(exec_pipeline, "pipeline")
ns.add_collection(exec_ns)

debug_ns = Collection("debug")
debug_ns.add_task(debug_on, "on")
debug_ns.add_task(debug_off, "off")
ns.add_collection(debug_ns)

queue_ns = Collection("queue")
queue_ns.add_task(view_downloads, "downloads")
queue_ns.add_task(view_uploads, "uploads")
queue_ns.add_task(retry_downloads, "retry-downloads")
queue_ns.add_task(retry_uploads, "retry-uploads")
ns.add_collection(queue_ns)

schedule_ns = Collection("schedule")
schedule_ns.add_task(import_schedule_from_opencast, "oc-import")
schedule_ns.add_task(import_schedule_from_csv, "csv-import")
schedule_ns.add_task(save_gsheets_creds, "save-creds")
ns.add_collection(schedule_ns)

logs_ns = Collection("logs")
logs_ns.add_task(logs, "all")
logs_ns.add_task(logs_on_demand, "on-demand")
logs_ns.add_task(logs_webhook, "webhook")
logs_ns.add_task(logs_downloader, "downloader")
logs_ns.add_task(logs_uploader, "uploader")
logs_ns.add_task(recording)
ns.add_collection(logs_ns)


###############################################################################


def getenv(var, required=True):
    val = env(var)
    if val is not None and val.strip() == "":
        val = None
    if required and val is None:
        raise Exit("{} not defined".format(var))
    return val


def profile_arg():
    if AWS_PROFILE is not None:
        return "--profile {}".format(AWS_PROFILE)
    return ""


def oc_host(ctx, layer_name):

    # this only works on layers with a single instance
    if layer_name.lower() not in ["admin", "engage"]:
        print(
            "Not possible to determine the host for the '{}' layer".format(
                layer_name
            )
        )

    cmd = (
        "aws {} ec2 describe-instances "
        '--filters "Name=tag:opsworks:stack,Values={}" '
        '"Name=tag:opsworks:layer:{},Values={}" --query '
        '"Reservations[].Instances[].PublicDnsName" '
        "--output text"
    ).format(
        profile_arg(),
        OC_CLUSTER_NAME,
        layer_name.lower(),
        layer_name.lower().capitalize(),
    )

    res = ctx.run(cmd, hide=1)
    return res.stdout.strip()


def __invoke_api(resource_id, event_body):
    """
    Test invoke a zoom ingester endpoint method
    """
    apig = boto3.client("apigateway")

    resp = apig.test_invoke_method(
        restApiId=rest_api_id(),
        resourceId=resource_id,
        httpMethod="POST",
        body=json.dumps(event_body),
    )

    return resp


def __update_release_alias(ctx, func, version, description):
    print(
        f"Setting {func} '{names.LAMBDA_RELEASE_ALIAS}' alias"
        f" to version {version}"
    )
    lambda_function_name = f"{STACK_NAME}-{func}"
    if description is None:
        description = "''"
    alias_cmd = (
        "aws {} lambda update-alias --function-name {} "
        "--name {} --function-version '{}' "
        "--description '{}'"
    ).format(
        profile_arg(),
        lambda_function_name,
        names.LAMBDA_RELEASE_ALIAS,
        version,
        description,
    )
    ctx.run(alias_cmd)


def __publish_version(ctx, func, description):

    print("Publishing new version of {}".format(func))
    lambda_function_name = f"{STACK_NAME}-{func}"
    if description is None:
        description = "''"
    version_cmd = (
        "aws {} lambda publish-version --function-name {} "
        "--description '{}' --query 'Version'"
    ).format(profile_arg(), lambda_function_name, description)
    res = ctx.run(version_cmd, hide=1)
    return int(res.stdout.replace('"', ""))


def __build_function(ctx, func, upload_to_s3=False):
    print(f"Building {func} function")
    req_file = join(
        dirname(__file__), "function_requirements/{}.txt".format(func)
    )

    zip_path = join(dirname(__file__), "dist/{}.zip".format(func))

    build_path = join(dirname(__file__), "dist/{}".format(func))
    if exists(build_path):
        shutil.rmtree(build_path)

    if exists(req_file):
        ctx.run(
            "pip install -U -r {} -t {}".format(req_file, build_path), hide=1
        )

    mkdir(join(build_path, "common"))
    modules = [
        "common/" + f.split(".")[0] for f in listdir("functions/common")
    ]
    modules.append(func)
    for module in modules:
        module_path = join(dirname(__file__), f"functions/{module}.py")
        module_dist_path = join(build_path, f"{module}.py")
        try:
            symlink(module_path, module_dist_path)
        except FileExistsError:
            pass

    # include the ffprobe binary with the downloader function package
    if func == names.DOWNLOAD_FUNCTION:
        ffprobe_path = join(dirname(__file__), "bin/ffprobe")
        ffprobe_dist_path = join(build_path, "ffprobe")
        try:
            symlink(ffprobe_path, ffprobe_dist_path)
        except FileExistsError:
            pass

    with ctx.cd(build_path):
        ctx.run("zip -r {} .".format(zip_path), hide=1)

    if upload_to_s3:
        s3_path = f"{LAMBDA_CODE_URI}/{func}.zip"
        print(f"uploading {func} to {s3_path}")
        ctx.run(f"aws {profile_arg()} s3 cp {zip_path} {s3_path}", hide=1)


def __update_function(ctx, func):
    print(f"Updating {func} function")
    func_name = f"{STACK_NAME}-{func}"
    zip_path = join(dirname(__file__), "dist", func + ".zip")

    if not exists(zip_path):
        raise Exit("{} not found!".format(zip_path))

    cmd = (
        "aws {} lambda update-function-code "
        "--function-name {} --zip-file fileb://{}"
    ).format(profile_arg(), func_name, zip_path)
    ctx.run(cmd, hide=1)


def __save_gsheets_credentials(filename):
    auth = GSheetsAuth()
    if not filename:
        filename = "service_account.json"
    auth.save_to_ssm(filename)


def __set_debug(ctx, debug_val):
    for func in [
        names.WEBHOOK_FUNCTION,
        names.DOWNLOAD_FUNCTION,
        names.UPLOAD_FUNCTION,
    ]:
        func_name = f"{STACK_NAME}-{func}"
        cmd = (
            "aws {} lambda get-function-configuration --output json "
            "--function-name {}"
        ).format(profile_arg(), func_name)
        res = ctx.run(cmd, hide=1)
        config = json.loads(res.stdout)
        func_env = config["Environment"]["Variables"]

        if (
            func_env.get("DEBUG") is not None
            and int(func_env.get("DEBUG")) == debug_val
        ):
            print(f"{func_name} DEBUG is off")
            continue

        func_env["DEBUG"] = debug_val
        new_vars = ",".join("{}={}".format(k, v) for k, v in func_env.items())

        cmd = (
            "aws {} lambda update-function-configuration "
            "--environment 'Variables={{{}}}' "
            "--function-name {}"
        ).format(profile_arg(), new_vars, func_name)
        ctx.run(cmd)


def __move_messages(deadletter_queue, source_queue, limit, uuid=None):
    if deadletter_queue is None or source_queue is None:
        print("Missing required queues.")
        return

    sqs = boto3.client("sqs")
    fifo = deadletter_queue.endswith("fifo")
    total_messages_moved = 0

    while total_messages_moved < limit:
        remaining = limit - total_messages_moved

        if fifo:
            response = sqs.receive_message(
                QueueUrl=deadletter_queue,
                AttributeNames=["MessageDeduplicationId"],
                MaxNumberOfMessages=10 if remaining > 10 else remaining,
                VisibilityTimeout=10,
                WaitTimeSeconds=10,
            )
        else:
            response = sqs.receive_message(
                QueueUrl=deadletter_queue,
                MaxNumberOfMessages=10 if remaining > 10 else remaining,
                VisibilityTimeout=10,
                WaitTimeSeconds=10,
            )

        if "Messages" not in response or len(response["Messages"]) == 0:
            if total_messages_moved == 0:
                print("No messages found!")
            else:
                print("Moved {} message(s)".format(total_messages_moved))
            return

        messages = response["Messages"]
        received_count = 0

        entries = []
        for i, message in enumerate(messages):
            message_body = json.loads(message["Body"])
            if uuid is None or uuid == message_body["uuid"]:
                print("\nMoving message:")
                pprint(message_body)
                received_count += 1
                new_entry = {
                    "Id": str(i),
                    "MessageBody": message["Body"],
                    "DelaySeconds": 0,
                }
                if fifo:
                    deduplication_id = message["Attributes"][
                        "MessageDeduplicationId"
                    ]
                    new_entry["MessageDeduplicationId"] = deduplication_id
                    new_entry["MessageGroupId"] = deduplication_id

                entries.append(new_entry)

        if received_count == 0:
            continue

        send_resp = sqs.send_message_batch(
            QueueUrl=source_queue, Entries=entries
        )
        moved_count = len(send_resp["Successful"])
        if moved_count < received_count:
            print(
                "One or more messages failed to be sent back to the source "
                f"queue. Received {received_count} messages and successfully "
                f"sent {moved_count} messages."
            )

        entries = []
        for message_moved in send_resp["Successful"]:
            moved_id = message_moved["Id"]
            entries.append(
                {
                    "Id": moved_id,
                    "ReceiptHandle": messages[int(moved_id)]["ReceiptHandle"],
                }
            )

        del_resp = sqs.delete_message_batch(
            QueueUrl=deadletter_queue, Entries=entries
        )
        deleted_count = len(del_resp["Successful"])
        if deleted_count < received_count:
            print(
                "One or more messages failed to be deleted from the deadletter"
                f" queue. Received {moved_count} messages and successfully "
                f"deleted {deleted_count} messages."
            )

        total_messages_moved += moved_count
        time.sleep(1)


def __view_messages(queue_url, limit):

    if queue_url is None:
        print("Missing required queues.")
        return

    sqs = boto3.client("sqs")
    fifo = queue_url.endswith("fifo")

    if "dlq" in queue_url:
        print("\nDEADLETTER QUEUE")
    else:
        print("\nMAIN QUEUE")

    total_messages_received = 0

    print("Fetching messages from {}...\n".format(queue_url.split("/")[-1]))

    while total_messages_received < limit:
        remaining = limit - total_messages_received

        if fifo:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                AttributeNames=["MessageDeduplicationId"],
                MessageAttributeNames=["FailedReason"],
                MaxNumberOfMessages=10,
                VisibilityTimeout=remaining + 10,
                WaitTimeSeconds=10,
            )

        else:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MessageAttributeNames=["FailedReason"],
                MaxNumberOfMessages=10,
                VisibilityTimeout=remaining + 10,
                WaitTimeSeconds=10,
            )

        if "Messages" not in response:
            if total_messages_received == 0:
                print("No messages found!")
            else:
                print("Found {} message(s)".format(total_messages_received))
            return

        total_messages_received += len(response["Messages"])

        for message in response["Messages"]:
            print("Body:")
            pprint(json.loads(message["Body"]))
            if "Attributes" in message:
                print("Attributes:")
                pprint(message["Attributes"])
            if (
                "MessageAttributes" in message
                and "FailedReason" in message["MessageAttributes"]
            ):
                print(
                    "{}: {}".format(
                        "ReportedError",
                        message["MessageAttributes"]["FailedReason"][
                            "StringValue"
                        ],
                    )
                )
            print()


def __get_dynamo_schedule(ctx, table_name):

    cmd = "aws {} dynamodb scan --table-name {}".format(
        profile_arg(), table_name
    )
    res = ctx.run(cmd, hide=True).stdout
    res = json.loads(res)

    from boto3.dynamodb.types import TypeDeserializer

    tds = TypeDeserializer()
    current_schedule = {}

    for item in res["Items"]:
        item = {k: tds.deserialize(v) for k, v in item.items()}
        current_schedule[item["zoom_series_id"]] = item

    return current_schedule


def __show_stack_status(ctx):
    cmd = (
        "aws {} cloudformation describe-stacks "
        "--stack-name {} --output table".format(profile_arg(), STACK_NAME)
    )
    ctx.run(cmd)


def __show_function_status(ctx):

    status_table = [
        ["function", "released", "desc", "timestamp", "$LATEST timestamp"]
    ]

    for func in names.FUNCTIONS:

        lambda_function_name = f"{STACK_NAME}-{func}"
        cmd = (
            "aws {} lambda list-aliases --function-name {} "
            "--query \"Aliases[?Name=='{}'].[FunctionVersion,Description]\" "
            "--output text"
        ).format(
            profile_arg(), lambda_function_name, names.LAMBDA_RELEASE_ALIAS
        )
        res = ctx.run(cmd, hide=True)

        try:
            released_version, description = res.stdout.strip().split()
        except ValueError:
            released_version = res.stdout.strip()
            description = ""

        status_row = [func, released_version, description]

        cmd = (
            "aws {} lambda list-versions-by-function --function-name {} "
            "--query \"Versions[?Version=='{}'].LastModified\" --output text"
        ).format(profile_arg(), lambda_function_name, released_version)
        status_row.append(ctx.run(cmd, hide=True).stdout)

        cmd = (
            "aws {} lambda list-versions-by-function --function-name {} "
            r"--query \"Versions[?Version=='\$LATEST'].LastModified\" "
            "--output text"
        ).format(profile_arg(), lambda_function_name)
        status_row.append(ctx.run(cmd, hide=True).stdout)

        status_table.append(status_row)

    print(tabulate(status_table, headers="firstrow", tablefmt="grid"))


def __show_sqs_status(ctx):

    status_table = [
        [
            "queue",
            "Messages",
            "MessagesNotVisible",
            "MessagesDelayed",
            "LastModified",
        ]
    ]

    for queue_name in names.QUEUES:
        url = queue_url(queue_name)

        cmd = (
            "aws {} sqs get-queue-attributes --queue-url {} "
            "--attribute-names All"
        ).format(profile_arg(), url)

        res = json.loads(ctx.run(cmd, hide=True).stdout)["Attributes"]

        last_modified = datetime_date.fromtimestamp(
            int(res["LastModifiedTimestamp"])
        ).strftime("%Y-%m-%d %I:%M:%S")

        status_row = [
            queue_name,
            res["ApproximateNumberOfMessages"],
            res["ApproximateNumberOfMessagesNotVisible"],
            res["ApproximateNumberOfMessagesDelayed"],
            last_modified,
        ]

        status_table.append(status_row)

    print()
    print(tabulate(status_table, headers="firstrow", tablefmt="grid"))


def __find_recording_log_events(ctx, function, uuid):
    if function == "zoom-webhook":
        filter_pattern = '{ $.message.payload.uuid = "' + uuid + '" }'
    elif function == "zoom-downloader" or function == "zoom-uploader":
        filter_pattern = '{ $.message.uuid = "' + uuid + '" }'
    else:
        return

    log_group = "/aws/lambda/{}-{}".format(STACK_NAME, function)

    for log_stream, request_id in __request_ids_from_logs(
        ctx, log_group, filter_pattern
    ):

        request_id_pattern = '{ $.aws_request_id = "' + request_id + '" }'
        cmd = (
            "aws logs filter-log-events {} --log-group-name {} "
            "--log-stream-name '{}' "
            "--output text --query 'events[].message' "
            "--filter-pattern '{}'"
        ).format(profile_arg(), log_group, log_stream, request_id_pattern)
        for line in ctx.run(cmd, hide=True).stdout.split("\t"):
            try:
                event = json.loads(line)
                print(json.dumps(event, indent=2))
            except json.JSONDecodeError:
                print(line)


def __request_ids_from_logs(ctx, log_group, filter_pattern):

    cmd = (
        "aws logs filter-log-events {} --log-group-name {} "
        "--output text --query 'events[][logStreamName,message]' "
        "--filter-pattern '{}'".format(
            profile_arg(), log_group, filter_pattern
        )
    )

    for line in ctx.run(cmd, hide=True).stdout.splitlines():
        log_stream, message = line.strip().split(maxsplit=1)
        message = json.loads(message)
        yield log_stream, message["aws_request_id"]


def resolve_function_arg(func=None):
    if func:
        if func not in names.FUNCTIONS:
            raise Exit(f"Function choices: {names.FUNCTIONS}")
        return [func]
    else:
        return names.FUNCTIONS


@lru_cache()
def cfn_exports():
    stack = boto3.resource("cloudformation").Stack(STACK_NAME)
    exports = {
        x["ExportName"]: x["OutputValue"]
        for x in stack.outputs
        if "ExportName" in x
    }
    return exports


def cfn_export_value(name):
    export_name = f"{STACK_NAME}-{name}"
    try:
        return cfn_exports()[export_name]
    except KeyError:
        raise Exception(f"Missing {export_name} export for stack {STACK_NAME}")


def queue_url(queue_name):
    return cfn_export_value(f"{queue_name.replace('.', '-')}-url")


def queue_is_empty(ctx, queue_name):

    cmd = (
        f"aws {profile_arg()} sqs get-queue-attributes "
        f"--queue-url {queue_url(queue_name)} "
        "--attribute-names ApproximateNumberOfMessages "
        '--query "Attributes.ApproximateNumberOfMessages" --output text'
    )

    num_queued = int(ctx.run(cmd, hide=True).stdout.strip())
    return num_queued == 0


def rest_api_id():
    return cfn_export_value(f"{names.REST_API}-id")


def webhook_resource_id():
    return cfn_export_value(f"{names.WEBHOOK_ENDPOINT}-resource-id")


def on_demand_resource_id():
    return cfn_export_value(f"{names.ON_DEMAND_ENDPOINT}-resource-id")
