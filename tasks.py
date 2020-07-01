import sys
import json
import boto3
from botocore.exceptions import ClientError
import jmespath
import requests
from requests.auth import HTTPDigestAuth
import time
import csv
import itertools
import shutil
from datetime import datetime, timedelta, date as datetime_date
from invoke import task, Collection
from invoke.exceptions import Exit
from os import symlink, getenv as env
from dotenv import load_dotenv
from os.path import join, dirname, exists, relpath
from tabulate import tabulate
from pprint import pprint
from functions.common import zoom_api_request
from pytz import timezone
from multiprocessing import Process
from urllib.parse import urlparse, quote

# suppress warnings for cases where we want to ingore dev cluster dummy certificates
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv(join(dirname(__file__), '.env'))

AWS_ACCOUNT_ID = None
AWS_PROFILE = env('AWS_PROFILE')
AWS_DEFAULT_REGION = env('AWS_DEFAULT_REGION', 'us-east-1')
STACK_NAME = env('STACK_NAME')
OC_CLUSTER_NAME = env('OC_CLUSTER_NAME')
PROD_IDENTIFIER = "prod"
NONINTERACTIVE = env('NONINTERACTIVE')
INGEST_ALLOWED_IPS = env('INGEST_ALLOWED_IPS', '')

FUNCTION_NAMES = [
    'zoom-webhook',
    'zoom-downloader',
    'zoom-uploader',
    'zoom-log-notifications',
    'opencast-op-counts',
    'zoom-on-demand'
]

if AWS_PROFILE is not None:
    boto3.setup_default_session(profile_name=AWS_PROFILE)


def get_queue_url(queue_name):

    global AWS_ACCOUNT_ID
    if AWS_ACCOUNT_ID is None:
        AWS_ACCOUNT_ID = boto3.client('sts').get_caller_identity()['Account']

    queue_url = 'https://queue.amazonaws.com/{}/{}-{}'.format(
        AWS_ACCOUNT_ID,
        STACK_NAME,
        queue_name
    )

    return queue_url


def queue_is_empty(ctx, queue_name):
    queue_url = get_queue_url(queue_name)

    cmd = ("aws {} sqs get-queue-attributes --queue-url {} "
           "--attribute-names ApproximateNumberOfMessages "
           "--query \"Attributes.ApproximateNumberOfMessages\" --output text"
           .format(profile_arg(), queue_url))

    num_queued = int(ctx.run(cmd, hide=True).stdout.strip())
    return num_queued == 0


@task
def production_failsafe(ctx):
    """
    This is not a standalone task and should not be added to any of the task collections.
    It is meant to be prepended to the execution of other tasks to force a confirmation
    when a task is being executed that could have an impact on a production stack
    """
    if not STACK_NAME:
        raise Exit("No STACK_NAME specified")

    if not NONINTERACTIVE and PROD_IDENTIFIER in STACK_NAME.lower():
        print("You are about to run this task on a production system")
        ok = input('are you sure? [y/N] ').lower().strip().startswith('y')
        if not ok:
            raise Exit("Aborting")


@task(pre=[production_failsafe],
      help={'revision': 'tag or branch name to build and release (required)'})
def codebuild(ctx, revision):
    """
    Execute a codebuild run. Optional: --revision=[tag or branch]
    """
    cmd = ("aws {} codebuild start-build "
           "--project-name {}-codebuild --source-version {} "
           " --environment-variables-override"
           " name='STACK_NAME',value={},type=PLAINTEXT"
           " name='LAMBDA_RELEASE_ALIAS',value={},type=PLAINTEXT"
           " name='NONINTERACTIVE',value=1") \
        .format(
            profile_arg(),
            STACK_NAME,
            revision,
            STACK_NAME,
            getenv("LAMBDA_RELEASE_ALIAS")
            )

    res = ctx.run(cmd, hide='out')
    build_id = json.loads(res.stdout)["build"]["id"]

    cmd = "aws {} codebuild batch-get-builds --ids={}" \
        .format(profile_arg(), build_id)
    current_phase = "IN_PROGRESS"
    print("Waiting for codebuild to finish...")
    while True:
        time.sleep(5)
        res = ctx.run(cmd, hide='out')
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


@task(help={'function': 'name of a specific function'})
def package(ctx, function=None, upload_to_s3=False):
    """
    Package function(s) + deps into a zip file.
    """
    if function is not None:
        functions = [function]
    else:
        functions = FUNCTION_NAMES

    for func in functions:
        __build_function(ctx, func, upload_to_s3)


@task(pre=[production_failsafe],
      help={'function': 'name of specific function'})
def deploy(ctx, function=None, do_release=False):
    """
    Package, upload and register new code for all lambda functions
    """
    if function is not None:
        functions = [function]
    else:
        functions = FUNCTION_NAMES

    for func in functions:
        __build_function(ctx, func)
        __update_function(ctx, func)
        if do_release:
            release(ctx, func)


@task(pre=[production_failsafe])
def deploy_on_demand(ctx, do_release=False):
    deploy(ctx, "zoom-on-demand", do_release)


@task(pre=[production_failsafe])
def deploy_opencast_op_counts(ctx, do_release=False):
    deploy(ctx, "opencast-op-counts", do_release)


@task(pre=[production_failsafe])
def deploy_webhook(ctx, do_release=False):
    deploy(ctx, "zoom-webhook", do_release)


@task(pre=[production_failsafe])
def deploy_downloader(ctx, do_release=False):
    deploy(ctx, "zoom-downloader", do_release)


@task(pre=[production_failsafe])
def deploy_uploader(ctx, do_release=False):
    deploy(ctx, "zoom-uploader", do_release)


@task(pre=[production_failsafe],
      help={'function': 'name of specific function'})
def release(ctx, function=None, description=None):
    """
    Publish a new version of the function(s) and update the release alias to point to it
    """
    if function is not None:
        functions = [function]
    else:
        functions = FUNCTION_NAMES

    for func in functions:
        new_version = __publish_version(ctx, func, description)
        __update_release_alias(ctx, func, new_version, description)


@task
def list_recordings(ctx, date=None):
    """
    Optional: --date='YYYY-MM-DD'
    """
    if date is None:
        date = datetime_date.today()

    meetings = __get_meetings(date)

    recordings_found = 0

    for meeting in meetings:
        uuid = meeting['uuid']
        series_id = meeting['id']

        r = zoom_api_request(
            "meetings/{}".format(series_id), ignore_failure=True,
        )
        if r.status_code == 404:
            continue
        r.raise_for_status()

        r = zoom_api_request(
            "meetings/{}/recordings".format(uuid), ignore_failure=True
        )
        if r.status_code == 404:
            continue
        r.raise_for_status()

        # sometimes this zoom endpoint sends an empty list of recording files
        if not len(r.json()['recording_files']):
            continue

        recordings_found += 1

        local_tz = timezone(getenv('LOCAL_TIME_ZONE'))
        utc = timezone('UTC')
        utc_start_time = r.json()['recording_files'][0]['recording_start']
        start_time = utc.localize(datetime.strptime(utc_start_time,
                                  "%Y-%m-%dT%H:%M:%SZ")).astimezone(local_tz)

        print("\n\tuuid: {}".format(uuid))
        print("\tSeries id: {}".format(r.json()["id"]))
        print("\tTopic: {}".format(r.json()["topic"]))
        print("\tStart time: {}".format(start_time))
        print("\tDuration: {} minutes".format(r.json()["duration"]))

    if recordings_found == 0:
        print("No recordings found on {}".format(date))
    else:
        print("Done!")


@task
def update_requirements(ctx):
    """
    Run a `pip-compile -U` on all requirements files
    """
    req_file = relpath(join(dirname(__file__), 'requirements.in'))
    ctx.run("pip-compile -r -U {}".format(req_file))
    for func in FUNCTION_NAMES:
        req_file = relpath(join(dirname(__file__),
                        'function_requirements/{}.in'.format(func)))
        ctx.run("pip-compile -r -U {}".format(req_file))


@task
def generate_resource_policy(ctx):
    """
    Update/set the api resource access policy
    """
    resource_policy = json.loads(API_RESOURCE_POLICY)

    if not INGEST_ALLOWED_IPS:
        ok = input(
            '\nYou have not configured any allowed ips for the ingest endpoint'
            '\nDo you still want to apply the access policy? [y/N] ').lower().strip().startswith(
            'y')
        if not ok:
            return
    else:
        resource_policy["Statement"][1]["Condition"]["NotIpAddress"] \
            ["aws:SourceIp"] = INGEST_ALLOWED_IPS.split(',')

    api_id = api_gateway_id(ctx)

    for s in resource_policy["Statement"]:
        s["Resource"] = s["Resource"].format(
            region=AWS_DEFAULT_REGION,
            account=account_id(ctx),
            api_id=api_id
        )

    print("Copy/paste the following into the API Gateway console")
    print(json.dumps(resource_policy, indent=2))


@task(help={"uuid": "meeting instance uuid",
            "oc_series_id": "opencast series id",
            "allow_multiple_ingests": ("whether to allow this recording to "
                                       "be ingested multiple times")})
def exec_on_demand(ctx, uuid, oc_series_id=None, allow_multiple_ingests=False):
    """
    Manually trigger an on demand ingest.
    """

    event_body = {
        "uuid": uuid.strip()
    }

    if oc_series_id:
        event_body["oc_series_id"] = oc_series_id.strip()

    if allow_multiple_ingests:
        event_body["allow_multiple_ingests"] = allow_multiple_ingests

    print(event_body)
    
    resp = __invoke_api(ctx, "ingest", event_body)

    print("Returned with status code: {}. {}".format(
        resp["status"],
        resp["body"])
    )


@task(help={'uuid': 'meeting instance uuid',
            'ignore_schedule': ('ignore schedule, use default series if '
                                'available'),
            'oc_series_id': ('opencast series id to use regardless of '
                                    'schedule')})
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

    if 'FunctionError' in resp:
        print("Downloader failed!")
        return

    # Keep retrying uploader until some messages are processed
    # or it fails.
    print("\nTriggering uploader...\n")
    resp = exec_uploader(ctx)

    if resp and 'FunctionError' in resp:
        print("Uploader failed!")
        return


@task(help={'uuid': 'meeting instance uuid', 'oc_series_id': 'opencast series id'})
def exec_webhook(ctx, uuid, oc_series_id=None):
    """
    Manually call the webhook endpoint. Positional arguments: uuid, host_id
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
            raise Exception(
                "No {} found in response.\n".format(field)
            )

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
                "object": data
            }
        }
    else:
        event_body = {
            "event": "recording.completed",
            "payload": {
                "object": data,
                "delay_seconds": 0
            }
        }

    resp = __invoke_api(ctx, "new_recording", event_body)

    print("Returned with status code: {}. {}".format(
        resp["status"],
        resp["body"])
    )


@task(help={
    'series_id': 'override normal opencast series id lookup',
    'ignore_schedule': 'do opencast series id lookup but ignore if meeting times don\'t match'
})
def exec_downloader(ctx, series_id=None, ignore_schedule=False, qualifier=None):
    """
    Manually trigger downloader.
    """

    if queue_is_empty(ctx, "downloads"):
        print("No downloads in queue")
        return

    payload = {'ignore_schedule': ignore_schedule}

    if series_id:
        payload['override_series_id'] = series_id

    if not qualifier:
        qualifier = getenv('LAMBDA_RELEASE_ALIAS')

    cmd = ("aws lambda invoke --function-name='{}-zoom-downloader-function' "
            "--payload='{}' --qualifier {} output.txt").format(
        STACK_NAME,
        json.dumps(payload),
        qualifier
    )
    print(cmd)
    res = json.loads(ctx.run(cmd).stdout)

    return res


@task
def exec_uploader(ctx, qualifier=None):
    """
    Manually trigger uploader.
    """
    if queue_is_empty(ctx, "uploads.fifo"):
        print("No uploads in queue")
        return

    if qualifier is None:
        qualifier = getenv('LAMBDA_RELEASE_ALIAS')

    cmd = ("aws lambda invoke --function-name='{}-zoom-uploader-function' "
           "--qualifier {} outfile.txt").format(STACK_NAME, qualifier)

    print(cmd)
    res = json.loads(ctx.run(cmd).stdout)

    if 'FunctionError' in res:
        ctx.run("cat outfile.txt && echo")

    return res


@task(pre=[production_failsafe])
def create(ctx):
    """
    Build the CloudFormation stack identified by $STACK_NAME
    """
    if stack_exists(ctx):
        raise Exit("Stack already exists!")

    code_bucket = getenv('LAMBDA_CODE_BUCKET')
    cmd = "aws {} s3 ls {}".format(profile_arg(), code_bucket)
    exists = ctx.run(cmd, hide=True, warn=True)
    if not exists.ok:
        print("Specified lambda code bucket does not exist!")
        return

    package(ctx, upload_to_s3=True)
    __create_or_update(ctx, "create-stack")
    release(ctx, description="initial release")


@task(pre=[production_failsafe])
def update(ctx):
    """
    Update the CloudFormation stack identified by $STACK_NAME
    """
    __create_or_update(ctx, "create-change-set")


@task(pre=[production_failsafe])
def delete(ctx):
    """
    Delete the CloudFormation stack identified by $STACK_NAME
    """
    if not stack_exists(ctx):
        raise Exit("Stack doesn't exist!")

    delete_files = "aws {} s3 rm s3://{}-recording-files --recursive"\
                   .format(profile_arg(), STACK_NAME)

    delete_stack = ("aws {} cloudformation delete-stack "
                    "--stack-name {}").format(profile_arg(), STACK_NAME)
    if input('\nAre you sure you want to delete stack "{}"?\n'
             'WARNING: This will also delete all recording files saved '
             'in the S3 bucket "{}-recording-files".\n'
             'Type stack name to confirm deletion: '.format(STACK_NAME, STACK_NAME))\
            == STACK_NAME:
        ctx.run(delete_files, echo=True)
        ctx.run(delete_stack, echo=True)
        __wait_for(ctx, 'stack-delete-complete')

        files = ["zoom-webhook.zip",
                 "zoom-downloader.zip",
                 "zoom-uploader.zip",
                 "zoom-log-notifications.zip"]

        for file in files:
            cmd = "aws {} s3 rm s3://{}/{}/{}" \
                .format(profile_arg(), getenv("LAMBDA_CODE_BUCKET"), STACK_NAME, file)
            ctx.run(cmd, echo=True)
    else:
        print("Stack deletion canceled.")


@task
def status(ctx):
    """
    Show table of CloudFormation stack details
    """
    __show_stack_status(ctx)
    __show_webhook_endpoint(ctx)
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
    ctx.run('py.test --cov-report term-missing --cov=functions tests -vv')


@task(pre=[production_failsafe])
def retry_downloads(ctx, limit=1, uuid=None):
    """
    Move SQS messages DLQ to source. Optional: --limit (default 1).
    """
    downloads_dlq = get_queue_url("downloads-deadletter")
    downloads_queue = get_queue_url("downloads")
    __move_messages(downloads_dlq, downloads_queue, limit=limit, uuid=uuid)


@task(pre=[production_failsafe])
def retry_uploads(ctx, limit=1, uuid=None):
    """
    Move SQS messages DLQ to source. Optional: --limit (default 1).
    """
    uploads_dql = get_queue_url("uploads-deadletter.fifo")
    uploads_queue = get_queue_url("uploads.fifo")
    __move_messages(uploads_dql, uploads_queue, limit=limit, uuid=uuid)


@task(pre=[production_failsafe])
def view_downloads(ctx, limit=20):
    """
    View items in download queues. Optional: --limit (default 20).
    """
    downloads_queue = get_queue_url("downloads")
    downloads_dlq = get_queue_url("downloads-deadletter")
    __view_messages(downloads_queue, limit=limit)
    __view_messages(downloads_dlq, limit=limit)


@task(pre=[production_failsafe])
def view_uploads(ctx, limit=20):
    """
    View items in upload queues. Optional: --limit (default 20).
    """
    uploads_queue = get_queue_url("uploads.fifo")
    uploads_dql = get_queue_url("uploads-deadletter.fifo")
    __view_messages(uploads_queue, limit=limit)
    __view_messages(uploads_dql, limit=limit)


@task(pre=[production_failsafe])
def import_schedule_from_opencast(ctx, endpoint=None):
    """
    Fetch schedule data from Opencast series endpoint
    """
    if endpoint is None:
        engage_host = oc_host(ctx, "engage")
        endpoint = "https://{}/otherpubs/search/series.json".format(engage_host)

    session = requests.Session()
    api_user = getenv("OPENCAST_API_USER")
    api_pass = getenv("OPENCAST_API_PASSWORD")
    session.auth = HTTPDigestAuth(api_user, api_pass)
    session.headers.update({
        "X-REQUESTED-AUTH": "Digest",
        "X-Opencast-Matterhorn-Authorization": "true",
    })

    try:
        print("Fetching helixEvents from {}".format(endpoint))
        r = session.get(endpoint, verify=False)
        r.raise_for_status()
        helix_events = r.json()["helixEvents"]
    except requests.HTTPError as e:
        raise Exit("Failed fetching schedule from {}: {}".format(endpoint,
                                                               str(e)))
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
        "F": ("FRI")
    }
    def get_day_of_week_code(day):
        if day in day_of_week_map:
            return day
        try:
            return next(
                k for k, v in day_of_week_map.items()
                if day in v
            )
        except StopIteration:
            raise Exit(
                "Unable to map day of week '{}' from schedule".format(day)
            )

    schedule_data = {}
    for event in helix_events["resultSet"]:
        try:
            zoom_link = urlparse(event["zoomLink"])
            zoom_series_id = zoom_link.path.split('/')[-1]
            schedule_data.setdefault(zoom_series_id, {})
            schedule_data[zoom_series_id]["zoom_series_id"] = zoom_series_id
            schedule_data[zoom_series_id]["opencast_series_id"] = event["seriesId"]
            schedule_data[zoom_series_id]["opencast_subject"] = event[
                "seriesNumber"]

            day = get_day_of_week_code(event["day"])
            schedule_data[zoom_series_id].setdefault("Days", [])
            if day not in schedule_data[zoom_series_id]["Days"]:
                schedule_data[zoom_series_id]["Days"].append(day)

            time_object = datetime.strptime(event["time"], "%I:%M %p")
            schedule_data[zoom_series_id]["Time"] = [
                datetime.strftime(time_object, "%H:%M"),
                (time_object + timedelta(minutes=30)).strftime("%H:%M"),
                (time_object + timedelta(hours=1)).strftime("%H:%M")
            ]
        except KeyError as e:
            raise Exit("helix event missing data: {}\n{}" \
                       .format(str(e), json.dumps(event, indent=2)))
        except Exception as e:
            raise Exit("Failed converting to dynamo item format: {}\n{}" \
                       .format(str(e), json.dumps(event, indent=2)))

    __schedule_json_to_dynamo(ctx, schedule_data=schedule_data)


@task(pre=[production_failsafe])
def import_schedule_from_csv(ctx, filepath):
    valid_days = ["M", "T", "W", "R", "F"]

    # make it so we can use lower-case keys in our row dicts;
    # there are lots of ways this spreadsheet data import could go wrong and
    # this is only one, but we do what we can.
    def lower_case_first_line(iter):
        header = next(iter).lower()
        return itertools.chain([header], iter)

    with open(filepath, "r") as f:
        reader = csv.DictReader(lower_case_first_line(f))
        rows = list(reader)

    schedule_data = {}
    for row in rows:

        try:
            zoom_link = urlparse(row["meeting id with password"])
            assert zoom_link.scheme.startswith("https")
        except AssertionError:
            zoom_link = None

        if zoom_link is None:
            print("Invalid zoom link value for {}: {}" \
                  .format(row["course code"], zoom_link))
            continue

        zoom_series_id = zoom_link.path.split("/")[-1]
        schedule_data.setdefault(zoom_series_id, {})
        schedule_data[zoom_series_id]["zoom_series_id"] = zoom_series_id

        opencast_series_id = urlparse(row["oc series"]) \
            .fragment.replace("/", "")
        schedule_data[zoom_series_id]["opencast_series_id"] = opencast_series_id

        subject = "{} - {}".format(row["course code"], row["type"])
        schedule_data[zoom_series_id]["opencast_subject"] = subject
        
        schedule_data[zoom_series_id].setdefault("Days", set())
        for day in row["day"].strip():
            if day not in valid_days:
                raise Exit("Got bad day value: {}".format(letter))
            schedule_data[zoom_series_id]["Days"].add(day)

        schedule_data[zoom_series_id].setdefault("Time", set())
        time_object = datetime.strptime(row["start"], "%H:%M")
        schedule_data[zoom_series_id]["Time"].update([
            datetime.strftime(time_object, "%H:%M"),
            (time_object + timedelta(minutes=30)).strftime("%H:%M"),
            (time_object + timedelta(hours=1)).strftime("%H:%M")
        ])

    for id, item in schedule_data.items():
        item["Days"] = list(item["Days"])
        item["Time"] = list(item["Time"])

    __schedule_json_to_dynamo(ctx, schedule_data=schedule_data)


@task
def logs(ctx, function=None, watch=False):

    if function is None:
        functions = FUNCTION_NAMES
    else:
        functions = [function]

    def _awslogs(group, watch=False):
        watch_flag = watch and "--watch" or ""
        cmd = "awslogs get {} ALL {} {}".format(
            group,
            watch_flag,
            profile_arg()
        )
        ctx.run(cmd)

    procs = []
    for func in functions:
        group = "/aws/lambda/{}-{}-function".format(STACK_NAME, func)
        procs.append(
            Process(target=_awslogs, name=func, args=(group, watch))
        )

    for p in procs:
        p.start()

    for p in procs:
        p.join()


@task
def recording(ctx, uuid, function=None):
    functions = function is None and FUNCTION_NAMES or [function]
    for function in functions:
        __find_recording_log_events(ctx, function, uuid)


@task
def logs_webhook(ctx, watch=False):
    logs(ctx, 'zoom-webhook', watch)


@task
def logs_downloader(ctx, watch=False):
    logs(ctx, 'zoom-downloader', watch)


@task
def logs_uploader(ctx, watch=False):
    logs(ctx, 'zoom-uploader', watch)


ns = Collection()
ns.add_task(test)
ns.add_task(codebuild)
ns.add_task(package)
ns.add_task(release)
ns.add_task(list_recordings)
ns.add_task(update_requirements)
ns.add_task(generate_resource_policy)

deploy_ns = Collection('deploy')
deploy_ns.add_task(deploy, 'all')
deploy_ns.add_task(deploy_webhook, 'webhook')
deploy_ns.add_task(deploy_downloader, 'downloader')
deploy_ns.add_task(deploy_uploader, 'uploader')
deploy_ns.add_task(deploy_opencast_op_counts, 'opencast-op-counts')
deploy_ns.add_task(deploy_on_demand, 'on-demand')
ns.add_collection(deploy_ns)

exec_ns = Collection('exec')
exec_ns.add_task(exec_on_demand, 'on-demand')
exec_ns.add_task(exec_webhook, 'webhook')
exec_ns.add_task(exec_downloader, 'downloader')
exec_ns.add_task(exec_uploader, 'uploader')
exec_ns.add_task(exec_pipeline, 'pipeline')
ns.add_collection(exec_ns)

debug_ns = Collection('debug')
debug_ns.add_task(debug_on, 'on')
debug_ns.add_task(debug_off, 'off')
ns.add_collection(debug_ns)

stack_ns = Collection('stack')
stack_ns.add_task(create)
stack_ns.add_task(update)
stack_ns.add_task(delete)
stack_ns.add_task(status)
ns.add_collection(stack_ns)

queue_ns = Collection('queue')
queue_ns.add_task(view_downloads, 'downloads')
queue_ns.add_task(view_uploads, 'uploads')
queue_ns.add_task(retry_downloads, 'retry-downloads')
queue_ns.add_task(retry_uploads, 'retry-uploads')
ns.add_collection(queue_ns)

schedule_ns = Collection('schedule')
schedule_ns.add_task(import_schedule_from_opencast, 'oc-import')
schedule_ns.add_task(import_schedule_from_csv, 'csv-import')
ns.add_collection(schedule_ns)

logs_ns = Collection('logs')
logs_ns.add_task(logs, 'all')
logs_ns.add_task(logs_webhook, 'webhook')
logs_ns.add_task(logs_downloader, 'downloader')
logs_ns.add_task(logs_uploader, 'uploader')
logs_ns.add_task(recording)
ns.add_collection(logs_ns)

###############################################################################


def getenv(var, required=True):
    val = env(var)
    if val is not None and val.strip() == '':
        val = None
    if required and val is None:
        raise Exit("{} not defined".format(var))
    return val


def profile_arg():
    if AWS_PROFILE is not None:
        return "--profile {}".format(AWS_PROFILE)
    return ""


def zoom_admin_id():
    # get admin user id from admin email
    r = zoom_api_request("users/{}".format(getenv("ZOOM_ADMIN_EMAIL")))
    return r.json()["id"]


def stack_tags():
    tags = "Key=cfn-stack,Value={}".format(STACK_NAME)
    extra_tags = getenv("STACK_TAGS")
    if extra_tags is not None:
        tags += " " + extra_tags
    return "--tags {}".format(tags)


def vpc_components(ctx):

    cmd = ("aws {} opsworks describe-stacks "
           "--query \"Stacks[?Name=='{}'].VpcId\" --output text")\
        .format(profile_arg(), OC_CLUSTER_NAME)
    res = ctx.run(cmd, hide=1)
    vpc_id = res.stdout.strip()

    if vpc_id is None:
        confirm = ("No VPC found "
                   "Uploader will not be able to communicate with "
                   "the opencast admin. Do you wish to proceed? [y/N] ")
        if not input(confirm).lower().strip().startswith('y'):
            print("aborting")
            raise Exit(0)
        return "", ""

    cmd = ("aws {} ec2 describe-subnets --filters "
           "'Name=vpc-id,Values={}' "
           "'Name=tag:aws:cloudformation:logical-id,Values=Private*' "
           "--query \"Subnets[0].SubnetId\" --output text") \
        .format(profile_arg(), vpc_id)
    res = ctx.run(cmd, hide=1)
    subnet_id = res.stdout

    cmd = ("aws {} ec2 describe-security-groups --filters "
           "'Name=vpc-id,Values={}' "
           "'Name=tag:aws:cloudformation:logical-id,Values=OpsworksLayerSecurityGroupCommon' "
           "--query \"SecurityGroups[0].GroupId\" --output text") \
        .format(profile_arg(), vpc_id)
    res = ctx.run(cmd, hide=1)
    sg_id = res.stdout

    return subnet_id, sg_id


def oc_host(ctx, layer_name):

    # this only works on layers with a single instance
    if layer_name.lower() not in ['admin', 'engage']:
        print("Not possible to determine the host for the '{}' layer".format(
            layer_name))

    cmd = ("aws {} ec2 describe-instances "
           "--filters \"Name=tag:opsworks:stack,Values={}\" "
           "\"Name=tag:opsworks:layer:{},Values={}\" --query "
           "\"Reservations[].Instances[].PublicDnsName\" "
           "--output text") \
        .format(
            profile_arg(),
            OC_CLUSTER_NAME,
            layer_name.lower(),
            layer_name.lower().capitalize()
        )

    res = ctx.run(cmd, hide=1)
    return res.stdout.strip()


def oc_db_url(ctx):

    cmd = ("aws {} rds describe-db-clusters "
           "--db-cluster-identifier '{}-cluster' --output text "
           "--query 'DBClusters[0].ReaderEndpoint' ") \
        .format(profile_arg(), OC_CLUSTER_NAME)

    res = ctx.run(cmd, hide=True)
    endpoint = res.stdout.strip()

    db_password = getenv('OC_DB_PASSWORD')
    if db_password is None:
        raise Exception("Missing OC_DB_PASSWORD env var")

    return "mysql://root:{}@{}:3306/opencast".format(db_password, endpoint)


def account_id(ctx):

    cmd = ("aws {} sts get-caller-identity "
           "--query 'Account' --output text").format(profile_arg())
    res = ctx.run(cmd, hide=1)
    return res.stdout.strip()


def api_gateway_id(ctx):

    cmd = ("aws {} apigateway get-rest-apis "
           "--query \"items[?name=='{}'].id\" --output text") \
            .format(profile_arg(), STACK_NAME)
    res = ctx.run(cmd, hide=1)
    return res.stdout.strip()


def stack_exists(ctx):
    cmd = "aws {} cloudformation describe-stacks --stack-name {}" \
        .format(profile_arg(), STACK_NAME)
    res = ctx.run(cmd, hide=True, warn=True, echo=False)
    return res.exited == 0


def __invoke_api(ctx, endpoint, event_body):
    """
    Test invoke a zoom ingester endpoint method
    """
    apig = boto3.client('apigateway')

    apis = apig.get_rest_apis()
    api_id = jmespath.search(
        "items[?name=='{}'].id | [0]".format(STACK_NAME),
        apis
    )
    if not api_id:
        raise Exit(
            "No api found, double check that your environment variables "
            "are correct."
        )

    api_resources = apig.get_resources(restApiId=api_id)
    resource_id = jmespath.search(
        f"items[?pathPart=='{endpoint}'].id | [0]",
        api_resources
    )

    resp = apig.test_invoke_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod="POST",
        body=json.dumps(event_body)
    )

    return resp


def __create_or_update(ctx, op):

    template_path = join(dirname(__file__), 'template.yml')

    subnet_id, sg_id = vpc_components(ctx)
    db_url = oc_db_url(ctx)

    default_publisher = getenv('DEFAULT_PUBLISHER', required=False)
    if default_publisher is None:
        default_publisher = getenv('NOTIFICATION_EMAIL')

    oc_admin_host = oc_host(ctx, 'admin')

    cmd = ("aws {} cloudformation {} {} "
           "--capabilities CAPABILITY_NAMED_IAM --stack-name {} "
           "--template-body file://{} "
           "--parameters "
           "ParameterKey=LambdaCodeBucket,ParameterValue={} "
           "ParameterKey=NotificationEmail,ParameterValue='{}' "
           "ParameterKey=ZoomApiBaseUrl,ParameterValue='{}' "
           "ParameterKey=ZoomApiKey,ParameterValue='{}' "
           "ParameterKey=ZoomApiSecret,ParameterValue='{}' "
           "ParameterKey=ZoomAdminId,ParameterValue='{}' "
           "ParameterKey=OpencastBaseUrl,ParameterValue='{}' "
           "ParameterKey=OpencastApiUser,ParameterValue='{}' "
           "ParameterKey=OpencastApiPassword,ParameterValue='{}' "
           "ParameterKey=DefaultOpencastSeriesId,ParameterValue='{}' "
           "ParameterKey=DefaultPublisher,ParameterValue='{}' "
           "ParameterKey=OverridePublisher,ParameterValue='{}' "
           "ParameterKey=OverrideContributor,ParameterValue='{}' "
           "ParameterKey=LocalTimeZone,ParameterValue='{}' "
           "ParameterKey=VpcSecurityGroupId,ParameterValue='{}' "
           "ParameterKey=VpcSubnetId,ParameterValue='{}' "
           "ParameterKey=LambdaReleaseAlias,ParameterValue='{}' "
           "ParameterKey=LogNotificationsFilterLogLevel,ParameterValue='{}' "
           "ParameterKey=OCWorkflow,ParameterValue='{}' "
           "ParameterKey=OCFlavor,ParameterValue='{}' "
           "ParameterKey=ParallelEndpoint,ParameterValue='{}' "
           "ParameterKey=DownloadMessagesPerInvocation,ParameterValue='{}' "
           "ParameterKey=OpencastDatabaseUrl,ParameterValue='{}' "
           "ParameterKey=BufferMinutes,ParameterValue='{}' "
           "ParameterKey=MinimumDuration,ParameterValue='{}' "
           "ParameterKey=OCTrackUploadMax,ParameterValue='{}' "
           ).format(
                profile_arg(),
                op,
                stack_tags(),
                STACK_NAME,
                template_path,
                getenv("LAMBDA_CODE_BUCKET"),
                getenv("NOTIFICATION_EMAIL"),
                getenv("ZOOM_API_BASE_URL"),
                getenv("ZOOM_API_KEY"),
                getenv("ZOOM_API_SECRET"),
                zoom_admin_id(),
                "http://{}".format(oc_admin_host),
                getenv("OPENCAST_API_USER"),
                getenv("OPENCAST_API_PASSWORD"),
                getenv("DEFAULT_SERIES_ID", required=False),
                default_publisher,
                getenv("OVERRIDE_PUBLISHER", required=False),
                getenv("OVERRIDE_CONTRIBUTOR", required=False),
                getenv("LOCAL_TIME_ZONE"),
                sg_id,
                subnet_id,
                getenv("LAMBDA_RELEASE_ALIAS"),
                getenv("LOG_NOTIFICATIONS_FILTER_LOG_LEVEL", required=False),
                getenv("OC_WORKFLOW"),
                getenv("OC_FLAVOR"),
                getenv("PARALLEL_ENDPOINT", required=False),
                getenv('DOWNLOAD_MESSAGES_PER_INVOCATION'),
                db_url,
                getenv("BUFFER_MINUTES"),
                getenv("MINIMUM_DURATION"),
                getenv("OC_TRACK_UPLOAD_MAX")
                )

    if op == 'create-change-set':
        ts = time.mktime(datetime.utcnow().timetuple())
        change_set_name = "stack-update-{}".format(int(ts))
        cmd += ' --change-set-name ' + change_set_name
        cmd += ' --output text --query "Id"'

    res = ctx.run(cmd)

    if res.failed:
        return

    if op == 'create-stack':
        __wait_for(ctx, 'stack-create-complete')
    else:
        change_set_id = res.stdout.strip()
        wait_res = __wait_for(ctx, "change-set-create-complete --change-set-name {} ".format(change_set_id))

        if wait_res.failed:
            cmd = ("aws {} cloudformation describe-change-set --change-set-name {} "
                   "--output text --query 'StatusReason'").format(
                profile_arg(),
                change_set_id
            )
            ctx.run(cmd)
        else:
            print("\nCloudFormation stack changeset created.\n")
            cmd = ("aws {} cloudformation describe-change-set --change-set-name {} "
                   "--query 'Changes[*].ResourceChange.{{ID:LogicalResourceId,Change:Details[*].CausingEntity}}' "
                   "--output text").format(
                profile_arg(),
                change_set_id
            )
            ctx.run(cmd)
            ok = input('\nView the full changeset details on the CloudFormation stack page.'
                       '\nAfter reviewing would you like to proceed? [y/N] ').lower().strip().startswith('y')
            if not ok:
                cmd = ("aws {} cloudformation delete-change-set "
                       "--change-set-name {}").format(profile_arg(), change_set_id)
                ctx.run(cmd, hide=True)
                print("Update cancelled.")
                return
            else:
                cmd = ("aws {} cloudformation execute-change-set "
                       "--change-set-name {}").format(profile_arg(), change_set_id)
                print("Executing update...")
                res = ctx.run(cmd)
                if res.exited == 0:
                    __wait_for(ctx, 'stack-update-complete')
    print("Done")


def __wait_for(ctx, wait_op):

    wait_cmd = ("aws {} cloudformation wait {} "
                "--stack-name {}").format(profile_arg(), wait_op, STACK_NAME)
    print("Waiting for stack operation to complete...")
    return ctx.run(wait_cmd, warn=True)


def __update_release_alias(ctx, func, version, description):
    print("Setting {} '{}' alias to version {}".format(func, getenv('LAMBDA_RELEASE_ALIAS'), version))
    lambda_function_name = "{}-{}-function".format(STACK_NAME, func)
    if description is None:
        description = "''"
    alias_cmd = ("aws {} lambda update-alias --function-name {} "
           "--name {} --function-version '{}' "
           "--description '{}'") \
        .format(
            profile_arg(),
            lambda_function_name,
            getenv("LAMBDA_RELEASE_ALIAS"),
            version,
            description
        )
    ctx.run(alias_cmd)


def __publish_version(ctx, func, description):

    print("Publishing new version of {}".format(func))
    lambda_function_name = "{}-{}-function".format(STACK_NAME, func)
    if description is None:
        description = "''"
    version_cmd = ("aws {} lambda publish-version --function-name {} "
                   "--description '{}' --query 'Version'") \
        .format(profile_arg(), lambda_function_name, description)
    res = ctx.run(version_cmd, hide=1)
    return int(res.stdout.replace('"', ''))


def __build_function(ctx, func, upload_to_s3=False):
    print(f"Building {func} function")
    req_file = join(dirname(__file__), 'function_requirements/{}.txt'.format(func))

    zip_path = join(dirname(__file__), 'dist/{}.zip'.format(func))

    build_path = join(dirname(__file__), 'dist/{}'.format(func))
    if exists(build_path):
        shutil.rmtree(build_path)

    if exists(req_file):
        ctx.run("pip install -U -r {} -t {}".format(req_file, build_path), hide=1)

    for module in [func, 'common']:
        module_path = join(dirname(__file__), 'functions/{}.py'.format(module))
        module_dist_path = join(build_path, '{}.py'.format(module))
        try:
            print("symlinking {} to {}".format(module_path, module_dist_path))
            symlink(module_path, module_dist_path)
        except FileExistsError:
            pass

    # include the ffprobe binary with the downloader function package
    if func == 'zoom-downloader':
        ffprobe_path = join(dirname(__file__), 'bin/ffprobe')
        ffprobe_dist_path = join(build_path, 'ffprobe')
        try:
            symlink(ffprobe_path, ffprobe_dist_path)
        except FileExistsError:
            pass

    with ctx.cd(build_path):
        ctx.run("zip -r {} .".format(zip_path), hide=1)

    if upload_to_s3:
        ctx.run("aws {} s3 cp {} s3://{}/{}/{}.zip".format(
            profile_arg(),
            zip_path,
            getenv("LAMBDA_CODE_BUCKET"),
            STACK_NAME,
            func),
            hide=1
        )


def __update_function(ctx, func):
    print(f"Updating {func} function")
    lambda_function_name = "{}-{}-function".format(STACK_NAME, func)
    zip_path = join(dirname(__file__), 'dist', func + '.zip')

    if not exists(zip_path):
        raise Exit("{} not found!".format(zip_path))

    cmd = ("aws {} lambda update-function-code "
           "--function-name {} --zip-file fileb://{}"
           ).format(
                profile_arg(),
                lambda_function_name,
                zip_path
            )
    ctx.run(cmd, hide=1)


def __set_debug(ctx, debug_val):
    for func in ['zoom-webhook', 'zoom-downloader', 'zoom-uploader']:
        func_name = "{}-{}-function".format(STACK_NAME, func)
        cmd = ("aws {} lambda get-function-configuration --output json "
               "--function-name {}").format(profile_arg(), func_name)
        res = ctx.run(cmd, hide=1)
        config = json.loads(res.stdout)
        func_env = config['Environment']['Variables']

        if func_env.get('DEBUG') is not None \
                and int(func_env.get('DEBUG')) == debug_val:
            continue

        func_env['DEBUG'] = debug_val
        new_vars = ','.join("{}={}".format(k, v) for k, v in func_env.items())

        cmd = ("aws {} lambda update-function-configuration "
               "--environment 'Variables={{{}}}' "
               "--function-name {}"
               ).format(profile_arg(), new_vars, func_name)
        ctx.run(cmd)


def __move_messages(deadletter_queue, source_queue, limit, uuid=None):
    if deadletter_queue is None or source_queue is None:
        print("Missing required queues.")
        return

    sqs = boto3.client('sqs')
    fifo = deadletter_queue.endswith('fifo')
    total_messages_moved = 0

    while total_messages_moved < limit:
        remaining = limit - total_messages_moved

        if fifo:
            response = sqs.receive_message(
                QueueUrl=deadletter_queue,
                AttributeNames=['MessageDeduplicationId'],
                MaxNumberOfMessages=10 if remaining > 10 else remaining,
                VisibilityTimeout=10,
                WaitTimeSeconds=10)
        else:
            response = sqs.receive_message(
                QueueUrl=deadletter_queue,
                MaxNumberOfMessages=10 if remaining > 10 else remaining,
                VisibilityTimeout=10,
                WaitTimeSeconds=10)

        if 'Messages' not in response or len(response['Messages']) == 0:
            if total_messages_moved == 0:
                print("No messages found!")
            else:
                print("Moved {} message(s)".format(total_messages_moved))
            return

        messages = response['Messages']
        received_count = 0

        entries = []
        for i, message in enumerate(messages):
            message_body = json.loads(message['Body'])
            if uuid is None or uuid == message_body['uuid']:
                print("\nMoving message:")
                pprint(message_body)
                received_count += 1
                new_entry = {
                    'Id': str(i),
                    'MessageBody': message['Body'],
                    'DelaySeconds': 0
                }
                if fifo:
                    deduplication_id = message['Attributes']['MessageDeduplicationId']
                    new_entry['MessageDeduplicationId'] = deduplication_id
                    new_entry['MessageGroupId'] = deduplication_id

                entries.append(new_entry)

        if received_count == 0:
            continue

        send_resp = sqs.send_message_batch(QueueUrl=source_queue, Entries=entries)
        moved_count = len(send_resp['Successful'])
        if moved_count < received_count:
            print("One or more messages failed to be sent back to the source queue."
                  "Received {} messages and successfully sent {} messages."
                  .format(received_count, moved_count))

        entries = []
        for message_moved in send_resp['Successful']:
            moved_id = message_moved['Id']
            entries.append({
                'Id': moved_id,
                'ReceiptHandle': messages[int(moved_id)]['ReceiptHandle']
            })

        del_resp = sqs.delete_message_batch(QueueUrl=deadletter_queue, Entries=entries)
        deleted_count = len(del_resp['Successful'])
        if deleted_count < received_count:
            print("One or more messages failed to be deleted from the deadletter queue."
                  "Received {} messages and successfully deleted {} messages."
                  .format(moved_count, deleted_count))

        total_messages_moved += moved_count
        time.sleep(1)


def __view_messages(queue_url, limit):

    if queue_url is None:
        print("Missing required queues.")
        return

    sqs = boto3.client('sqs')
    fifo = queue_url.endswith("fifo")

    if "deadletter" in queue_url:
        print("\nDEADLETTER QUEUE")
    else:
        print("\nMAIN QUEUE")

    total_messages_received = 0

    print("Fetching messages from {}...\n".format(queue_url.split('/')[-1]))

    while total_messages_received < limit:
        remaining = limit - total_messages_received

        if fifo:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                AttributeNames=['MessageDeduplicationId'],
                MessageAttributeNames=['FailedReason'],
                MaxNumberOfMessages=10,
                VisibilityTimeout=remaining+10,
                WaitTimeSeconds=10)

        else:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MessageAttributeNames=['FailedReason'],
                MaxNumberOfMessages=10,
                VisibilityTimeout=remaining + 10,
                WaitTimeSeconds=10)

        if 'Messages' not in response:
            if total_messages_received == 0:
                print("No messages found!")
            else:
                print("Found {} message(s)".format(total_messages_received))
            return

        total_messages_received += len(response['Messages'])

        for message in response['Messages']:
            print("Body:")
            pprint(json.loads(message['Body']))
            if 'Attributes' in message:
                print("Attributes:")
                pprint(message['Attributes'])
            if 'MessageAttributes' in message and 'FailedReason' in message['MessageAttributes']:
                print("{}: {}".format('ReportedError', message['MessageAttributes']['FailedReason']['StringValue']))
            print()


def __schedule_json_to_dynamo(ctx, json_file=None, schedule_data=None):

    if json_file is not None:
        with open(json_file, "r") as file:
            try:
                schedule_data = json.load(file)
            except Exception as e:
                print("Unable to load {}: {}" \
                      .format(json_file, str(e)))
                return
    elif schedule_data is None:
        raise Exit("{} called with no json_file or schedule_data args".format(
            sys._getframe().f_code.co_name
        ))

    try:
        dynamodb = boto3.resource('dynamodb')
        table_name = STACK_NAME + '-schedule'
        table = dynamodb.Table(table_name)

        for item in schedule_data.values():
            table.put_item(Item=item)
    except ClientError as e:
        error = e.response['Error']
        raise Exit("{}: {}".format(error['Code'], error['Message']))

    print("Schedule updated")

    # todo print diff using value from `__get_dyanmo_schedule`
    # pprint(__get_dynamo_schedule(ctx, table_name))


def __get_dynamo_schedule(ctx, table_name):

    cmd = "aws {} dynamodb scan --table-name {}".format(profile_arg(), table_name)
    res = ctx.run(cmd, hide=True).stdout
    res = json.loads(res)

    from boto3.dynamodb.types import TypeDeserializer
    tds = TypeDeserializer()
    current_schedule = {}

    for item in res["Items"]:
        item = { k: tds.deserialize(v) for k, v in item.items() }
        current_schedule[item["zoom_series_id"]] = item

    return current_schedule


def __show_stack_status(ctx):
    cmd = ("aws {} cloudformation describe-stacks "
           "--stack-name {} --output table"
           .format(profile_arg(), STACK_NAME))
    ctx.run(cmd)


def __show_webhook_endpoint(ctx):

    cmd = ("aws {} cloudformation describe-stack-resources --stack-name {} "
           "--query \"StackResources[?ResourceType=='AWS::ApiGateway::RestApi'].PhysicalResourceId\" "
           "--output text").format(profile_arg(), STACK_NAME)
    rest_api_id = ctx.run(cmd, hide=True).stdout.strip()

    webhook_url = "https://{}.execute-api.{}.amazonaws.com/{}/new_recording" \
        .format(rest_api_id, AWS_DEFAULT_REGION, getenv("LAMBDA_RELEASE_ALIAS"))
    print(tabulate([["Webhook Endpoint", webhook_url]], tablefmt="grid"))

    on_demand_url = "https://{}.execute-api.{}.amazonaws.com/{}/ingest" \
        .format(rest_api_id, AWS_DEFAULT_REGION, getenv("LAMBDA_RELEASE_ALIAS"))
    print(tabulate([["On-Demand Endpoint", on_demand_url]], tablefmt="grid"))


def __show_function_status(ctx):

    status_table = [
        [
            'function',
            'released',
            'desc',
            'timestamp',
            '$LATEST timestamp'
        ]
    ]

    for func in FUNCTION_NAMES:

        lambda_function_name = "{}-{}-function".format(STACK_NAME, func)
        cmd = ("aws {} lambda list-aliases --function-name {} "
               "--query \"Aliases[?Name=='{}'].[FunctionVersion,Description]\" "
               "--output text") \
            .format(
                profile_arg(),
                lambda_function_name,
                getenv('LAMBDA_RELEASE_ALIAS')
            )
        res = ctx.run(cmd, hide=True)

        try:
            released_version, description = res.stdout.strip().split()
        except ValueError:
            released_version = res.stdout.strip()
            description = ""

        status_row = [func, released_version, description]

        cmd = ("aws {} lambda list-versions-by-function --function-name {} "
               "--query \"Versions[?Version=='{}'].LastModified\" --output text") \
            .format(profile_arg(), lambda_function_name, released_version)
        status_row.append(ctx.run(cmd, hide=True).stdout)

        cmd = ("aws {} lambda list-versions-by-function --function-name {} "
               "--query \"Versions[?Version=='\$LATEST'].LastModified\" --output text") \
            .format(profile_arg(), lambda_function_name)
        status_row.append(ctx.run(cmd, hide=True).stdout)

        status_table.append(status_row)

    print(tabulate(status_table, headers="firstrow", tablefmt="grid"))


def __show_sqs_status(ctx):

    status_table = [
        [
            'queue',
            'Messages',
            'MessagesNotVisible',
            'MessagesDelayed',
            'LastModified'
        ]
    ]

    queue_names = [
        'uploads.fifo',
        'uploads-deadletter.fifo',
        'downloads',
        'downloads-deadletter'
    ]

    for queue_name in queue_names:
        url = get_queue_url(queue_name)

        cmd = ("aws {} sqs get-queue-attributes --queue-url {} "
               "--attribute-names All").format(profile_arg(), url)

        res = json.loads(ctx.run(cmd, hide=True).stdout)["Attributes"]

        last_modified = datetime_date.fromtimestamp(
            int(res["LastModifiedTimestamp"])).strftime("%Y-%m-%d %I:%M:%S")

        status_row = [queue_name,
                      res["ApproximateNumberOfMessages"],
                      res["ApproximateNumberOfMessagesNotVisible"],
                      res["ApproximateNumberOfMessagesDelayed"],
                      last_modified]

        status_table.append(status_row)

    print()
    print(tabulate(status_table, headers="firstrow", tablefmt="grid"))


def __get_meetings(date):
    print("Requesting meeting data for {}...".format(date))
    requested_page_size = 300
    zoom_dashboard_rate_limit_secs = 5
    meetings = []

    for mtg_type in ["past", "pastOne"]:
        base_path = ("metrics/meetings/?page_size={}&type={}&from={}&to={}"
                     .format(requested_page_size, mtg_type, date, date))

        next_page_token = None
        count = 0
        while True:
            if next_page_token:
                path = ("{}&next_page_token={}"
                        .format(base_path, next_page_token))
            else:
                path = base_path

            r = zoom_api_request(path, ignore_failure=True)
            if r.status_code == 429:
                print("API rate limited, waiting {} seconds to retry..."
                      .format(zoom_dashboard_rate_limit_secs))
                time.sleep(zoom_dashboard_rate_limit_secs)
                continue
            else:
                r.raise_for_status

            resp_data = r.json()
            if 'next_page_token' not in resp_data:
                break
            next_page_token = resp_data['next_page_token'].strip()
            if not next_page_token:
                break
            meetings.extend(r.json()['meetings'])

            count += min(requested_page_size, resp_data["page_size"])
            print("Retrieved {} of {} '{}' meetings."
                  .format(count, resp_data["total_records"], mtg_type))
            print("Waiting {} seconds for next request "
                  "to avoid Zoom API rate limit..."
                  .format(zoom_dashboard_rate_limit_secs))
            time.sleep(zoom_dashboard_rate_limit_secs)

        time.sleep(1)

    return meetings


def __find_recording_log_events(ctx, function, uuid):
    if function == 'zoom-webhook':
        filter_pattern = '{ $.message.payload.uuid = "' + uuid + '" }'
    elif function == 'zoom-downloader' or function == 'zoom-uploader':
        filter_pattern = '{ $.message.uuid = "' + uuid + '" }'
    else:
        return

    log_group = '/aws/lambda/{}-{}-function'.format(STACK_NAME, function)

    for log_stream, request_id in __request_ids_from_logs(ctx, log_group, filter_pattern):

        request_id_pattern = '{ $.aws_request_id = "' + request_id + '" }'
        cmd = ("aws logs filter-log-events {} --log-group-name {} "
               "--log-stream-name '{}' "
               "--output text --query 'events[].message' "
               "--filter-pattern '{}'") \
            .format(profile_arg(), log_group, log_stream, request_id_pattern)
        for line in ctx.run(cmd, hide=True).stdout.split("\t"):
            try:
                event = json.loads(line)
                print(json.dumps(event, indent=2))
            except json.JSONDecodeError:
                print(line)


def __request_ids_from_logs(ctx, log_group, filter_pattern):

    cmd = ("aws logs filter-log-events {} --log-group-name {} "
           "--output text --query 'events[][logStreamName,message]' "
           "--filter-pattern '{}'"
           .format(profile_arg(), log_group, filter_pattern))

    for line in ctx.run(cmd, hide=True).stdout.splitlines():
        log_stream, message = line.strip().split(maxsplit=1)
        message = json.loads(message)
        yield log_stream, message['aws_request_id']


API_RESOURCE_POLICY = """
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": "execute-api:Invoke",
            "Resource": "arn:aws:execute-api:{region}:{account}:{api_id}/*"
        },
        {
            "Effect": "Deny",
            "Principal": "*",
            "Action": "execute-api:Invoke",
            "Resource": "arn:aws:execute-api:{region}:{account}:{api_id}/*/POST/ingest",
            "Condition": {
                "NotIpAddress": {
                    "aws:SourceIp": []
                }
            }
        }
    ]
}
"""
