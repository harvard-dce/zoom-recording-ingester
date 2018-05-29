import json
import boto3
import jmespath
import time
import csv
import shutil
import datetime
from urllib.parse import urlencode
from invoke import task, Collection
from invoke.exceptions import Exit
from os import symlink, getenv as env
from dotenv import load_dotenv
from os.path import join, dirname, exists
from tabulate import tabulate
from functions.common import gen_token
import requests

load_dotenv(join(dirname(__file__), '.env'))

AWS_PROFILE = env('AWS_PROFILE')
AWS_DEFAULT_REGION = env('AWS_DEFAULT_REGION', 'us-east-1')
STACK_NAME = env('STACK_NAME')
PROD_IDENTIFIER = "prod"
NONINTERACTIVE = env('NONINTERACTIVE')
ZOOM_API_KEY = env('ZOOM_API_KEY')
ZOOM_API_SECRET = env('ZOOM_API_SECRET')
ZOOM_BASE_URL = "http://api.zoom.us/v2/"

FUNCTION_NAMES = [
    'zoom-webhook',
    'zoom-downloader',
    'zoom-uploader',
    'zoom-log-notifications'
]

if AWS_PROFILE is not None:
    boto3.setup_default_session(profile_name=AWS_PROFILE)


@task
def production_failsafe(ctx):
    """
    This is not a standalone task and should not be added to any of the task collections.
    It is meant to be prepended to the execution of other tasks to force a confirmation
    when a task is being executed that could have an impact on a production stack
    """
    if not NONINTERACTIVE and PROD_IDENTIFIER in STACK_NAME.lower():
        print("You are about to run this task on a production system")
        ok = input('are you sure? [y/N] ').lower().strip().startswith('y')
        if not ok:
            raise Exit("Aborting")

@task
def create_code_bucket(ctx):
    """
    Create the s3 bucket for storing packaged lambda code
    """
    code_bucket = getenv('LAMBDA_CODE_BUCKET')
    cmd = "aws {} s3 ls {}".format(profile_arg(), code_bucket)
    exists = ctx.run(cmd, hide=True, warn=True)
    if exists.ok:
        print("Bucket exists!")
    else:
        cmd = "aws {} s3 mb s3://{}".format(profile_arg(), code_bucket)
        ctx.run(cmd)


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
           " name='NONINTERACTIVE',value=1" ) \
        .format(
            profile_arg(),
            STACK_NAME,
            revision,
            STACK_NAME,
            getenv("LAMBDA_RELEASE_ALIAS")
            )
    ctx.run(cmd)


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

    for  func in functions:
        __build_function(ctx, func)
        __update_function(ctx, func)
        if do_release:
            release(ctx, func)


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
def list_recordings(ctx, date=str(datetime.date.today())):
    """
    Optional: --date='YYYY-MM-DD'
    """
    meetings = __get_meetings(date)

    recordings_found = 0

    for meeting in meetings:
        time.sleep(0.1)
        token = gen_token(key=ZOOM_API_KEY, secret=ZOOM_API_SECRET)
        uuid = meeting['uuid']
        series_id = meeting['id']

        r = requests.get("{}meetings/{}".format(ZOOM_BASE_URL, series_id),
                         headers={"Authorization": "Bearer %s" % token.decode()})

        if r.status_code == 404:
            continue

        r.raise_for_status()

        host_id = r.json()['host_id']

        r = requests.get("{}meetings/{}/recordings".format(ZOOM_BASE_URL, uuid),
                         headers={"Authorization": "Bearer %s" % token.decode()})

        if r.status_code == 404:
            continue

        r.raise_for_status()
        recordings_found += 1

        print("\n--uuid='{}' --host_id='{}'".format(uuid, host_id))
        print("\tSeries id: {}".format(r.json()["id"]))
        print("\tTopic: {}".format(r.json()["topic"]))
        print("\tDuration: {} minutes".format(r.json()["duration"]))

    if recordings_found == 0:
        print("No recordings found on {}".format(date))
    else:
        print("Done!")


@task(help={'uuid': 'meeting instance uuid', 'host_id': 'meeting host id'})
def exec_webhook(ctx, uuid, host_id, status=None, webhook_version=2):
    """
    Manually call the webhook endpoint. Required: --uuid, --host_id
    """

    apig = boto3.client('apigateway')

    apis = apig.get_rest_apis()
    api_id = jmespath.search("items[?name=='{}'].id | [0]".format(STACK_NAME), apis)

    api_resources = apig.get_resources(restApiId=api_id)
    resource_id = jmespath.search("items[?pathPart=='new_recording'].id | [0]", api_resources)

    if webhook_version == 2:
        if status is None:
            status = 'recording_completed'

        event_body = json.dumps(
            {'event': status,
             'payload': {
                 'meeting': {
                     'uuid': uuid,
                     'host_id': host_id
                 }
             }}
        )

    else:
        if status is None:
            status = 'RECORDING_MEETING_COMPLETED'

        content = json.dumps({'uuid': uuid, 'host_id': host_id})
        event_body = "type={}&{}".format(status,  urlencode({'content': content}))

    resp = apig.test_invoke_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod='POST',
        body=event_body
    )

    print(json.dumps(resp, indent=True))


@task(pre=[production_failsafe])
def create(ctx):
    """
    Build the Cloudformation stack identified by $STACK_NAME
    """
    if stack_exists(ctx):
        raise Exit("Stack already exists!")

    package(ctx, upload_to_s3=True)
    __create_or_update(ctx, "create")
    release(ctx, description="initial release")


@task(pre=[production_failsafe])
def update(ctx):
    """
    Update the Cloudformation stack identified by $STACK_NAME
    """
    __create_or_update(ctx, "update")


@task(pre=[production_failsafe])
def delete(ctx):
    """
    Delete the Cloudformation stack identified by $STACK_NAME
    """
    if not stack_exists(ctx):
        raise Exit("Stack doesn't exist!")

    cmd = ("aws {} cloudformation delete-stack "
           "--stack-name {}").format(profile_arg(), STACK_NAME)
    if input('are you really, really sure? [y/N] ').lower().strip().startswith('y'):
        ctx.run(cmd, echo=True)
        __wait_for(ctx, "delete")
    else:
        print("not deleting stack")


@task
def status(ctx):
    """
    Show table of cloudformation stack details
    """
    __show_stack_status(ctx)
    __show_webhook_endpoint(ctx)
    __show_function_status(ctx)


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
    ctx.run('py.test --cov-report term-missing --cov=functions tests')


@task(pre=[production_failsafe])
def retry_uploads(ctx, limit=1, uuid=None):
    """
    Move SQS messages DLQ to source. Optional: --limit (default 1).
    """
    __move_messages("uploads", limit=limit, uuid=uuid)


@task(pre=[production_failsafe])
def retry_downloads(ctx, limit=1, uuid=None):
    """
    Move SQS messages DLQ to source. Optional: --limit (default 1).
    """
    __move_messages("downloads", limit=limit, uuid=uuid)


@task(pre=[production_failsafe])
def view_uploads(ctx, limit=20):
    """
    View Uploader DLQ. Optional: --limit (default 20).
    """
    __view_messages("uploads", limit=limit)


@task(pre=[production_failsafe])
def view_downloads(ctx, limit=20):
    """
    View Downloader DLQ. Optional: --limit (default 20).
    """
    __view_messages("downloads", limit=limit)


@task(pre=[production_failsafe])
def import_schedule(ctx, csv_name="classes.csv", year=None, semester=None):
    """
    Csv to dynamo. Optional: --csv_name, --year, --semester
    """
    __schedule_csv_to_json(csv_name, "classes.json", year=year, semester=semester)
    __schedule_json_to_dynamo("classes.json")


ns = Collection()
ns.add_task(create_code_bucket)
ns.add_task(test)
ns.add_task(codebuild)
ns.add_task(package)
ns.add_task(deploy)
ns.add_task(release)
ns.add_task(list_recordings)

exec_ns = Collection('exec')
exec_ns.add_task(exec_webhook, 'webhook')
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

dlq_ns = Collection('dlq')
dlq_ns.add_task(retry_uploads, 'retry-uploads')
dlq_ns.add_task(retry_downloads, 'retry-downloads')
dlq_ns.add_task(view_uploads, 'view-uploads')
dlq_ns.add_task(view_downloads, 'view-downloads')
ns.add_collection(dlq_ns)

schedule_ns = Collection('schedule')
schedule_ns.add_task(import_schedule, 'import')
ns.add_collection(schedule_ns)

###############################################################################


def getenv(var, required=True):
    val = env(var)
    if required and val is None:
        raise Exit("{} not defined".format(var))
    return val


def profile_arg():
    if AWS_PROFILE is not None:
        return "--profile {}".format(AWS_PROFILE)
    return ""


def stack_tags():
    tags = "Key=cfn-stack,Value={}".format(STACK_NAME)
    extra_tags = getenv("STACK_TAGS")
    if extra_tags is not None:
        tags += " " + extra_tags
    return "--tags {}".format(tags)


def vpc_components(ctx):

    vpc_id = getenv("VPC_ID", False)
    if vpc_id is None:
        confirm = ("No $VPC_ID defined. "
                   "Uploader will not be able to communicate with "
                   "the opencast admin. Do you wish to proceed? [y/N] ")
        if not input(confirm).lower().strip().startswith('y'):
            print("aborting")
            raise Exit(0)
        return "", ""

    cmd = ("aws {} ec2 describe-subnets --filters "
           "'Name=vpc-id,Values={}' "
           "'Name=tag:aws:cloudformation:logical-id,Values=PrivateSubnet'") \
        .format(profile_arg(), vpc_id)

    res = ctx.run(cmd, hide=1)
    subnet_data = json.loads(res.stdout)
    subnet_id = subnet_data['Subnets'][0]['SubnetId']

    cmd = ("aws {} ec2 describe-security-groups --filters "
           "'Name=vpc-id,Values={}' "
           "'Name=tag:aws:cloudformation:logical-id,Values=OpsworksLayerSecurityGroupCommon'") \
        .format(profile_arg(), vpc_id)
    res = ctx.run(cmd, hide=1)
    sg_data = json.loads(res.stdout)
    sg_id = sg_data['SecurityGroups'][0]['GroupId']

    return subnet_id, sg_id


def stack_exists(ctx):
    cmd = "aws {} cloudformation describe-stacks --stack-name {}" \
        .format(profile_arg(), STACK_NAME)
    res = ctx.run(cmd, hide=True, warn=True, echo=False)
    return res.exited == 0


def __create_or_update(ctx, op):

    template_path = join(dirname(__file__), 'template.yml')
    lambda_objects = {}

    for func in ['zoom-webhook', 'zoom-downloader', 'zoom-uploader']:
        zip_path = join(dirname(__file__), 'dist', func + '.zip')
        if not exists(zip_path):
            print("No zip found for {}!".format(func))
            print("Did you run the package* commands?")
            raise Exit(1)
        func_code = '/'.join([getenv("LAMBDA_CODE_BUCKET"), func + '.zip'])
        lambda_objects[func] = func_code

    subnet_id, sg_id = vpc_components(ctx)

    cmd = ("aws {} cloudformation {}-stack {} "
           "--capabilities CAPABILITY_NAMED_IAM --stack-name {} "
           "--template-body file://{} "
           "--parameters "
           "ParameterKey=LambdaCodeBucket,ParameterValue={} "
           "ParameterKey=NotificationEmail,ParameterValue='{}' "
           "ParameterKey=ZoomApiKey,ParameterValue='{}' "
           "ParameterKey=ZoomApiSecret,ParameterValue='{}' "
           "ParameterKey=ZoomLoginUser,ParameterValue='{}' "
           "ParameterKey=ZoomLoginPassword,ParameterValue='{}' "
           "ParameterKey=OpencastBaseUrl,ParameterValue='{}' "
           "ParameterKey=OpencastApiUser,ParameterValue='{}' "
           "ParameterKey=OpencastApiPassword,ParameterValue='{}' "
           "ParameterKey=DefaultOpencastSeriesId,ParameterValue='{}' "
           "ParameterKey=LocalTimeZone,ParameterValue='{}' "
           "ParameterKey=VpcSecurityGroupId,ParameterValue='{}' "
           "ParameterKey=VpcSubnetId,ParameterValue='{}' "
           "ParameterKey=LambdaReleaseAlias,ParameterValue='{}' "
           ).format(
                profile_arg(),
                op,
                stack_tags(),
                STACK_NAME,
                template_path,
                getenv('LAMBDA_CODE_BUCKET'),
                getenv("NOTIFICATION_EMAIL"),
                getenv("ZOOM_API_KEY"),
                getenv("ZOOM_API_SECRET"),
                getenv("ZOOM_LOGIN_USER"),
                getenv("ZOOM_LOGIN_PASSWORD"),
                getenv("OPENCAST_BASE_URL"),
                getenv("OPENCAST_API_USER"),
                getenv("OPENCAST_API_PASSWORD"),
                getenv("DEFAULT_SERIES_ID", False),
                getenv("LOCAL_TIME_ZONE"),
                sg_id,
                subnet_id,
                getenv("LAMBDA_RELEASE_ALIAS")
                )
    res = ctx.run(cmd)

    if res.exited == 0:
        __wait_for(ctx, op)


def __wait_for(ctx, op):
    wait_cmd = ("aws {} cloudformation wait stack-{}-complete "
                "--stack-name {}").format(profile_arg(), op, STACK_NAME)
    print("Waiting for stack {} to complete...".format(op))
    ctx.run(wait_cmd)
    print("Done")


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
    req_file = join(dirname(__file__), 'functions/{}.txt'.format(func))

    zip_path = join(dirname(__file__), 'dist/{}.zip'.format(func))

    build_path = join(dirname(__file__), 'dist/{}'.format(func))
    if exists(build_path):
        shutil.rmtree(build_path)

    if exists(req_file):
        ctx.run("pip install -U -r {} -t {}".format(req_file, build_path))

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
        ctx.run("zip -r {} .".format(zip_path))

    if upload_to_s3:
        ctx.run("aws {} s3 cp {} s3://{}".format(
            profile_arg(),
            zip_path,
            getenv("LAMBDA_CODE_BUCKET"))
        )


def __update_function(ctx, func):
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
    ctx.run(cmd)


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


def __move_messages(queue_type, limit, uuid=None):
    source_name = "{}-{}.fifo".format(STACK_NAME, queue_type)
    dl_name = "{}-{}-deadletter.fifo".format(STACK_NAME, queue_type)

    # using the low-level client in order to access the deduplication id in the received message
    # (which is not an attribute of the sqs.resource message object)
    sqs = boto3.client('sqs')

    deadletter_queue = sqs.get_queue_url(QueueName=dl_name)['QueueUrl']
    source_queue = sqs.get_queue_url(QueueName=source_name)['QueueUrl']
    total_messages_moved = 0

    while total_messages_moved < limit:
        remaining = limit - total_messages_moved

        response = sqs.receive_message(
            QueueUrl=deadletter_queue,
            AttributeNames=['MessageDeduplicationId'],
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
            deduplication_id = message['Attributes']['MessageDeduplicationId']
            if uuid is None or uuid == json.loads(message['Body'])['uuid']:
                received_count += 1
                entries.append({
                    'Id': str(i),
                    'MessageBody': message['Body'],
                    'MessageDeduplicationId': deduplication_id,
                    'MessageGroupId': deduplication_id,
                    'DelaySeconds': 0
                })

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


def __view_messages(queue_type, limit):
    queue_name = "{}-{}-deadletter.fifo".format(STACK_NAME, queue_type)

    sqs = boto3.client('sqs')
    queue_url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
    total_messages_received = 0

    print("\nFetching messages from {}...\n".format(queue_name))

    while total_messages_received < limit:
        remaining = limit - total_messages_received

        response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=['MessageDeduplicationId'],
            MessageAttributeNames=['FailedReason'],
            MaxNumberOfMessages=10,
            VisibilityTimeout=remaining+10,
            WaitTimeSeconds=10)

        if 'Messages' not in response :
            if total_messages_received == 0:
                print("No messages found!")
            else:
                print("Found {} message(s)".format(total_messages_received))
            return

        total_messages_received += len(response['Messages'])

        for message in response['Messages']:
            print("Attributes: {}\nBody: {}".format(message['Attributes'], message['Body']))
            if 'MessageAttributes' in message and 'FailedReason' in message['MessageAttributes']:
                print("{}: {}".format('ReportedError', message['MessageAttributes']['FailedReason']['StringValue']))
            print()


def __schedule_csv_to_json(csv_name, json_name, year=None, semester=None):
    if year is None:
        year = str(datetime.datetime.now().year)
    if semester is None:
        month = datetime.datetime.now().month
        if month < 6:
            semester = "02"
        elif month < 9:
            semester = "03"
        else:
            semester = "01"
    elif semester not in ["01", "02", "03"]:
        print("Semester must be '01' for fall. '02' for winter, or '03' for summer.")
        return

    csv_file = open(csv_name, "r")
    json_file = open(json_name, "w")

    reader = csv.DictReader(csv_file)

    data = {}

    for row in reader:
        del row[""]

        # filter out empty fields for dynamo
        course = {}
        for key, val in row.items():
            if val != '':
                course[key] = val

        if 'https://zoom.us' not in row['Links']:
            # not a zoom course (probably Adobe)
            continue
        else:
            zoom_series_id = row['Links'].split('/')[-1]

        opencast_series_id = year + semester + row["CRN"].strip()
        opencast_subject = row["Subject"].strip()\
                           + (" S-" if semester == "03" else " E-") \
                           + row["Course Number"].strip()

        course['opencast_series_id'] = opencast_series_id
        course['opencast_subject'] = opencast_subject
        course['zoom_series_id'] = zoom_series_id
        course['Days'] = [x.strip() for x in course['Day'].split("/")]
        del course['Day']

        data[zoom_series_id] = course

    json.dump(data, json_file, indent=2)

    csv_file.close()
    json_file.close()


def __schedule_json_to_dynamo(json_name):

    dynamodb = boto3.resource('dynamodb')

    table_name = STACK_NAME + '-schedule'
    table = dynamodb.Table(table_name)

    file = open(json_name, "r")

    classes = json.load(file)

    for item in classes.values():
        table.put_item(Item=item)

    file.close()


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
    invoke_url = "https://{}.execute-api.{}.amazonaws.com/{}/new_recording" \
        .format(rest_api_id, AWS_DEFAULT_REGION, getenv("LAMBDA_RELEASE_ALIAS"))

    print(tabulate([["Webhookd Endpoint", invoke_url]], tablefmt="grid"))

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
        status_row = []
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


def __get_meetings(date):
    page_size = 300
    mtg_type = "past"

    url = "{}metrics/meetings/".format(ZOOM_BASE_URL)
    url += "?page_size=%s&type=%s&from=%s&to=%s" % (page_size, mtg_type, date, date)

    token = gen_token(key=ZOOM_API_KEY, secret=ZOOM_API_SECRET)
    r = requests.get(url, headers={"Authorization": "Bearer %s" % token.decode()})
    r.raise_for_status()
    response = r.json()
    meetings = response['meetings']

    while 'next_page_token' in response and response['next_page_token'].strip() is True:
        token = gen_token(key=ZOOM_API_KEY, secret=ZOOM_API_SECRET)
        r = requests.get(url + "&next_page_token=" + response['next_page_token'],
                         headers={"Authorization": "Bearer %s" % token.decode()})
        r.raise_for_status()
        meetings.extend(r.json()['meetings'])
        time.sleep(60)

    time.sleep(1)

    return meetings
