import json
import boto3
import jmespath
import time
import csv
from datetime import datetime
from urllib.parse import urlencode
from invoke import task, Collection
from invoke.exceptions import Exit
from os import symlink, getenv as env
from dotenv import load_dotenv
from os.path import join, dirname, exists

load_dotenv(join(dirname(__file__), '.env'))

AWS_PROFILE = env('AWS_PROFILE')
STACK_NAME = env('STACK_NAME')

if AWS_PROFILE is not None:
    boto3.setup_default_session(profile_name=AWS_PROFILE)


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


@task
def package_all(ctx):
    """
    Create zip packages w/ lambda function code + dependencies and upload to s3
    """
    package_webhook(ctx)
    package_downloader(ctx)
    package_uploader(ctx)
    package_log_notifications(ctx)


@task
def package_webhook(ctx):
    __package_function(ctx, 'zoom-webhook')
    __function_to_s3(ctx, 'zoom-webhook')


@task
def package_downloader(ctx):
    __package_function(ctx, 'zoom-downloader')
    __function_to_s3(ctx, 'zoom-downloader')


@task
def package_uploader(ctx):
    __package_function(ctx, 'zoom-uploader')
    __function_to_s3(ctx, 'zoom-uploader')


@task
def package_log_notifications(ctx):
    __package_function(ctx, 'zoom-log-notifications')
    __function_to_s3(ctx, 'zoom-log-notifications')


@task
def update_all(ctx):
    """
    Package, upload and register new code for all lambda functions
    """
    update_webhook(ctx)
    update_downloader(ctx)
    update_uploader(ctx)
    update_log_notifications(ctx)


@task
def update_webhook(ctx):
    package_webhook(ctx)
    __update_function(ctx, 'zoom-webhook')


@task
def update_downloader(ctx):
    package_downloader(ctx)
    __update_function(ctx, 'zoom-downloader')


@task
def update_uploader(ctx):
    package_uploader(ctx)
    __update_function(ctx, 'zoom-uploader')


@task
def exec_webhook(ctx, uuid, host_id, status=None):
    """
    Positional arguments: uuid, host_id
    """

    if status is None:
        status = 'RECORDING_MEETING_COMPLETED'

    stack_name = getenv('STACK_NAME')
    apig = boto3.client('apigateway')

    apis = apig.get_rest_apis()
    api_id = jmespath.search("items[?name=='{}'].id | [0]".format(stack_name), apis)

    api_resources = apig.get_resources(restApiId=api_id)
    resource_id = jmespath.search("items[?pathPart=='new_recording'].id | [0]", api_resources)

    content = json.dumps({'uuid': uuid, 'host_id': host_id})
    event_body = "type={}&{}".format(status, urlencode({'content': content}))

    resp = apig.test_invoke_method(
        restApiId=api_id,
        resourceId=resource_id,
        httpMethod='POST',
        body=event_body
    )
    print(resp)


@task
def update_log_notifications(ctx):
    package_log_notifications(ctx)
    __update_function(ctx, 'zoom-log-notifications')


@task
def create(ctx):
    """
    Build the Cloudformation stack identified by $STACK_NAME
    """
    __create_or_update(ctx, "create")


@task
def update(ctx):
    """
    Update the Cloudformation stack identified by $STACK_NAME
    """
    __create_or_update(ctx, "update")


@task
def refresh_all(ctx):
    refresh_webhook(ctx)
    refresh_downloader(ctx)
    refresh_uploader(ctx)


@task
def refresh_webhook(ctx):
    __refresh_function(ctx, 'zoom-webhook')


@task
def refresh_downloader(ctx):
    __refresh_function(ctx, 'zoom-downloader')


@task
def refresh_uploader(ctx):
    __refresh_function(ctx, 'zoom-uploader')


@task
def delete(ctx):
    """
    Delete the Cloudformation stack identified by $STACK_NAME
    """
    cmd = ("aws {} cloudformation delete-stack "
           "--stack-name {}").format(profile_arg(), getenv("STACK_NAME"))
    if input('are you sure? [y/N] ').lower().strip().startswith('y'):
        ctx.run(cmd, echo=True)
    else:
        print("not deleting stack")


@task
def status(ctx):
    """
    Show table of cloudformation stack details
    """
    cmd = ("aws {} cloudformation describe-stacks "
           "--stack-name {} --output table"
           .format(profile_arg(), getenv('STACK_NAME')))
    ctx.run(cmd)


@task
def debug_on(ctx):
    """
    Enable debug logging in all lambda functions
    """
    _set_debug(ctx, 1)


@task
def debug_off(ctx):
    """
    Disable debug logging in all lambda functions
    """
    _set_debug(ctx, 0)


@task
def test(ctx):
    """
    Execute the pytest tests
    """
    ctx.run('py.test --cov-report term-missing --cov=functions tests')


@task
def retry_uploads(ctx, limit=1, uuid=None):
    """
        Move SQS messages DLQ to source. Optional: --limit (default 1).
    """
    _move_messages("uploads", limit=limit, uuid=uuid)


@task
def retry_downloads(ctx, limit=1, uuid=None):
    """
        Move SQS messages DLQ to source. Optional: --limit (default 1).
    """
    _move_messages("downloads", limit=limit, uuid=uuid)


@task
def view_uploads(ctx, limit=20):
    """
        View Uploader DLQ. Optional: --limit (default 20).
    """
    _view_messages("uploads", limit=limit)


@task
def view_downloads(ctx, limit=20):
    """
        View Downloader DLQ. Optional: --limit (default 20).
    """
    _view_messages("downloads", limit=limit)


@task
def import_schedule(ctx, csv_name="classes.csv", year=None, semester=None):
    """
        Csv to dynamo. Optional: --csv_name, --year, --semester
    """
    _schedule_csv_to_json(csv_name, "classes.json", year=year, semester=semester)
    _schedule_json_to_dynamo("classes.json")


ns = Collection()
ns.add_task(create_code_bucket)
ns.add_task(test)

package_ns = Collection('package')
package_ns.add_task(package_all, 'all')
package_ns.add_task(package_webhook, 'webhook')
package_ns.add_task(package_downloader, 'downloader')
package_ns.add_task(package_uploader, 'uploader')
package_ns.add_task(package_log_notifications, 'log-notifications')
ns.add_collection(package_ns)

update_ns = Collection('update')
update_ns.add_task(update_all, 'all')
update_ns.add_task(update_webhook, 'webhook')
update_ns.add_task(update_downloader, 'downloader')
update_ns.add_task(update_uploader, 'uploader')
update_ns.add_task(update_log_notifications, 'log-notifications')
ns.add_collection(update_ns)

exec_ns = Collection('exec')
exec_ns.add_task(exec_webhook, 'webhook')
ns.add_collection(exec_ns)

refresh_ns = Collection('refresh')
refresh_ns.add_task(refresh_all, 'all')
refresh_ns.add_task(refresh_webhook, 'webhook')
refresh_ns.add_task(refresh_downloader, 'downloader')
refresh_ns.add_task(refresh_uploader, 'uploader')
ns.add_collection(refresh_ns)

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
    tags = getenv("STACK_TAGS")
    if tags is not None:
        return "--tags {}".format(tags)
    return ""


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


def __create_or_update(ctx, op):

    template_path = join(dirname(__file__), 'template.yml')
    lambda_objects = {}

    for func in ['zoom-webhook', 'zoom-downloader', 'zoom-uploader']:
        zip_path = join(dirname(__file__), 'functions', func + '.zip')
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
           ).format(
                profile_arg(),
                op,
                stack_tags(),
                getenv("STACK_NAME"),
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
                subnet_id
                )
    print(cmd)
    ctx.run(cmd)


def __package_function(ctx, func):
    req_file = join(dirname(__file__), 'functions/{}.txt'.format(func))
    build_path = join(dirname(__file__), 'dist/{}'.format(func))
    zip_path = join(dirname(__file__), 'functions/{}.zip'.format(func))

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

    with ctx.cd(build_path):
        ctx.run("zip -r {} .".format(zip_path))


def __function_to_s3(ctx, func):
    zip_path = join(dirname(__file__), 'functions/{}.zip'.format(func))
    ctx.run("aws {} s3 cp {} s3://{}".format(
        profile_arg(),
        zip_path,
        getenv("LAMBDA_CODE_BUCKET"))
    )


def __update_function(ctx, func):
    lambda_function_name = "{}-{}-function".format(getenv("STACK_NAME"), func)
    cmd = ("aws {} lambda update-function-code "
           "--function-name {} --publish --s3-bucket {} --s3-key {}.zip"
           ).format(
                profile_arg(),
                lambda_function_name,
                getenv('LAMBDA_CODE_BUCKET'),
                func
            )
    ctx.run(cmd)


def __refresh_function(ctx, func):
    lambda_function_name = "{}-{}-function".format(getenv("STACK_NAME"), func)
    now = datetime.utcnow().isoformat()
    cmd = ("aws {} lambda update-function-configuration "
           "--function-name {} --description '{}'"
           ).format(profile_arg(), lambda_function_name, now)
    ctx.run(cmd, echo=True)


def _set_debug(ctx, debug_val):
    for func in ['zoom-webhook', 'zoom-downloader', 'zoom-uploader']:
        func_name = "{}-{}-function".format(getenv("STACK_NAME"), func)
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


def _move_messages(queue_type, limit, uuid=None):
    source_name = "{}-{}.fifo".format(env('STACK_NAME'), queue_type)
    dl_name = "{}-{}-deadletter.fifo".format(env('STACK_NAME'), queue_type)

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


def _view_messages(queue_type, limit):
    queue_name = "{}-{}-deadletter.fifo".format(env('STACK_NAME'), queue_type)

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


def _schedule_csv_to_json(csv_name, json_name, year=None, semester=None):
    if year is None:
        year = str(datetime.now().year)
    if semester is None:
        month = datetime.now().month
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


def _schedule_json_to_dynamo(json_name):

    dynamodb = boto3.resource('dynamodb')

    table_name = STACK_NAME + '-schedule'
    table = dynamodb.Table(table_name)

    file = open(json_name, "r")

    classes = json.load(file)

    for item in classes.values():
        table.put_item(Item=item)

    file.close()
