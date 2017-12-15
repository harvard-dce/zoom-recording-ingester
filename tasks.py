
import shutil
from invoke import task
from invoke.exceptions import Exit
from os import getenv as env
from dotenv import load_dotenv
from os.path import join, dirname, exists

load_dotenv(join(dirname(__file__), '.env'))

AWS_PROFILE = env('AWS_PROFILE')
STACK_TAGS = env('STACK_TAGS')
STACK_NAME = env('STACK_NAME', 'zoom-ingester')
LAMBDA_CODE_BUCKET = env('LAMBDA_CODE_BUCKET')


def profile_arg():
    if AWS_PROFILE is not None:
        return "--profile {}".format(AWS_PROFILE)
    return ""

def stack_tags():
    if STACK_TAGS is not None:
        return "--tags {}".format(STACK_TAGS)
    return ""


@task
def package_webhook(ctx):
    __package_function(ctx, 'zoom-webhook')
    __function_to_s3(ctx, 'zoom-webhook')


@task
def package_downloader(ctx):
    __package_function(ctx, 'zoom-downloader')
    __function_to_s3(ctx, 'zoom-downloader')


@task
def update_webhook(ctx):
    __package_function(ctx, 'zoom-webhook')
    __update_function(ctx, 'zoom-webhook')


@task
def update_downloader(ctx):
    __package_function(ctx, 'zoom-downloader')
    __update_function(ctx, 'zoom-downloader')


@task
def create(ctx):
    __create_or_update(ctx, "create")


@task
def update(ctx):
    __create_or_update(ctx, "update")


@task
def delete(ctx):

    cmd = ("aws {} cloudformation delete-stack "
           "--stack-name {}").format(profile_arg(), STACK_NAME)
    if input('are you sure? [y/N] ').lower().strip().startswith('y'):
        ctx.run(cmd)
    else:
        print("not deleting stack")


def __create_or_update(ctx, op):
    template_path = join(dirname(__file__), 'template.yml')

    lambda_objects = {}

    for func in ['zoom-webhook', 'zoom-downloader']:
        zip_path = join(dirname(__file__), 'functions', func + '.zip')
        if not exists(zip_path):
            print("No zip found for {}! Did you run the package-* commands?".format(func))
            raise Exit(1)
        lambda_objects[func] = '/'.join([LAMBDA_CODE_BUCKET, func + '.zip'])

    cmd = ("aws {} cloudformation {}-stack {} "
           "--capabilities CAPABILITY_NAMED_IAM --stack-name {} "
           "--template-body file://{} "
           "--parameters "
           "ParameterKey=WebhookLambdaCode,ParameterValue={} "
           "ParameterKey=ZoomDownloaderLambdaCode,ParameterValue={} "
           ).format(profile_arg(), op, stack_tags(), STACK_NAME, template_path,
                    lambda_objects['zoom-webhook'], lambda_objects['zoom-downloader'])
    print(cmd)
    ctx.run(cmd)


def __package_function(ctx, func):
    req_file = join(dirname(__file__), 'functions/{}.txt'.format(func))
    build_path = join(dirname(__file__), 'dist/{}'.format(func))
    zip_path = join(dirname(__file__), 'functions/{}.zip'.format(func))
    function_path = join(dirname(__file__), 'functions/{}.py'.format(func))
    ctx.run("pip install -U -r {} -t {}".format(req_file, build_path))
    ctx.run("ln -s -f -r -t {} {}".format(build_path, function_path))
    with ctx.cd(build_path):
        ctx.run("zip -r {} .".format(zip_path))


def __function_to_s3(ctx, func):
    zip_path = join(dirname(__file__), 'functions/{}.zip'.format(func))
    ctx.run("aws {} s3 cp {} s3://{}".format(profile_arg(), zip_path, LAMBDA_CODE_BUCKET))


def __update_function(ctx, func):
    zip_path = join(dirname(__file__), 'functions/{}.zip'.format(func))
    lambda_function_name = "{}-{}-function".format(STACK_NAME, func)
    cmd = ("aws {} lambda update-function-code "
           "--function-name {} --zip-file fileb://{} --publish"
           ).format(profile_arg(), lambda_function_name, zip_path)
    print(cmd)
    ctx.run(cmd)

