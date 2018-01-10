
from invoke import task
from invoke.exceptions import Exit
from os import symlink, getenv
from dotenv import load_dotenv
from os.path import join, dirname, exists


@task
def _load_env(ctx):
    global ENV
    load_dotenv(join(dirname(__file__), '.env'))
    ENV = {var: getenv(var) for var in [
        'AWS_PROFILE',
        'STACK_TAGS',
        'STACK_NAME',
        'LAMBDA_CODE_BUCKET',
        'NOTIFICATION_EMAIL',
        'ZOOM_API_KEY',
        'ZOOM_API_SECRET',
        'ZOOM_LOGIN_USER',
        'ZOOM_LOGIN_PASSWORD'
    ]}
    for var in ['STACK_NAME', 'LAMBDA_CODE_BUCKET', 'NOTIFICATION_EMAIL']:
        if ENV[var] is None:
            raise Exit("You must set {} in .env".format(var))


@task(_load_env)
def create_code_bucket(ctx):
    cmd = "aws {} s3 ls {}".format(profile_arg(), ENV['LAMBDA_CODE_BUCKET'])
    exists = ctx.run(cmd, hide=True, warn=True)
    if exists.ok:
        print("Bucket exists!")
    else:
        cmd = "aws {} s3 mb s3://{}".format(profile_arg(), ENV['LAMBDA_CODE_BUCKET'])
        ctx.run(cmd)

@task(_load_env)
def package_webhook(ctx):
    __package_function(ctx, 'zoom-webhook')
    __function_to_s3(ctx, 'zoom-webhook')


@task(_load_env)
def package_downloader(ctx):
    __package_function(ctx, 'zoom-downloader')
    __function_to_s3(ctx, 'zoom-downloader')


@task(_load_env)
def update_webhook(ctx):
    __package_function(ctx, 'zoom-webhook')
    __update_function(ctx, 'zoom-webhook')


@task(_load_env)
def update_downloader(ctx):
    __package_function(ctx, 'zoom-downloader')
    __update_function(ctx, 'zoom-downloader')


@task(_load_env)
def create(ctx):
    __create_or_update(ctx, "create")


@task(_load_env)
def update(ctx):
    __create_or_update(ctx, "update")


@task(_load_env)
def delete(ctx):

    cmd = ("aws {} cloudformation delete-stack "
           "--stack-name {}").format(profile_arg(), ENV['STACK_NAME'])
    if input('are you sure? [y/N] ').lower().strip().startswith('y'):
        ctx.run(cmd)
    else:
        print("not deleting stack")


def profile_arg():
    if ENV['AWS_PROFILE'] is not None:
        return "--profile {}".format(ENV['AWS_PROFILE'])
    return ""

def stack_tags():
    if ENV['STACK_TAGS'] is not None:
        return "--tags {}".format(ENV['STACK_TAGS'])
    return ""

def __create_or_update(ctx, op):
    template_path = join(dirname(__file__), 'template.yml')

    lambda_objects = {}

    for func in ['zoom-webhook', 'zoom-downloader']:
        zip_path = join(dirname(__file__), 'functions', func + '.zip')
        if not exists(zip_path):
            print("No zip found for {}! Did you run the package-* commands?".format(func))
            raise Exit(1)
        lambda_objects[func] = '/'.join([ENV['LAMBDA_CODE_BUCKET'], func + '.zip'])

    cmd = ("aws {} cloudformation {}-stack {} "
           "--capabilities CAPABILITY_NAMED_IAM --stack-name {} "
           "--template-body file://{} "
           "--parameters "
           "ParameterKey=WebhookLambdaCode,ParameterValue={} "
           "ParameterKey=ZoomDownloaderLambdaCode,ParameterValue={} "
           "ParameterKey=NotificationEmail,ParameterValue='{}' "
           "ParameterKey=ZoomApiKey,ParameterValue='{}' "
           "ParameterKey=ZoomApiSecret,ParameterValue='{}' "
           "ParameterKey=ZoomLoginUser,ParameterValue='{}' "
           "ParameterKey=ZoomLoginPassword,ParameterValue='{}' "
           ).format(
                profile_arg(),
                op,
                stack_tags(),
                ENV['STACK_NAME'],
                template_path,
                lambda_objects['zoom-webhook'],
                lambda_objects['zoom-downloader'],
                ENV['NOTIFICATION_EMAIL'],
                ENV['ZOOM_API_KEY'],
                ENV['ZOOM_API_SECRET'],
                ENV['ZOOM_LOGIN_USER'],
                ENV['ZOOM_LOGIN_PASSWORD'],
                )
    print(cmd)
    ctx.run(cmd)


def __package_function(ctx, func):
    req_file = join(dirname(__file__), 'functions/{}.txt'.format(func))
    build_path = join(dirname(__file__), 'dist/{}'.format(func))
    zip_path = join(dirname(__file__), 'functions/{}.zip'.format(func))
    function_path = join(dirname(__file__), 'functions/{}.py'.format(func))
    function_dist_path = join(build_path, '{}.py'.format(func))
    ctx.run("pip install -U -r {} -t {}".format(req_file, build_path))
    try:
        symlink(function_path, function_dist_path)
    except FileExistsError:
        pass
    with ctx.cd(build_path):
        ctx.run("zip -r {} .".format(zip_path))


def __function_to_s3(ctx, func):
    zip_path = join(dirname(__file__), 'functions/{}.zip'.format(func))
    ctx.run("aws {} s3 cp {} s3://{}".format(profile_arg(), zip_path, ENV['LAMBDA_CODE_BUCKET']))


def __update_function(ctx, func):
    zip_path = join(dirname(__file__), 'functions/{}.zip'.format(func))
    lambda_function_name = "{}-{}-function".format(ENV['STACK_NAME'], func)
    cmd = ("aws {} lambda update-function-code "
           "--function-name {} --zip-file fileb://{} --publish"
           ).format(profile_arg(), lambda_function_name, zip_path)
    print(cmd)
    ctx.run(cmd)

