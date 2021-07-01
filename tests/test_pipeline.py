import site
from os.path import dirname, join
from importlib import import_module

site.addsitedir(join(dirname(dirname(__file__)), "functions"))

webhook = import_module("zoom-webhook")
downloader = import_module("zoom-downloader")
uploader = import_module("zoom-uploader")


def test_status():
    pass
