import site
from os.path import dirname, join
site.addsitedir(join(dirname(dirname(__file__)), 'functions'))

# from importlib import import_module
#
# uploader = import_module('zoom-uploader')
#
#
# def test_uploader_handler(handler):
#     pass
