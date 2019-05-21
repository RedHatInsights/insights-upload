from importlib import import_module

import config

storage = import_module("storage.{}".format(config.STORAGE_DRIVER))
