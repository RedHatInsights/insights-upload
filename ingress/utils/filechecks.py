import re
import logging

import config
import log
import metrics

content_regex = r"^application/vnd\.redhat\.(?P<service>[a-z0-9-]+)\.(?P<category>[a-z0-9-]+).*"

logger = logging.getLogger("ingress")

def get_service(content_type):
    """
    Returns the service that content_type maps to.
    """
    if content_type in config.SERVICE_MAP:
        return config.SERVICE_MAP[content_type]
    else:
        m = re.search(content_regex, content_type)
        if m:
            return m.groupdict()
    raise Exception("Could not resolve a service from the given content_type")
