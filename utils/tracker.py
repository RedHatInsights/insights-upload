import datetime

from utils import config


def payload_tracker(request_id, account, status, status_msg):

    payload_msg = {"topic": config.TRACKER_TOPIC}

    payload_status = {
        'service': config.APP_NAME,
        'status': status,
        'status_msg': status_msg,
        "account": account,
        "request_id": request_id,
        "payload_id": request_id,
    }

    payload_status["date"] = str(datetime.datetime.now())

    payload_msg["msg"] = payload_status

    return payload_msg
