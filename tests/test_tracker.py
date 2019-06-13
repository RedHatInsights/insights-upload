from utils import tracker

from unittest import TestCase
from freezegun import freeze_time


class TestTrackerMessage(TestCase):

    @freeze_time("2019-06-13- 09:30:54.233478")
    def test_payload_tracker(self):

        topic = "platform.payload-status"
        good_msg = {"service": "ingress",
                    "request_id": "dootdoot",
                    "payload_id": "dootdoot",
                    "status": "test",
                    "status_msg": "unittest",
                    "account": "123456",
                    "date": tracker.get_time()}

        self.assertEqual({"topic": topic, "msg": good_msg}, tracker.payload_tracker("dootdoot", "123456", "test", "unittest"))
