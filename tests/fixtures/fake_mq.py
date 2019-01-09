from kafka.errors import KafkaError

from typing import Text, Dict, ByteString
from tornado import gen

from tests.fixtures import MockMessage, MockTopicPartition, StopLoopException


class FakeMQ(object):
    """
    :arg connection_failing_attempt_countdown: how many times you want to try to "connect" to the broker
                                                until gets positive return, e.g: if set to 4, NoBrokersError will be
                                                raised for 4 times and then, in the 5th attempt, you'll get "True"
                                                as return of connect() method

    :arg disconnect_in_operation: force to "disconnect" in operation <number>, e.g: if set as 2, NoBrokersError will be
                                                raised in the 2nd operation, i.e in the second time you try to consume
                                                some topic

    :arg stop_iteration_countdown: how many times you'll try to run some operation that doesn't return/do anything, such
                                                consume an empty queue or if you are stuck in a "while True"

    :arg consume_return_messages_count: counts how many times a consumer was able to get some messages
    """

    _topics = None
    connection_failing_attempt_countdown = 0
    disconnect_in_operation = 0
    stop_iteration_countdown = 5

    consume_calls_count = 0
    produce_calls_count = 0
    disconnect_in_operation_called = False
    trying_to_connect_failures_calls = 0
    consume_return_messages_count = 0

    is_down = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def __init__(self, *args, **kwargs):
        for k, v in kwargs.items():
            if not hasattr(self, k):
                raise Exception("Bad parameter {} - FakeMQ".format(k))
            setattr(self, k, v)
        self._topics = {}

    def _raise_if_need_to(self, calls: int = 0, avoid_iteration_control=False):
        if not avoid_iteration_control:
            if self.disconnect_in_operation > 0 and self.disconnect_in_operation == (calls - 1):
                self.disconnect_in_operation = 0
                self.disconnect_in_operation_called = True
                raise KafkaError("Failed to connect to the FakeMQ")

            if self.stop_iteration_countdown <= 0:
                raise StopLoopException('Stopping the iteration')

            self.stop_iteration_countdown -= 1

    @gen.coroutine
    def start(self):
        if self.connection_failing_attempt_countdown <= 0 and not self.is_down:
            return True

        self.connection_failing_attempt_countdown -= 1
        self.trying_to_connect_failures_calls += 1
        raise KafkaError("Failed to connect to the FakeMQ")

    @gen.coroutine
    def send_and_wait(self, topic: Text, message: ByteString, avoid_iteration_control=False):
        self._raise_if_need_to(self.produce_calls_count, avoid_iteration_control)

        if not topic or not message:
            raise Exception("You must provide a topic and a message")

        self._topics.setdefault(topic, [])
        self._topics[topic].append(MockMessage(message))
        self.produce_calls_count += 1
        return True

    @gen.coroutine
    def getmany(self, avoid_iteration_control=False, timeout_ms=None, max_records=None) -> Dict:
        # At the moment, this is the only topic we consume from ...
        topic = "platform.upload.validation"

        self._raise_if_need_to(self.consume_calls_count, avoid_iteration_control)
        self.consume_calls_count += 1

        if not self._topics.get(topic):
            return {}

        msgs, self._topics[topic] = self._topics[topic], []

        if msgs:
            self.consume_return_messages_count += 1

        return {MockTopicPartition(topic): msgs}

    def count_topic_messages(self, topic):
        return len(self._topics.get(topic, []))
