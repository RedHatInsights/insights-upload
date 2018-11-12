class StopLoopException(Exception):
    """Used to stop iteration in coroutines during tests"""
    pass


class MockMessage(object):
    """Mocks an aiokafka message."""
    def __init__(self, value):
        self.value = value


class MockTopicPartition(object):
    """Mocks aiokafka 'topic-partition' key.

    AIOkafka's consumer getmany() method returns a dict with 'TopicPartition'
    as the key and a list of messages as the value
    """
    def __init__(self, topic):
        self.topic = topic
