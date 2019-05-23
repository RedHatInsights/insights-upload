import tempfile
from utils import config


class TestGetValidTopics:
    """
    Test the get_valid_topics method
    """

    def test_with_topics_json(self, monkeypatch):
        """
        Test that get_valid_topics returns the correct application names from
        the topics.json file.
        """
        fp = tempfile.NamedTemporaryFile(delete=False)
        fp.write(b"""
        [{
           'TOPIC_NAME': 'platform.upload.advisor',
           'PARTITIONS': 3,
           'REPLICAS': 3
         },
         {
           'TOPIC_NAME': 'platform.upload.testareno',
           'PARTITIONS': 3,
           'REPLICAS': 3
        }]
        """)
        fp.close()
        monkeypatch.setattr(config, "TOPIC_CONFIG", fp.name)
        assert config.get_valid_topics() == ["advisor", "testareno"]
        fp.close()

    def test_without_topics_json(self, monkeypatch):
        """
        Test that get_valid_topics returns the default set of applications if there is
        no topics.json file
        """
        fp = tempfile.NamedTemporaryFile()
        monkeypatch.setattr(config, "TOPIC_CONFIG", fp.name)
        fp.close()
        assert config.get_valid_topics() == [
            'advisor', 'validation', 'testareno', 'hccm', 'compliance', 'qpc'
        ]
