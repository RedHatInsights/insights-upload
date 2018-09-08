#!/usr/bin/env python3

from os import getenv
from subprocess import check_call

TOPICS = ["advisor",
          "available",
          "testareno",
          "uploadvalidation"]

zookeeper = getenv("ZOOKEEPER", "zookeeper:32181")

for topic in TOPICS:
    check_call(["kafka-topics",
                "--create",
                "--if-not-exists",
                "--topic={}".format(topic),
                "--partitions=1",
                "--replication-factor=1",
                "--zookeeper={}".format(zookeeper)])
