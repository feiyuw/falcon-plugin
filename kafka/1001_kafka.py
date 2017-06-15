#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import time
import socket
from kafka.client import KafkaClient


ENDPOINT = socket.getfqdn()
SERVER = '%s:9092' % ENDPOINT
STEP = 60
TYPE_GAUGE = 'GAUGE'


class KafkaMetrics(object):
    def __init__(self):
        self.client = KafkaClient(bootstrap_servers=SERVER)
        self.cluster = self.client.cluster

    def run(self):
        data = []
        data.append(self.get_brokers_total())
        data.append(self.get_topics_total())

        print json.dumps(data)

    def get_brokers_total(self):
        metric = 'kafka.brokers.total'

        return self._build_metric(metric,
                len(self.cluster.brokers()))

    def get_topics_total(self):
        metric = 'kafka.topics.total'

        return self._build_metric(metric,
                len(filter(lambda t: t!='__consumer_offsets',
                    self.cluster.topics())))

    def _build_metric(self, metric, value, counter_type=TYPE_GAUGE, tags=''):
        return {
                'metric': metric,
                'endpoint': ENDPOINT,
                'timestamp': int(time.time()),
                'step': STEP,
                'value': value,
                'counterType': counter_type,
                'tags': tags
                }


KafkaMetrics().run()
