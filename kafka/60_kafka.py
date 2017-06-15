#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import time
import socket
from kafka.client import KafkaClient


ENDPOINT = socket.getfqdn()
# ENDPOINT = 'kafka-1.prismcdn.internal'
SERVER = '%s:9092' % ENDPOINT
STEP = 60
TYPE_GAUGE = 'GAUGE'


class KafkaMetrics(object):
    def __init__(self):
        self.client = KafkaClient(bootstrap_servers=SERVER)
        self.cluster = self.client.cluster

    def run(self):
        data = []
        for collector in [getattr(self, f) for f in dir(self) if f.startswith('get_')]:
            items = collector()
            if isinstance(items, dict):
                data.append(items)
            else:
                for item in items:
                    data.append(item)

        print json.dumps(data)

    def get_brokers_total(self):
        metric = 'kafka.brokers.total'

        return self._build_metric(metric, len(self.cluster.brokers()))

    def get_topics_total(self):
        metric = 'kafka.topics.total'

        return self._build_metric(metric, len(self.cluster.topics()))

    def get_partitions_for_topic(self):
        metric = 'kafka.partitions.count'
        for topic in self.cluster.topics():
            if topic in ('__consumer_offsets', ):
                continue
            yield self._build_metric(metric,
                    len(self.cluster.partitions_for_topic(topic)),
                    tags='topic='+topic)

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
