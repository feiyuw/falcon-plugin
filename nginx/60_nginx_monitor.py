#!/usr/bin/env python
# -*- coding: utf-8 -*-
''' monitor nginx status based on https://github.com/GuyCheung/falcon-ngx_metric'''
import re
import json
import time
import urllib
import socket
import itertools


BASIC_STATUS_URI = 'http://127.0.0.1:9091/monitor/basic_status'
NGINX_STATUS_URI = 'http://127.0.0.1:9091/monitor/nginx_status'
ENDPOINT = socket.getfqdn()
STEP = 60
TYPE_GAUGE = 'GAUGE'
TYPE_COUNTER = 'COUNTER'
EXCLUDE_APIS = ('/monitor/basic_status', '/monitor/nginx_status')


INT_PTN = re.compile(r'\d+')


class NginxMonitor(object):
    def run(self):
        data = []
        for item in self._handle_nginx_status():
            data.append(item)
        for item in self._handle_basic_status():
            data.append(item)

        print(json.dumps(data))

    def _handle_nginx_status(self):
        metrics = []
        timestamp = int(time.time())
        try:
            output = urllib.urlopen(NGINX_STATUS_URI).read()
        except IOError:
            return metrics

        for line in output.splitlines():
            if line.startswith('Active connections'):
                metrics.append({
                    'metric': 'connections.open',
                    'endpoint': ENDPOINT,
                    'timestamp': timestamp,
                    'step': STEP,
                    'value': int(line.split(':')[1].strip()),
                    'counterType': TYPE_GAUGE,
                    'tags': ''})
            elif line.startswith(' '):
                conn_accepted, conn_handled, req_handled = map(int, line.strip().split())
                metrics.extend([{
                    'metric': metric,
                    'endpoint': ENDPOINT,
                    'timestamp': timestamp,
                    'step': STEP,
                    'value': value,
                    'counterType': TYPE_COUNTER,
                    'tags': ''
                    } for (metric, value) in [
                        ('connections.accepted', conn_accepted),
                        ('connections.handled', conn_handled),
                        ('requests.handled', req_handled)]])
            elif line.startswith('Reading'):
                conn_reading, conn_writing, conn_waiting = map(int, INT_PTN.findall(line))
                metrics.extend([{
                    'metric': metric,
                    'endpoint': ENDPOINT,
                    'timestamp': timestamp,
                    'step': STEP,
                    'value': value,
                    'counterType': TYPE_GAUGE,
                    'tags': ''
                    } for (metric, value) in [
                        ('connections.reading', conn_reading),
                        ('connections.writing', conn_writing),
                        ('connections.waiting', conn_waiting)]])

        return metrics

    def _handle_basic_status(self):
        metrics = []
        total_query_count = 0
        total_err_count = 0
        timestamp = int(time.time())

        try:
            output = urllib.urlopen(BASIC_STATUS_URI).read()
        except IOError:
            return metrics

        f_metric = lambda line: line.split('|')[0]
        metric_groups = itertools.groupby(sorted(output.splitlines(), key=f_metric), f_metric)
        f_err_tags = lambda api_uri, err_code: 'api=%s,errcode=%s' % (api_uri, err_code)
        f_other_tags = lambda api_uri: 'api=%s' % api_uri

        for metric, grp in metric_groups:
            for line in grp:
                if metric == 'err_count':
                    _, _, api_uri, err_code, value = line.split('|')
                else:
                    _, _, api_uri, value = line.split('|')
                metrics.append({
                    'metric': metric=='err_count' and 'error_count' or metric,
                    'endpoint': ENDPOINT,
                    'timestamp': timestamp,
                    'step': STEP,
                    'value': float(value),
                    'counterType': TYPE_GAUGE,
                    'tags': metric=='err_count' and \
                            f_err_tags(api_uri, err_code) or \
                            f_other_tags(api_uri)})
                if metric == 'query_count':
                    total_query_count += int(value)
                elif metric == 'err_count':
                    total_err_count += int(value)
        metrics.append({
                'metric': 'query_count',
                'endpoint': ENDPOINT,
                'timestamp': timestamp,
                'step': STEP,
                'value': total_query_count,
                'counterType': TYPE_GAUGE,
                'tags': 'api=__serv__'})
        metrics.append({
                'metric': 'error_count',
                'endpoint': ENDPOINT,
                'timestamp': timestamp,
                'step': STEP,
                'value': total_err_count,
                'counterType': TYPE_GAUGE,
                'tags': 'api=__serv__'})
        metrics.append({
                'metric': 'error_rate',
                'endpoint': ENDPOINT,
                'timestamp': timestamp,
                'step': STEP,
                'value': total_query_count+total_err_count>0 and float(total_err_count)/(total_query_count+total_err_count) or 0,
                'counterType': TYPE_GAUGE,
                'tags': 'api=__serv__'})

        return metrics

NginxMonitor().run()

