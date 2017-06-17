#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import re
import json
import time
import socket


ENDPOINT = socket.getfqdn()
STEP = 60
TYPE_GAUGE = 'GAUGE'

_PID_PTN = re.compile(r'^\d+$')

class ProcessMonitor(object):
    def run(self):
        data = []
        for item in self.get_fd():
            data.append(item)

        print(json.dumps(data))

    def get_fd(self):
        metric = 'process.fd'

        groups = {}

        for pid in os.listdir('/proc'):
            if not _PID_PTN.match(pid) or not os.path.exists('/proc/%s/status' % pid):
                continue
            proc_name, fd_size = None, None
            with open('/proc/%s/status' % pid) as fp:
                for line in fp:
                    if line.startswith('Name:'):
                        proc_name = line.split(':')[1].strip()
                    if line.startswith('FDSize:'):
                        fd_size = int(line.split(':')[1].strip())
            if proc_name is not None and fd_size is not None:
                groups[proc_name] = groups.get(proc_name, 0) + fd_size

        for proc_name, fd_size in groups.iteritems():
            yield {
                    'metric': metric,
                    'endpoint': ENDPOINT,
                    'timestamp': int(time.time()),
                    'step': STEP,
                    'value': fd_size,
                    'counterType': TYPE_GAUGE,
                    'tags': 'name='+proc_name
                    }


ProcessMonitor().run()

