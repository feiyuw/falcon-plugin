#!/usr/bin/env python
import json
import time
import socket
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError


ENDPOINT = socket.getfqdn()
STEP = 60
TYPE_GAUGE = 'GAUGE'
TYPE_COUNTER = 'COUNTER'
COUNTER_METRICS = [
        'asserts_msg',
        'asserts_regular',
        'asserts_rollovers',
        'asserts_user',
        'asserts_warning',
        'page_faults',
        'connections_totalCreated',
        'locks_Global_acquireCount_ISlock',
        'locks_Global_acquireCount_IXlock',
        'locks_Global_acquireCount_Slock',
        'locks_Global_acquireCount_Xlock',
        'locks_Global_acquireWaitCount_ISlock',
        'locks_Global_acquireWaitCount_IXlock',
        'locks_Global_timeAcquiringMicros_ISlock',
        'locks_Global_timeAcquiringMicros_IXlock',
        'locks_Database_acquireCount_ISlock',
        'locks_Database_acquireCount_IXlock',
        'locks_Database_acquireCount_Slock',
        'locks_Database_acquireCount_Xlock',
        'locks_Collection_acquireCount_ISlock',
        'locks_Collection_acquireCount_IXlock',
        'locks_Collection_acquireCount_Xlock',
        'opcounters_command',
        'opcounters_insert',
        'opcounters_delete',
        'opcounters_update',
        'opcounters_query',
        'opcounters_getmore',
        'opcountersRepl_command',
        'opcountersRepl_insert',
        'opcountersRepl_delete',
        'opcountersRepl_update',
        'opcountersRepl_query',
        'opcountersRepl_getmore',
        'network_bytesIn',
        'network_bytesOut',
        'network_physicalBytesIn',
        'network_physicalBytesOut',
        'network_numRequests',
        'backgroundFlushing_flushes',
        'backgroundFlushing_last_ms',
        'commands_insert_failed',
        'commands_insert_total',
        'commands_find_failed',
        'commands_find_total',
        'commands_findAndModify_failed',
        'commands_findAndModify_total',
        'commands_update_failed',
        'commands_update_total',
        'commands_drop_failed',
        'commands_drop_total',
        'commands_distinct_failed',
        'commands_distinct_total',
        'commands_delete_failed',
        'commands_delete_total',
        'commands_count_failed',
        'commands_count_total',
        'commands_aggregate_failed',
        'commands_aggregate_total',
        'commands_mapReduce_failed',
        'commands_mapReduce_total',
        'commands_getnonce_failed',
        'commands_getnonce_total',
        'commands_getMore_failed',
        'commands_getMore_total',
        'document_deleted',
        'document_inserted',
        'document_returned',
        'document_updated',
        'operation_scanAndOrder',
        'operation_writeConflicts',
        'cursor_timedOut',
        'wt_cache_readinto_bytes',
        'wt_cache_writtenfrom_bytes',
        'wt_bm_bytes_read',
        'wt_bm_bytes_written',
        'wt_bm_blocks_read',
        'wt_bm_blocks_written']


class MongoMonitor(object):
    def __init__(self, host='mongodb://127.0.0.1:27017'):
        self.db = MongoClient('mongodb://127.0.0.1:27017').admin


    def run(self):
        data = []
        try:
            ts = int(time.time())
            server_status = self.db.command('serverStatus')
            data.append(self._build_metric('mongo_local_alive', 1, ts))
            for (metric, value) in self._parse_server_status(server_status):
                data.append(self._build_metric(metric, value, ts))
        except ServerSelectionTimeoutError:
            data.append(self._build_metric('mongo_local_alive', 0, ts))

        print(json.dumps(data))


    def _parse_server_status(self, server_status):
        yield ('mongo_uptime', int(server_status['uptime']))

        yield ('page_faults', server_status['extra_info']['page_faults'])

        # asserts
        for k, v in server_status['asserts'].iteritems():
            yield ('asserts_'+k, v)

        # connections
        conn_current = server_status['connections']['current']
        conn_available = server_status['connections']['available']
        yield ('connections_current', conn_current)
        yield ('connections_available', conn_available)
        yield ('connections_used_percent', int(conn_current*100/(conn_current+conn_available)))
        yield ('connections_totalCreated', server_status['connections']['totalCreated'])

        #  'globalLock' currentQueue
        yield ('globalLock_currentQueue_total', server_status['globalLock']['currentQueue']['total'])
        yield ('globalLock_currentQueue_readers', server_status['globalLock']['currentQueue']['readers'])
        yield ('globalLock_currentQueue_writers', server_status['globalLock']['currentQueue']['writers'])

        # locks
        lock_type_name = {'R': 'Slock', 'W': 'Xlock', 'r': 'ISlock', 'w': 'IXlock'}
        for lock_scope, v in server_status['locks'].iteritems():
            for lock_metric, tv in v.iteritems():
                for lock_type, mv in tv.iteritems():
                    yield ('locks_'+lock_scope+'_'+lock_metric+'_'+lock_type_name[lock_type], mv)

        # network, opcounters, opcountersRepl
        for section in ('network', 'opcounters', 'opcountersRepl'):
            for k, v in server_status[section].iteritems():
                yield (section+'_'+k, v)

        # mem
        for k, v in server_status['mem'].iteritems():
            if k not in ('bits', 'supported'):
                yield ('mem_'+k, v)

        # dur
        if 'dur' in server_status:
            yield ('dur_journaledMB', server_status['dur']['journaledMB'])
            yield ('dur_writeToDataFilesMB', server_status['dur']['writeToDataFilesMB'])
            yield ('dur_commitsInWriteLock', server_status['dur']['commitsInWriteLock'])

        # metrics
        # commands
        for cmd in ('insert', 'find', 'findAndModify', 'update', 'drop', 'distinct', 'delete', 'count', 'aggregate', 'mapReduce', 'getnonce', 'getMore'):
            for field in ('failed', 'total'):
                yield ('commands_'+cmd+'_'+field, server_status['metrics']['commands'][cmd][field])
        # document
        for k, v in server_status['metrics']['document'].iteritems():
            yield ('document_'+k, v)
        # operation
        for k, v in server_status['metrics']['operation'].iteritems():
            yield ('operation_'+k, v)
        # cursor
        yield ('cursor_timedOut', server_status['metrics']['cursor']['timedOut'])
        for k, v in server_status['metrics']['cursor']['open'].iteritems():
            yield ('cursor_open_'+k, v)

        # wiredTiger
        if 'wiredTiger' in server_status:
            serverStatus_wt = server_status['wiredTiger']
            #cache
            wt_cache = serverStatus_wt['cache']
            yield ('wt_cache_used_total_bytes', wt_cache['bytes currently in the cache'])
            yield ('wt_cache_dirty_bytes', wt_cache['tracked dirty bytes in the cache'])
            yield ('wt_cache_readinto_bytes', wt_cache['bytes read into cache'])
            yield ('wt_cache_writtenfrom_bytes', wt_cache['bytes written from cache'])

            #concurrentTransactions
            wt_concurrentTransactions = serverStatus_wt['concurrentTransactions']
            yield ('wt_concurrentTransactions_write', wt_concurrentTransactions['write']['available'])
            yield ('wt_concurrentTransactions_read', wt_concurrentTransactions['read']['available'])

            #'block-manager' section
            wt_block_manager = serverStatus_wt['block-manager']
            yield ('wt_bm_bytes_read', wt_block_manager['bytes read'])
            yield ('wt_bm_bytes_written', wt_block_manager['bytes written'])
            yield ('wt_bm_blocks_read', wt_block_manager['blocks read'])
            yield ('wt_bm_blocks_written', wt_block_manager['blocks written'])


    def _build_metric(self, metric, value, ts, tags=''):
        if metric in COUNTER_METRICS:
            counter_type = TYPE_COUNTER
        else:
            counter_type = TYPE_GAUGE

        return {
                'metric': metric,
                'endpoint': ENDPOINT,
                'timestamp': ts,
                'step': STEP,
                'value': value,
                'counterType': counter_type,
                'tags': tags
                }


MongoMonitor().run()

