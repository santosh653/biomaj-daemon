import os
import logging

import requests
import yaml

from biomaj_daemon.daemon.daemon_service import DaemonService
from biomaj_core.utils import Utils

config_file = 'config.yml'
if 'BIOMAJ_CONFIG' in os.environ:
        config_file = os.environ['BIOMAJ_CONFIG']

config = None
with open(config_file, 'r') as ymlfile:
    config = yaml.load(ymlfile)
    Utils.service_config_override(config)


def on_executed(bank, procs):
    metrics = []
    if not procs:
        metric = {'bank': bank, 'error': 1}
        metrics.append(metrics)
    else:
        for proc in procs:
            metric = {'bank': bank}
            if 'action' in proc:
                metric['action'] = proc['action']
            else:
                metric['action'] = 'none'
            if 'updated' in proc:
                metric['updated'] = 1 if proc['updated'] else 0
            else:
                metric['updated'] = 0
            if 'error' in proc and proc['error']:
                metric['error'] = 1
            else:
                metric['execution_time'] = proc['execution_time']
            metrics.append(metric)
        try:
            requests.post(config['web']['local_endpoint'] + '/api/daemon/metrics', json=metrics)
        except Exception as e:
            logging.error('Failed to post metrics: ' + str(e))


process = DaemonService(config_file)
process.on_executed_callback(on_executed)
process.supervise()
process.wait_for_messages()
