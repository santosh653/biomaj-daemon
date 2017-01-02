import ssl
import os
import yaml
import logging

from flask import Flask
from flask import jsonify
from flask import request
from flask import abort

import requests

from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client.exposition import generate_latest
from prometheus_client import multiprocess
from prometheus_client import CollectorRegistry

import consul
import redis
from pymongo import MongoClient
import pika

from biomaj.schema_version import SchemaVersion

from biomaj.options import Options as BmajOptions
from biomaj_core.config import BiomajConfig
from biomaj_core.utils import Utils

from biomaj_daemon.daemon.utils import biomaj_client_action

config_file = 'config.yml'
if 'BIOMAJ_CONFIG' in os.environ:
        config_file = os.environ['BIOMAJ_CONFIG']

config = None
with open(config_file, 'r') as ymlfile:
    config = yaml.load(ymlfile)
    Utils.service_config_override(config)

BiomajConfig.load_config(config['biomaj']['config'])

last_status_check = None
last_status = None

data_dir = BiomajConfig.global_config.get('GENERAL', 'data.dir')
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
log_dir = BiomajConfig.global_config.get('GENERAL', 'log.dir')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
process_dir = BiomajConfig.global_config.get('GENERAL', 'process.dir')
if not os.path.exists(process_dir):
    os.makedirs(process_dir)
cache_dir = BiomajConfig.global_config.get('GENERAL', 'cache.dir')
if not os.path.exists(cache_dir):
    os.makedirs(cache_dir)
lock_dir = BiomajConfig.global_config.get('GENERAL', 'lock.dir')
if not os.path.exists(lock_dir):
    os.makedirs(lock_dir)

redis_client = redis.StrictRedis(
    host=config['redis']['host'],
    port=config['redis']['port'],
    db=config['redis']['db'],
    decode_responses=True
)

logging.info("Check database schema and upgrade if necessary")
SchemaVersion.migrate_pendings()

app = Flask(__name__)

biomaj_metric = Counter("biomaj_daemon_total", "Bank total update execution.", ['bank', 'action', 'updated'])
biomaj_error_metric = Counter("biomaj_daemon_errors", "Bank total update errors.", ['bank', 'action'])
biomaj_time_metric = Gauge("biomaj_daemon_time", "Bank update execution time in seconds.", ['bank', 'action', 'updated'], multiprocess_mode='all')


def consul_declare(config):
    if config['consul']['host']:
        consul_agent = consul.Consul(host=config['consul']['host'])
        consul_agent.agent.service.register('biomaj-daemon', service_id=config['consul']['id'], address=config['web']['hostname'], port=config['web']['port'], tags=['biomaj'])
        check = consul.Check.http(url='http://' + config['web']['hostname'] + ':' + str(config['web']['port']) + '/api/daemon', interval=20)
        consul_agent.agent.check.register(config['consul']['id'] + '_check', check=check, service_id=config['consul']['id'])


consul_declare(config)

OPTIONS_PARAMS = {
    'config': None,
    'check': False,
    'update': False,
    'fromscratch': False,
    'publish': False,
    'unpublish': False,
    'release': None,
    'from_task': None,
    'process': None,
    'log': None,
    'remove': False,
    'removeall': False,
    'removepending': False,
    'status': False,
    'bank': None,
    'owner': None,
    'stop_before': None,
    'stop_after': None,
    'freeze': False,
    'unfreeze': False,
    'force': False,
    'help': False,
    'search': False,
    'formats': None,
    'types': None,
    'query': None,
    'show': False,
    'newbank': None,
    'newdir': None,
    'visibility': None,
    'maintenance': None,
    'version': False,
    'statusko': False,
    'trace': False,
    'whatsup': False
}


class Options(object):
    def __init__(self, d):
        self.__dict__ = d
        for key in list(OPTIONS_PARAMS.keys()):
            if not self.has_option(key):
                setattr(self, key, OPTIONS_PARAMS[key])

    def has_option(self, option):
        if hasattr(self, option):
            return True
        else:
            return False

    def get_option(self, option):
        """
        Gets an option if present, else return None
        """
        if hasattr(self, option):
            return getattr(self, option)
        return None


@app.route('/api/daemon', methods=['GET'])
def ping():
    return jsonify({'msg': 'pong'})


@app.route('/api/daemon/status', methods=['GET'])
def biomaj_status_info():
    '''
    if last_status_check is not None:
        cur = datetime.datetime.now()
        if (cur - last_status_check).total_seconds() < 10:
            return status
    last_status_check = datetime.datetime.now()
    '''
    status = {
        'status': [{'service': 'biomaj-public-proxy', 'status': 1, 'count': 1}],
        'biomaj_services': [],
        'general_services': [{
            'id': 'biomaj-public-proxy',
            'host': '',
            'status': True
        }]
    }

    # Check redis
    logging.debug("Status: check redis")
    redis_ok = False
    try:
        pong = redis_client.ping()
        if pong:
            redis_ok = True
    except Exception as e:
        logging.error('Failed to ping redis: ' + str(e))
    if not redis_ok:
        status['status'].append({'service': 'biomaj-redis', 'status': -1, 'count': 0})
    else:
        status['status'].append({'service': 'biomaj-redis', 'status': 1, 'count': 1})
        status['general_services'].append({
            'id': 'biomaj-redis',
            'host': config['redis']['host'],
            'status': True
        })

    # Check internal proxy
    r = requests.get(config['web']['local_endpoint'] + '/api/user')
    if not r.status_code == 200:
        status['status'].append({'service': 'biomaj-internal-proxy', 'status': -1, 'count': 0})
    else:
        status['status'].append({'service': 'biomaj-internal-proxy', 'status': 1, 'count': 1})
        status['general_services'].append({
            'id': 'biomaj-internal-proxy',
            'host': config['web']['local_endpoint'],
            'status': True
        })

    # Check mongo
    logging.debug("Status: check mongo")
    if 'mongo' in config:
        mongo_ok = False
        try:
            biomaj_mongo = MongoClient(config['mongo']['url'])
            biomaj_mongo.server_info()
            mongo_ok = True
        except Exception as e:
            logging.error('Failed to connect to mongo')
        if not mongo_ok:
            status['status'].append({'service': 'biomaj-mongo', 'status': -1, 'count': 0})
        else:
            status['status'].append({'service': 'biomaj-mongo', 'status': 1, 'count': 1})
            status['general_services'].append({
                'id': 'biomaj-mongo',
                'host': config['mongo']['url'],
                'status': True
            })

    # Check rabbitmq
    logging.debug("Status: check rabbit")
    if 'rabbitmq' in config and 'host' in config['rabbitmq'] and config['rabbitmq']['host']:
        rabbit_ok = False
        channel = None
        try:
            connection = None
            rabbitmq_port = config['rabbitmq']['port']
            rabbitmq_user = config['rabbitmq']['user']
            rabbitmq_password = config['rabbitmq']['password']
            rabbitmq_vhost = config['rabbitmq']['virtual_host']
            if rabbitmq_user:
                credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
                connection = pika.BlockingConnection(pika.ConnectionParameters(config['rabbitmq']['host'], rabbitmq_port, rabbitmq_vhost, credentials))
            else:
                connection = pika.BlockingConnection(pika.ConnectionParameters(config['rabbitmq']['host']))
            channel = connection.channel()
            rabbit_ok = True
        except Exception as e:
            logging.error('Rabbitmq connection error: ' + str(e))
        finally:
            if channel:
                channel.close()
        if not rabbit_ok:
            status['status'].append({'service': 'biomaj-rabbitmq', 'status': -1, 'count': 0})
        else:
            status['status'].append({'service': 'biomaj-rabbitmq', 'status': 1, 'count': 1})
            status['general_services'].append({
                'id': 'biomaj-rabbitmq',
                'host': config['rabbitmq']['host'],
                'status': True
            })

    logging.debug("Status: check consul services")
    r = requests.get('http://' + config['consul']['host'] + ':8500/v1/agent/services')
    if not r.status_code == 200:
        status['status'].append({'service': 'biomaj-consul', 'status': -1, 'count': 0})
        last_status = status
        return status

    status['status'].append({'service': 'biomaj-consul', 'status': 1, 'count': 1})
    status['general_services'].append({
        'id': 'biomaj-consul',
        'host': config['consul']['host'],
        'status': True
    })

    consul_services = r.json()
    services = []
    for consul_service in list(consul_services.keys()):
        current_service = consul_services[consul_service]['Service']
        if current_service in services or current_service == 'consul':
            continue
        else:
            services.append(current_service)
        check_r = requests.get('http://' + config['consul']['host'] + ':8500/v1/health/service/' + consul_services[consul_service]['Service'])
        if not check_r.status_code == 200:
            status['status'].append({'service': 'biomaj-consul', 'status': -1, 'count': 0})
            last_status = status
            return status
        checks = check_r.json()
        nb_service = 0
        nb_service_ok = 0
        for service_check in checks:
            nb_service += 1
            check_status = True
            for check in service_check['Checks']:
                if check['Status'] != 'passing':
                    check_status = False
                    break
            if check_status:
                nb_service_ok += 1
            status['biomaj_services'].append({
                'id': service_check['Service']['Service'],
                'host': service_check['Service']['Address'],
                'status': check_status
            })
        check_status = -1
        if nb_service == nb_service_ok:
            check_status = 1
        elif nb_service_ok == 0:
            check_status = -1
        else:
            check_status = 0
        status['status'].append({'service': consul_services[consul_service]['Service'], 'count': nb_service, 'status': check_status})

    # Check missing services
    biomaj_services = ['biomaj-watcher', 'biomaj-daemon', 'biomaj-download', 'biomaj-process', 'biomaj-user', 'biomaj-cron', 'biomaj-ftp']
    for biomaj_service in biomaj_services:
        if biomaj_service not in services:
            status['status'].append({'service': biomaj_service, 'count': 0, 'status': -1})

    last_status = status

    return jsonify(status)


@app.route('/api/daemon', methods=['POST'])
def biomaj_daemon():
    '''
    Execute a command request (bank update, removal, ...)
    '''
    apikey = request.headers.get('Authorization')
    token = None

    if apikey:
        bearer = apikey.split()
        if bearer[0] == 'APIKEY':
            token = bearer[1]
    try:
        params = request.get_json()
        options = params['options']
        options_object = Options(options)
        options_object.token = token
        options_object.user = None
        options_object.redis_host = config['redis']['host']
        options_object.redis_port = config['redis']['port']
        options_object.redis_db = config['redis']['db']
        options_object.redis_prefix = config['redis']['prefix']

        user = None
        if token:
            r = requests.get(config['web']['local_endpoint'] + '/api/user/info/apikey/' + token)
            if not r.status_code == 200:
                abort(404, {'message': 'Invalid API Key or connection issue'})
            user = r.json()['user']
            if user:
                options_object.user = user['id']

        if options_object.bank:
            bmaj_options = BmajOptions(options_object)
            BiomajConfig(options_object.bank, bmaj_options)

            if not options_object.search and not options_object.show and not options_object.check and not options_object.status:
                if not user:
                    abort(403, {'message': 'This action requires authentication with api key'})

        (res, msg) = biomaj_client_action(options_object, config)
    except Exception as e:
        logging.exception(e)
        return jsonify({'status': False, 'msg': str(e)})
    return jsonify({'status': res, 'msg': msg})


@app.route('/metrics', methods=['GET'])
def metrics():
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    return generate_latest(registry)


@app.route('/api/daemon/metrics', methods=['POST'])
def add_metrics():
    '''
    Expects a JSON request with an array of {'bank': 'bank_name', 'error': 'error_message', 'execution_time': seconds_to_execute, 'action': 'update|remove', 'updated': 0|1}
    '''
    procs = request.get_json()
    for proc in procs:
        if 'error' in proc and proc['error']:
            biomaj_error_metric.labels(proc['bank'], proc['action']).inc()
        else:
            biomaj_metric.labels(proc['bank'], proc['action'], proc['updated']).inc()
            biomaj_time_metric.labels(proc['bank'], proc['action'], proc['updated']).set(proc['execution_time'])
    return jsonify({'msg': 'OK'})


if __name__ == "__main__":
    context = None
    if config['tls']['cert']:
        context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        context.load_cert_chain(config['tls']['cert'], config['tls']['key'])
    app.run(host='0.0.0.0', port=config['web']['port'], ssl_context=context, threaded=True, debug=config['web']['debug'])
