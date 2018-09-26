import ssl
import os
import yaml
import logging
from collections import deque
import copy

from flask import Flask
from flask import jsonify
from flask import request
from flask import abort
from flask import Response

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
from biomaj.bank import Bank

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
SchemaVersion.set_version()

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
    'check': False,
    'config': None,
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
    'whatsup': False,
    'lastlog': None,
    'tail': 100,
    'stats': False,
    'json': False,
    'updatestatus': False,
    'updatecancel': False,
    'aboutme': False,
    'userlogin': None,
    'userpassword': None,
    'proxy': None,
    'schedule': False,
    'history': False,
    'historyLimit': 20
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
    proxy = Utils.get_service_endpoint(config, 'user')
    r = requests.get(proxy + '/api/user')
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

@app.route('/api/daemon/bank/<bank>/log', methods=['GET'])
def biomaj_bank_log(bank):
    return biomaj_bank_log_tail(bank, 0)

@app.route('/api/daemon/bank/<bank>/log/<tail>', methods=['GET'])
def biomaj_bank_log_tail(bank, tail=100):
    apikey = request.headers.get('Authorization')
    token = None

    if apikey:
        bearer = apikey.split()
        if bearer[0] == 'APIKEY':
            token = bearer[1]
    log_file = None
    try:
        user = None
        options_object = Options(OPTIONS_PARAMS)
        if token:
            proxy = Utils.get_service_endpoint(config, 'user')
            r = requests.get(proxy + '/api/user/info/apikey/' + token)
            if not r.status_code == 200:
                abort(404, {'message': 'Invalid API Key or connection issue'})
            user = r.json()['user']
            options_object = Options({'user': user['id']})

        bank_log = Bank(bank, options=options_object, no_log=True)
        if bank_log.bank['properties']['visibility'] != 'public' and not bank_log.is_owner():
            abort(403, {'message': 'not authorized to access this bank'})
        if 'status' not in bank_log.bank or 'log_file' not in bank_log.bank['status'] or not bank_log.bank['status']['log_file']['status']:
            return "No log file available"
        log_file = bank_log.bank['status']['log_file']['status']
    except Exception as e:
        logging.exception(e)
        return "Failed to access log file: " + str(e)
    if not log_file or not os.path.exists(log_file):
        return "Cannot access log file %s" % (str(log_file))

    def generate():
        with open(log_file) as fp:
            tail_l = int(tail)
            if tail_l == 0:
                for line in fp:
                    yield line
            else:
                dq = deque(fp, maxlen=tail_l)
                for line in dq:
                    yield line
        yield "##END_OF_LOG"

    return Response(generate(), mimetype='text/plain')



def daemon_api_auth(request):
    apikey = request.headers.get('Authorization')
    token = None
    options_object = None
    if apikey:
        bearer = apikey.split()
        if bearer[0] == 'APIKEY':
            token = bearer[1]
    if 'X-API-KEY' in request.headers:
        token = request.headers['X-API-KEY']
    try:
        options = copy.deepcopy(OPTIONS_PARAMS)
        options_object = Options(options)
        options_object.json = True
        options_object.token = token
        options_object.user = None
        options_object.redis_host = config['redis']['host']
        options_object.redis_port = config['redis']['port']
        options_object.redis_db = config['redis']['db']
        options_object.redis_prefix = config['redis']['prefix']
        options_object.proxy = True
        user = None
        if token:
            proxy = Utils.get_service_endpoint(config, 'user')
            r = requests.get(proxy + '/api/user/info/apikey/' + token)
            if not r.status_code == 200:
                return (404, options, {'message': 'Invalid API Key or connection issue'})
            user = r.json()['user']
            if user:
                options_object.user = user['id']
    except Exception as e:
        logging.exception(e)
        return (500, options_object, str(e))

    return (200, options_object, None)

@app.route('/api/daemon/aboutme', methods=['POST'])
def biomaj_daemon_aboutme():
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)

    options.aboutme = True
    params = request.get_json()
    try:
        options.userlogin = params['login']
        options.userpassword = params['password']
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))

@app.route('/api/daemon/maintenance', methods=['POST'])
def biomaj_daemon_maintenance_on():
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)
    if not options.user or 'admin' not in config['biomaj'] or options.user not in config['biomaj']['admin']:
        abort(401, 'This action requires authentication with api key')
    options.maintenance = 'on'
    try:
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))

@app.route('/api/daemon/maintenance', methods=['DELETE'])
def biomaj_daemon_maintenance_off():
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)
    if not options.user or 'admin' not in config['biomaj'] or options.user not in config['biomaj']['admin']:
        abort(401, 'This action requires authentication with api key')
    options.maintenance = 'off'
    try:
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))

@app.route('/api/daemon/maintenance', methods=['GET'])
def biomaj_daemon_maintenance_status():
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)
    options.maintenance = 'status'
    try:
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))

@app.route('/api/daemon/whatsup', methods=['GET'])
def biomaj_daemon_maintenance_whatsup():
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)
    options.whatsup = True
    try:
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))

@app.route('/api/daemon/stats', methods=['GET'])
def biomaj_daemon_stats():
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)
    options.stats = True
    try:
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))

@app.route('/api/daemon/version', methods=['GET'])
def biomaj_daemon_version():
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)
    options.version = True
    try:
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))


@app.route('/api/daemon/history', methods=['GET'])
def biomaj_daemon_history():
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)

    options.history = True
    options.historyLimit = request.args.get('limit', 100)
    try:
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))


@app.route('/api/daemon/bank', methods=['GET'])
def biomaj_daemon_banks_status():
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)
    options.status = True
    try:
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))

@app.route('/api/daemon/bank/<bank>', methods=['GET'])
def biomaj_daemon_bank_status(bank):
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)
    options.status = True
    options.bank = bank
    try:
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))

@app.route('/api/daemon/bank/<bank>/check', methods=['GET'])
def biomaj_daemon_bank_check(bank):
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)
    options.check = True
    options.bank = bank
    try:
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))

@app.route('/api/daemon/bank/<bank>/status', methods=['GET'])
def biomaj_daemon_bank_update_status(bank):
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)
    options.updatestatus = True
    options.bank = bank
    try:
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))

@app.route('/api/daemon/status/ko', methods=['GET'])
def biomaj_daemon_bank_status_ko(bank):
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)
    options.statusko = True
    try:
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))

@app.route('/api/daemon/bank/<bank>/cancel', methods=['PUT'])
def biomaj_daemon_bank_cancel(bank):
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)
    if not options.user:
        abort(401, 'This action requires authentication with api key')
    options.updatecancel = True
    options.bank = bank
    try:
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))

@app.route('/api/daemon/bank/<bank>/owner/<owner>', methods=['PUT'])
def biomaj_daemon_bank_upate_owner(bank, owner):
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)
    if not options.user:
        abort(401, 'This action requires authentication with api key')
    options.owner = owner
    options.bank = bank
    try:
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))

@app.route('/api/daemon/bank/<bank>/visibility/<visibility>', methods=['PUT'])
def biomaj_daemon_bank_upate_visibility(bank, visibility):
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)
    if not options.user:
        abort(401, 'This action requires authentication with api key')
    options.visibility = visibility
    options.bank = bank
    try:
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))

@app.route('/api/daemon/bank/<bank>/name/<name>', methods=['PUT'])
def biomaj_daemon_bank_update_name(bank, name):
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)
    if not options.user:
        abort(401, 'This action requires authentication with api key')
    options.newbank = name
    options.bank = bank
    try:
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))

@app.route('/api/daemon/bank/<bank>/move', methods=['PUT'])
def biomaj_daemon_bank_update_directory(bank):
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)
    if not options.user:
        abort(401, 'This action requires authentication with api key')
    params = request.get_json()
    options.bank = bank
    try:
        options.newdir = params['path']
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))

@app.route('/api/daemon/bank/<bank>', methods=['POST'])
def biomaj_daemon_bank_update(bank):
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)
    if not options.user:
        abort(401, 'This action requires authentication with api key')
    options.update = True
    options.bank = bank
    params = request.get_json()
    if params is None:
        params = {}
    if 'publish' in params:
        options.publish = params['publish']
    if 'fromscratch' in params:
        options.fromscratch = params['fromscratch']
    if 'stop_before' in params:
        options.stop_before = params['stop_before']
    if 'stop_after' in params:
        options.stop_after = params['stop_after']
    if 'from_task' in params:
        options.from_task = params['from_task']
    if 'process' in params:
        options.process = params['process']
    if 'release' in params:
        options.release = params['release']
    try:
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))

@app.route('/api/daemon/bank/<bank>/publish', methods=['PUT'])
def biomaj_daemon_bank_publish(bank):
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)
    if not options.user:
        abort(401, 'This action requires authentication with api key')
    options.bank = bank
    options.publish = True
    params = request.get_json()
    if params is None:
        params = {}
    if 'release' in params:
        options.release = params['release']
    try:
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))

@app.route('/api/daemon/bank/<bank>/publish', methods=['DELETE'])
def biomaj_daemon_bank_unpublish(bank):
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)
    if not options.user:
        abort(401, 'This action requires authentication with api key')
    options.bank = bank
    options.unpublish = True
    try:
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))

@app.route('/api/daemon/bank/<bank>', methods=['DELETE'])
def biomaj_daemon_bank_remove(bank):
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)
    if not options.user:
        abort(401, 'This action requires authentication with api key')
    options.bank = bank
    params = request.get_json()
    if params is None:
        params = {}
    if 'all' in params and params['all']:
        options.removeall = True
        if 'force' in params and params['force']:
            options.force = params['force']
    elif 'pending' in params and params['pending']:
        options.removepending = True
    else:
        options.remove = True
        if 'release' in params:
            options.release = params['release']
        else:
            abort(400, 'missing release')

    try:
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))

@app.route('/api/daemon/bank/<bank>/freeze/<release>', methods=['PUT'])
def biomaj_daemon_bank_freeze(bank, release):
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)
    if not options.user:
        abort(401, 'This action requires authentication with api key')
    options.bank = bank
    options.release = release
    options.freeze = True
    try:
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))

@app.route('/api/daemon/bank/<bank>/freeze/<release>', methods=['DELETE'])
def biomaj_daemon_bank_unfreeze(bank, release):
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)
    if not options.user:
        abort(401, 'This action requires authentication with api key')
    options.bank = bank
    options.release = release
    options.unfreeze = True
    try:
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))

@app.route('/api/daemon/search', methods=['GET'])
def biomaj_daemon_bank_search(bank):
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)
    options.search = True
    params = request.get_json()
    if params is None:
        params = {}
    if 'formats' in params and params['formats']:
        options.formats = ','.join(params['formats'])
    if 'types' in params and params['types']:
        options.types = ','.join(params['types'])
    if 'query' in params and params['query']:
        options.query = ','.join(params['query'])
    try:
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))

@app.route('/api/daemon/bank/<bank>/show', methods=['GET'])
def biomaj_daemon_bank_show(bank):
    (http_code, options, error) = daemon_api_auth(request)
    if error:
        abort(http_code, error)
    if not options.user:
        abort(401, 'This action requires authentication with api key')
    options.bank = bank
    options.show = True
    params = request.get_json()
    if params is None:
        params = {}
    if 'release' in params:
        options.release = params['release']
    try:
        (res, msg) = biomaj_client_action(options, config)
        if res:
            if isinstance(msg, dict):
                return jsonify(msg)
            else:
                return jsonify({'msg': msg})
    except Exception as e:
        abort(500, str(e))

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
            proxy = Utils.get_service_endpoint(config, 'user')
            r = requests.get(proxy + '/api/user/info/apikey/' + token)
            if not r.status_code == 200:
                abort(404, {'message': 'Invalid API Key or connection issue'})
            user = r.json()['user']
            if user:
                options_object.user = user['id']


        if options_object.maintenance in ['on', 'off']:
            if not options_object.user or 'admin' not in config['biomaj'] or options_object.user not in config['biomaj']['admin']:
                abort(401, {'message': 'This action requires authentication with api key'})

        if options_object.bank:
            bmaj_options = BmajOptions(options_object)
            BiomajConfig(options_object.bank, bmaj_options)

            if not options_object.search and not options_object.show and not options_object.check and not options_object.status:
                if not user:
                    abort(401, {'message': 'This action requires authentication with api key'})

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
