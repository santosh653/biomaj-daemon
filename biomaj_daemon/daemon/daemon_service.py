import logging
import logging.config
import yaml
import traceback
import datetime
import time
import json
import threading

import redis
import consul
from flask import Flask
from flask import jsonify

from biomaj_core.config import BiomajConfig
from biomaj.bank import Bank
from biomaj.notify import Notify
from biomaj_core.utils import Utils

from biomaj_zipkin.zipkin import Zipkin


app = Flask(__name__)
app_log = logging.getLogger('werkzeug')
app_log.setLevel(logging.ERROR)


@app.route('/api/daemon-message')
def ping():
    return jsonify({'msg': 'pong'})


def start_web(config):
    app.run(host='0.0.0.0', port=config['web']['port'])


def consul_declare(config):
    if config['consul']['host']:
        consul_agent = consul.Consul(host=config['consul']['host'])
        consul_agent.agent.service.register(
            'biomaj-daemon-message',
            service_id=config['consul']['id'],
            address=config['web']['hostname'],
            port=config['web']['port'],
            tags=['biomaj']
        )
        check = consul.Check.http(
            url='http://' + config['web']['hostname'] + ':' + str(config['web']['port']) + '/api/daemon-message',
            interval=20
        )
        consul_agent.agent.check.register(
            config['consul']['id'] + '_check',
            check=check,
            service_id=config['consul']['id']
        )
        return True
    else:
        return False


class Options(object):
    def __init__(self, d):
        self.__dict__ = d

    def get_option(self, option):
        """
        Gets an option if present, else return None
        """
        if hasattr(self, option):
            return getattr(self, option)
        return None


class DaemonService(object):

    channel = None

    def supervise(self):
        if consul_declare(self.config):
            web_thread = threading.Thread(target=start_web, args=(self.config,))
            web_thread.start()

    def __init__(self, config_file):
        self.logger = logging
        self.session = None
        self.executed_callback = None
        with open(config_file, 'r') as ymlfile:
            self.config = yaml.load(ymlfile)
            Utils.service_config_override(self.config)

        Zipkin.set_config(self.config)

        BiomajConfig.load_config(self.config['biomaj']['config'])

        if 'log_config' in self.config:
            for handler in list(self.config['log_config']['handlers'].keys()):
                self.config['log_config']['handlers'][handler] = dict(self.config['log_config']['handlers'][handler])
            logging.config.dictConfig(self.config['log_config'])
            self.logger = logging.getLogger('biomaj')

        self.redis_client = redis.StrictRedis(
            host=self.config['redis']['host'],
            port=self.config['redis']['port'],
            db=self.config['redis']['db'],
            decode_responses=True
        )

        self.logger.info('Daemon service started')

    def close(self):
        if self.channel:
            self.channel.close()

    def on_executed_callback(self, func):
        self.executed_callback = func

    def __start_action(self, bank, action):
        whatsup = bank + ':' + str(action)
        self.redis_client.hset(
            self.config['redis']['prefix'] + ':daemons:status',
            self.config['consul']['id'],
            whatsup
        )
        # Expires in 7 days if no update
        self.redis_client.expire(
            self.config['redis']['prefix'] + ':daemons:status',
            3600*24*7
        )

    def __end_action(self):
        self.redis_client.hset(
            self.config['redis']['prefix'] + ':daemons:status',
            self.config['consul']['id'],
            'pending'
        )

    def execute(self, options):
        '''
        Execute update or remove command
        '''
        start_time = datetime.datetime.now()
        start_time = time.mktime(start_time.timetuple())

        is_ok = None
        is_updated = False
        action = None
        try:
            options.no_log = False
            if options.update:
                action = 'update'
                self.__start_action(options.bank, action)
                bmaj = Bank(options.bank, options)
                self.logger.debug('Log file: ' + bmaj.config.log_file)
                is_ok = bmaj.update(depends=True)
                is_updated = bmaj.session.get('update')
                Notify.notifyBankAction(bmaj)
                self.__end_action()
            elif options.remove or options.removeall:
                action = 'remove'
                self.__start_action(options.bank, action)
                if options.removeall:
                    bmaj = Bank(options.bank, options, no_log=True)
                    print('Log file: ' + bmaj.config.log_file)
                    is_ok = bmaj.removeAll(options.force)
                else:
                    bmaj = Bank(options.bank, options)
                    self.logger.debug('Log file: ' + bmaj.config.log_file)
                    is_ok = bmaj.remove(options.release)
                    Notify.notifyBankAction(bmaj)
                self.__end_action()
            elif options.removepending:
                bmaj = Bank(options.bank, options, no_log=True)
                bmaj.remove_pending(options.release)
        except Exception as e:
            self.logger.exception('Exception: ' + str(e))
            is_ok = False

        end_time = datetime.datetime.now()
        end_time = time.mktime(end_time.timetuple())

        execution_time = end_time - start_time
        return {
            'error': not is_ok,
            'execution_time': execution_time,
            'action': action,
            'updated': is_updated
        }

    def callback_messages(self, body):
        '''
        Manage bank update or removal
        '''
        try:
            options = Options(body)
            info = self.execute(options)
            if self.executed_callback and options.bank:
                self.executed_callback(options.bank, [info])
        except Exception as e:
            self.logger.error('Error with message: %s' % (str(e)))
            traceback.print_exc()

    def wait_for_messages(self):
        '''
        Loop queue waiting for messages
        '''
        while True:
            msg = self.redis_client.rpop(self.config['redis']['prefix'] + ':queue')
            if msg:
                msg = json.loads(msg)
                info = None
                if 'update' in msg and msg['update']:
                    info = 'update'
                if 'remove' in msg and msg['remove']:
                    info = 'remove'
                if 'removeall' in msg and msg['removeall']:
                    info = 'removeall'
                logging.info('Biomaj:Daemon:' + msg['bank'] + ':' + str(info))

                span = None
                if 'trace' in msg and msg['trace']:
                    span = Zipkin('biomaj', 'workflow')
                    span.add_binary_annotation('operation', info)
                    msg['traceId'] = span.get_trace_id()
                    msg['spanId'] = span.get_span_id()

                self.callback_messages(msg)

                if span:
                    span.trace()

                logging.info('Biomaj:Daemon:' + msg['bank'] + ':over')
            time.sleep(1)
