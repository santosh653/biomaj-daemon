import logging
import logging.config
import yaml
import traceback
import datetime
import time
import json

import redis

from biomaj_core.config import BiomajConfig
from biomaj.bank import Bank
from biomaj.notify import Notify
from biomaj_core.utils import Utils


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

    def __init__(self, config_file):
        self.logger = logging
        self.session = None
        self.executed_callback = None
        with open(config_file, 'r') as ymlfile:
            self.config = yaml.load(ymlfile)
            Utils.service_config_override(self.config)

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

    def execute(self, options):
        '''
        List remote content
        '''
        start_time = datetime.datetime.now()
        start_time = time.mktime(start_time.timetuple())

        error = None
        try:
            options.no_log = False
            if options.update:
                bmaj = Bank(options.bank, options)
                self.logger.debug('Log file: ' + bmaj.config.log_file)
                error = bmaj.update(depends=True)
                Notify.notifyBankAction(bmaj)
            elif options.remove or options.removeall:
                if options.removeall:
                    bmaj = Bank(options.bank, options, no_log=True)
                    print('Log file: ' + bmaj.config.log_file)
                    error = bmaj.removeAll(options.force)
                else:
                    bmaj = Bank(options.bank, options)
                    self.logger.debug('Log file: ' + bmaj.config.log_file)
                    error = bmaj.remove(options.release)
                    Notify.notifyBankAction(bmaj)
        except Exception as e:
            self.logger.exception('Exception: ' + str(e))
            error = True

        end_time = datetime.datetime.now()
        end_time = time.mktime(end_time.timetuple())

        execution_time = end_time - start_time
        return {'error': error, 'execution_time': execution_time}

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
                self.callback_messages(msg)
                logging.info('Biomaj:Daemon:' + msg['bank'] + ':over')
            time.sleep(1)
