import os
import pkg_resources
import shutil
import json
import datetime
import time
import redis
import sys
import logging

import requests

from tabulate import tabulate

from biomaj.bank import Bank
from biomaj_core.config import BiomajConfig
from biomaj.workflow import Workflow
from biomaj.workflow import UpdateWorkflow
from biomaj.workflow import RemoveWorkflow
from biomaj.notify import Notify

if sys.version < '3':
    import ConfigParser as configparser
else:
    import configparser


def biomaj_version(options, config):
    '''
    Get biomaj version
    '''
    version = pkg_resources.require('biomaj')[0].version
    return (True, 'Version: ' + str(version))


def check_options(options, config):
    if options.stop_after or options.stop_before or options.from_task:
        available_steps = []
        for flow in UpdateWorkflow.FLOW:
            available_steps.append(flow['name'])
        for flow in RemoveWorkflow.FLOW:
            available_steps.append(flow['name'])
        if options.stop_after:
            if options.stop_after not in available_steps:
                return (False, 'Invalid step: ' + options.stop_after)
        if options.stop_before:
            if options.stop_before not in available_steps:
                return (False, 'Invalid step: ' + options.stop_before)
        if options.from_task:
            if options.from_task not in available_steps:
                return (False, 'Invalid step: ' + options.from_task)
    return (True, None)


def biomaj_maintenance(options, config):
    '''
    Maintenance management
    '''
    if options.maintenance not in ['on', 'off', 'status']:
        print("Wrong maintenance value [on,off,status]")
        return (False, "Wrong maintenance value [on,off,status]")

    data_dir = BiomajConfig.global_config.get('GENERAL', 'data.dir')
    if BiomajConfig.global_config.has_option('GENERAL', 'lock.dir'):
        lock_dir = BiomajConfig.global_config.get('GENERAL', 'lock.dir')
    else:
        lock_dir = data_dir

    maintenance_lock_file = os.path.join(lock_dir, 'biomaj.lock')
    if options.maintenance == 'status':
        if os.path.exists(maintenance_lock_file):
            return (True, "Maintenance: On")
        else:
            return (True, "Maintenance: Off")

    if options.maintenance == 'on':
        f = open(maintenance_lock_file, 'w')
        f.write('1')
        f.close()
        return (True, "Maintenance set to On")

    if options.maintenance == 'off':
        if os.path.exists(maintenance_lock_file):
            os.remove(maintenance_lock_file)
        return (True, "Maintenance set to Off")


def biomaj_owner(options, config):
    '''
    Bank ownership management
    '''
    if not options.bank:
        return (False, "Bank option is missing")
    bank = Bank(options.bank, options=options, no_log=True)
    bank.set_owner(options.owner)
    return (True, None)


def biomaj_visibility(options, config):
    '''
    Bank visibility management
    '''
    if not options.bank:
        return (False, "Bank option is missing")
    if options.visibility not in ['public', 'private']:
        return (False, "Valid values are public|private")

    bank = Bank(options.bank, options=options, no_log=True)
    bank.set_visibility(options.visibility)
    return (True, "Do not forget to update accordingly the visibility.default parameter in the configuration file")


def biomaj_move_production_directories(options, config):
    '''
    Change bank production directories
    '''
    if not options.bank:
        return (False, "Bank option is missing")

    if not os.path.exists(options.newdir):
        return (False, "Destination directory does not exists")

    bank = Bank(options.bank, options=options, no_log=True)
    if not bank.bank['production']:
        return (False, "Nothing to move, no production directory")

    bank.load_session(Workflow.FLOW, None)
    w = Workflow(bank)
    res = w.wf_init()
    if not res:
        return (False, 'Bank initialization failure')

    for prod in bank.bank['production']:
        session = bank.get_session_from_release(prod['release'])
        bank.load_session(Workflow.FLOW, session)
        prod_path = bank.session.get_full_release_directory()
        if os.path.exists(prod_path):
            shutil.move(prod_path, options.newdir)
        prod['data_dir'] = options.newdir
    bank.banks.update({'name': options.bank}, {'$set': {'production': bank.bank['production']}})
    w.wf_over()
    return (True, "Bank production directories moved to " + options.newdir + "\nWARNING: do not forget to update accordingly the data.dir and dir.version properties")


def biomaj_newbank(options, config):
    '''
    Rename a bank
    '''
    if not options.bank:
        return (False, "Bank option is missing")

    bank = Bank(options.bank, options=options, no_log=True)
    conf_dir = BiomajConfig.global_config.get('GENERAL', 'conf.dir')
    bank_prop_file = os.path.join(conf_dir, options.bank + '.properties')
    config_bank = configparser.SafeConfigParser()
    config_bank.read([os.path.join(conf_dir, options.bank + '.properties')])
    config_bank.set('GENERAL', 'db.name', options.newbank)
    newbank_prop_file = open(os.path.join(conf_dir, options.newbank + '.properties'), 'w')
    config_bank.write(newbank_prop_file)
    newbank_prop_file.close()
    bank.banks.update({'name': options.bank}, {'$set': {'name': options.newbank}})
    os.remove(bank_prop_file)
    return (True, "Bank " + options.bank + " renamed to " + options.newbank)


def biomaj_search(options, config):
    '''
    Search within banks
    '''
    msg = ''
    if options.query:
        res = Bank.searchindex(options.query)
        msg += 'Query matches for :' + options.query + '\n'
        results = [["Release", "Format(s)", "Type(s)", "Files"]]
        for match in res:
            results.append([match['_source']['release'],
                            str(match['_source']['format']),
                            str(match['_source']['types']),
                            ','.join(match['_source']['files'])])
        msg += tabulate(results, headers="firstrow", tablefmt="grid")
    else:
        formats = []
        if options.formats:
            formats = options.formats.split(',')
        types = []
        if options.types:
            types = options.types.split(',')
        msg += "Search by formats=" + str(formats) + ", types=" + str(types) + '\n'
        res = Bank.search(formats, types, False)
        results = [["Name", "Release", "Format(s)", "Type(s)", 'Published']]
        for bank in sorted(res, key=lambda bank: (bank['name'])):
            b = bank['name']
            bank['production'].sort(key=lambda n: n['release'], reverse=True)
            for prod in bank['production']:
                iscurrent = ""
                if prod['session'] == bank['current']:
                    iscurrent = "yes"
                results.append([b if b else '', prod['release'], ','.join(prod['formats']),
                                ','.join(prod['types']), iscurrent])
        msg += tabulate(results, headers="firstrow", tablefmt="grid")
    return (True, msg)


def biomaj_show(options, config):
    '''
    show bank details
    '''
    if not options.bank:
        return (False, "Bank option is required")

    bank = Bank(options.bank, options=options, no_log=True)
    results = [["Name", "Release", "Format(s)", "Type(s)", "Tag(s)", "File(s)"]]
    current = None
    fformat = None
    if 'current' in bank.bank and bank.bank['current']:
        current = bank.bank['current']
    for prod in bank.bank['production']:
        include = True
        release = prod['release']
        if current == prod['session']:
            release += ' (current)'
        if options.release and (prod['release'] != options.release and prod['prod_dir'] != options.release):
            include = False
        if include:
            session = bank.get_session_from_release(prod['release'])
            formats = session['formats']
            afiles = []
            atags = []
            atypes = []
            for fformat in list(formats.keys()):
                for elt in formats[fformat]:
                    atypes.append(','.join(elt['types']))
                    for tag in list(elt['tags'].keys()):
                        atags.append(elt['tags'][tag])
                    for eltfile in elt['files']:
                        afiles.append(eltfile)
            results.append([
                bank.bank['name'],
                release,
                fformat,
                ','.join(atypes),
                ','.join(atags),
                ','.join(afiles)])
    msg = tabulate(results, headers="firstrow", tablefmt="grid")
    return (True, msg)


def biomaj_check(options, config):
    '''
    Check bank properties
    '''
    if not options.bank:
        return (False, "Bank name is missing")

    bank = Bank(options.bank, options=options, no_log=True)
    msg = options.bank + " check: " + str(bank.check()) + "\n"
    return (True, msg)


def biomaj_status(options, config):
    '''
    Get bank status information
    '''
    msg = ''
    if options.bank:
        bank = Bank(options.bank, options=options, no_log=True)
        if bank.bank['properties']['visibility'] != 'public' and not bank.is_owner():
            return (False, 'Access forbidden')
        info = bank.get_bank_release_info(full=True)
        msg += tabulate(info['info'], headers='firstrow', tablefmt='psql') + '\n'
        msg += tabulate(info['prod'], headers='firstrow', tablefmt='psql') + '\n'
        # do we have some pending release(s)
        if 'pend' in info and len(info['pend']) > 1:
            msg += tabulate(info['pend'], headers='firstrow', tablefmt='psql') + '\n'
    else:
        banks = Bank.list()
        # Headers of output table
        banks_list = [["Name", "Type(s)", "Release", "Visibility", "Last update"]]
        for bank in sorted(banks, key=lambda k: k['name']):
            try:
                bank = Bank(bank['name'], options=options, no_log=True)
                if bank.bank['properties']['visibility'] == 'public' or bank.is_owner():
                    banks_list.append(bank.get_bank_release_info()['info'])
            except Exception as e:
                logging.error('Failed to load bank %s: %s' % (bank['name'], str(e)))
        msg += tabulate(banks_list, headers="firstrow", tablefmt="psql") + '\n'
    return (True, msg)


def biomaj_status_ko(options, config):
    '''
    Get failed banks
    '''
    banks = Bank.list()
    banks_list = [["Name", "Type(s)", "Release", "Visibility", "Last run"]]
    for bank in sorted(banks, key=lambda k: k['name']):
        try:
            bank = Bank(bank['name'], options=options, no_log=True)
            bank.load_session(UpdateWorkflow.FLOW)
            if bank.session is not None:
                if bank.use_last_session and not bank.session.get_status(Workflow.FLOW_OVER):
                    wf_status = bank.session.get('workflow_status')
                    if wf_status is None or not wf_status:
                        banks_list.append(bank.get_bank_release_info()['info'])
        except Exception as e:
            return (False, str(e))
    return (True, tabulate(banks_list, headers="firstrow", tablefmt="psql"))


def biomaj_bank_update_request(options, config):
    '''
    Send bank update request to rabbitmq
    '''
    return biomaj_send_message(options, config)


def biomaj_whatsup(options, config):
    redis_client = None
    whatsup = []

    if options.proxy:
        redis_client = redis.StrictRedis(
            host=config['redis']['host'],
            port=config['redis']['port'],
            db=config['redis']['db'],
            decode_responses=True
        )
        if not redis_client:
            return (False, 'Redis not configured')

        pending_len = redis_client.llen(config['redis']['prefix'] + ':queue')
        if not pending_len:
            pending_len = 0

        whatsup.append(['queue', str(pending_len), 'waiting'])

    if not options.proxy:
        data_dir = BiomajConfig.global_config.get('GENERAL', 'data.dir')
        lock_dir = data_dir
        if BiomajConfig.global_config.has_option('GENERAL', 'lock.dir'):
            lock_dir = BiomajConfig.global_config.get('GENERAL', 'lock.dir')
        lock_files = [f for f in os.listdir(lock_dir) if os.path.isfile(os.path.join(lock_dir, f))]
        for lock_file in lock_files:
            bank_name = lock_file.replace('.lock', '')
            whatsup.append(['system', str(bank_name), 'running'])
        if not whatsup:
            whatsup.append(['system', '', 'pending'])
    else:
        daemons_status = redis_client.hgetall(config['redis']['prefix'] + ':daemons:status')
        msg = 'All daemons pending'
        for daemon in list(daemons_status.keys()):
            if daemons_status[daemon]:
                # bank:action
                proc = daemons_status[daemon].split(':')
                if len(proc) == 2:
                    whatsup.append([daemon] + proc)
                else:
                    whatsup.append([daemon, ''] + proc)
    if whatsup:
        msg = tabulate(whatsup, ['daemon', 'bank', 'action'], tablefmt="simple")
    return (True, msg)


def biomaj_send_message(options, config):
    '''
    Send message to rabbitmq listener
    '''
    if not options.proxy:
        return (False, 'option not allowed without --proxy option')
    redis_client = redis.StrictRedis(
        host=config['redis']['host'],
        port=config['redis']['port'],
        db=config['redis']['db'],
        decode_responses=True
    )
    if not redis_client:
        return (False, 'Redis not configured')
    cur = datetime.datetime.now()
    options.timestamp = time.mktime(cur.timetuple())
    redis_client.lpush(config['redis']['prefix'] + ':queue', json.dumps(options.__dict__))
    return (True, None)


def biomaj_bank_update(options, config):
    '''
    Update a bank
    '''
    if not options.bank:
        return (False, "Bank name is missing")
    banks = options.bank.split(',')
    gres = True
    msg = ''
    for bank in banks:
        options.bank = bank
        no_log = True
        if not options.proxy:
            no_log = False
        # logging.debug('Options: '+str(options.__dict__))
        bmaj = Bank(bank, options=options, no_log=no_log)
        if bmaj.is_locked():
            return (False, 'Bank is locked due to an other action')
        check_status = bmaj.check()
        if not check_status:
            msg += 'Skip bank ' + options.bank + ': wrong config\n'
            gres = False
            continue
        else:
            msg += 'Bank update request sent for ' + options.bank + '\n'
            if not options.proxy:
                res = bmaj.update(depends=True)
                Notify.notifyBankAction(bmaj)
            else:
                res = biomaj_bank_update_request(options, config)
            if not res:
                msg += 'Failed to send update request for ' + options.bank + '\n'
                gres = False

    if not gres:
        return (False, msg)
    return (True, msg)


def biomaj_freeze(options, config):
    '''
    freeze a bank
    '''
    if not options.bank:
        return (False, "Bank name is missing")
    if not options.release:
        return (False, "Bank release is missing")
    bmaj = Bank(options.bank, options=options)
    res = bmaj.freeze(options.release)
    if not res:
        return (False, 'Failed to freeze the bank release')
    return (True, None)


def biomaj_unfreeze(options, config):
    '''
    unfreeze a bank
    '''
    if not options.bank:
        return (False, "Bank name is missing")
    if not options.release:
        return (False, "Bank release is missing")

    bmaj = Bank(options.bank, options=options)
    res = bmaj.unfreeze(options.release)
    if not res:
        return (False, 'Failed to unfreeze the bank release')
    return (True, None)


def biomaj_remove_request(options, config):
    '''
    Send remove request to rabbitmq
    '''
    return biomaj_send_message(options, config)


def biomaj_remove(options, config):
    '''
    Remove a bank
    '''
    if not options.bank:
        return (False, "Bank name is missing")

    if options.remove and not options.release:
        return (False, "Bank release is missing")

    no_log = True
    if not options.proxy:
        no_log = False
    # If removeall, do not set logs as log dir will be deleted
    if options.removeall:
        no_log = True
    bmaj = Bank(options.bank, options=options, no_log=no_log)
    if bmaj.is_locked():
        return (False, 'Bank is locked due to an other action')

    res = True
    if options.removeall:
        if not options.proxy:
            res = bmaj.removeAll(options.force)
            return (res, '')
        res = biomaj_remove_request(options, config)
    else:
        if not options.proxy:
            res = bmaj.remove(options.release)
            Notify.notifyBankAction(bmaj)
            return (res, '')
        res = biomaj_remove_request(options, config)
    if not res:
        return (False, 'Failed to send removal request')
    return (True, 'Bank removal request sent')


def biomaj_remove_pending_request(options, config):
    '''
    Send remove pending request to rabbitmq
    '''
    return biomaj_send_message(options, config)


def biomaj_remove_pending(options, config):
    '''
    Remove pending releases
    '''
    if not options.bank:
        return (False, "Bank name is missing")
    bmaj = Bank(options.bank, options=options, no_log=True)
    if bmaj.is_locked():
        return (False, 'Bank is locked due to an other action')
    if not options.proxy:
        res = bmaj.remove_pending(options.release)
        return (res, '')
    res = biomaj_remove_pending_request(options, config)
    if not res:
        return (False, 'Failed to send removal request')
    return (True, 'Request sent')


def biomaj_unpublish(options, config):
    '''
    Unpublish a bank
    '''
    if not options.bank:
        return (False, "Bank name is missing")

    bmaj = Bank(options.bank, options=options, no_log=True)
    bmaj.load_session()
    bmaj.unpublish()
    return (True, None)


def biomaj_publish(options, config):
    '''
    Publish a bank
    '''
    if not options.bank:
        return (False, "Bank name or release is missing")
    bmaj = Bank(options.bank, options=options, no_log=True)
    bmaj.load_session()
    bank = bmaj.bank
    session = None
    if options.get_option('release') is None:
        # Get latest prod release
        if len(bank['production']) > 0:
            prod = bank['production'][len(bank['production']) - 1]
            for s in bank['sessions']:
                if s['id'] == prod['session']:
                    session = s
                    break
    else:
        # Search production release matching release
        for prod in bank['production']:
            if prod['release'] == options.release or prod['prod_dir'] == options.release:
                # Search session related to this production release
                for s in bank['sessions']:
                    if s['id'] == prod['session']:
                        session = s
                        break
                break
    if session is None:
        return (False, "No production session could be found for this release")
    bmaj.session._session = session
    bmaj.publish()
    return (True, None)


def biomaj_update_cancel(options, config):
    '''
    Cancel current update of a Bank

    Running actions (download, process) will continue on remote services but will not manage next actions.
    Biomaj process will exit when ready with a *False* status.
    '''
    if not options.proxy:
        return (False, 'option not allowed without --proxy option')
    redis_client = redis.StrictRedis(
        host=config['redis']['host'],
        port=config['redis']['port'],
        db=config['redis']['db'],
        decode_responses=True
    )
    if not redis_client:
        return (False, 'Redis not configured')
    if not options.bank:
        return (False, "Bank name is missing")
    redis_client.set(config['redis']['prefix'] + ':' + options.bank + ':action:cancel', 1)
    return (True, 'Requested to cancel update of bank ' + options.bank + ', update will stop once current actions are over')


def biomaj_update_status(options, config):
    '''
    get the status of a bank during an update cycle
    '''
    if not options.proxy:
        return (False, 'option not allowed without --proxy option')
    redis_client = redis.StrictRedis(
        host=config['redis']['host'],
        port=config['redis']['port'],
        db=config['redis']['db'],
        decode_responses=True
    )
    if not redis_client:
        return (False, 'Redis not configured')
    pending = redis_client.llen(config['redis']['prefix'] + ':queue')
    pending_actions = [['Pending actions', 'Date']]
    for index in range(pending):
        pending_action = redis_client.lindex(config['redis']['prefix'] + ':queue', index)
        if pending_action:
            pending_bank = json.loads(pending_action)
            action_time = datetime.datetime.utcfromtimestamp(pending_bank['timestamp'])
            if pending_bank['bank'] == options.bank:
                if pending_bank['update']:
                    pending_actions.append(['Update', action_time])
                elif pending_bank['remove'] or pending_bank['removeall']:
                    pending_actions.append(['Removal - release ' + str(pending_bank['release']), action_time])

    bmaj = Bank(options.bank, options=options, no_log=True)
    if 'status' not in bmaj.bank:
        return (True, 'No update information available')
    status_info = bmaj.bank['status']

    msg = [["Workflow step", "Status"]]

    if 'log_file' in status_info:
        msg.append(['log_file', str(status_info['log_file']['status'])])
    if 'session' in status_info:
        msg.append(['session', str(status_info['session'])])
    for flow in UpdateWorkflow.FLOW:
        step = flow['name']
        if step in status_info:
            if status_info[step]['status'] is True:
                msg.append([step, 'over'])
            elif status_info[step]['status'] is False:
                msg.append([step, 'error'])
            else:
                if status_info[Workflow.FLOW_OVER]['status'] is True:
                    msg.append([step, 'skipped'])
                else:
                    msg.append([step, 'waiting'])
        if step in ['postprocess', 'preprocess', 'removeprocess']:
            progress = status_info[step]['progress']
            for proc in list(progress.keys()):
                msg.append([proc, str(progress[proc])])
    return (True, tabulate(pending_actions, headers="firstrow", tablefmt="grid") + tabulate(msg, headers="firstrow", tablefmt="grid"))


def biomaj_user_info(options, config):
    '''
    Get user info, need login/password
    '''
    if not options.userlogin or not options.userpassword:
        return (False, 'Missing login or password')
    if not options.proxy:
        return (False, 'Missing proxy information')
    bindinfo = {'type': 'password', 'value': options.userpassword}
    try:
        r = requests.post(config['web']['local_endpoint'] + '/api/user/bind/user/' + options.userlogin, json=bindinfo)
        if not r.status_code == 200:
            return (False, 'Invalid credentials')
        user = r.json()['user']
    except Exception as e:
        return (False, 'Connection error to proxy: ' + str(e))
    msg = 'User: ' + str(user['id']) + '\n'
    msg += 'Email: ' + str(user['email']) + '\n'
    msg += 'Api key: ' + str(user['apikey']) + '\n'
    return (True, msg)


def biomaj_client_action(options, config=None):
    check_options(options, config)
    if options.version:
        return biomaj_version(options, config)

    if options.whatsup:
        return biomaj_whatsup(options, config)

    if options.maintenance:
        return biomaj_maintenance(options, config)

    if options.owner:
        return biomaj_owner(options, config)

    if options.visibility:
        return biomaj_visibility(options, config)

    if options.newdir:
        return biomaj_move_production_directories(options, config)

    if options.newbank:
        return biomaj_newbank(options, config)

    if options.search:
        return biomaj_search(options, config)

    if options.show:
        return biomaj_show(options, config)

    if options.check:
        return biomaj_check(options, config)

    if options.status:
        return biomaj_status(options, config)

    if options.statusko:
        return biomaj_status_ko(options, config)

    if options.update:
        return biomaj_bank_update(options, config)

    if options.freeze:
        return biomaj_freeze(options, config)

    if options.unfreeze:
        return biomaj_unfreeze(options, config)

    if options.remove or options.removeall:
        return biomaj_remove(options, config)

    if options.removepending:
        return biomaj_remove_pending(options, config)

    if options.unpublish:
        return biomaj_unpublish(options, config)

    if options.publish:
        return biomaj_publish(options, config)

    if options.updatestatus:
        return biomaj_update_status(options, config)

    if options.updatecancel:
        return biomaj_update_cancel(options, config)

    if options.aboutme:
        return biomaj_user_info(options, config)
