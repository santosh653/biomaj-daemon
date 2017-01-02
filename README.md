# About

Microservice to manage biomaj, acts as a frontend to receive biomaj-cli commands and execute operations

Needs mongo and redis



# Development

    flake8 --ignore E501 biomaj_daemon


# Run

## Message consumer:

    export BIOMAJ_CONFIG=path_to_config.yml
    python bin/biomaj_daemon_consumer.py

## Web server

If package is installed via pip, you need a file named *gunicorn_conf.py* containing somehwhere on local server:

    def worker_exit(server, worker):
        from prometheus_client import multiprocess
        multiprocess.mark_process_dead(worker.pid)

If you cloned the repository and installed it via python setup.py install, just refer to the *gunicorn_conf.py* in the cloned repository.

    export BIOMAJ_CONFIG=path_to_config.yml
    rm -rf ..path_to/godocker-prometheus-multiproc
    mkdir -p ..path_to/godocker-prometheus-multiproc
    export prometheus_multiproc_dir=..path_to/godocker-prometheus-multiproc
    gunicorn -c ../path_to/gunicorn_conf.py biomaj_daemon.daemon.biomaj_daemon_web:app

Web processes should be behind a proxy/load balancer, API base url /api/daemon
