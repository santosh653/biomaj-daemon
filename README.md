# About

Microservice to manage biomaj, acts as a frontend to receive biomaj-cli commands and execute operations

Needs mongo and redis



# Development

    flake8 --ignore E501 biomaj_daemon

# Prometheus metrics

Endpoint: /api/download/metrics


# Run

## Message consumer:
export BIOMAJ_CONFIG=path_to_config.yml
python bin/biomaj_daemon_consumer.py

## Web server

In bin directory:
export BIOMAJ_CONFIG=path_to_config.yml
gunicorn biomaj_daemon.daemon.biomaj_daemon_web:app

Web processes should be behind a proxy/load balancer, API base url /api/daemon
