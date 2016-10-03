# About

Experimental (in progress) microservice to manage biomaj.

Needs mongo and redis



# Development

    flake8 --ignore E501 biomaj_daemon

# Prometheus metrics

Endpoint: /api/download/metrics


# Run

## Message consumer:
export BIOMAJ_CONFIG=path_to_config.yml
python bin/download_consumer.py

## Web server

In bin directory:
export BIOMAJ_CONFIG=path_to_config.yml
gunicorn download:app

Web processes should be behind a proxy/load balancer, API base url /api/download
