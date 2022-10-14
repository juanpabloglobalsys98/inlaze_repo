#!/bin/sh

set -e

# python manage.py migrate --database=default
# python manage.py migrate --database=admin

uwsgi --socket :9000 --workers 4 --master --enable-threads --module betenlace.wsgi