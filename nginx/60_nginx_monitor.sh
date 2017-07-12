#!/bin/bash

CURDIR="$(dirname $0)"
python $CURDIR/nginx_collect.py --service=$HOSTNAME --format=falcon
