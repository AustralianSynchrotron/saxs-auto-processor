#!/bin/bash
cd $(dirname $(readlink -f $0))

if [ -e ../env/bin/activate ]; then
	. ../env/bin/activate
fi

exec python2.7 Sim3.py
