#!/bin/bash
cd $(dirname $(readlink -f $0))

if [ -e env/bin/activate ]; then
	. env/bin/activate
fi


if [ $(screen -ls | grep engine | wc -l) -ne 0 ]; then
	echo "Already started type 'screen -r engine' to join."
	exit
fi
screen -dmS engine python2.7 Engine.py
echo "Started type 'screen -r engine' to join."
