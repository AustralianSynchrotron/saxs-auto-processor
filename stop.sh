#!/bin/bash

PID=$(ps --no-headers -C "python2.7 Engine.py" | awk '{ print $1 }')

if [ "$PID" ]; then
	kill -15 $PID
	echo "Stopped."
else
	echo "Already stopped."
fi
