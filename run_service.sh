#!/bin/sh

while true; do
	./service.py
	if [ $? -ne 143 ]; then
		exit 0
	fi
done
