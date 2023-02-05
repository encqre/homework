#!/bin/sh
docker build -t "task2-mysql" .
docker run -d -p 3306:3306 task2-mysql
sleep 30
python3 task2.py $@