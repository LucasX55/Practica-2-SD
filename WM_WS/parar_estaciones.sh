#!/bin/bash

docker rm -f $(docker ps -a -q --filter "name=monitor-st") $(docker ps -aq --filter "name=engine-st") 2>/dev/null
echo "Estaciones paradas"