#!/bin/bash

docker rm -f $(docker ps -a -q --filter "name=fo-st") $(docker ps -aq --filter "name=fo-engine") 2>/dev/null
echo "Estaciones paradas"