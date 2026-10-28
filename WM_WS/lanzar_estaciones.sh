#!/bin/bash

if [ $# -lt 4 ]; then
  echo "Usage: $0 <cuantas> <ip_central> <puerto_central> <kafka_ip:puerto> [primera] [caudal]"
  exit 1

fi
CUANTAS=$1
IP_CENTRAL=$2
PUERTO_CENTRAL=$3
KAFKA=$4
PRIMERA=${5:-1}
CAUDAL=${6:-12}
PUERTO_MONITOR=${PUERTO_MONITOR:-6001}
docker network create wm 2>/dev/null
for i in $(seq $PRIMERA $((PRIMERA + CUANTAS - 1))); do
    docker run -d --name monitor-st$i --network wm wm \
    python WM_WS/WM_WS_M.py $PUERTO_MONITOR $IP_CENTRAL $PUERTO_CENTRAL ST$i "Jardin $i"
    docker run -dit --name engine-st$i --network wm wm \
    python WM_WS/WM_WS_E.py $KAFKA monitor-st$i $PUERTO_MONITOR $CAUDAL
    echo "Lanzada ST$i"
done
