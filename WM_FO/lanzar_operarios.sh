#!/bin/bash

if [ $# -lt 2 ]; then
    echo "Usage: $0 <cuantas> <kafka_ip:puerto> [fichero] [primero]"
    exit 1
fi
CUANTOS=$1
KAFKA=$2
FICHERO=$3
PRIMERO=${4:-1}
IP_KAFKA=${KAFKA%:*}
PUERTO_KAFKA=${KAFKA##*:}

for i in $(seq $PRIMERO $((PRIMERO + CUANTOS - 1))); do
    docker run -dit --name fo-op$i wm python WM_FO/WM_FO.py $IP_KAFKA $PUERTO_KAFKA OP$i $FICHERO
    echo "Lanzado OP$i"
done