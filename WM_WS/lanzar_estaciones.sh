#!/bin/bash

#================================================================#

#Se lanzan N estaciones de riego cada una en 2 contenedores
#El Docker: un monitor y un engine (stN) 

# Uso: ./WM_WS/lanzar_estaciones.sh <cuantas> <ip_central> <puerto_central> <kafka_ip:puerto> [primera] [caudal]
# Ejemplo: ./WM_WS/lanzar_estaciones.sh 3 192.168.1.40 5050 192.168.1.40:9092 -> lanza ST1, ST2 y ST3 (con "primera"=5 lanzaría ST5, ST6 y ST7)
#================================================================#

#Los 4 argumentos son obligatorios,, si faltan se muestra el uso y se sale
if [ $# -lt 4 ]; then
  echo "Usage: $0 <cuantas> <ip_central> <puerto_central> <kafka_ip:puerto> [primera] [caudal]"
  exit 1

fi
CUANTAS=$1
IP_CENTRAL=$2
PUERTO_CENTRAL=$3
KAFKA=$4
PRIMERA=${5:-1}                           #Numero de la primera estacion, por defecto es 1
CAUDAL=${6:-10}                           #Litros por minuto de cada Engine, por defecto es 10

#Los programas piden la IP y el puerto de kafka por separado

IP_KAFKA=${KAFKA%:*}                      #lo que hay antes de los ":"
PUERTO_KAFKA=${KAFKA##*:}                 #lo que hay despues de los ":"

#Puerto en el que escucha cada Monitor, puede ser el mismo para todos porque cada contenedor tiene su red interna
PUERTO_MONITOR=${PUERTO_MONITOR:-6001}  

#Red docker wm, el engine localiza a su monitor por el nombre del contenedor
docker network create wm 2>/dev/null

#Una vuelta por estacion
for i in $(seq $PRIMERA $((PRIMERA + CUANTAS - 1))); do

    #Monitor, se conecta a la central por IP
    docker run -d --name monitor-st$i --network wm wm \
    python WM_WS/WM_WS_M.py $PUERTO_MONITOR $IP_CENTRAL $PUERTO_CENTRAL ST$i "Jardin $i"

    #Engine, para poder pulsar F, R y P con el docker attach
    #Su monitor es monitor-stN y se conecta a Kafka por ip
    docker run -dit --name engine-st$i --network wm wm \
    python WM_WS/WM_WS_E.py $IP_KAFKA $PUERTO_KAFKA monitor-st$i $PUERTO_MONITOR ST$i $CAUDAL
    echo "Lanzada ST$i"
done
