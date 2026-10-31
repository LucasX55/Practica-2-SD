#!/bin/bash

#================================================================#
#Se lanzan N operarios cada una en un contenedor Docker

# Uso: ./WM_FO/lanzar_operarios.sh <cuantas> <kafka_ip:puerto> [fichero] [primero]
# Ejemplo: ./WM_FO/lanzar_operarios.sh 2 192.168.1.40:9092 WM_FO/servicios.txt -> lanza OP1 y OP2, y los dos procesan servicios.txt al arrancar
#================================================================#

#Los 2 argumentos son obligatorios,, si faltan se muestra el uso y se sale

if [ $# -lt 2 ]; then
    echo "Usage: $0 <cuantas> <kafka_ip:puerto> [fichero] [primero]"
    exit 1
fi
CUANTOS=$1
KAFKA=$2
FICHERO=$3                      #Si no se pasa, queda vacio y el operario arranca solo
PRIMERO=${4:-1}                 #Numero del primer operario, por defecto 1

#El FO pide la IP y el puerto de Kafka por separado, se parte "ip:puerto"

IP_KAFKA=${KAFKA%:*}            #lo que hay antes de los ":"
PUERTO_KAFKA=${KAFKA##*:}       #lo que hay despues de los ":"

#Una vuelta por operario
for i in $(seq $PRIMERO $((PRIMERO + CUANTOS - 1))); do

    # -dit: segundo plano (-d) pero con terminal (-it), para poder escribir
    # pedir/parar/ver con "docker attach fo-opN" (se sale sin pararlo con Ctrl+P, Ctrl+Q).
    # El ID del operario (OPN) tiene que estar dado de alta en la Central
    docker run -dit --name fo-op$i wm python WM_FO/WM_FO.py $IP_KAFKA $PUERTO_KAFKA OP$i $FICHERO
    echo "Lanzado OP$i"
done