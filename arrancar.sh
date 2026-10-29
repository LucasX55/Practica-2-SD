#!/bin/bash
# Arranca en este PC estaciones y operarios contra la Central (que esta en AWS).
# Uso: ./arrancar_pc.sh <estaciones> <operarios>      (ejemplo: ./arrancar_pc.sh 5 2)
# Los numeros se eligen solos: el primer PC coge ST1.. y OP1.., el siguiente sigue donde acabo
# el anterior. Si relanzas en el mismo PC, reutiliza sus numeros.
# Pon como mucho tantos operarios como estaciones, para que los OP de dos PCs no se repitan.
if [ $# -ne 2 ]; then
  echo "Uso: $0 <estaciones> <operarios>"
  exit 1
fi
IP=3.218.31.175     # IP de la Central y Kafka (AWS)
ESTACIONES=$1
OPERARIOS=$2
if [ $OPERARIOS -gt $ESTACIONES ]; then
  echo "Pon como mucho $ESTACIONES operarios (tantos como estaciones)"
  exit 1
fi
 
#### 1. Esperar a que se pueda llegar a la Central y a Kafka (si la red los bloquea, se queda aqui)
for PUERTO in 5050 9092 8080; do
  until timeout 3 bash -c "echo > /dev/tcp/$IP/$PUERTO" 2>/dev/null; do
    echo "No llego a $IP:$PUERTO, reintentando... (comprueba la red: datos del movil)"
    sleep 3
  done
  echo "Puerto $PUERTO OK"
done
 
#### 2. Elegir el primer numero
# Si este PC ya tenia estaciones lanzadas, usa el mismo numero que entonces
PRIMERA=$(docker ps -a --format "{{.Names}}" | grep "^monitor-st" | sed 's/monitor-st//' | sort -n | head -n 1)
if [ -z "$PRIMERA" ]; then
  # Si no, el siguiente libre: la estacion mas alta que conoce la Central + 1
  MAXIMA=$(curl -s http://$IP:8080/estado | grep -o '"id": *"ST[0-9]*"' | grep -o '[0-9]*' | sort -n | tail -n 1)
  PRIMERA=$(( ${MAXIMA:-0} + 1 ))
fi
ULTIMO_OP=$((PRIMERA + OPERARIOS - 1))
echo "Este PC: ST$PRIMERA a ST$((PRIMERA + ESTACIONES - 1)) y OP$PRIMERA a OP$ULTIMO_OP"
 
#### 3. Quitar lo que hubiera de antes en este PC y construir la imagen
./WM_FO/parar_operarios.sh 2>/dev/null
./WM_WS/parar_estaciones.sh 2>/dev/null
docker build -t wm . || exit 1
 
#### 4. Dar de alta los operarios en la Central (lo mismo que escribir "altas N" en el panel)
curl -s -X POST --data "altas $ULTIMO_OP" http://$IP:8080/orden && echo "Operarios OP1 a OP$ULTIMO_OP dados de alta"
 
#### 5. Lanzar estaciones y operarios
./WM_WS/lanzar_estaciones.sh $ESTACIONES $IP 5050 $IP:9092 $PRIMERA
./WM_FO/lanzar_operarios.sh $OPERARIOS $IP:9092 "" $PRIMERA
 
#### 6. Reiniciar los FO una vez para que lean bien sus respuestas (topics recien creados)
sleep 10
for i in $(seq $PRIMERA $ULTIMO_OP); do docker restart fo-op$i > /dev/null; done
echo "Listo. Panel: http://$IP:8080   Operario: docker attach fo-op$PRIMERA"