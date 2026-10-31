#!/bin/bash

# Para y borra todas las estaciones lanzadas con lanzar_estaciones.sh.
# Uso: ./WM_WS/parar_estaciones.sh

# docker ps -a -q    -> ids de todos los contenedores (-a: también los parados; -q: solo el id)
# --filter "name=X"  -> solo los que tienen X en su nombre (monitor-st1, monitor-st2...)
# docker rm -f       -> los borra, aunque estén en marcha (-f: fuerza la parada)
# 2>/dev/null        -> si no hay ninguno, docker rm se queja de que le faltan argumentos; con esto no se ve ese error
docker rm -f $(docker ps -a -q --filter "name=monitor-st") $(docker ps -aq --filter "name=engine-st") 2>/dev/null
echo "Estaciones paradas"