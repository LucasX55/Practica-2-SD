#!/bin/bash

# Para y borra todos los operarios lanzados con lanzar_operarios.sh.
# Uso: ./WM_FO/parar_operarios.sh

# Mismo mecanismo: ids de los contenedores cuyo nombre contiene "fo-op" (fo-op1, fo-op2...)
# y se borran con rm -f. Los de las estaciones no se tocan
docker rm -f $(docker ps -a -q --filter "name=fo-op") 2>/dev/null
echo "Operarios parados"