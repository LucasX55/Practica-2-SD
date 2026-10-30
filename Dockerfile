#########################################################################
# Una sola imagen para todos los modulos (Central, Monitor, Engine y FO)
# Lleva python, las librerias y todo nuestro codigo. Por defecto arranca la
# Central; para lanzar otro modulo se cambia el comando al hacer docker run
# (es lo que hacen los scripts lanzar_estaciones.sh y lanzar_operarios.sh)
#########################################################################

# Partimos de una imagen con python 3.12 ya instalado (version reducida, "slim")
FROM python:3.12-slim

# Carpeta de trabajo dentro del contenedor: todo lo de abajo se copia y se ejecuta aqui
WORKDIR /app

# Primero solo requirements.txt y las librerias (confluent-kafka): asi docker
# guarda este paso y no lo repite cada vez que cambiamos el codigo
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Nuestro codigo, carpeta por carpeta
COPY common/ common/
COPY WM_Central/ WM_Central/
COPY WM_WS/ WM_WS/
COPY WM_FO/ WM_FO/

# Carpeta para la BD de la Central (el compose monta aqui el volumen)
RUN mkdir -p /data

# Que python escriba en pantalla al momento, sin guardarse el texto (si no, los logs llegan tarde)
ENV PYTHONUNBUFFERED=1
# Ruta de la BD: la lee WM_Central con la variable WM_DB
ENV WM_DB=/data/wm_central.db

# Comando por defecto: arrancar la Central con el puerto de sockets y kafka que
# le pasen en PUERTO_SOCKETS y KAFKA (si no, 5050 y localhost:9092)
CMD ["sh", "-c", "python WM_Central/WM_Central.py ${PUERTO_SOCKETS:-5050} ${KAFKA:-localhost:9092}"]