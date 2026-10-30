# WaterManagement · Sistemas Distribuidos 2026/27

Simulación de una red de estaciones de riego controlada por una central. Release 1.

## Estructura

| Carpeta / archivo | Qué hay |
|-------------------|---------|
| `WM_Central/`     | Central: sockets con los monitores, base de datos SQLite, Kafka y panel web |
| `WM_WS/`          | Estación de riego: Monitor (`WM_WS_M.py`) y Engine (`WM_WS_E.py`), y scripts para lanzar varias |
| `WM_FO/`          | Aplicación del operario (`WM_FO.py`), fichero de servicios de ejemplo y scripts para lanzar varios |
| `common/`         | Código compartido: protocolo de tramas, base de datos, modelo de estados y utilidades de Kafka |
| `arrancar.sh`     | Lanza en un PC estaciones y operarios contra la Central con un solo comando |
| `prompts/`        | Prompts de IA usados durante el desarrollo |
| `PROTOCOLO.md`    | Mensajes de sockets y de Kafka, argumentos y parámetros |
| `Nombres.txt`     | Autores |

## Arranque rápido (Central en la nube)

La Central y Kafka están desplegadas en una máquina EC2 de AWS (IP fija `3.218.31.175`) con `docker compose`.
Arrancan solas al encender la máquina (`restart: unless-stopped`).

- Panel web: **http://3.218.31.175:8080**
- En cada PC que vaya a tener estaciones y operarios (con Docker abierto):

```
./arrancar.sh <estaciones> <operarios>        # ejemplo: ./arrancar.sh 3 3
```

El script espera a que respondan los puertos 5050 y 9092, elige solo los números (el primer PC coge ST1.. y OP1..,
el siguiente sigue donde acabó el anterior), da de alta a los operarios y lanza todo. Se lanza en un PC y luego en
el otro, no a la vez. Para recoger: `./WM_FO/parar_operarios.sh; ./WM_WS/parar_estaciones.sh`.

Usar un operario: `docker attach fo-op1` (salir sin cerrarlo: Ctrl+P y Ctrl+Q; en Git Bash de Windows, `winpty` delante).

La red de la universidad bloquea los puertos 5050 y 9092: para llegar a AWS hay que usar otra red (por ejemplo, datos del móvil).

## Lanzar cada módulo a mano

```
python WM_Central/WM_Central.py <puerto_sockets> <kafka_ip:puerto> [puerto_web]
python WM_WS/WM_WS_M.py <puerto_escolta> <ip_central> <puerto_central> <id_estacion> <ubicacion>
python WM_WS/WM_WS_E.py <ip_kafka> <puerto_kafka> <ip_monitor> <puerto_monitor> <id_estacion> [caudal_l_min]
python WM_FO/WM_FO.py <ip_kafka> <puerto_kafka> <id_operador> [fichero]
```

Ningún valor está fijo en el código: puertos, IPs, IDs, ubicación y caudal van por argumento, y los tiempos
(latidos, timeouts, reintentos) por variables de entorno. La lista completa está en `PROTOCOLO.md`.

## Docker sin la nube (Central en un PC)

```
IP_KAFKA=<ip_de_este_pc> docker compose up -d --build                         # Kafka + Central
docker build -t wm .                                                          # una imagen para todos los módulos
./WM_WS/lanzar_estaciones.sh <cuantas> <ip_central> <puerto> <kafka> [primera] [caudal]
./WM_FO/lanzar_operarios.sh <cuantos> <kafka> [fichero] [primero]
```

Antes de lanzar operarios hay que darlos de alta: `altas <N>` en el panel web.

## Despliegue en AWS (una vez)

1. AWS Academy Learner Lab → Start Lab → EC2: instancia Ubuntu 24.04 (t3.small) con IP elástica.
2. Grupo de seguridad, reglas de entrada TCP: 22, 5050, 8080 y 9092 desde `0.0.0.0/0`.
3. Por `ssh`: instalar Docker, clonar el repositorio y `sudo IP_KAFKA=<ip_elastica> docker compose up -d --build`.

Al cambiar el código de la Central: `git pull` en la máquina y repetir el `docker compose up -d --build`.