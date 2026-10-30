# Protocolo WaterManagement

## 1. Sockets (Monitor ↔ Central, Engine ↔ Monitor)

Todas las tramas van empaquetadas con `common/protocol.py`: `<STX>DATOS<ETX><LRC>`,
donde LRC es el XOR byte a byte de DATOS. Quien recibe responde `ACK` (0x06) si el LRC
cuadra o `NACK` (0x15) si no; el emisor reintenta hasta 3 veces.
Campos separados por `#`. En cada socket solo pregunta un lado y el otro responde.

| Quién → quién     | Mensaje                   | Respuesta                                   | Cuándo |
|-------------------|---------------------------|---------------------------------------------|--------|
| Monitor → Central | `REGISTRAR#id#ubicacion`  | `OK`                                        | Al arrancar y al reconectar (alta, o cambio de ubicación si ya existía) |
| Monitor → Central | `AUTENTICAR#id`           | `OK` / `KO#estacion no registrada` / `KO#estacion ya conectada` | Al arrancar y al reconectar |
| Monitor → Central | `FUGA#id`                 | `OK` / `KO#motivo`                          | El Engine responde KO o deja de responder |
| Monitor → Central | `RESUELTA#id`             | `OK` / `KO#motivo`                          | El Engine vuelve a responder OK |
| Monitor → Central | `PING#id`                 | `OK`                                        | Cada `WM_LATIDO` segundos (2 por defecto) |
| Monitor → Engine  | `STATUS#id`               | `OK` / `KO`                                 | Cada segundo |

- Central da una estación por desconectada si pasan `WM_TIMEOUT_MONITOR` segundos (10) sin recibir nada de su Monitor.
- El Monitor empieza en avería (manda `FUGA` al autenticarse) hasta que su Engine contesta `OK`.
- El Engine responde `KO` mientras tiene una avería. Si pasa `WM_TIMEOUT_MONITOR` segundos (5) sin `STATUS`, cierra la válvula (`MONITOR_CAIDO`).

## 2. Kafka

Todos los mensajes son JSON.

| Topic                    | Quién escribe → quién lee | Contenido |
|--------------------------|---------------------------|-----------|
| `wm_peticiones_riego`    | FO o Engine → Central     | Regar: `{"id_operador": "OP1", "id_ws": "ST1", "duracion_s": 30}` (0 = sin límite) · Parar: `{"accion": "PARAR", "id_operador": "OP1", "id_ws": "ST1"}` |
| `wm_ordenes_<ID_WS>`     | Central → Engine          | `{"accion": "INICIAR", "id_operador": "OP1", "duracion_s": 30}` · `{"accion": "DETENER", "motivo": "FUGA"}` · `{"accion": "DENEGADO", "motivo": "..."}` |
| `wm_telemetria`          | Engine → Central          | `{"tipo": "ARRANQUE", "id_ws": "ST1"}` · `{"tipo": "INICIADO", "id_ws": "ST1", "id_operador": "OP1"}` · `{"tipo": "TELEMETRIA", "id_ws": "ST1", "caudal": 10.0, "volumen": 0.33}` · `{"tipo": "FINALIZADO", "id_ws": "ST1", "motivo": "TIEMPO", "volumen": 5.0, "duracion_s": 30.2}` |
| `wm_respuestas_<ID_OP>`  | Central → un FO           | `{"estado": "RECIBIDA" \| "AUTORIZADO" \| "DENEGADO" \| "INICIADO" \| "REGANDO" \| "INTERRUMPIDO" \| "FINALIZADO", "id_ws": "ST1", ...}` |
| `wm_estado_estaciones`   | Central → todos los FO    | `{"estaciones": [{"id": "ST1", "ubicacion": "Jardin 1", "estado": "DISPONIBLE", "caudal": 0.0, "volumen": 0.0, "operario": ""}]}` (cada segundo) |

Grupos de consumidores:

| Quién lee | Topic | Grupo | Por qué |
|-----------|-------|-------|---------|
| Central   | `wm_peticiones_riego` | nuevo en cada arranque, `latest` | Con Central caída no se admiten peticiones |
| Central   | `wm_telemetria` | fijo (`central-eventos`) | Al volver, Central procesa lo que pasó mientras estaba caída |
| Engine    | `wm_ordenes_<ID>` | nuevo en cada arranque, `latest` | Un Engine reiniciado no debe ejecutar órdenes viejas |
| FO        | `wm_respuestas_<ID>` | fijo (`fo-<ID>`) | Al volver, el FO ve el resultado de su servicio |
| FO        | `wm_estado_estaciones` | nuevo en cada arranque, `latest` | Solo interesa el estado actual |

- `id_operador` especial: `MANUAL` (pedido con la tecla `R` en el Engine) y `CENTRAL` (orden de Central). El FO añade `"accion": "SOLICITAR"` a sus peticiones; la Central solo mira si es `PARAR`.
- `REGANDO` lleva `caudal` y `volumen`; `DENEGADO` lleva `motivo`; `INTERRUMPIDO` lleva `motivo` y `volumen`;
  `FINALIZADO` lleva `motivo`, `volumen_total` y `duracion_s`.
- Motivos de fin: `TIEMPO`, `PARADO_EN_ESTACION`, `PARADO_POR_<operario>`, `PARADO_POR_CENTRAL`, `BLOQUEADA`, `FUGA`,
  `DESCONECTADA`, `MONITOR_CAIDO`, `AVERIA`, `ENGINE_REINICIADO`.

## 3. Fichero de servicios del FO

Una línea por servicio: `ESTACION,SEGUNDOS`. Las líneas vacías y las que empiezan por `#` se ignoran.

## 4. Argumentos y parámetros

| Programa      | Argumentos |
|---------------|------------|
| `WM_Central`  | `<puerto_sockets> <kafka_ip:puerto> [puerto_web]` |
| `WM_WS_M`     | `<puerto_escolta> <ip_central> <puerto_central> <id_estacion> <ubicacion>` |
| `WM_WS_E`     | `<ip_kafka> <puerto_kafka> <ip_monitor> <puerto_monitor> <id_estacion> [caudal_l_min]` (10 por defecto) |
| `WM_FO`       | `<ip_kafka> <puerto_kafka> <id_operador> [fichero]` |
| `lanzar_estaciones.sh` | `<cuantas> <ip_central> <puerto_central> <kafka_ip:puerto> [primera] [caudal]` |
| `lanzar_operarios.sh`  | `<cuantos> <kafka_ip:puerto> [fichero] [primero]` |
| `arrancar.sh`          | `<estaciones> <operarios>` (IP de la Central dentro del script; numeración automática, operarios ≤ estaciones) |

Variables de entorno (opcionales): `WM_DB`, `PORT`, `WM_TIMEOUT_MONITOR` (Central);
`WM_TIMEOUT_ENGINE`, `WM_TIMEOUT_CENTRAL`, `WM_LATIDO`, `WM_REINTENTO` (Monitor);
`WM_TIMEOUT_MONITOR`, `WM_REINTENTO` (Engine); `WM_TIEMPO_CENTRAL` (FO).