# Protocolo WaterManagement

## 1. Sockets (Monitor ↔ Central, Engine ↔ Monitor)

Todas las tramas van empaquetadas con `common/protocol.py`: `<STX>DATOS<ETX><LRC>`,
donde LRC es el XOR byte a byte de DATOS. Quien recibe responde `ACK` (0x06) si el LRC
cuadra o `NACK` (0x15) si no; el emisor reintenta hasta 3 veces.
Campos separados por `#`. En cada socket solo pregunta un lado y el otro responde.

| Quién → quién     | Mensaje                   | Respuesta                                   | Cuándo |
|-------------------|---------------------------|---------------------------------------------|--------|
| Monitor → Central | `REGISTRAR#id#ubicacion`  | `OK`                                        | Al arrancar, si se le pasa la ubicación (alta o cambio de ubicación) |
| Monitor → Central | `AUTENTICAR#id`           | `OK` / `KO#estacion no registrada` / `KO#estacion ya conectada` | Al arrancar y al reconectar |
| Monitor → Central | `FUGA#id`                 | `OK` / `KO#motivo`                          | El Engine responde KO o deja de responder |
| Monitor → Central | `RESUELTA#id`             | `OK` / `KO#motivo`                          | El Engine vuelve a responder OK |
| Monitor → Central | `PING#id`                 | `OK`                                        | Cada `WM_LATIDO` segundos (2 por defecto) |
| Monitor → Engine  | `STATUS#id`               | `OK` / `KO`                                 | Cada segundo |

- Central da una estación por desconectada si pasan `WM_TIMEOUT_MONITOR` segundos (6) sin recibir nada de su Monitor.
- El Engine responde `KO` mientras tiene una avería o todavía no escucha órdenes de Kafka.

## 2. Kafka

Todos los mensajes son JSON.

| Topic                    | Quién escribe → quién lee | Contenido |
|--------------------------|---------------------------|-----------|
| `wm_peticiones_riego`    | FO o Engine → Central     | Regar: `{"id_operador": "OP1", "id_ws": "ST1", "duracion_s": 30}` (0 = sin límite) · Parar: `{"accion": "PARAR", "id_operador": "OP1", "id_ws": "ST1"}` |
| `wm_ordenes_<ID_WS>`     | Central → Engine          | `{"accion": "INICIAR", "id_operador": "OP1", "duracion_s": 30}` · `{"accion": "DETENER", "motivo": "FUGA"}` · `{"accion": "DENEGADO", "motivo": "..."}` |
| `wm_telemetria`          | Engine → Central          | `{"tipo": "ARRANQUE", "id_ws": "ST1"}` · `{"tipo": "INICIADO", "id_ws": "ST1", "id_operador": "OP1"}` · `{"tipo": "TELEMETRIA", "id_ws": "ST1", "caudal": 12.0, "volumen": 0.4}` · `{"tipo": "FINALIZADO", "id_ws": "ST1", "motivo": "TIEMPO", "volumen": 6.0, "duracion_s": 30.2}` |
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

- `id_operador` especial: `MANUAL` (pedido desde el menú de la estación) y `CENTRAL` (orden de Central).
- `REGANDO` lleva `caudal` y `volumen`; `DENEGADO` lleva `motivo`; `INTERRUMPIDO` lleva `motivo` y `volumen`;
  `FINALIZADO` lleva `motivo`, `volumen_total` y `duracion_s`.
- Motivos de fin: `TIEMPO`, `PARADO_EN_ESTACION`, `PARADO_POR_<operario>`, `PARADO_POR_CENTRAL`, `BLOQUEADA`, `FUGA`,
  `DESCONECTADA`, `MONITOR_CAIDO`, `AVERIA`, `AVERIA_ENGINE`, `ENGINE_REINICIADO`.

## 3. Fichero de servicios del FO

Una línea por servicio: `ESTACION,SEGUNDOS`. Las líneas vacías y las que empiezan por `#` se ignoran.

## 4. Argumentos y parámetros

| Programa      | Argumentos |
|---------------|------------|
| `WM_Central`  | `<puerto_sockets> <kafka_ip:puerto> [puerto_web]` |
| `WM_WS_M`     | `<puerto_escolta> <ip_central> <puerto_central> <id> [ubicacion]` |
| `WM_WS_E`     | `<kafka_ip:puerto> <ip_monitor> <puerto_monitor> [caudal_l_min]` |
| `WM_FO`       | `<kafka_ip:puerto> <id_operador> [fichero]` |

Variables de entorno (opcionales): `WM_DB`, `PORT`, `WM_TIMEOUT_MONITOR` (Central);
`WM_TIMEOUT_ENGINE`, `WM_TIMEOUT_CENTRAL`, `WM_LATIDO`, `WM_REINTENTO` (Monitor);
`WM_TIEMPO_RESPUESTA`, `WM_REINTENTO`, `WM_TIMEOUT_MONITOR`, `WM_CARPETA_ESTADO` (Engine);
`WM_TIEMPO_RESPUESTA`, `WM_TIEMPO_CENTRAL`, `WM_ESPERA_FICHERO` (FO).
