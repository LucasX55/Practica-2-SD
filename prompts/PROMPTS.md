# Prompts de IA usados

Herramienta: Claude. Lo usamos sobre todo como tutor: para que nos explicara conceptos, revisara el
código que íbamos escribiendo, encontrara errores y propusiera correcciones, y para guiarnos en las pruebas.

- **Sockets, Kafka y lógica de la práctica (Central, Monitor, Engine, FO):** la IA nos daba explicaciones y código
  de ejemplo en el que basarnos, no la práctica hecha para copiar. El código lo escribimos nosotros a partir de eso;
  después la IA lo revisaba, nos señalaba fallos y proponía cambios concretos (líneas o fragmentos) que revisamos y
  aplicamos nosotros.
- **Despliegue y herramientas (Dockerfile, docker-compose, scripts `.sh`, `arrancar.sh`, pasos de AWS):** esto sí
  lo generó en buena parte la IA; nosotros lo adaptamos, lo probamos y corregimos lo que fallaba.
- **Pruebas:** la IA nos preparó guías de pruebas (en un PC, con Docker y con dos PCs) y nos ayudó a interpretar
  cada error que salía; las pruebas las hicimos nosotros en nuestros equipos, en el laboratorio y contra AWS.
- **Documentación y estudio:** la IA nos ayudó a redactar `Readme.md` y `PROTOCOLO.md`, a comentar el código y nos
  preparó páginas de estudio para entender a fondo lo que hace cada parte y entender la parte del otro.

### 1. Aprender y montar la práctica
- Partíamos casi de cero en sockets y Kafka, sin saber Docker y con poco Python → explicaciones desde lo básico antes de cada bloque.
- 2026-10-04 · "Queremos hacer la práctica nosotros paso a paso para saber montarla; guíame" → explicación de sockets TCP, tramas STX/DATOS/ETX/LRC con ACK/NACK, SQLite y cómo dividir la Central en bloques.
- 2026-10-05 · "Revisa todo con el enunciado por si acaso" → lista de cosas que faltaban o no cuadraban con el enunciado.
- 2026-10-13 · "Crea una guía explicativa paso a paso dividida por partes" → reparto del trabajo por módulos (Central y common; Monitor, Engine y FO) y orden de desarrollo.
- 2026-10-18 · "Queremos hacer lo que nos piden ni más ni menos, revisa nuestro código extra o inútil fuera" → decidimos quitar el token de autenticación de las estaciones y dejar en la Central solo el panel web como interfaz de órdenes.
- 2026-10-19 · "Nos acaban de pasar la guía de corrección, vamos a ajustar nuestro código a ella" → repaso punto por punto: sin valores fijos, N estaciones y operarios, riego con tiempo o sin límite, caídas, consola y panel web.

### 2. Revisión de la Central (sockets, Kafka, BD)
- "Te pasamos el código y nos dices si hay algo mal" → errores encontrados en nuestro código y cómo arreglarlos:
  la firma de `crear_consumidor` en `kafka_utils` (añadir `solo_nuevos`), un bloqueo (deadlock) en `cambiar_fuga`
  por llamar a `interrumpir` dentro del candado, una errata en la clave `id_operador`, los hilos que faltaban por
  arrancar en `main`, la ubicación al registrar y un bucle mal indentado en `cargar_estaciones`.
- "Antes de hacer esos cambios en kafka utils nos sale un error" / "solo nos salen estos" → configurar el intérprete y el entorno virtual en VS Code.
- "Ahora solo queda este error de Kafka con el grupo" → explicación de los grupos de consumidores de Kafka (grupo fijo frente a grupo nuevo con `latest`) y qué topic usa cada uno.
- "¿Lo ves todo bien?" → segunda revisión del código ya corregido.
- Separar el panel web en `panel_web.py` → cómo mover el servidor HTTP a su propio archivo sin cambiar lo que hace.

### 3. Revisión del Monitor, Engine y FO
- "Revisa lo que pasa en este error con `daemon`" → revisión del Monitor; se encontró que faltaba `daemon=True` en el hilo del Monitor.

### 4. Entender nuestro código
- "Estamos pensando en comentar el código… te dejamos a ti la tarea, que siga como nosotros explicamos las cosas… solo quiero comentarios, no tocar nada del código" → comentarios en nuestro estilo, sin cambiar el código.
- "Queremos entender en profundidad qué hace nuestro código… créame un HTML completo, explícalo para alguien que no sabe mucho del tema" → la IA generó una página de estudio ("Cómo funciona la Central") que fue la principal herramienta para entender la práctica. La fuimos ampliando con varios prompts:
  - "Repasa si con esto podemos aprender de 0 a 100" → revisó qué faltaba para alguien que empieza de cero y añadió las ideas base (Python, sockets, hilos, Kafka, JSON, BD) antes de entrar en nuestro código.
  - "Genera imágenes y vídeos auxiliares para hacer de guía, como un seguimiento de las órdenes" → añadió animaciones de las tramas y el LRC, los hilos con el candado, los grupos de Kafka, un riego completo y las caídas.
  - Cada apartado explica una parte de nuestro código con fragmentos reales, ejemplos de lo que sale por pantalla y qué pasaría si cambiara una línea. Con eso resolvimos la mayoría de nuestras dudas sobre sockets, Kafka e hilos, y entendimos los fallos que la IA había encontrado en la revisión (por ejemplo, por qué se bloqueaba `cambiar_fuga`).
  - Incluye preguntas que nos pueden hacer en la corrección y ejercicios para trazar nosotros solos qué pasa en cada caso.
  - También nos sirvió para seguir programando: al entender cómo funcionaban el protocolo y los consumidores de Kafka, pudimos corregir y completar nosotros partes de la Central.

  Preguntas que responde cada apartado:
  - **Ideas base**
    - A0 · ¿Qué Python necesitamos saber para entender el código (diccionarios, funciones, `with`, excepciones)?
    - A1 · ¿Qué es un sistema distribuido?
    - A2 · ¿Qué es un socket y cómo se conectan y se mandan datos dos programas?
    - A3 · ¿Qué es un hilo y por qué la Central necesita varios?
    - A4 · ¿Para qué sirve el candado (`Lock`) y qué es un deadlock?
    - A5 · ¿Cómo funciona Kafka: topics, productores, consumidores y grupos?
    - A6 · ¿Qué es JSON y por qué lo usamos en los mensajes?
    - A7 · ¿Para qué sirve la base de datos y por qué sobrevive a una caída?
    - A8 · ¿Qué hacen el Monitor, el Engine y el FO?
  - **Nuestro código, archivo por archivo**
    - B1 · ¿Cómo viaja un mensaje por el socket (STX/ETX, LRC, ACK/NACK)?
    - B2 · ¿Qué estados puede tener una estación y cuándo cambia de uno a otro?
    - B3 · ¿Qué guarda la base de datos de la Central?
    - B4 · ¿Qué hacen las funciones de `kafka_utils.py`?
    - B5 · ¿Qué pasa cuando arranca la Central y qué hilos lanza?
    - B6 · ¿Cómo se registra y se autentica un Monitor, y para qué son los latidos?
    - B7 · ¿Qué hace la Central cuando hay una fuga o una avería?
    - B8 · ¿Qué pasa si se cae un Monitor?
    - B9 · ¿Qué hace la Central con una petición de riego?
    - B10 · ¿Qué cuenta el Engine durante el riego y qué hace la Central con eso?
    - B11 · ¿Cómo funcionan parar, bloquear y las órdenes del panel?
    - B12 · ¿Cómo funcionan el panel de consola y el panel web?
    - B13 · ¿Qué hace la Central cuando vuelve después de una caída?
  - **Todo junto**
    - C1 · ¿Qué pasa, paso a paso, en un riego completo?
    - C2 · ¿Qué pasa cuando se cae cada pieza?
    - C3 · ¿Qué nos pueden preguntar en la corrección?
    - C4 · ¿Dónde puede correr la Central: en local, en Docker y en la nube?
    - C5 · ¿Sé trazar nosotros solos lo que pasa en cada caso? (ejercicios)
    - C6 · ¿Qué significa cada término? (glosario)

### 5. Prompts en los que la IA generó código
- 2026-10-13 · "Crea una guía paso a paso dividida por partes con el código necesario" → guía por bloques con código de ejemplo para cada parte, que usamos como base para escribir el nuestro.
- Código repetitivo o mecánico → la IA generó partes que se repetían o eran de plantilla y que luego adaptamos:
  - las funciones parecidas entre sí de la Central (las respuestas a cada mensaje del Monitor, los avisos al operario con cada estado del riego);
  - las funciones de `kafka_utils.py` (crear topics, productor, consumidor) a partir de los ejemplos del tutorial de Kafka;
  - la lectura de argumentos y variables de entorno de cada programa;
  - el HTML y CSS del panel web y la tabla de colores del panel de consola;
  - los mensajes de log y de error.
- Separar el panel web → la IA escribió `panel_web.py` moviendo el servidor HTTP que ya tenía la Central.
- Corrección de errores → cuando algo fallaba al probar, la IA nos devolvía la función corregida y nosotros la comparábamos con la nuestra antes de cambiarla.

### 6. Pruebas
- "¿Abro Windows o Ubuntu?" / "guíame desde ahí para hacer todas las pruebas" / "hazme la guía de nuevo para probar la práctica desde dos ordenadores diferentes" → guía de pruebas y ficheros de comandos para Ubuntu y Windows.
- Errores al probar (`ModuleNotFoundError`, `ipconfig` no encontrado, permisos de los `.sh`, nombre del compose…) → explicación de cada error y cómo resolverlo.
- "¿Nos pueden pedir un número concreto de máquinas de riego?" → cómo se parametriza el número de estaciones.
- "¿Podemos modificar el caudal?" / "¿pueden ser dinámicos?" → el caudal va por parámetro en cada estación.

### 7. Docker y despliegue
- "Vamos a pasar a Docker" → Dockerfile, docker-compose (Kafka + Central) y scripts para lanzar N estaciones y operarios.
- "Vamos con lo de Railway, explícamelo todo" → Railway tenía los registros cerrados; alternativa con AWS Academy (EC2, IP elástica, grupo de seguridad).
- "¿Se puede con AWS o es muy difícil?" y todo el proceso → despliegue paso a paso y diagnóstico de por qué las estaciones no conectaban (la red de la universidad bloquea los puertos 5050 y 9092).
- "¿Cómo pruebo con dos PCs diferentes?" → pruebas distribuidas con la Central en AWS.
- "Llegan los mensajes pero no de la Central al operario" → explicación (topic recién creado) y solución reiniciando el FO.
- "¿Hay alguna forma de automatizar más el lanzamiento?" / "que el único parámetro sea el número de estaciones o de operarios" → script `arrancar.sh` y `restart: unless-stopped` en el compose.

### 8. Documentación
- Actualizar `PROTOCOLO.md` y `Readme.md`, y preparar el guion de la corrección.
-Ayudar a comentar gran parte de central BD y web por su gran volumen de lineas (pricipalmente lineas auxiliares)