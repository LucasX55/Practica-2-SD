import json #los mensajes viajan como texto JSON

import time #para esperar entre reintentos

import uuid #para inventar nombres de grupo unicos

from confluent_kafka import Producer, Consumer, KafkaError, KafkaException #la libreria de kafka para python
from confluent_kafka.admin import AdminClient, NewTopic #para crear topics

#nombre de los topics
T_PETICIONES = "wm_peticiones_riego" #FO O ENGINE -> CENTRAL
T_TELEMETRIA = "wm_telemetria" #ENGINE -> CENTRAL
T_ESTADOS = "wm_estado_estaciones" #CEntral -> FO

#########################################################################
# Topic de ordenes de una estacion: cada engine tiene el suyo (wm_ordenes_ST1...)
#########################################################################
def t_ordenes(st_id):
    #central -> engina de una estacion
    return f"wm_ordenes_{st_id}"


#########################################################################
# Topic de respuestas de un operario: cada FO solo recibe lo suyo (wm_respuestas_OP1...)
#########################################################################
def t_respuestas(op_id):
    #-central ->operario
    return f"wm_respuestas_{op_id}"



#########################################################################
# Crea los topics que le pasamos (si ya existen no pasa nada)
# Kafka puede tardar en arrancar, sobre todo en docker: por eso se reintenta
# hasta 15 veces, esperando 2 segundos entre intento e intento
#########################################################################
def crear_topics(servidor, nombres):

    admin = AdminClient({"bootstrap.servers": servidor}) #cliente de administracion: sirve para crear y borrar topics
    nuevos = []
    for nombre in nombres:
        nuevos.append(NewTopic(nombre, num_partitions=1, replication_factor=1)) #1 particion y 1 copia: tenemos un solo broker

    for intento in range(15): #como mucho 15 intentos
        try:
            futuros = admin.create_topics(nuevos) #devuelve un futuro por topic: el resultado llega despues
            for nombre in futuros:
                try:
                    futuros[nombre].result(timeout=5) #esperamos a que kafka confirme que lo ha creado
                    print(f"[Kafka] Topic creado: {nombre}")
                except KafkaException as e:
                    if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS: #si ya existia no es un error
                        raise
            return #todo creado: salimos
        except Exception as e: #kafka aun no esta listo: esperamos y lo volvemos a intentar
            print(f"[Kafka] Error creando topics: {e}")
            time.sleep(2)


#########################################################################
# Crea el productor, para enviar mensajes
#########################################################################
def crear_productor(servidor):
    return Producer({"bootstrap.servers": servidor})


#########################################################################
# Envia un diccionario a un topic, como texto JSON
#########################################################################
def enviar (productor, topic, datos):
    productor.produce(topic, value = json.dumps(datos).encode("utf-8")) #lo pasamos a JSON y a bytes, que es lo que viaja por kafka
    productor.poll(0) #deja que el productor termine envios pendientes


#########################################################################
# Crea un consumidor suscrito a unos topics
# solo_nuevos=True: grupo nuevo en cada arranque, solo lee lo que llegue a partir
# de ahora (peticiones, ordenes y estados: lo viejo ya no sirve).
# solo_nuevos=False: grupo fijo, al reiniciar sigue donde se quedo y no se pierde
# nada de lo que llego mientras estaba caido (eventos de la central, respuestas del FO)
#########################################################################
def crear_consumidor(servidor, grupo, topics, solo_nuevos=True):
    if solo_nuevos:
        grupo = f"{grupo}-{uuid.uuid4().hex[:6]}" #le añadimos 6 letras al azar: cada arranque es un grupo distinto
    consumidor = Consumer({
        "bootstrap.servers": servidor,
        "group.id": grupo,
        "auto.offset.reset": "latest", #un grupo nuevo empieza por el final, no lee lo antiguo
        "session.timeout.ms": 10000, #si un consumidor cae, kafka lo da por muerto a los 10 s (por defecto 45 s)
        "heartbeat.interval.ms": 3000, #cada cuanto le dice a kafka que sigue vivo
    })
    asignado = [] #se llena cuando kafka nos asigna los topics
    consumidor.subscribe(topics, on_assign=lambda c, particiones: asignado.append(True)) #on_assign avisa cuando kafka nos ha dado los topics
    if solo_nuevos:
        while not asignado: #esperamos a eso, para no perder los primeros mensajes
            consumidor.poll(0.5)
    return consumidor




#########################################################################
# Lee un mensaje: devuelve (topic, diccionario) o (None, None) si no ha llegado nada
#########################################################################
def leer(consumidor):
    msg = consumidor.poll(1.0) #espera hasta 1 segundo
    if msg is None: #no ha llegado nada
        return None, None
    if msg.error(): #kafka avisa de algo (por ejemplo, un topic que aun no existe)
        print(f"[Kafka] Aviso: {msg.error()}")
        return None, None
    return msg.topic(), json.loads(msg.value().decode("utf-8")) #pasamos el texto JSON a diccionario