import json

import time

import uuid

from confluent_kafka import Producer, Consumer, KafkaError, KafkaException  
from confluent_kafka.admin import AdminClient, NewTopic

#nombre de los topics
T_PETICIONES = "wm_peticiones_riego" #FO O ENGINE -> CENTRAL
T_TELEMETRIA = "wm_telemetria" #ENGINE -> CENTRAL
T_ESTADOS = "wm_estado_estaciones" #CEntral -> FO

def t_ordenes(st_id):
    #central -> engina de una estacion
    return f"wm_ordenes_{st_id}"


def t_respuestas(op_id):
    #-central ->operario
    return f"wm_respuestas_{op_id}"



def crear_topics(servidor, nombres):

    admin = AdminClient({"bootstrap.servers": servidor})
    nuevos = []
    for nombre in nombres:
        nuevos.append(NewTopic(nombre, num_partitions=1, replication_factor=1))

    for intento in range(15):
        try:
            futuros = admin.create_topics(nuevos)
            for nombre in futuros:
                try:
                    futuros[nombre].result(timeout=5)
                    print(f"[Kafka] Topic creado: {nombre}")
                except KafkaException as e:
                    if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                        raise
            return
        except Exception as e:
            print(f"[Kafka] Error creando topics: {e}")
            time.sleep(2)


def crear_productor(servidor):
    return Producer({"bootstrap.servers": servidor})


def enviar (productor, topic, datos):
    productor.produce(topic, value = json.dumps(datos).encode("utf-8"))
    productor.poll(0)  


def crear_consumidor(servidor, nombre, topics, solo_nuevos=True):
    if solo_nuevos:
        grupo = f"{grupo}-{uuid.uuid4().hex[:6]}"
    consumidor = Consumer({
        "bootstrap.servers": servidor,
        "group.id": grupo,
        "auto.offset.reset": "latest"
    })
    asignado = []
    consumidor.subscribe(topics, on_assign=lambda c, particiones: asignado.append(True))
    if solo_nuevos:
        while not asignado:
            consumidor.poll(0.5)
    return consumidor




def leer(consumidor):
    msg = consumidor.poll(1.0)
    if msg is None:
        return None, None
    if msg.error():
        print(f"[Kafka] Aviso: {msg.error()}")
        return None, None
    return msg.topic(), json.loads(msg.value().decode("utf-8"))