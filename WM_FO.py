import socket
import sys
import threading
import time
import json
from kafka import KafkaProducer, KafkaConsumer


FORMAT= 'utf-8'

riego_activo = False


def consumidor(ip_kafka, puerto_kafka, ip_operador):
    
    global riego_activo
    
    consumidor = KafkaConsumer(f'{ip_operador}',Bootstrap_server=[f"{ip_kafka},{puerto_kafka}"],
                               value= lambda x: json.loads(x.decode(FORMAT)), offset_reset= 'Último')
    
    
    print(f"[{ip_operador}] Escuchando a la cenrtal")
    
    for mensaje in consumidor:
        res = mensaje.value
        print(f"{ip_operador} Mensaje proveniente de la Central: {res}")
        
        if res.get("estado") in ["FIN", "ERROR", "DENEGADO"]:
            riego_activo= False