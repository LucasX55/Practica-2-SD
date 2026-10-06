import socket
import sys
import threading
import time
from kafka import KafkaConsumer,KafkaProducer
import os
import json

sys.path.insert(0,os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from common.protocol import send_message,recv_message,ConnectionClosed

estado_actual= "OK"
regando= False
volumen_acumulado= 0.0



def escuchar_fuga():
    global estado_actual,regando
    print(f"Pon F y pulsa ENTER para simular una fuga")
    while True:
        tecla = input()
        if tecla.upper() == "F":
            estado_actual = "KO"
            print("Se ha producido una fuga. Se enviará el mensaje al monitor")
            if estado_actual=="KO":
                regando=False


def escuchar_monitor(cliente_socket):
    global estado_actual
    connected = True

    while connected:
        try:
            msg= recv_message(cliente_socket)
            
            if "STATUS" in msg:
                send_message(cliente_socket,estado_actual)
                if estado_actual=="KO":
                    print("Se ha producido una fuga")
        except(Exception,ConnectionClosed):
            print("Error de conexión con el monitor.")
            connected = False



def productor(ip_kafka,puerto_kafka,st_id):
    global regando,volumen_acumulado,estado_actual
    productor=KafkaProducer(
        bootstrap_servers= [f"{ip_kafka}:{puerto_kafka}"],
        value_serializer= lambda x:json.dumps(x).encode('utf-8')
    ) 
    caudal=10.0/60
    while True:
        if regando and estado_actual == 'OK':
            volumen_acumulado +=caudal
            datos={"id_ws": st_id, "caudal": 10.0,"volumen_acumulado": round(volumen_acumulado,2)}
            productor.send("wm_telemetria", value=datos)
            
        time.sleep(1)
            
            
def consumidor(ip_kafka,puerto_kafka,st_id):
    global regando,volumen_acumulado,estado_actual
    consumidor=KafkaConsumer(
        f'wm_ordenes_{st_id}',
        bootstrap_servers= [f"{ip_kafka}:{puerto_kafka}"],
        value_deserializer= lambda x:json.loads(x.decode('utf-8')),
        auto_offset_reset='latest'
    )
    
    for mensaje in consumidor:
        orden=mensaje.value
        if orden.get("accion")=="INICIAR" and estado_actual=="OK":
            regando=True
            print("Valvula abierta")
            
        elif orden.get("accion")=="DETENER":
            regando=False
            print("Valvula cerrada")
 

if  (len(sys.argv) == 6):
    IP_KAFKA = sys.argv[1]
    PUERTO_KAFKA = int(sys.argv[2])    
    IP_MONITOR = sys.argv[3]
    PUERTO_MONITOR = int(sys.argv[4])
    ST_ID = sys.argv[5]
    
    threading.Thread(target=escuchar_fuga, daemon=True).start()
    threading.Thread(target=productor,args=(IP_KAFKA, PUERTO_KAFKA, ST_ID), daemon=True).start()
    threading.Thread(target=consumidor,args=(IP_KAFKA, PUERTO_KAFKA, ST_ID), daemon=True).start()



    ADDR_MONITOR = (IP_MONITOR, PUERTO_MONITOR)
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        client_socket.connect(ADDR_MONITOR)
        print (f"Establecida conexión en [{ADDR_MONITOR}]")

        escuchar_monitor(client_socket)
        
    except Exception as e:
        print(f"No se pudo conectar al monitor: {e}")

    finally:
        client_socket.close()
        print("Conexión con el monitor cerrada.")
        
    
else:
    print("Error")