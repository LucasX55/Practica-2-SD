import socket
import sys
import threading
import time
import os
import json

sys.path.insert(0,os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from common.protocol import send_message,recv_message,ConnectionClosed
from common import kafka_utils as k

TIMEOUT_MONITOR = float(os.environ.get("WM_TIMEOUT_MONITOR", 5))
REINTENTO= float(os.environ.get("WM_REINTENTO",3))

estado_actual= "OK"
regando= False
volumen_acumulado= 0.0
duracion_riego= 0
inicio_riego= 0.0
monitor_ok= False
productor_kafka= None

def evento(datos):
    datos["id_ws"]=ST_ID
    k.enviar(productor_kafka,k.T_TELEMETRIA,datos)



def cerrar_valvula(motivo):
    global regando
    if not regando:
        return
    regando=False
    duracion= round(time.time()-inicio_riego,1)
    print(f"Valvula cerrada ({motivo}). {round(volumen_acumulado,2)} L en {duracion} s")
    evento({"tipo": "FINALIZADO", "motivo": motivo, "volumen": round(volumen_acumulado,2), "duracion": duracion})



def escuchar_teclado():
    global estado_actual

    print(f"Pon F y pulsa ENTER para simular una fuga (otra vez F para arreglarla)")
    print(f"R [segundos] + ENTER: pedir un riego (sin segundos= sin limite) | P + ENTER: parar el riego")
    while True:
        try: 
            tecla = input().strip().upper()

        except EOFError:
            return

        if tecla == "F":
            if estado_actual == "OK":
                estado_actual = "KO"
                print("Se ha producido una fuga. Se enviará el mensaje al monitor")
                
                cerrar_valvula("FUGA")
            else:
                estado_actual = "OK"
                print("Fuga resuelta")

        elif tecla.startswith("R"):
            partes= tecla.split()
            segundos = 0
            if len(partes) > 1 and partes[1].isdigit():
                segundos = int(partes[1])

            if estado_actual == "KO" or not monitor_ok:
                print("Estacion AVERIADA: no se puede regar")
                continue

            k.enviar(productor_kafka, k.T_PETICIONES, {"id_operador": "MANUAL", "id_ws": ST_ID, "duracion_s": segundos})
            print("Petición de riego enviada a la central")

        elif tecla == "P":
            cerrar_valvula("PARADO_EN_ESTACION")
            


def escuchar_monitor(cliente_socket):
    global estado_actual, monitor_ok
    connected = True
    cliente_socket.settimeout(TIMEOUT_MONITOR)

    while connected:
        try:
            msg= recv_message(cliente_socket)
            
            if "STATUS" in msg:
                monitor_ok =True
                send_message(cliente_socket, estado_actual)
                if estado_actual == "KO":
                    print ("Se ha producido una fuga")


        except (Exception, ConnectionClosed):
            print(f"Error al recibir mensaje del monitor. Estacion AVERIADA")
            monitor_ok = False
            cerrar_valvula("MONITOR_CAIDO")
            connected = False



def productor(st_id):
    global volumen_acumulado
    while True:
        if regando and estado_actual == 'OK':
            if duracion_riego > 0 and (time.time() - inicio_riego) >= duracion_riego:
                cerrar_valvula("TIEMPO")

            else:
                volumen_acumulado += CAUDAL/60
                evento({"tipo": "TELEMETRIA", "caudal": CAUDAL, "volumen": round(volumen_acumulado,2)})            
        time.sleep(1)
            
            
def consumidor(st_id):
    global regando, volumen_acumulado, duracion_riego, inicio_riego
    topic_ordenes= k.t_ordenes(st_id)
    k.crear_topics(KAFKA, [topic_ordenes])

    consumidor= k.crear_consumidor(KAFKA, f"engine-{st_id}", [topic_ordenes])

    while True:
        topic, orden= k.leer(consumidor)

        if orden is None:
            continue

        if orden.get("accion") == "INICIAR" and estado_actual=="OK" and not regando:
            volumen_acumulado= 0.0
            duracion_riego= int(orden.get("duracion_s",0))
            inicio_riego= time.time()
            regando= True
            print(f"Valvula abierta")
            evento({"tipo": "INICIADO", "id_operador": orden.get("id_operador")})

        elif orden.get("accion") == "INICIAR":
            evento({"tipo": "FINALIZADO", "motivo": "AVERIA", "volumen": 0.0, "duracion_s": 0})

        elif orden.get("accion")== "DETENER": 
            cerrar_valvula(orden.get("motivo","ORDEN_CENTRAL"))

        elif orden.get("accion")== "DENEGADO":
            print(f"Riego denegado: {orden.get('motivo')}")

        

if  (len(sys.argv) in (6,7)):
    IP_KAFKA = sys.argv[1]
    PUERTO_KAFKA = int(sys.argv[2])    
    IP_MONITOR = sys.argv[3]
    PUERTO_MONITOR = int(sys.argv[4])
    ST_ID = sys.argv[5]
    CAUDAL= float(sys.argv[6]) if len(sys.argv) == 7 else 10.0
    KAFKA= f"{IP_KAFKA}:{PUERTO_KAFKA}"

    productor_kafka= k.crear_productor(KAFKA)
    evento({"tipo": "ARRANQUE"})

    threading.Thread(target=escuchar_teclado, daemon=True).start()
    threading.Thread(target=productor,args=(ST_ID,), daemon=True).start()
    threading.Thread(target=consumidor,args=(ST_ID,), daemon=True).start()



    ADDR_MONITOR = (IP_MONITOR, PUERTO_MONITOR)

    while True:
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

        time.sleep(REINTENTO)
    
else:
    print("Uso: python WM_WS_E.py <ip_kafka> <puerto_kafka> <ip_monitor> <puerto_monitor> <id_estacion> [caudal_l_min]")