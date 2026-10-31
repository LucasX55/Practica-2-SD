#Librerias
import socket
import sys
import threading
import time
import os
import json

sys.path.insert(0,os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from common.protocol import send_message,recv_message,ConnectionClosed
from common import kafka_utils as k

#Si el monitor tarda mas de esto en manda status, se tiene en cuenta que se ha caido
TIMEOUT_MONITOR = float(os.environ.get("WM_TIMEOUT_MONITOR", 5))

#Segundos antes de volver a intentar conectar con el monitor
REINTENTO= float(os.environ.get("WM_REINTENTO",3))

#Se comparten los estados entre los hilos
estado_actual= "OK"                         #OK o KO. Es lo que contesta al status
regando= False                              #Si esta a True, la valvula esta abierta
volumen_acumulado= 0.0                      #litros regados en el riego actual
duracion_riego= 0                           #segundos que dura el riego (Si esta a 0 es que no hay limite)
inicio_riego= 0.0                           #momento en el que se abre la valvula
monitor_ok= False                           #True mientras conteste el monitor
productor_kafka= None                       #Productor de kafka


#Manda un evento a la central por el topic de telemetria
def evento(datos):
    datos["id_ws"]=ST_ID                    #Se agrega el id de la estacion
    k.enviar(productor_kafka,k.T_TELEMETRIA,datos)


#Cierra la valvula y avisa a la central con un FINALIZADO, si ya estaba cerrada, no hace nada, asi no se mandan duplicados.
# Motivos posibles: Fuga, Monitor que se ha caido , tiempo, parado en estacion y orden central 
def cerrar_valvula(motivo):
    global regando
    if not regando:
        return
    regando=False
    duracion= round(time.time()-inicio_riego,1)
    print(f"Valvula cerrada ({motivo}). {round(volumen_acumulado,2)} L en {duracion} s")
    evento({"tipo": "FINALIZADO", "motivo": motivo, "volumen": round(volumen_acumulado,2), "duracion_s": duracion})


#Hilo del teclado, para simular cosas en la estacion 
def escuchar_teclado():
    global estado_actual

    print(f"Pon F y pulsa ENTER para simular una fuga (otra vez F para arreglarla)")
    print(f"R [segundos] + ENTER: pedir un riego (sin segundos= sin limite) | P + ENTER: parar el riego")
    while True:
        try: 
            tecla = input().strip().upper()

        except EOFError:
            
            #No hay teclado, el hilo termina sin molestar
            return

        if tecla == "F":
            if estado_actual == "OK":
                estado_actual = "KO"

                #Fuga, pasa da KO (el monitor se entera en su proximo status) y se cierra la valvula
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

            #No se puede pedir riego si hay fuga o el monitor no responde
            if estado_actual == "KO" or not monitor_ok:
                print("Estacion AVERIADA: no se puede regar")
                continue

            #La peticion no abre la valvula, la central decide, tendra que contestar con INICIAR o DENEGADO
            k.enviar(productor_kafka, k.T_PETICIONES, {"id_operador": "MANUAL", "id_ws": ST_ID, "duracion_s": segundos})
            print("Petición de riego enviada a la central")

        elif tecla == "P":
            cerrar_valvula("PARADO_EN_ESTACION")
            


#Conversacion con el monitor, el monitor cada segundo manda STATUS#id y el engina contestara con KO o OK
#SI el monitor no escribe en TIMEOUT_MONITOR segundos o se cierra la conexion, la estacion se considera averiada, se cierra la valvula y se sale del bucle
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



        except (Exception, ConnectionClosed):
            print(f"Error al recibir mensaje del monitor. Estacion AVERIADA")
            monitor_ok = False
            cerrar_valvula("MONITOR_CAIDO")
            connected = False


#Hilo que simula el agua, cada segundo, si hay riego y no hay fuga,
#si pasa la duracion pedida, se cierra la valvula
#si no, suma el volumen de ese segundo
#y manda una TELEMETRIA con el caudal y los litros acumulados
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
            

#Hilo que escucha las ordenes de la central en el topic de ordenes de esta estacion
#El grupo de consumidores es fijo, asi que cuando se relanza el engine, no se repiten ordenes antiguas que se hayan leido
def consumidor(st_id):
    global regando, volumen_acumulado, duracion_riego, inicio_riego
    topic_ordenes= k.t_ordenes(st_id)
    k.crear_topics(KAFKA, [topic_ordenes])

    consumidor= k.crear_consumidor(KAFKA, f"engine-{st_id}", [topic_ordenes])

    while True:
        topic, orden= k.leer(consumidor)

        #Nada nuevo en el intervalo, asi que se sigue esperando
        if orden is None:
            continue


        #INICIAR y la estacion esta bien y parada, se abre la valvula y se avisa con INICIADO
        if orden.get("accion") == "INICIAR" and estado_actual=="OK" and not regando:
            volumen_acumulado= 0.0
            duracion_riego= int(orden.get("duracion_s",0))
            inicio_riego= time.time()
            regando= True
            print(f"Valvula abierta")
            evento({"tipo": "INICIADO", "id_operador": orden.get("id_operador")})

        
        #INICIAR pero hay averia o se estaba regando -> se responde con FINALIZADO
        #por AVERIA para que la central y el operario no se queden esperando
        elif orden.get("accion") == "INICIAR":
            evento({"tipo": "FINALIZADO", "motivo": "AVERIA", "volumen": 0.0, "duracion_s": 0})

        #DETENER la Central o un operario a traves de ella, se pide cerrar la valvula
        elif orden.get("accion")== "DETENER": 
            cerrar_valvula(orden.get("motivo","ORDEN_CENTRAL"))

        #DENEGADO, la central rechazo el riego pedido con la tecla R
        elif orden.get("accion")== "DENEGADO":
            print(f"Riego denegado: {orden.get('motivo')}")

        
######################### MAIN ##########################

if  (len(sys.argv) in (6,7)):
    IP_KAFKA = sys.argv[1]
    PUERTO_KAFKA = int(sys.argv[2])    
    IP_MONITOR = sys.argv[3]
    PUERTO_MONITOR = int(sys.argv[4])
    ST_ID = sys.argv[5]
    CAUDAL= float(sys.argv[6]) if len(sys.argv) == 7 else 10.0      #litros por minuto (Son 10 por defecto)
    KAFKA= f"{IP_KAFKA}:{PUERTO_KAFKA}"

    #Avisa a la centaral de que el engine acaba de arrancar, si la central creia que habia un riego en curso,
    #asi se sabev que ese riego ya no sigue
    productor_kafka= k.crear_productor(KAFKA)
    evento({"tipo": "ARRANQUE"})

    #Hilos en segundo plano (daemon): teclado, simulacion del agua y ordenes de la central
    threading.Thread(target=escuchar_teclado, daemon=True).start()
    threading.Thread(target=productor,args=(ST_ID,), daemon=True).start()
    threading.Thread(target=consumidor,args=(ST_ID,), daemon=True).start()



    ADDR_MONITOR = (IP_MONITOR, PUERTO_MONITOR)

    #El hilo principal se encarga del monitor, se se cae o no existe, espera el REINTENTO segundos y intenta reconectar sin cerrar el engine
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