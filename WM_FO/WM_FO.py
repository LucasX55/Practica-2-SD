import os
import socket
import sys
import threading
import time
import json

sys.path.insert(0,os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from common import kafka_utils as k

FORMAT= 'utf-8'
riego_activo = False

TIEMPO_CENTRAL= float(os.environ.get("WM_TIEMPO_CENTRAL", 5))

riego_activo = False
estaciones=[]
central_caida=False
productor=None

def consumidor(id_operador):
    global riego_activo
    
    consumidor = k.crear_consumidor(KAFKA, f"fo_{id_operador}", [k.t_respuestas(id_operador)], solo_nuevos=False)
    
    
    print(f"[{id_operador}] Escuchando a la central")

    while True:
        topic, res= k.leer(consumidor)

        if res is None:
            continue

        print(f"[{id_operador}] Mensaje proveniente de la Central: {res}")
        
        if res.get("estado") in ["FINALIZADO", "ERROR_FUGA", "DENEGADO"]:
            riego_activo= False

def escuchar_estados():
    global estaciones, central_caida
    consumidor = k.crear_consumidor(KAFKA, "fo_estados", [k.T_ESTADOS])
    ultimo= time.time()        

    while True:
        topic, datos = k.leer(consumidor)
        if datos is not None:
            estaciones= datos["estaciones"]
            ultimo= time.time()

            if central_caida:
                central_caida=False
                print("WM_Central vuelve a estar disponible")

        elif not central_caida and (time.time()-ultimo) > TIEMPO_CENTRAL:
            central_caida=True
            print("Imposible conectar con WM_Central")


def pedir_riego(id_operador, id_ws, segundos):
    global riego_activo
    
    if central_caida:
        print("Imposible conectar con WM_Central: no se pueden pedir riegos")
        return False

    peticion= {"id_operador":id_operador,"id_ws":id_ws,"duracion_s": segundos,"accion":"SOLICITAR"}
    print(f"[{id_operador}] Solicitando riego: {peticion}")
    k.enviar(productor, k.t_PETICIONES, peticion)
    riego_activo=True
    return True

def ver_estaciones():
    if central_caida:
        print("Imposible conectar con WM_Central")
        return 

    for s in estaciones:
        marca= " <- disponible" if s["estado"]=="DISPONIBLE" else ""
        print(f" {s['id']:<6}{s['ubicacion']:<16}{s['estado']<18}{marca}")


def procesar_fichero(id_operador, fichero):
    global riego_activo
    
   
    
    try: 
        with open(fichero,'r') as f:
            lineas=f.readlines()
            
        for linea in lineas:
            linea=linea.strip()
            
            if not linea:
                continue

            if not linea or linea.startswith("#"):
                continue

            
            partes=linea.split(',')
            if len(partes)==2:
                if not pedir_riego(id_operador, partes[0].strip(), int(partes[1].strip())):
                    break 

                while riego_activo and not central_caida:
                    time.sleep(0.5)
                    
                print(f"[{id_operador}] Servicio terminado.")
                time.sleep(4)  #Segundos de seguridad
        print(f"[{id_operador}] Todas las peticiones han sido procesadas")
        
    except FileNotFoundError:
        print(f"No hay fichero {fichero}")
        
    except Exception as e:
        print(f"Error en la petición: {e}")


def menu(id_operador):
    print("Ordenes: pedir <estacion> [segundos] | parar <estacion> | ver | salir (pedir sin segundos= riego sin limite)")

    while True:
        try:
            partes= input("FO> ").split()

        except EOFError:
            break

        if not partes:
            continue

        orden= partes[0].lower()

        if orden == "pedir" and len(partes) > 2:
            segundos = 0

            if len(partes) > 2 and partes[2].isdigit():
                segundos = int(partes[2])

            pedir_riego(id_operador,partes[1],segundos)

        elif orden == "parar" and len(partes) == 2:
            k.enviar(productor, k.T_PETICIONES, {"accion": "PARAR", "id_operador": id_operador, "id_ws": partes[1]})
            print(f"[{id_operador}] Pidiendo parar el riego de {partes[1]}")

        elif orden == "ver":
            ver_estaciones()

        elif orden == "salir":
            break

        else: 
            print("Ordenes: pedir <estacion> [segundos] | parar <estacion> | ver | salir")


######################### MAIN ##########################
if  (len(sys.argv) in (4,5)):
    IP_KAFKA = sys.argv[1]
    PUERTO_KAFKA = int(sys.argv[2])    
    ID_OPERADOR = sys.argv[3]
    KAFKA = f"{IP_KAFKA}:{PUERTO_KAFKA}"

    k.crear_topics(KAFKA, [k.t_respuestas(ID_OPERADOR), k.T_ESTADOS, k.T_PETICIONES])
    productor= k.crear_productor(KAFKA)
    threading.Thread(target=consumidor,args=(ID_OPERADOR), daemon=True).start()
    threading.Thread(target=escuchar_estados, daemon=True).start()
    time.sleep(1)

    if len(sys.argv)==5:

        procesar_fichero(ID_OPERADOR, sys.argv[4])
    menu(ID_OPERADOR)
    
else:
    print("Uso: python WM_FO.py <ip_kafka> <puerto_kafka> <id_operador> [fichero]")
