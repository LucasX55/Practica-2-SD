# ============================================================================
# WM_FO.py  -  FIELD OPERATOR (operario de campo)
#
# El operario pide riegos a la Central y ve cómo van. Todo por Kafka:
#   - Manda peticiones (SOLICITAR / PARAR) al topic de peticiones.
#   - Recibe las respuestas de la Central en su topic personal (respuestas del operador).
#   - Recibe la lista de estaciones y sus estados en el topic de estados.
#
# Se puede manejar de dos formas: con un fichero de servicios (uno detrás de otro)
# y/o con el menú de órdenes por teclado (pedir / parar / ver / salir).
#
# Uso: python WM_FO.py <ip_kafka> <puerto_kafka> <id_operador> [fichero]
# ============================================================================
 

#Librerias

import os
import sys
import threading
import time
import json

#Para importar la carpeta /common
sys.path.insert(0,os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from common import kafka_utils as k

#Si pasan mas de estos segundos sin recibir la lista de estaciones de la Central,
#Se considera que la Central esta caida
TIEMPO_CENTRAL= float(os.environ.get("WM_TIEMPO_CENTRAL", 5))

#Se comparten los estados entre hilos

riego_activo = False        #True es igual a que hay un riego pedido que aun no ha terminado
estaciones=[]               #Ultima lista de estaciones recibida de la central
central_caida=False         #True -> la central no responde
productor=None              #productor de kafka

#Hilo que escucha las respuestas de la Central a este operador
#El grupo de consumidores es fijo (fo-<id>) y con solo_nuevos=False: si el
# operador se cae y se relanza, recibe tambien las respuestas que llegaron mientras estabacaido
def consumidor(id_operador):
    global riego_activo
    
    consumidor = k.crear_consumidor(KAFKA, f"fo-{id_operador}", [k.t_respuestas(id_operador)], solo_nuevos=False)
    
    
    print(f"[{id_operador}] Escuchando a la central")

    while True:
        topic, res= k.leer(consumidor)

        #Nada nuevo, se sigue esperando
        if res is None:
            continue

        print(f"[{id_operador}] Mensaje proveniente de la Central: {res}")

        #El riego ha terminado, ya se porque termino bien, por fuga o porque se ha denegado, y se libera al operador
        if res.get("estado") in ["FINALIZADO", "ERROR_FUGA", "DENEGADO"]:
            riego_activo= False


#Hilo que escucha la lista de estaciones que publica la central cada cierto tiempo
#Sirve para tener la lista actualizada ("ver") y detectar si la central se cae (Se pasa de TIEMPO_CENTRAL y no se recibe nada)
def escuchar_estados():
    global estaciones, central_caida
    consumidor = k.crear_consumidor(KAFKA, "fo-estados", [k.T_ESTADOS])
    ultimo= time.time()        #Momento del ultimo mensaje recibido de la central

    while True:
        topic, datos = k.leer(consumidor)
        if datos is not None:
            estaciones= datos["estaciones"]
            ultimo= time.time()

            #Si estaba caida y vuelve a llegar info, la Central ha vuelto
            if central_caida:
                central_caida=False
                print("WM_Central vuelve a estar disponible")

        #No llega nada y se pasa del tiempo permitda, central caida
        elif not central_caida and (time.time()-ultimo) > TIEMPO_CENTRAL:
            central_caida=True
            print("Imposible conectar con WM_Central")


#Manda una peticion de riego a la central. Si hay segundos =0 significa que no habra limite
#Devuelve false si no se pudo pedir (Se ha caido la central) para que quien la llame lo sepa
#Marca riego_activo a true, hasta que la central conteste con el final, y el operador se considerara ocupado
def pedir_riego(id_operador, id_ws, segundos):
    global riego_activo
    
    if central_caida:
        print("Imposible conectar con WM_Central: no se pueden pedir riegos")
        return False

    peticion= {"id_operador":id_operador,"id_ws":id_ws,"duracion_s": segundos,"accion":"SOLICITAR"}
    print(f"[{id_operador}] Solicitando riego: {peticion}")
    k.enviar(productor, k.T_PETICIONES, peticion)
    riego_activo=True
    return True

#Muestra la lista de estaciones y marca las disponibles, si la central esta caida, no se muestra nada, ya que la informacion seria antigua
def ver_estaciones():
    if central_caida:
        print("Imposible conectar con WM_Central")
        return 

    for s in estaciones:
        marca= " <- disponible" if s["estado"]=="DISPONIBLE" else ""
        print(f" {s['id']:<6}{s['ubicacion']:<16}{s['estado']:<18}{marca}")


# Lee el fichero de servicios (formato: "ST1,5" = estación y segundos, una por línea)
# y los hace de uno en uno: pide el riego, espera a que termine y pasa al siguiente.
# Se ignoran las líneas vacías y las que empiezan por # (comentarios).
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

                #Esperar a que la Central avise del final (riego_activa a false) o a que se caiga la central (para que no se quede esperando)
                while riego_activo and not central_caida:
                    time.sleep(0.5)
                    
                print(f"[{id_operador}] Servicio terminado.")
                time.sleep(4)  #Segundos de seguridad
        print(f"[{id_operador}] Todas las peticiones han sido procesadas")
        
    except FileNotFoundError:
        print(f"No hay fichero {fichero}")
        
    except Exception as e:
        print(f"Error en la petición: {e}")

#Menu
# pedir <estacion> [segundos] -> pide un riego, si no hay segundos, no hay limite
# para <estacion> -> pide a la central que pare el riego
#ver -> muestra las estaciones
#salir -> termina el operario
def menu(id_operador):
    print("Ordenes: pedir <estacion> [segundos] | parar <estacion> | ver | salir (pedir sin segundos= riego sin limite)")

    while True:
        try:
            partes= input("FO> ").split()

        except EOFError:
            #Sin teclado, se sale del menu
            break

        if not partes:
            continue

        orden= partes[0].lower()

        if orden == "pedir" and len(partes) >= 2:
            segundos = 0

            #Si hay un tercer dato y es un numero, son los segundos, si no, sin limite
            if len(partes) > 2 and partes[2].isdigit():
                segundos = int(partes[2])

            pedir_riego(id_operador,partes[1],segundos)

        elif orden == "parar" and len(partes) == 2:
            #La parada tambien va a la Central, quer es la que ordena a la estacion
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

    #Se aseguran los topics que necesita el operario y se crea el productor
    k.crear_topics(KAFKA, [k.t_respuestas(ID_OPERADOR), k.T_ESTADOS, k.T_PETICIONES])
    productor= k.crear_productor(KAFKA)

    #Dos hilos en segundo plano (daemon) y respuestas de la central y lista de estaciones
    threading.Thread(target=consumidor,args=(ID_OPERADOR,), daemon=True).start()
    threading.Thread(target=escuchar_estados, daemon=True).start()
    time.sleep(1)       #margen para que los consumidores arranquen antes de pedir cualquier cosa

    #Si se paso fichero, se procesa primero y despues queda el menu
    if len(sys.argv)==5:

        procesar_fichero(ID_OPERADOR, sys.argv[4])
    menu(ID_OPERADOR)
    
else:
    print("Uso: python WM_FO.py <ip_kafka> <puerto_kafka> <id_operador> [fichero]")
