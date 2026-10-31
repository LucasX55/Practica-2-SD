# ============================================================================
# WM_WS_M.py  -  MONITOR de una estación de riego (WaterStation Monitor)
#
# Qué hace:
#   - Hacia la CENTRAL (sockets): se registra, se autentica, manda un latido
#     (PING) cada pocos segundos y le avisa de FUGA / RESUELTA.
#   - Hacia el ENGINE (sockets): hace de servidor, le pregunta "STATUS" cada
#     segundo y según conteste (OK / KO) o deje de contestar, avisa a la Central.
#
# Uso: python WM_WS_M.py <puerto_escolta> <ip_central> <puerto_central> <id_estacion> <ubicacion>
# ============================================================================

#Librerias
import socket
import sys
import threading
import time
import os
import json

#Para importar la carpeta /common
sys.path.insert(0,os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from common.protocol import send_message,recv_message,ConnectionClosed

#Cada cuantos segundos manda ping a la central
LATIDO= float(os.environ.get("WM_LATIDO", 2))

#Si el engina tarda mas de esto en contestar al status, se considera que se ha caido
TIMEOUT_ENGINE = float(os.environ.get("WM_TIMEOUT_ENGINE", 3))

#Si la central tarda mas de esto en contestar, se considera caida
TIMEOUT_CENTRAL= float(os.environ.get("WM_TIMEOUT_CENTRAL", 10))

#Segundos que se espera entre reintentos de conexion
REINTENTO= float(os.environ.get("WM_REINTENTO",3))

central_socket = None           #Socket abierto con la central
candado=threading.Lock()        #Evita que dos hilos usen el socket de la central al mismo tiempo
en_fuga= True                   #Si es igual a true, la estacion estara averiada


#AAbre la conexion con la central, se registra y autentica.
def conectar_central():
    global central_socket

    while True:
        try:
            central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            central_socket.settimeout(TIMEOUT_CENTRAL)
            central_socket.connect(ADDR_CENTARLITA)
            print(f"Establecida conexion con {ADDR_CENTARLITA}")

            #Se registra, se da alya la estacion con su debida ubicacion
            regmeg= f"REGISTRAR#{WATERSTATION_ID}#{UBICACION}"
            print("Envio a la Central: ",regmeg)
            send_message(central_socket,regmeg)
            print(f"Respuesta: {recv_message(central_socket)}")

            #Se identifica ya como registrada, siempre se lee la respuesta, asi no se desordena el socket
            auth_msg = f"AUTENTICAR#{WATERSTATION_ID}"
            print("Envio a la Central: ",auth_msg)
            send_message(central_socket,auth_msg)
            respuesta=recv_message(central_socket)
            print(f"Respuesta: {respuesta}")

            #Si no contesta KO, significa que la cosa ha ido bien
            if "KO" not in respuesta:
                
                #Si la estacion estyaba averiada antes de recorectar, se lo recordamos a la central para QUe no la crea sana
                if en_fuga:
                    send_message(central_socket,f"FUGA#{WATERSTATION_ID}")
                    recv_message(central_socket)

                return
            #Si ha contestado KO, cerramos y se vuelve a intentar
            central_socket.close()

        except (Exception, ConnectionClosed) as e:
            print(f"Imposible conectar con WM_Central: {e}")

        print(f"Reintentando en {REINTENTO} segundos...")
        time.sleep(REINTENTO)

#Manda un mensaje a la central y devuelve su respuesta. Lo usan el hilo del latido y los hilos de com_engine, 
#asi un envio y su respuesta no se mezclan con los de otro hilo.
#Si falla se reconecta con la Central y vuelve a intentarlo.
def enviar_central(msg):
    with candado:
        while True:
            try:
                send_message(central_socket,msg)
                return recv_message(central_socket)

            except (Exception, ConnectionClosed) as e:
                print(f"Imposible conectar con WM_Central: {e}")
                conectar_central()

#Avisaa la Central de que la estacion esta averiada, solo avisa una vez por averia.
def fuga(id_ws):

    global en_fuga

    if en_fuga:
        return 
    
    en_fuga= True
    msg = f"FUGA#{id_ws}"
    print(f"[{id_ws}] Envio de averia a Central",msg)    #Si el Engine falla o se desconecta se lo comunica a central
    try:
        enviar_central(msg)
        
    except Exception as e:
        print(f"Error al enviar el mensaje de fuga: {e}")  #Si hay problemas al mandar el mensaje salta la excepcion

        
        
#Avisa a la central de que se ha resuelto la averia. 
def resuelta(id_ws):
    global en_fuga
    if not en_fuga:
        return
    
    en_fuga= False
    print(f"[{id_ws}] Averia resuelta")    #Si el Engine falla o se desconecta se lo comunica a central
    enviar_central(f"RESUELTA#{id_ws}")


#Un hilo por cada Engine que este conectado. Cada segundo pregunta STATUS y actua segun que respuesta haya
def com_engine(conn, addr, id_ws):
    print(f"[NUEVA CONEXION ENGINE] {addr} connected.")

    #Si el engine no conteta a tiempo, recv_message lanza la excepcion y se trata como si fuera una caida
    conn.settimeout(TIMEOUT_ENGINE)
    connected= True
    
    while connected:
        try:
            ping_msg= f"STATUS#{id_ws}"
            send_message(conn, ping_msg)
            
            msg= recv_message(conn)     

            #El Engine dice KO: fuga simuilada. Avisamos a la central.
            if "KO" in msg:
                if not en_fuga:
                    print(f"[{id_ws}] Fuga o averia detectada")
                    fuga(id_ws)


            #El engine dice OK si estaba averiada, ahora esta resuelta, si no se recibe respuesta desde el WM_WS_E
            elif msg=='OK':
                resuelta(id_ws)

            time.sleep(1) 
            
        except (Exception, ConnectionClosed) as e:   

            #Engine caido, cerrado o sin contestar a tiempo se avisa y se corta
            print(f"Error al comunicar con Engine: {e}")  #Si hay problemas salta la excepcion
            fuga(id_ws)
            connected=False
            
            
    print("ADIOS. TE ESPERO EN OTRA OCASION")
    conn.close()
    
           
#El hilo del latido, basicamente, cada segundos manda ping a la central
def latido(id_ws):
    while True:
        time.sleep(LATIDO)
        enviar_central(f"PING#{id_ws}") 


#Servidor que espera a los engines, cada conexion nueva se atiende en su propio hilo. 
#daemon= True, si se cierra el monitor, estos hilos no lo mantienen vivo
def start(server_socket,id,addr_escuchar):
    server_socket.listen()
    print(f"[LISTENING] Servidor a la escucha en {addr_escuchar}")
    
    while True:
        conn, addr = server_socket.accept()
        thread = threading.Thread(target=com_engine, args=(conn, addr,id), daemon= True)
        thread.start()

        CONEX_ACTIVAS = threading.active_count()-1
        print(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")





######################### MAIN ##########################


if  (len(sys.argv) == 6):
    PUERTO_ESCOLTA = int(sys.argv[1])   #Puerto donde se escucha al engine
    IP_CENTRALITA = sys.argv[2]
    PUERTO_CENTRALITA = int(sys.argv[3])    
    WATERSTATION_ID = sys.argv[4]
    UBICACION=sys.argv[5]
    
    ADDR_CENTARLITA = (IP_CENTRALITA, PUERTO_CENTRALITA)
    
    #Se bloquea hasta conectar con la central
    conectar_central()

    #Arraca el latido en segundo plano
    threading.Thread(target=latido,args=(WATERSTATION_ID,), daemon=True).start()

    #Abrir el servidor para el engine, SO_REUSADOR permite relanzar el monitor enseguida
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                            
    ADDR_ESCOLTA = ('0.0.0.0', PUERTO_ESCOLTA)
    server_socket.bind(ADDR_ESCOLTA)
    
    print(f"Iniciando Monitor {WATERSTATION_ID}")
    start(server_socket, WATERSTATION_ID,ADDR_ESCOLTA)
    
else:
    print("Uso: python WM_WS_M.py <puerto_escolta> <ip_central> <puerto_central> <id_estacion> <ubicacion>")