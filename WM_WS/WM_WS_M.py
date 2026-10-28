import socket
import sys
import threading
import time
import os
import json


sys.path.insert(0,os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from common.protocol import send_message,recv_message,ConnectionClosed

LATIDO= float(os.environ.get("WM_LATIDO", 2))

TIMEOUT_ENGINE = float(os.environ.get("WM_TIMEOUT_ENGINE", 3))

TIMEOUT_CENTRAL= float(os.environ.get("WM_TIMEOUT_CENTRAL", 10))

REINTENTO= float(os.environ.get("WM_REINTENTO",3))

central_socket = None
candado=threading.Lock()
en_fuga= True

def conectar_central():
    global central_socket

    while True:
        try:
            central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            central_socket.settimeout(TIMEOUT_CENTRAL)
            central_socket.connect(ADDR_CENTARLITA)
            print(f"Establecida conexion con {ADDR_CENTARLITA}")

            regmeg= f"REGISTRAR#{WATERSTATION_ID}{UBICACION}"
            print("Envio a la Central: ",regmeg)
            send_message(central_socket,regmeg)
            print(f"Respuesta: {recv_message(central_socket)}")

            auth_msg = f"AUTENTICAR#{WATERSTATION_ID}"
            print("Envio a la Central: ",auth_msg)
            send_message(central_socket,auth_msg)
            respuesta=recv_message(central_socket)
            print(f"Respuesta: {respuesta}")

            if "KO" not in respuesta:
                if en_fuga:
                    send_message(central_socket,f"FUGA#{WATERSTATION_ID}")
                    recv_message(central_socket)

                return
            central_socket.close()

        except (Exception, ConnectionClosed) as e:
            print(f"Imposible conectar con WM_Central: {e}")

        print(f"Reintentando en {REINTENTO} segundos...")
        time.sleep(REINTENTO)


def enviar_central(msg):
    with candado:
        while True:
            try:
                send_message(central_socket,msg)
                return recv_message(central_socket)

            except (Exception, ConnectionClosed) as e:
                print(f"Error al enviar mensaje a la central: {e}")
                conectar_central()

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

        
        

def resuelta(id_ws):
    global en_fuga
    if not en_fuga:
        return
    
    en_fuga= False
    print(f"[{id_ws}] Averia resuelta")    #Si el Engine falla o se desconecta se lo comunica a central
    enviar_central(f"RESUELTA#{id_ws}")



def com_engine(conn, addr, id_ws):
    print(f"[NUEVA CONEXION ENGINE] {addr} connected.")
    conn.settimeout(TIMEOUT_ENGINE)
    connected= True
    
    while connected:
        try:
            ping_msg= f"STATUS#{id_ws}"
            send_message(conn, ping_msg)
            
            msg= recv_message(conn)     

            if "KO" in msg:
                if not en_fuga:
                    print(f"[{id_ws}] Fuga o averia detectada")
                    fuga(id_ws)

            
            #Si no se recibe respuesta desde el WM_WS_E
            elif msg=='OK':
                resuelta(id_ws)

            time.sleep(1) 
            
        except (Exception, ConnectionClosed) as e:   
            print(f"Error al comunicar con Engine: {e}")  #Si hay problemas salta la excepcion
            fuga(id_ws)
            connected=False
            
            
    print("ADIOS. TE ESPERO EN OTRA OCASION")
    conn.close()
    
           

def latido(id_ws):
    while True:
        time.sleep(LATIDO)
        enviar_central(f"PING#{id_ws}") 


def start(server_socket,id,addr_escuchar):
    server_socket.listen()
    print(f"[LISTENING] Servidor a la escucha en {addr_escuchar}")
    
    while True:
        conn, addr = server_socket.accept()
        thread = threading.Thread(target=com_engine, args=(conn, addr,id))
        thread.start()

        CONEX_ACTIVAS = threading.active_count()-1
        print(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")





######################### MAIN ##########################


if  (len(sys.argv) == 6):
    PUERTO_ESCOLTA = int(sys.argv[1])
    IP_CENTRALITA = sys.argv[2]
    PUERTO_CENTRALITA = int(sys.argv[3])    
    WATERSTATION_ID = sys.argv[4]
    UBICACION=sys.argv[5]
    
    ADDR_CENTARLITA = (IP_CENTRALITA, PUERTO_CENTRALITA)
    conectar_central()

    threading.Thread(target=latido,args=(WATERSTATION_ID,), daemon=True).start()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                            
    ADDR_ESCOLTA = ('0.0.0.0', PUERTO_ESCOLTA)
    server_socket.bind(ADDR_ESCOLTA)
    
    print(f"Iniciando Monitor {WATERSTATION_ID}")
    start(server_socket, WATERSTATION_ID,ADDR_ESCOLTA)
    
else:
    print("Uso: python WM_WS_M.py <puerto_escolta> <ip_central> <puerto_central> <id_estacion> <ubicacion>")