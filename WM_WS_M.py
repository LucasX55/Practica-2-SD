import socket
import sys
import threading
import time
import os
import json


sys.path.insert(0,os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from common.protocol import send_message,recv_message,ConnectionClosed



    
    
def fuga(central_socket, id_ws):
    
    msg = f"FUGA#{id_ws}"
    print(f"[{id_ws}] Envio de averia a Central",msg)    #Si el Engine falla o se desconecta se lo comunica a central
    try:
        send_message(central_socket,msg)
        
    except Exception as e:
        print(f"Error al enviar el mensaje de fuga: {e}")  #Si hay problemas al mandar el mensaje salta la excepcion
        

def com_engine(conn, addr,central_socket, id_ws):
    print(f"[NUEVA CONEXION ENGINE] {addr} connected.")
    connected= True
    
    while connected:
        try:
            ping_msg= f"STATUS#{id_ws}"
            send_message(conn, ping_msg)
            
            msg= recv_message(conn)     
            print(f"[{id_ws}] Respuesta de Engine: {msg}")

            if "KO" in msg:
                print(f"[{id_ws}] Fuga o avería detectada")
                fuga(central_socket,id_ws)
                connected= False

            
            #Si no se recibe respuesta desde el WM_WS_E
            elif msg=='OK':
                send_message(central_socket,f"RESUELTA#{id_ws}")
        
            time.sleep(1)
            
        except (Exception, ConnectionClosed) as e:   
            print(f"Error al comunicar con Engine: {e}")  #Si hay problemas salta la excepcion
            fuga(central_socket,id_ws)
            connected=False
            
            
    print("ADIOS. TE ESPERO EN OTRA OCASION")
    conn.close()
    
    
def start(server_socket,central_socket,id,addr_escuchar):
    server_socket.listen()
    print(f"[LISTENING] Servidor a la escucha en {addr_escuchar}")
    
    while True:
        conn, addr = server_socket.accept()
        thread = threading.Thread(target=com_engine, args=(conn, addr,central_socket,id))
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
    
    central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ADDR_CENTARLITA = (IP_CENTRALITA, PUERTO_CENTRALITA)
    
    
    try:
        central_socket.connect(ADDR_CENTARLITA)
        print (f"Establecida conexión en [{ADDR_CENTARLITA}]")

        auth_msg = f"AUTENTICAR#{WATERSTATION_ID}#{UBICACION}"
        print("Envio a la Central: ",auth_msg)
        send_message(central_socket,auth_msg)
        respuesta=recv_message(central_socket)
        print(f"Respuesta: {respuesta}")
        
        if "KO" in respuesta:
            sys.exit(1)
            
    except Exception as e:
            print(f"No se entrar a la central")
            sys.exit(1)



    server_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    ADDR_ESCOLTA = ('0.0.0.0', PUERTO_ESCOLTA)
    server_socket.bind(ADDR_ESCOLTA)
    
    print(f"Iniciando Monitor {WATERSTATION_ID}")
    start(server_socket, central_socket,WATERSTATION_ID,ADDR_ESCOLTA)
    
else:
    print("Error")