import socket
import sys
import threading
import time


HEADER = 64
FORMAT = 'utf-8'


def send(socket,msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))

    socket.send(send_length)
    socket.send(message)
    
    
def fuga(central_socket, id):
    
    msg = f" <STX><DATA><ETX><LRC>"
    print(f"[{id}] Envio de averia a Central",msg)    #Si el Engine falla o se desconecta se lo comunica a central
    try:
        send(central_socket,msg)
        
    except Exception as e:
        print(f"Error al enviar el mensaje de fuga: {e}")  #Si hay problemas al mandar el mensaje salta la excepcion
        

def com_engine(conn, addr,central_socket, id):
    print(f"[NUEVA CONEXION ENGINE] {addr} connected.")
    connected= True
    
    while connected:
        try:
            ping_msg= f" <STX><DATA><ETX><LRC>"
            send(conn, ping_msg)
            
            msg_length_data= conn.recv(HEADER).decode(FORMAT)
            
            if msg_length_data:
                msg_length= int(msg_length_data)
                msg = conn.recv(msg_length).decode(FORMAT)

                print(f"[{id}] Recibo del engine [{addr}]: {msg}")

                #Si se recibe respuesta de tipo K.O
                if "KO" in msg:
                    print(f"[{id}] Fuga o avería detectada")
                    fuga(central_socket,id)
                    connected= False

            
            #Si no se recibe respuesta desde el WM_WS_E
            else: 
                print(f"El Engine se ha desconectado")
                fuga(central_socket,id)
                connected=False
        
            time.sleep(1)
            
        except Exception as e:   
            print(f"Error al comunicar con Engine: {e}")  #Si hay problemas salta la excepcion
            fuga(central_socket,id)
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


if  (len(sys.argv) == 5):
    PUERTO_ESCOLTA = int(sys.argv[1])
    IP_CENTRALITA = sys.argv[2]
    PUERTO_CENTRALITA = int(sys.argv[3])    
    WATERSTATION_ID = sys.argv[4]
    
    central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ADDR_CENTARLITA = (IP_CENTRALITA, PUERTO_CENTRALITA)
    
    
    try:
        central_socket.connect(ADDR_CENTARLITA)
        print (f"Establecida conexión en [{ADDR_CENTARLITA}]")


        send(central_socket,msg)

