import socket
import sys
import threading
import time

HEADER = 64
FORMAT = 'utf-8'


def send(cliente_socket, msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    cliente_socket.send(send_length)
    cliente_socket.send(message)



def escuchar_fuga():
    global estado_actual
    print(f"Pon F y pulsa ENTER para simular una fuga")
    while True:
        tecla = input()
        if tecla == "F":
            estado_actual = "KO"
            print("Se ha producido una fuga. Se enviará el mensaje al monitor")
            break


def escuchar_monitor(cliente_socket):
    global estado_actual
    connected = True

    while connected:
        try:
            msg_length = cliente_socket.recv(HEADER).decode(FORMAT)
            if msg_length:
                msg_length = int(msg_length)
                msg = cliente_socket.recv(msg_length).decode(FORMAT)

                if "STATUS_REQ" in msg:
                    respuesta = f"<STX>STATUS#{estado_actual}<ETX><LRC>"
                    send(cliente_socket, respuesta)

                    if estado_actual == "KO":
                        print("Se ha producido una fuga. Se enviará el mensaje al monitor")

            else:
                print("Monitor desconectado.")
                connected = False
        except:
            print("Error de conexión con el monitor.")
            connected = False





if  (len(sys.argv) == 5):
    IP_KAFKA = sys.argv[1]
    PUERTO_KAFKA = int(sys.argv[2])    
    IP_MONITOR = sys.argv[3]
    PUERTO_MONITOR = int(sys.argv[4])

    teclado_thread = threading.Thread(target=escuchar_fuga,daemon=True)
    teclado_thread.start()

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