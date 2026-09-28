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
            