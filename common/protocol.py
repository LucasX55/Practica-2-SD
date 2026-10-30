#########################################################################
# Bytes especiales del protocolo <STX>DATOS<ETX><LRC>
# Son caracteres de control que nunca aparecen en un texto normal, por eso sirven
# para marcar donde empieza y donde acaba cada mensaje dentro del socket
#########################################################################
STX = b'\x02' #Start of TeXt: empieza un mensaje
ETX = b'\x03' #End of TeXt: acaba el mensaje
ACK = b'\x06' #recibido bien
NACK = b'\x15' #recibido mal, repitemelo





#########################################################################
# Calcula el LRC: el XOR de todos los bytes del mensaje
# El que envia lo pone al final de la trama y el que recibe lo vuelve a calcular;
# si no coinciden, algun byte se ha estropeado por el camino
#########################################################################
def lrc(data: bytes) -> int:
    resultado = 0 #el 0 no cambia nada al hacer XOR
    for byte in data:
        resultado ^= byte #^ es el XOR, byte a byte
    return resultado


#########################################################################
# Monta la trama completa: STX + datos + ETX + LRC
#########################################################################
def build_message(mensaje: str) -> bytes:
    data = mensaje.encode("utf-8") #el texto pasa a bytes, que es lo que viaja por el socket
    return STX + data + ETX + bytes([lrc(data)]) #el LRC es un numero de 0 a 255: lo convertimos en 1 byte


#########################################################################
# Error propio para cuando el otro lado cierra la conexion o no acepta los mensajes
#########################################################################
class ConnectionClosed(Exception):
    """El otro extremo cerró la conexión."""


#########################################################################
# Lee un solo byte del socket y avisa si el otro lado ha cerrado
#########################################################################
def _leer_byte(sock):
    b=sock.recv(1) #lee 1 byte del socket
    if not b: #vacio el otro lado hac errado
        raise ConnectionClosed()
    return b

#########################################################################
# Lee una trama entera y dice si ha llegado bien
# Ignora lo que haya antes del STX, junta los bytes hasta el ETX, lee el LRC
# y devuelve el texto y True si el LRC cuadra (False si no)
#########################################################################
def _leer_trama(sock):
    b = _leer_byte(sock)
    while b != STX: #saltamos lo que no sea el principio de un mensaje
        b = _leer_byte(sock)
    data = b"" #aqui vamos juntando los datos
    b = _leer_byte(sock)
    while b != ETX: #todo lo que hay hasta el ETX son los datos
        data += b
        b = _leer_byte(sock)
    lrc_recibido = _leer_byte(sock)[0] #el byte de despues del ETX es el LRC; [0] lo pasa a numero
    return data.decode("utf-8"), lrc_recibido == lrc(data) #lo comparamos con el que calculamos nosotros


#########################################################################
# Recibe un mensaje y contesta ACK o NACK
# Si el LRC no cuadra contesta NACK y espera a que se lo repitan; asi el que
# llama solo ve mensajes que han llegado bien
#########################################################################
def recv_message(sock):
    while True:
        mensaje, ok = _leer_trama(sock)
        if ok: #ha llegado bien
            sock.sendall(ACK) #confirmamos que lo hemos recibido
            return mensaje
        sock.sendall(NACK) #ha llegado mal: pedimos que lo repita y volvemos a leer



#########################################################################
# Envia un mensaje y espera el ACK
# Si le contestan NACK lo vuelve a mandar, hasta 3 veces; si aun asi no llega
# bien, damos la conexion por perdida
#########################################################################
def send_message(sock, mensaje):
    trama = build_message(mensaje) #STX + datos + ETX + LRC
    for _ in range(3): #como mucho 3 intentos
        sock.sendall(trama)
        if _leer_byte(sock) == ACK: #esperamos la respuesta del otro lado
            return
    raise ConnectionClosed("El receptor rechazo el mensaje 3 veces") #3 NACK seguidos: algo va mal en la conexion