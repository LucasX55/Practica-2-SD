STX = b'\x02'
ETX = b'\x03'
ACK = b'\x06'
NACK = b'\x15'





def lrc(data: bytes) -> int:
    resultado = 0
    for byte in data:
        resultado ^= byte
    return resultado


def build_message(mensaje: str) -> bytes:
    data = mensaje.encode("utf-8")
    return STX + data + ETX + bytes([lrc(data)]) 


class ConnectionClosed(Exception):
    """El otro extremo cerró la conexión."""


def _leer_byte(sock):
    b=sock.recv(1) #lee 1 byte del socket
    if not b: #vacio el otro lado hac errado
        raise ConnectionClosed()
    return b

def _leer_trama(sock):
    b = _leer_byte(sock)
    while b != STX:
        b = _leer_byte(sock)
    data = b""
    b = _leer_byte(sock)
    while b != ETX:
        data += b
        b = _leer_byte(sock)
    lrc_recibido = _leer_byte(sock)[0]
    return data.decode("utf-8"), lrc_recibido == lrc(data)


def recv_message(sock):
    while True:
        mensaje, ok = _leer_trama(sock)
        if ok:
            sock.sendall(ACK)
            return mensaje
        sock.sendall(NACK)



def send_message(sock, mensaje):
    trama = build_message(mensaje)
    for _ in range(3):
        sock.sendall(trama)
        if _leer_byte(sock) == ACK:
            return
    raise ConnectionClosed("El receptor rechazo el mensaje 3 veces")



