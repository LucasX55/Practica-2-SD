import os
import socket
import threading
import sys
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from common.protocol import recv_message, send_message, ConnectionClosed

from common.database import Database
from common.modelo import Station

db = Database(os.environ.get("WM_DB", "wm_central.db"))
estaciones = {}
candado = threading.Lock()





def cargar_estaciones():
    for fila in db.get_stations():
        est = Station(fila["id"], fila["location"], bool(fila["blocked"]))
        estaciones[fila["id"]] = est
        print (f"[Central] Cargada de la BD: {est.id} en {est.ubicacion} "f"-> {est.estado()} (bloqueada={est.bloqueada})")


def autenticar(st_id, ubicacion):
    with candado:
        est = estaciones.get(st_id)
        if est is None:
            est = Station(st_id, ubicacion)
            estaciones[st_id] = est
            db.add_station(st_id, ubicacion)
            print(f"[Central] Nueva estación: {st_id} en {ubicacion}")
        elif est.conectada:
            print(f"[Central] Autenticación fallida: estación {st_id} ya conectada ")
            return "KO#estacion ya conectada"
        est.token = uuid.uuid4().hex[:12]
        est.conectada = True
        print(f"[Central] Estación {st_id} autentificada")
        return f"OK#{est.token}"






def desconectar(st_id):
    if st_id is None:
        return
    with candado:
        est = estaciones.get(st_id)    
        if est:
            est.conectada = False
            est.token = None
            print(f"[Central] Estación {st_id} desconectada")



def cambiar_fuga(st_id, hay_fuga):
    with candado:
        est = estaciones.get(st_id)
        est.fuga = hay_fuga
        if hay_fuga:
            print(f"[Central] FUGA detectada en estación {st_id} Estado {est.estado()}")
        else:
            print(f"[Central] FUGA resuelta en estación {st_id} Estado {est.estado()}")
    return "OK"



def atender_monitor(conn,addr):
    """Se eecuta en un hilo: atiendo a un cliente conectadoo""" 
    print(f"[Central] Conectado a {addr}")
    st_id = None
    try:
        while True:
            mensaje = recv_message(conn)
            print(f"[Central] Recibido: {mensaje}")
            partes = mensaje.split("#")
            if partes[0] == "AUTENTICAR" and len(partes) >= 3:
                if st_id is not None:
                    respuesta = "KO#ya autentificado"
                else:
                    respuesta = autenticar(partes[1], partes[2])
                    if respuesta.startswith("OK"):
                
                        st_id = partes[1]
            elif partes[0] == "FUGA" and st_id and len(partes) >=2:
                respuesta = cambiar_fuga(st_id, True) if partes[1] == st_id else "KO#id no coincide"
            elif partes[0] == "RESUELTA" and st_id and len(partes) >=2:
                respuesta = cambiar_fuga(st_id, False) if partes[1] == st_id else "KO#id no coincide"
            else:
                respuesta = "KO#mensaje no valido"
            send_message(conn, respuesta)
            
    except (ConnectionClosed, OSError) :
        pass
    finally:
        desconectar(st_id)
        conn.close()
        print (f"[Central]{addr} desconectado  ")



def main ():
    if len(sys.argv) != 3:
        print("Uso: python WM_Central.py <puerto_sockets> <kafka_ip:puerto>")
        return
    puerto = int(sys.argv[1])
    kafka = sys.argv[2]

    cargar_estaciones()

    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind(("0.0.0.0", puerto))
    servidor.listen()
    print(f"[Central] Escuchando en el puerto {puerto} ")

    while True:
        conn, addr = servidor.accept()
        hilo = threading.Thread(target=atender_monitor, args=(conn, addr), daemon=True)
        hilo.start()
if __name__ == "__main__":
    main()