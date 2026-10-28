import os
import socket
import threading
import sys
import time
import json
import panel_web

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from common.protocol import recv_message, send_message, ConnectionClosed
from common.database import Database
from common.modelo import Station, DISPONIBLE, REGANDO, FUGA, FUERA_DE_SERVICIO, DESCONECTADA
from common import kafka_utils as k

db = Database(os.environ.get("WM_DB", "wm_central.db"))
TIMEOUT_MONITOR = float(os.environ.get("WM_TIMEOUT_MONITOR", 10))
estaciones = {}
candado = threading.Lock()

SERVIDOR_KAFKA = None
producer = None
mensajes = []

AYUDA = ("Órdenes: iniciar <id|todas> [segundos] | parar <id|todas> | bloquear <id|todas> | ""activar <id|todas> | alta <operario> [nombre] | altas <numero>")


# Colores de la consola (codigos ANSI): fondo verde, verde parpadeando, rojo, naranja y gris
COLORES = {DISPONIBLE: "\033[30;42m", REGANDO: "\033[5;30;42m", FUGA: "\033[97;41m", FUERA_DE_SERVICIO: "\033[30;48;5;208m", DESCONECTADA: "\033[30;47m"}

RESET = "\033[0m"


NOMBRES = {DISPONIBLE: "Available", REGANDO: "Watering", FUGA: "Leak", FUERA_DE_SERVICIO: "Out of Service", DESCONECTADA: "Disconnected"}

def cargar_estaciones():
    for fila in db.get_stations():
        est = Station(fila["id"], fila["location"], bool(fila["blocked"]))
        estaciones[fila["id"]] = est
        log(f"Cargada desde la BD: {est.id} en {est.ubicacion} -> {est.estado()} (bloqueada={est.bloqueada})")
        for fila in db.get_active():
            est = estaciones.get(fila["station"])
            if est:
                est.riego = {"operador": fila["operator"], "duracion_s": fila["duration_s"], "inicio": fila["started_at"], "caudal": 0.0, "volumen": 0.0, "interrumpido": False}
                log(f"Riego en curso recuperao de la BD: {est.id} ({fila['operator']}) ")                                                              

def registrar(st_id, ubicacion):
    with candado:
        est = estaciones.get(st_id)
        if est is None:
            estaciones[st_id] = Station(st_id, ubicacion)
            db.add_station(st_id, ubicacion)
            log(f"Nueva estación registrada: {st_id} en {ubicacion}")
        else:
            est.ubicacion = ubicacion.db.set_location(st_id, ubicacion)
            db.set_location(st_id, ubicacion)
            log(f"Estación {st_id} ya registrada, ubicación: {ubicacion}")
    return "OK"

def orden_natural(st_id):
    return(len(st_id), st_id)


def log(texto):
    hora = time.strftime("%H:%M:%S")
    print (f"[Central {hora}] {texto}", flush=True)
    mensajes.append(f"[{hora}] {texto}")
    if len(mensajes) > 10:
        mensajes.pop(0)


def autenticar(st_id):
    with candado:
        est = estaciones.get(st_id)
        if est is None:
            log(f"Autentificacion denegada: estacion {st_id} no registrada")
            return "KO#estacion no registrada"

        if est.conectada:
            log(f"Autetificacion denegada: estacion {st_id} ya conectada")
            return "KO#estacion ya conectada"
        est.conectada = True
        log(f"Estación {st_id} autentificada")
        return "OK"






def desconectar(st_id):
    if st_id is None:
        return
    with candado:
        est = estaciones.get(st_id)    
        if est is None:
            return
        est.conectada = False
    log(f"Estacion {st_id} desconectada")
    interrumpir(st_id, "DESCONECTADA")


def cambiar_fuga(st_id, hay_fuga):
    with candado:
        est = estaciones.get(st_id)
        est.fuga = hay_fuga
        estado = est.estado()
        if hay_fuga:
            log(f"FUGA/averia en estacion {st_id}. Estado {estado}")
            interrumpir(st_id, "FUGA")
        else:
            log(f"Averia resuelta en estacion {st_id}. Estado {estado}")
    return "OK"



def atender_monitor(conn,addr):
    """Se eecuta en un hilo: atiendo a un cliente conectado""" 
    log(f"Conectado a {addr}")
    conn.settimeout(TIMEOUT_MONITOR)
    st_id = None
    try:
        while True:
            mensaje = recv_message(conn)
            partes = mensaje.split("#")
            if partes[0] != "PING":
                log(f"Recibido: {mensaje}")
            if partes[0] == "REGISTRAR" and len(partes) >= 3:
                respuesta = registrar(partes[1], partes[2])
            elif partes[0] == "AUTENTICAR" and len(partes) >= 2:
                if st_id is not None:
                    respuesta = "KO#ya autentificado"
                else:
                    respuesta = autenticar(partes[1])
                    if respuesta == "OK":
                        st_id = partes[1]
            elif partes[0] == "FUGA" and st_id and len(partes) >=2:
                respuesta = cambiar_fuga(st_id, True) if partes[1] == st_id else "KO#id no coincide"
            elif partes[0] == "RESUELTA" and st_id and len(partes) >=2:
                respuesta = cambiar_fuga(st_id, False) if partes[1] == st_id else "KO#id no coincide"
            elif partes[0] == "PING" and st_id:
                respuesta = "OK"
            else:
                respuesta = "KO#mensaje no valido"
            send_message(conn, respuesta)
    except socket.timeout:
        log(f"{st_id}: no llegan latidos del monitor")      
    except (ConnectionClosed, OSError) :
        pass
    finally:
        desconectar(st_id)
        conn.close()
        log(f"{addr} desconectado")


def procesar_peticion(p):
    op_id = p.get("id_operaador")
    st_id = p.get("id_ws")
    if p.get("accion") == "PARAR":
        parar(st_id, op_id)
        return
    duracion = int(p.get("duracion_s", 0))
    texto_duracion = f"{duracion} s" if duracion > 0 else "sin límite"
    log(f"Petición de {op_id}: regar {st_id} ({texto_duracion})")
    notificar(op_id, st_id, {"estado": "RECIBIDA"})
    motivo = None
    with candado:
        est = estaciones.get(st_id)
        if op_id != "MANUAL" and op_id != "CENTRAL" and not db.operator_exists(op_id):
            motivo = "operario no registrado"
        elif est is None:
            motivo = "la estación no existe"
        elif est.estado() != DISPONIBLE:
            motivo =f"ña estación está {est.estado()}"
        elif duracion < 0:
            motivo = "duracion no válida"
        else:
            est.riego = {"operador": op_id, "duracion_s": duracion, "inicio": time.time(), "caudal": 0.0, "volumen": 0.0, "interrumpido": False}
            db.add_active(st_id, op_id, est.riego["inicio"], duracion)
    if motivo:
        log(f"DENEGADA petición de {op_id} en {st_id}: {motivo}")
        notificar(op_id, st_id, {"estado": "DENEGADO", "motivo": motivo})
        return
    log(f"AUTORIZADO riego de {op_id} en {st_id}")
    notificar(op_id, st_id, {"estado": "AUTORIZADO"})
    k.enviar(productor, k.t_ordenes(st_id), {"accion": "INICIAR", "id_operador": op_id, "duracion_s": duracion})


def parar(st_id, quien):
    # Para un riego en marcha. Un operario solo puede parar el suyo
    with candado:
        est = estaciones.get(st_id)
        riego = est.riego if est else None
    if riego is None:
        log(f"{quien} quiere parar {st_id}, pero no está regando")
        notificar(quien, st_id, {"estado": "DENEGADO", "motivo": "la estación no está regando"})
        return
    if quien != "CENTRAL" and quien != riego["operador"]:
        log(f"{quien} quiere parar {st_id}, pero el riego es de {riego['operador']}")
        notificar(quien, st_id, {"estado": "DENEGADO", "motivo": "ese riego no es tuyo"})
        return
    log(f"{st_id}: {quien} para el riego")
    k.enviar(productor, k.t_ordenes(st_id), {"accion": "DETENER", "motivo": f"PARADO_POR_{quien}"})

def bloquear(st_id, bloqueada):
    with candado:
        est = estaciones.get(st_id)
        if est is None:
            log(f"No existe la estación {st_id}")
            return
        est.bloqueada = bloqueada
        regando = est.riego is not None
    db.set_blocked(st_id, bloqueada)
    if bloqueada:
        log(f"{st_id} BLOQUEADA (fuera de servicio)")
    else:
        log(f"{st_id} ACTIVADA")
    if bloqueada and regando:
        k.enviar(productor, k.t_ordenes(st_id), {"accion": "DETENER", "motivo": "BLOQUEADA"})   


def ejecutar_orden(linea):
    partes = linea.split()
    if len(partes) == 0:
        return
    orden = partes[0].lower()
    if orden == "alta" and len(partes) >= 2:
        nombre = partes[1]
        if len(partes) > 2:
            nombre = " ".join(partes[2:])
        db.add_operator(partes[1], nombre)
        log(f"Operario {partes[1]} ({nombre}) dado de alta")
    elif orden == "altas" and len(partes) == 2 and partes[1].isdigit():
        for i in range(1, int(partes[1]) + 1):   # OP1, OP2, ... OPn
            db.add_operator(f"OP{i}", f"Operario {i}")
        log(f"Operarios OP1 a OP{partes[1]} dados de alta")
    elif orden in ("iniciar", "parar", "bloquear", "activar") and len(partes) >= 2:
        if partes[1].lower() == "todas":
            with candado:
                ids = []
                for st_id in sorted(estaciones.keys(), key=orden_natural):
                    if orden != "parar" or estaciones[st_id].riego:   
                        ids.append(st_id)
        else:
            ids = [partes[1]]
        for st_id in ids:
            if orden == "bloquear":
                bloquear(st_id, True)
            elif orden == "activar":
                bloquear(st_id, False)
            elif orden == "parar":
                parar(st_id, "CENTRAL")
            else:
                segundos = 0   
                if len(partes) > 2 and partes[2].isdigit():
                    segundos = int(partes[2])
                procesar_peticion({"id_operador": "CENTRAL", "id_ws": st_id, "duracion_s": segundos})
    else:
        log(AYUDA)



def escuchar_peticiones():
    """Se ejecuta en un hilo: peticiones de riego. Solo las nuevas: si la central estaba caida no se admiten"""
    consumidor = k.crear_consumidor(SERVIDOR_KAFKA, "central-peticiones", [k.T_PETICIONES])
    while True:
        topic, datos = k.leer(consumidor)
        if datos is None:
            continue
        try:
            procesar_peticion(datos)
        except Exception as e:
            log(f"Petición no válida {datos}: {e}")   # un mensaje malo no tumba el hilo


def procesar_evento(e):
    st_id = e.get("id_ws")
    tipo = e.get("tipo")
    with candado:
        est = estaciones.get(st_id)
        if est is None or est.riego is None:
            return   
        riego = est.riego
        if tipo == "TELEMETRIA":
            riego["caudal"] = e.get("caudal", 0.0)
            riego["volumen"] = e.get("volumen", 0.0)
        op_id = riego["operador"]

    if tipo == "INICIADO":
        log(f"{st_id}: electroválvula abierta, riego en marcha ({op_id})")
        notificar(op_id, st_id, {"estado": "INICIADO"})
    elif tipo == "TELEMETRIA":
        notificar(op_id, st_id, {"estado": "REGANDO", "caudal": e.get("caudal"), "volumen": e.get("volumen")})
    elif tipo == "FINALIZADO":
        finalizar_riego(st_id, e.get("motivo", "FIN"), e.get("volumen", 0.0), e.get("duracion_s", 0.0))
    elif tipo == "ARRANQUE":
        finalizar_riego(st_id, "ENGINE_REINICIADO", round(riego["volumen"], 2), round(time.time() - riego["inicio"], 1))



def finalizar_riego(st_id, motivo, volumen, duracion):
    with candado:
        est = estaciones.get(st_id)
        if est is None or est.riego is None:
            return
        riego = est.riego
        est.riego = None
    db.delete_active(st_id)
    db.add_irrigation(st_id, riego["operador"], duracion, volumen, motivo)
    log(f"{st_id}: riego terminado ({motivo}). {volumen} L en {duracion} s")
    notificar(riego["operador"], st_id, {"estado": "FINALIZADO", "motivo": motivo, "volumen_total": volumen, "duracion_s": duracion})


def escuchar_eventos():
    """Se ejecuta en un hilo: eventos de los engines. Grupo fijo: al volver la central lee lo que llego mientras estaba caida"""
    consumidor = k.crear_consumidor(SERVIDOR_KAFKA, "central-eventos", [k.T_TELEMETRIA], solo_nuevos=False)
    while True:
        topic, datos = k.leer(consumidor)
        if datos is None:
            continue
        try:
            procesar_evento(datos)
        except Exception as e:
            log(f"Evento no válido {datos}: {e}")


def notificar(op_id, st_id, datos):
    datos["id_ws"]= st_id
    if op_id == "CENTRAL":
        return

    if op_id == "MANUAL":
        if datos["estado"]== "DENEGADO":
            k.enviar(productor, k.t_ordenes(st_id), {"accion": "DENEGADO", "motivo": datos["motivo"]})
        return
    k.enviar(productor, k.t_respuestas(op_id), datos)


def interrumpir(st_id, motivo):
    with candado:
        est = estaciones.get(st_id)
        if est is None or est.riego is None or est.riego["interrumpido"]:
            return
        est.riego["interrumpido"] = True
        riego = est.riego
    log (f"{st_id} : riego interrumpido ({motivo}). Llevaba {round(riego['volumen'],2)} L")
    k.enviar(productor, k.t_ordenes(st_id), {"accion": "DETENER", "motivo": motivo})
    notificar(riego["operador"], st_id, {"estado": "INTERRUMPIDO", "motivo": motivo, "volumen": round(riego["volumen"],2)})


def foto():
    lista = []
    with candado:
        for st_id in sorted(estaciones.keys(), key=orden_natural):
            est = estaciones[st_id]
            fila = {"id": est.id, "ubicacion": est.ubicacion, "estado": est.estado(),
                    "caudal": 0.0, "volumen": 0.0, "operario": ""}
            if est.riego:
                fila["caudal"] = est.riego["caudal"]
                fila["volumen"] = round(est.riego["volumen"], 2)
                fila["operario"] = est.riego["operador"]
            lista.append(fila)
    return lista



def tabla_panel(lista, con_colores):
   
    lineas = [f"{'ID':<6}{'UBICACION':<16}{'ESTADO':^18}  {'L/MIN':>6}  {'LITROS':>7}  OPERARIO"]
    for s in lista:
        texto = f"{NOMBRES[s['estado']]:^18}"
        if con_colores:
            texto = COLORES[s["estado"]] + texto + RESET
        lineas.append(f"{s['id']:<6}{s['ubicacion'][:15]:<16}{texto}  {s['caudal']:>6.1f}  {s['volumen']:>7.1f}  {s['operario']}")
    return lineas


def dibujar_panel(lista):
   
    salida = ["\033[2J\033[H" + "WM_Central · estaciones de riego (las órdenes, en el panel web)", ""]
    salida += tabla_panel(lista, True)
    salida += ["", "Últimos mensajes:"] + mensajes
    print("\n".join(salida), flush=True)



def bucle_estado():
    """Se ejecuta en un hilo: cada segundo guarda los cambios de estado en la BD, avisa a los FO y muestra el panel"""
    guardados = {}
    en_terminal = sys.stdout.isatty()   
    while True:
        lista = foto()
        cambios = False
        for s in lista:
            if guardados.get(s["id"]) != s["estado"]:
                db.set_status(s["id"], s["estado"])
                guardados[s["id"]] = s["estado"]
                cambios = True
        k.enviar(productor, k.T_ESTADOS, {"estaciones": lista})
        if en_terminal:
            dibujar_panel(lista)
        elif cambios:   
            print("\n".join(tabla_panel(lista, False)), flush=True)
        time.sleep(1)



def main ():
    global SERVIDOR_KAFKA, productor
    if len(sys.argv) not in (3, 4):
        print("Uso: python WM_Central.py <puerto_sockets> <kafka_ip:puerto>[puerto_web]")
        return
    puerto = int(sys.argv[1])
    SERVIDOR_KAFKA = sys.argv[2]
    if len(sys.argv) == 4:
        puerto_web = int(sys.argv[3])
    else:
        puerto_web = int(os.environ.get("PORT", 8080))

    cargar_estaciones()

    k.crear_topics(SERVIDOR_KAFKA, [k.T_PETICIONES, k.T_TELEMETRIA, k.T_ESTADOS])

    productor = k.crear_productor(SERVIDOR_KAFKA)

    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind(("0.0.0.0", puerto))
    servidor.listen()
    log(f" Escuchando en el puerto {puerto}")

    while True:
        conn, addr = servidor.accept()
        hilo = threading.Thread(target=atender_monitor, args=(conn, addr), daemon=True)
        hilo.start()
if __name__ == "__main__":
    main()