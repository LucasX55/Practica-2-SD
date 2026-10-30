import os
import socket
import threading
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')) #para que python encuentre la carpeta common, que esta un nivel arriba
from common.protocol import recv_message, send_message, ConnectionClosed
from common.database import Database
from common.modelo import Station, DISPONIBLE, REGANDO, FUGA, FUERA_DE_SERVICIO, DESCONECTADA
from common import kafka_utils as k
import panel_web #el panel web esta en otro archivo de esta misma carpeta

db = Database(os.environ.get("WM_DB", "wm_central.db")) #base de datos; la ruta se puede cambiar con la variable WM_DB
TIMEOUT_MONITOR = float(os.environ.get("WM_TIMEOUT_MONITOR", 10)) #segundos sin saber nada de un monitor para darlo por caido
estaciones = {} #todas las estaciones en memoria: {"ST1": Station, ...}
candado = threading.Lock() #solo un hilo puede tocar las estaciones a la vez

SERVIDOR_KAFKA = None #ip:puerto de kafka, se rellena en el main
productor = None #para enviar mensajes a kafka, se crea en el main
mensajes = [] #ultimos 10 mensajes, para enseñarlos en los paneles

AYUDA = ("Órdenes: iniciar <id|todas> [segundos] | parar <id|todas> | bloquear <id|todas> | ""activar <id|todas> | alta <operario> [nombre] | altas <numero>") #lo que se enseña cuando no se entiende una orden


# Colores de la consola (codigos ANSI): fondo verde, verde parpadeando, rojo, naranja y gris
COLORES = {DISPONIBLE: "\033[30;42m", REGANDO: "\033[5;30;42m", FUGA: "\033[97;41m", FUERA_DE_SERVICIO: "\033[30;48;5;208m", DESCONECTADA: "\033[30;47m"}

RESET = "\033[0m" #quita el color


NOMBRES = {DISPONIBLE: "Available", REGANDO: "Watering", FUGA: "Leak", FUERA_DE_SERVICIO: "Out of Service", DESCONECTADA: "Disconnected"} #como se llama cada estado en el panel, en ingles como pide la guia

#########################################################################
# Carga de la BD las estaciones y los riegos que estaban a medias
# Se llama al arrancar la central: asi, aunque la central se caiga y vuelva, sigue
# conociendo todas las estaciones (salen en gris hasta que su monitor conecta)
# y recupera los riegos que estaban en marcha para poder cerrarlos bien
#########################################################################
def cargar_estaciones():
    for fila in db.get_stations(): #recorremos las estaciones guardadas
        est = Station(fila["id"], fila["location"], bool(fila["blocked"]))
        estaciones[fila["id"]] = est #la guardamos en memoria; sale desconectada hasta que conecte su monitor
        log(f"Cargada desde la BD: {est.id} en {est.ubicacion} -> {est.estado()} (bloqueada={est.bloqueada})")
    for fila in db.get_active(): #riegos que estaban en marcha cuando se cayo la central
        est = estaciones.get(fila["station"])
        if est:
            est.riego = {"operador": fila["operator"], "duracion_s": fila["duration_s"], "inicio": fila["started_at"], "caudal": 0.0, "volumen": 0.0, "interrumpido": False} #lo volvemos a poner regando; caudal y litros nos los manda el engine
            log(f"Riego en curso recuperado de la BD: {est.id} ({fila['operator']}) ")                                                              

#########################################################################
# Registra una estacion nueva o cambia su ubicacion si ya existia
#########################################################################
def registrar(st_id, ubicacion):
    with candado: #cogemos el candado para que ningun otro hilo toque las estaciones mientras
        est = estaciones.get(st_id)
        if est is None: #es nueva
            estaciones[st_id] = Station(st_id, ubicacion) #la creamos en memoria
            db.add_station(st_id, ubicacion) #y la guardamos en la BD
            log(f"Nueva estación registrada: {st_id} en {ubicacion}")
        else: #ya existia: solo cambiamos su ubicacion
            est.ubicacion = ubicacion
            db.set_location(st_id, ubicacion)
            log(f"Estación {st_id} ya registrada, ubicación: {ubicacion}")
    return "OK" #respuesta para el monitor

#########################################################################
# Para ordenar ST1, ST2 ... ST10 y no ST1, ST10, ST2
#########################################################################
def orden_natural(st_id):
    return(len(st_id), st_id) #primero por largo y luego por texto


#########################################################################
# Escribe un mensaje con la hora y lo guarda para los paneles
#########################################################################
def log(texto):
    hora = time.strftime("%H:%M:%S")
    print (f"[Central {hora}] {texto}", flush=True)
    mensajes.append(f"[{hora}] {texto}") #lo guardamos para enseñarlo en los paneles
    if len(mensajes) > 10: #solo nos quedamos con los 10 ultimos
        mensajes.pop(0) #quitamos el mas antiguo


#########################################################################
# Comprueba si la estacion puede entrar
#########################################################################
def autenticar(st_id):
    with candado:
        est = estaciones.get(st_id)
        if est is None: #si no esta registrada no la dejamos entrar
            log(f"Autentificacion denegada: estacion {st_id} no registrada")
            return "KO#estacion no registrada"

        if est.conectada: #si ya hay un monitor conectado con ese id, tampoco
            log(f"Autentificacion denegada: estacion {st_id} ya conectada")
            return "KO#estacion ya conectada"
        est.conectada = True #a partir de aqui la estacion esta conectada
        log(f"Estación {st_id} autentificada")
        return "OK"






#########################################################################
# Marca la estacion como desconectada cuando se cae su monitor
#########################################################################
def desconectar(st_id):
    if st_id is None: #el monitor se fue antes de autenticarse, no hay nada que hacer
        return
    with candado:
        est = estaciones.get(st_id)    
        if est is None:
            return
        est.conectada = False #pasa a gris
    log(f"Estacion {st_id} desconectada")
    interrumpir(st_id, "DESCONECTADA") #fuera del candado porque interrumpir tambien lo coge


#########################################################################
# Pone o quita la fuga de una estacion
#########################################################################
def cambiar_fuga(st_id, hay_fuga):
    with candado:
        est = estaciones.get(st_id)
        est.fuga = hay_fuga
        estado = est.estado() #calculamos el estado dentro del candado y lo usamos fuera
    if hay_fuga: #ya fuera del candado: si interrumpir se llamara dentro, se quedaria bloqueado para siempre
        log(f"FUGA/averia en estacion {st_id}. Estado {estado}")
        interrumpir(st_id, "FUGA") #si estaba regando, el riego se corta en el momento
    else:
        log(f"Averia resuelta en estacion {st_id}. Estado {estado}")
    return "OK"



#########################################################################
# Habla con un monitor hasta que se desconecte (un hilo por monitor)
# Es la parte de sockets de la central: lee cada trama del monitor (REGISTRAR,
# AUTENTICAR, FUGA, RESUELTA o PING), hace lo que toca y le contesta OK o KO.
# Si el monitor deja de mandar latidos o cierra la conexion, la estacion pasa a
# desconectada y, si estaba regando, el riego se corta
#########################################################################
def atender_monitor(conn,addr):
    """Se ejecuta en un hilo: atiendo a un cliente conectado""" 
    log(f"Conectado a {addr}")
    conn.settimeout(TIMEOUT_MONITOR) #si en ese tiempo no llega nada, salta socket.timeout
    st_id = None #todavia no sabemos que estacion es
    try:
        while True: #cada vuelta, un mensaje del monitor
            mensaje = recv_message(conn) #esperamos un mensaje; recv_message ya contesta el ACK
            partes = mensaje.split("#") #separamos el tipo de los datos, ej: FUGA#ST1 -> [FUGA, ST1]
            if partes[0] != "PING": #los PING no se escriben para no llenar el log
                log(f"Recibido: {mensaje}")
            if partes[0] == "REGISTRAR" and len(partes) >= 3: #REGISTRAR#id#ubicacion
                respuesta = registrar(partes[1], partes[2])
            elif partes[0] == "AUTENTICAR" and len(partes) >= 2: #AUTENTICAR#id
                if st_id is not None: #este monitor ya se habia autenticado
                    respuesta = "KO#ya autentificado"
                else:
                    respuesta = autenticar(partes[1])
                    if respuesta == "OK": #si entra, guardamos su id para el resto de mensajes
                        st_id = partes[1]
            elif partes[0] == "FUGA" and st_id and len(partes) >=2: #solo si ya se ha autenticado
                respuesta = cambiar_fuga(st_id, True) if partes[1] == st_id else "KO#id no coincide" #el id del mensaje tiene que ser el suyo
            elif partes[0] == "RESUELTA" and st_id and len(partes) >=2: #la averia se ha arreglado
                respuesta = cambiar_fuga(st_id, False) if partes[1] == st_id else "KO#id no coincide"
            elif partes[0] == "PING" and st_id: #latido: el monitor sigue vivo
                respuesta = "OK"
            else: #cualquier otra cosa no la entendemos
                respuesta = "KO#mensaje no valido"
            send_message(conn, respuesta) #siempre contestamos, el monitor se queda esperando la respuesta
    except socket.timeout: #no han llegado latidos a tiempo: el monitor se ha colgado
        log(f"{st_id}: no llegan latidos del monitor")      
    except (ConnectionClosed, OSError) : #el monitor ha cerrado la conexion o se ha caido
        pass
    finally: #pase lo que pase, la estacion se queda desconectada
        desconectar(st_id)
        conn.close()
        log(f"{addr} desconectado")


#########################################################################
# Atiende una peticion de riego (o de parar) que llega por kafka
# Comprueba que el operario este dado de alta y que la estacion este disponible.
# Si todo va bien apunta el riego (en memoria y en la BD) y le manda al engine la
# orden de abrir la valvula; si no, se la deniega al que la pidio diciendo por que.
# Al operario se le va avisando de cada paso: RECIBIDA, AUTORIZADO o DENEGADO
#########################################################################
def procesar_peticion(p):
    op_id = p.get("id_operador") #quien la pide: un operario, MANUAL (desde el engine) o CENTRAL
    st_id = p.get("id_ws")
    if p.get("accion") == "PARAR": #en vez de regar, quiere parar
        parar(st_id, op_id)
        return
    duracion = int(p.get("duracion_s", 0)) #segundos de riego; 0 es sin limite
    texto_duracion = f"{duracion} s" if duracion > 0 else "sin límite"
    log(f"Petición de {op_id}: regar {st_id} ({texto_duracion})")
    notificar(op_id, st_id, {"estado": "RECIBIDA"}) #le decimos que la hemos recibido
    motivo = None #si al final tiene algo, la peticion se deniega
    with candado:
        est = estaciones.get(st_id)
        if op_id != "MANUAL" and op_id != "CENTRAL" and not db.operator_exists(op_id): #el operario tiene que estar dado de alta
            motivo = "operario no registrado"
        elif est is None:
            motivo = "la estación no existe"
        elif est.estado() != DISPONIBLE: #solo se riega si esta disponible
            motivo =f"la estación está {est.estado()}"
        elif duracion < 0:
            motivo = "duracion no válida"
        else: #todo bien: apuntamos el riego
            est.riego = {"operador": op_id, "duracion_s": duracion, "inicio": time.time(), "caudal": 0.0, "volumen": 0.0, "interrumpido": False}
            db.add_active(st_id, op_id, est.riego["inicio"], duracion) #y lo guardamos en la BD por si se cae la central
    if motivo: #denegada: avisamos y no seguimos
        log(f"DENEGADA petición de {op_id} en {st_id}: {motivo}")
        notificar(op_id, st_id, {"estado": "DENEGADO", "motivo": motivo})
        return
    log(f"AUTORIZADO riego de {op_id} en {st_id}")
    notificar(op_id, st_id, {"estado": "AUTORIZADO"})
    k.enviar(productor, k.t_ordenes(st_id), {"accion": "INICIAR", "id_operador": op_id, "duracion_s": duracion}) #le decimos al engine de esa estacion que abra la valvula


#########################################################################
# Para un riego
#########################################################################
def parar(st_id, quien):
    # Para un riego en marcha. Un operario solo puede parar el suyo
    with candado:
        est = estaciones.get(st_id)
        riego = est.riego if est else None #si la estacion no existe, como si no estuviera regando
    if riego is None: #no hay nada que parar
        log(f"{quien} quiere parar {st_id}, pero no está regando")
        notificar(quien, st_id, {"estado": "DENEGADO", "motivo": "la estación no está regando"})
        return
    if quien != "CENTRAL" and quien != riego["operador"]: #la central puede parar cualquiera, un operario solo el suyo
        log(f"{quien} quiere parar {st_id}, pero el riego es de {riego['operador']}")
        notificar(quien, st_id, {"estado": "DENEGADO", "motivo": "ese riego no es tuyo"})
        return
    log(f"{st_id}: {quien} para el riego")
    k.enviar(productor, k.t_ordenes(st_id), {"accion": "DETENER", "motivo": f"PARADO_POR_{quien}"}) #el engine cierra la valvula y nos manda el FINALIZADO

#########################################################################
# Bloquea (fuera de servicio) o activa una estacion
#########################################################################
def bloquear(st_id, bloqueada):
    with candado:
        est = estaciones.get(st_id)
        if est is None:
            log(f"No existe la estación {st_id}")
            return
        est.bloqueada = bloqueada #True: fuera de servicio, False: vuelve a estar disponible
        regando = est.riego is not None #miramos si estaba regando antes de soltar el candado
    db.set_blocked(st_id, bloqueada) #lo guardamos para que siga igual si se reinicia la central
    if bloqueada:
        log(f"{st_id} BLOQUEADA (fuera de servicio)")
    else:
        log(f"{st_id} ACTIVADA")
    if bloqueada and regando: #si la bloqueamos mientras riega, el riego se corta
        k.enviar(productor, k.t_ordenes(st_id), {"accion": "DETENER", "motivo": "BLOQUEADA"})   


#########################################################################
# Ejecuta una orden escrita en el panel web o en la consola
# Ordenes: iniciar, parar, bloquear y activar (a una estacion o a todas), y alta/altas
# de operarios. Las dos interfaces llaman a esta misma funcion, asi hacen lo mismo
#########################################################################
def ejecutar_orden(linea):
    partes = linea.split() #separamos por espacios, ej: iniciar ST1 30 -> [iniciar, ST1, 30]
    if len(partes) == 0: #linea vacia
        return
    orden = partes[0].lower() #da igual mayusculas o minusculas
    if orden == "alta" and len(partes) >= 2: #alta OP1 Lucas
        nombre = partes[1]
        if len(partes) > 2:
            nombre = " ".join(partes[2:])
        db.add_operator(partes[1], nombre)
        log(f"Operario {partes[1]} ({nombre}) dado de alta")
    elif orden == "altas" and len(partes) == 2 and partes[1].isdigit(): #altas 5: da de alta de OP1 a OP5
        for i in range(1, int(partes[1]) + 1):   # OP1, OP2, ... OPn
            db.add_operator(f"OP{i}", f"Operario {i}")
        log(f"Operarios OP1 a OP{partes[1]} dados de alta")
    elif orden in ("iniciar", "parar", "bloquear", "activar") and len(partes) >= 2: #ordenes que van a una estacion o a todas
        if partes[1].lower() == "todas": #hacemos la lista de estaciones a las que afecta
            with candado:
                ids = []
                for st_id in sorted(estaciones.keys(), key=orden_natural):
                    if orden != "parar" or estaciones[st_id].riego:   
                        ids.append(st_id)
        else:
            ids = [partes[1]] #solo una
        for st_id in ids:
            if orden == "bloquear":
                bloquear(st_id, True)
            elif orden == "activar":
                bloquear(st_id, False)
            elif orden == "parar":
                parar(st_id, "CENTRAL")
            else: #iniciar: se hace como una peticion mas, con operario CENTRAL
                segundos = 0 #sin segundos es sin limite
                if len(partes) > 2 and partes[2].isdigit():
                    segundos = int(partes[2])
                procesar_peticion({"id_operador": "CENTRAL", "id_ws": st_id, "duracion_s": segundos})
    else: #orden que no entendemos: enseñamos la ayuda
        log(AYUDA)



#########################################################################
# Hilo que lee las peticiones de riego de kafka
#########################################################################
def escuchar_peticiones():
    """Se ejecuta en un hilo: peticiones de riego. Solo las nuevas: si la central estaba caida no se admiten"""
    consumidor = k.crear_consumidor(SERVIDOR_KAFKA, "central-peticiones", [k.T_PETICIONES]) #solo nuevas: las que llegaron con la central caida no se atienden
    while True:
        topic, datos = k.leer(consumidor) #esperamos hasta 1 segundo a que llegue algo
        if datos is None: #no ha llegado nada, volvemos a esperar
            continue
        try: #si una peticion viene mal, que no se pare el hilo
            procesar_peticion(datos)
        except Exception as e:
            log(f"Petición no válida {datos}: {e}")   # un mensaje malo no tumba el hilo


#########################################################################
# Atiende lo que nos cuenta el engine sobre un riego
# El engine manda por kafka cuando abre la valvula (INICIADO), cada segundo el
# caudal y los litros (TELEMETRIA), cuando acaba (FINALIZADO) y cuando arranca
# (ARRANQUE). Con eso la central actualiza el panel y avisa al operario
#########################################################################
def procesar_evento(e):
    st_id = e.get("id_ws")
    tipo = e.get("tipo") #INICIADO, TELEMETRIA, FINALIZADO o ARRANQUE
    with candado:
        est = estaciones.get(st_id)
        if est is None or est.riego is None: #si ese riego ya lo hemos cerrado, lo ignoramos
            return   
        riego = est.riego
        if tipo == "TELEMETRIA": #actualizamos caudal y litros para los paneles
            riego["caudal"] = e.get("caudal", 0.0)
            riego["volumen"] = e.get("volumen", 0.0)
        op_id = riego["operador"] #a quien hay que avisar

    if tipo == "INICIADO": #el engine ya ha abierto la valvula
        log(f"{st_id}: electroválvula abierta, riego en marcha ({op_id})")
        notificar(op_id, st_id, {"estado": "INICIADO"})
    elif tipo == "TELEMETRIA": #cada segundo le contamos al operario cuanto lleva
        notificar(op_id, st_id, {"estado": "REGANDO", "caudal": e.get("caudal"), "volumen": e.get("volumen")})
    elif tipo == "FINALIZADO": #el riego ha terminado (por tiempo, parado, fuga...)
        finalizar_riego(st_id, e.get("motivo", "FIN"), e.get("volumen", 0.0), e.get("duracion_s", 0.0))
    elif tipo == "ARRANQUE": #el engine se ha reiniciado: ese riego ya no sigue, lo cerramos con lo ultimo que sabiamos
        finalizar_riego(st_id, "ENGINE_REINICIADO", round(riego["volumen"], 2), round(time.time() - riego["inicio"], 1))



#########################################################################
# Cierra un riego: lo guarda en el historico y avisa al operario
#########################################################################
def finalizar_riego(st_id, motivo, volumen, duracion):
    with candado:
        est = estaciones.get(st_id)
        if est is None or est.riego is None:
            return
        riego = est.riego
        est.riego = None #la estacion queda libre
    db.delete_active(st_id) #ya no esta en curso
    db.add_irrigation(st_id, riego["operador"], duracion, volumen, motivo) #lo apuntamos en el historico
    log(f"{st_id}: riego terminado ({motivo}). {volumen} L en {duracion} s")
    notificar(riego["operador"], st_id, {"estado": "FINALIZADO", "motivo": motivo, "volumen_total": volumen, "duracion_s": duracion}) #el resumen final, con litros y duracion


#########################################################################
# Hilo que lee los eventos de los engines
#########################################################################
def escuchar_eventos():
    """Se ejecuta en un hilo: eventos de los engines. Grupo fijo: al volver la central lee lo que llego mientras estaba caida"""
    consumidor = k.crear_consumidor(SERVIDOR_KAFKA, "central-eventos", [k.T_TELEMETRIA], solo_nuevos=False) #grupo fijo: si la central se cae, al volver sigue donde lo dejo
    while True:
        topic, datos = k.leer(consumidor)
        if datos is None:
            continue
        try:
            procesar_evento(datos)
        except Exception as e:
            log(f"Evento no válido {datos}: {e}")


#########################################################################
# Avisa a quien pidio el riego
#########################################################################
def notificar(op_id, st_id, datos):
    datos["id_ws"]= st_id #siempre decimos de que estacion es
    if op_id == "CENTRAL": #riegos de la central: no hay operario al que avisar
        return

    if op_id == "MANUAL": #riegos pedidos desde el propio engine
        if datos["estado"]== "DENEGADO": #solo le avisamos si se lo denegamos, por su topic de ordenes
            k.enviar(productor, k.t_ordenes(st_id), {"accion": "DENEGADO", "motivo": datos["motivo"]})
        return
    k.enviar(productor, k.t_respuestas(op_id), datos) #a un operario, por su topic de respuestas


#########################################################################
# Corta un riego por un problema (fuga o monitor caido) y avisa al operario
# No cierra el riego: le dice al engine que pare y es el engine el que manda
# el FINALIZADO con los litros reales. Ojo: no se puede llamar con el candado
# cogido, porque esta funcion tambien lo coge y se quedaria bloqueada
#########################################################################
def interrumpir(st_id, motivo):
    with candado:
        est = estaciones.get(st_id)
        if est is None or est.riego is None or est.riego["interrumpido"]: #sin riego, o ya interrumpido antes: no repetimos el aviso
            return
        est.riego["interrumpido"] = True #lo marcamos para no avisar dos veces
        riego = est.riego
    log (f"{st_id} : riego interrumpido ({motivo}). Llevaba {round(riego['volumen'],2)} L")
    k.enviar(productor, k.t_ordenes(st_id), {"accion": "DETENER", "motivo": motivo}) #le decimos al engine que cierre
    notificar(riego["operador"], st_id, {"estado": "INTERRUMPIDO", "motivo": motivo, "volumen": round(riego["volumen"],2)}) #el riego no se cierra aqui: se cierra cuando llegue el FINALIZADO del engine


#########################################################################
# Lista con el estado de todas las estaciones (para los paneles y los FO)
#########################################################################
def foto():
    lista = []
    with candado:
        for st_id in sorted(estaciones.keys(), key=orden_natural): #en orden: ST1, ST2...
            est = estaciones[st_id]
            fila = {"id": est.id, "ubicacion": est.ubicacion, "estado": est.estado(), #por defecto sin caudal ni litros
                    "caudal": 0.0, "volumen": 0.0, "operario": ""}
            if est.riego: #si esta regando, rellenamos los datos del riego
                fila["caudal"] = est.riego["caudal"]
                fila["volumen"] = round(est.riego["volumen"], 2)
                fila["operario"] = est.riego["operador"]
            lista.append(fila)
    return lista



#########################################################################
# Monta las lineas de la tabla del panel de consola
#########################################################################
def tabla_panel(lista, con_colores):
   
    lineas = [f"{'ID':<6}{'UBICACION':<16}{'ESTADO':^18}  {'L/MIN':>6}  {'LITROS':>7}  OPERARIO"] #cabecera; :<6 es alinear a la izquierda en 6 huecos y :^18 centrar en 18
    for s in lista:
        texto = f"{NOMBRES[s['estado']]:^18}" #nombre del estado, centrado
        if con_colores: #en la terminal le ponemos su color, en los logs no
            texto = COLORES[s["estado"]] + texto + RESET
        lineas.append(f"{s['id']:<6}{s['ubicacion'][:15]:<16}{texto}  {s['caudal']:>6.1f}  {s['volumen']:>7.1f}  {s['operario']}") #una fila por estacion
    return lineas


#########################################################################
# Pinta el panel en la terminal
#########################################################################
def dibujar_panel(lista):

    salida = ["\033[2J\033[H" + "WM_Central · estaciones de riego (escribe una orden y pulsa Enter, o usa el panel web)", ""] #\033[2J borra la pantalla y \033[H sube el cursor arriba
    salida += tabla_panel(lista, True)
    salida += ["", "Últimos mensajes:"] + mensajes #debajo, los ultimos mensajes
    print("\n".join(salida), flush=True)



#########################################################################
# Hilo que cada segundo actualiza el estado
# Guarda en la BD los cambios de estado, manda a todos los FO la lista de
# estaciones (asi ven cuales estan disponibles y saben si la central sigue viva)
# y repinta el panel de consola
#########################################################################
def bucle_estado():
    """Se ejecuta en un hilo: cada segundo guarda los cambios de estado en la BD, avisa a los FO y muestra el panel"""
    guardados = {} #ultimo estado que hemos guardado de cada estacion
    en_terminal = sys.stdout.isatty() #True en una terminal; False en los logs o en docker sin -t
    while True:
        lista = foto()
        cambios = False
        for s in lista:
            if guardados.get(s["id"]) != s["estado"]: #solo escribimos en la BD si ha cambiado
                db.set_status(s["id"], s["estado"])
                guardados[s["id"]] = s["estado"]
                cambios = True
        k.enviar(productor, k.T_ESTADOS, {"estaciones": lista}) #se lo mandamos a los FO; si deja de llegar, saben que la central se ha caido
        if en_terminal: #en la terminal redibujamos cada segundo
            dibujar_panel(lista)
        elif cambios: #en los logs solo cuando cambia algo, para no llenarlos
            print("\n".join(tabla_panel(lista, False)), flush=True)
        time.sleep(1) #una vuelta por segundo

#########################################################################
# Hilo que lee ordenes escritas en la terminal de la central
# Cada linea se pasa a ejecutar_orden, igual que las del panel web
#########################################################################
def leer_consola():
    while True:
        try:
            linea = input() #espera a que se escriba una linea y se pulse Enter
        except EOFError:   #no hay teclado (por ejemplo, Docker sin -it): se deja de leer
            return
        if linea.strip(): #las lineas vacias no hacen nada
            ejecutar_orden(linea)

#########################################################################
# Principal
# Lee los argumentos, carga la BD, crea los topics de kafka, arranca un hilo para
# cada tarea (peticiones, eventos, estado, panel web y consola) y se queda
# esperando monitores: a cada uno que conecta le da su propio hilo
#########################################################################
def main ():
    global SERVIDOR_KAFKA, productor #las vamos a cambiar: son las de arriba del archivo
    if len(sys.argv) not in (3, 4): #el puerto web es opcional
        print("Uso: python WM_Central.py <puerto_sockets> <kafka_ip:puerto>[puerto_web]")
        return
    puerto = int(sys.argv[1])
    SERVIDOR_KAFKA = sys.argv[2]
    if len(sys.argv) == 4:
        puerto_web = int(sys.argv[3])
    else:
        puerto_web = int(os.environ.get("PORT", 8080)) #si no nos lo pasan, el de la variable PORT o el 8080

    cargar_estaciones() #primero lo que habia en la BD

    k.crear_topics(SERVIDOR_KAFKA, [k.T_PETICIONES, k.T_TELEMETRIA, k.T_ESTADOS]) #creamos los topics que usa la central (si ya existen no pasa nada)

    productor = k.crear_productor(SERVIDOR_KAFKA)

    threading.Thread(target=escuchar_peticiones, daemon=True).start() #cada cosa en su hilo para que funcionen a la vez
    threading.Thread(target=escuchar_eventos, daemon=True).start()
    threading.Thread(target=bucle_estado, daemon=True).start()
    log(f"Panel web en http://localhost:{puerto_web}")
    threading.Thread(target=panel_web.arrancar, args=(puerto_web, foto, ejecutar_orden, mensajes), daemon=True).start() #al panel le pasamos las funciones (sin parentesis) y la lista de mensajes
    threading.Thread(target=leer_consola, daemon=True).start()
    
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #socket TCP para los monitores
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) #para poder reiniciar la central sin esperar a que se libere el puerto
    servidor.bind(("0.0.0.0", puerto)) #0.0.0.0: acepta conexiones de cualquier ordenador
    servidor.listen()
    log(f" Escuchando en el puerto {puerto}")

    while True:
        conn, addr = servidor.accept() #esperamos a que conecte un monitor
        hilo = threading.Thread(target=atender_monitor, args=(conn, addr), daemon=True) #cada monitor en su propio hilo
        hilo.start()
if __name__ == "__main__":
    main()