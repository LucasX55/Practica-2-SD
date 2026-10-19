import socket
import sys
import threading
import time
import json
from kafka import KafkaProducer, KafkaConsumer


FORMAT= 'utf-8'
riego_activo = False


def consumidor(ip_kafka, puerto_kafka, id_operador):
    global riego_activo
    
    consumidor = KafkaConsumer(f'wm_respuestas_{id_operador}',bootstrap_servers=[f"{ip_kafka}:{puerto_kafka}"],
                               value_deserializer= lambda x: json.loads(x.decode(FORMAT)), auto_offset_reset= 'latest')
    
    
    print(f"[{id_operador}] Escuchando a la central")
    
    for mensaje in consumidor:
        res = mensaje.value
        print(f"[{id_operador}] Mensaje proveniente de la Central: {res}")
        
        if res.get("estado") in ["FINALIZADO", "ERROR_FUGA", "DENEGADO"]:
            riego_activo= False
            
            
            
def procesar_fichero(ip_kafka, puerto_kafka, id_operador,fichero):
    global riego_activo
    
    productor=KafkaProducer(
        bootstrap_servers= [f"{ip_kafka}:{puerto_kafka}"],
        value_serializer= lambda x:json.dumps(x).encode('utf-8')
    ) 
    
    
    try: 
        with open(fichero,'r') as f:
            lineas=f.readlines()
            
        for linea in lineas:
            linea=linea.strip()
            
            if not linea:
                continue
            
            partes=linea.split(',')
            if len(partes)==2:
                peticion={"id_operador":id_operador,"id_ws":partes[0].strip(),"duracion_s": int(partes[1].strip()),"accion":"SOLICITAR"}
                
                print(f"[{id_operador}] Solicitando riego: {peticion}")
                productor.send('wm_peticiones_riego',value=peticion)
                riego_activo=True
                
                while riego_activo:
                    time.sleep(0.5)
                    
                print(f"[{id_operador}] Servicio terminado.")
                time.sleep(4)  #Segundos de seguridad
        print(f"[{id_operador}] Todas las peticiones han sido procesadas")
        
    except FileNotFoundError:
        print(f"No hay fichero {fichero}")
        
    except Exception as e:
        print(f"Error en la petición: {e}")
        
        
if  (len(sys.argv) == 5):
    IP_KAFKA = sys.argv[1]
    PUERTO_KAFKA = int(sys.argv[2])    
    ID_OPERADOR = sys.argv[3]
    FICHERO = sys.argv[4]
    
    threading.Thread(target=consumidor,args=(IP_KAFKA, PUERTO_KAFKA, ID_OPERADOR), daemon=True).start()
    time.sleep(1)
    procesar_fichero(IP_KAFKA,PUERTO_KAFKA,ID_OPERADOR,FICHERO)
    
else:
    print("Error de argumentos")
