#########################################################################
# Los cinco estados de una estacion. Se guardan como texto para poder
# mandarlos tal cual por kafka y guardarlos en la BD
#########################################################################
DISPONIBLE = "DISPONIBLE" #verde: lista para regar
REGANDO = "REGANDO" #verde parpadeando: esta regando
FUGA = "FUGA" #rojo: averia o fuga, no se puede regar
FUERA_DE_SERVICIO = "FUERA DE SERVICIO" #naranja: bloqueada por la central
DESCONECTADA = "DESCONECTADA" #gris: su monitor no esta conectado



#########################################################################
# Una estacion tal como la ve la central
# No guarda el estado directamente: guarda lo que sabe de ella (si su monitor esta
# conectado, si tiene fuga, si esta bloqueada y si esta regando) y el estado se
# calcula a partir de eso en estado()
#########################################################################
class Station:
    def __init__(self, id,ubicacion, bloqueada=False):
        self.id = id #ST1, ST2...
        self.ubicacion = ubicacion #el parque o jardin
        self.bloqueada = bloqueada #True si la central la ha puesto fuera de servicio
        self.conectada = False #monitor conectado?
        self.fuga = False #Hay fduga?
        self.riego = None #se esta regando?


    #########################################################################
    # Calcula el estado. El orden de los if importa: lo primero que se cumple gana
    # Si no esta conectada da igual lo demas (no sabemos nada de ella); una fuga va
    # por delante de bloqueada; y solo si no hay ningun problema miramos si riega
    #########################################################################
    def estado(self):
        if not self.conectada:
            return DESCONECTADA
        if self.fuga:
            return FUGA
        if self.bloqueada:
            return FUERA_DE_SERVICIO
        if self.riego:
            return REGANDO
        return DISPONIBLE