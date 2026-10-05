DISPONIBLE = "DISPONIBLE"
REGANDO = "REGANDO"
FUGA = "FUGA"
FUERA_DE_SERVICIO = "FUERA DE SERVICIO"
DESCONECTADA = "DESCONECTADA"



class Station:
    def __init__(self, id,ubicacion, bloqueada=False):
        self.id = id
        self.ubicacion = ubicacion
        self.bloqueada = bloqueada
        self.conectada = False #monitor conectado?
        self.fuga = False #Hay fduga?
        self.riego = None #se esta regando?
        self.token = None 


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