import sqlite3 #la base de datos que pide el enunciado; viene con python
import threading #para el candado de la conexion

#########################################################################
# Base de datos SQLite de la central
# Guarda los operarios dados de alta, las estaciones registradas (con su ubicacion,
# si estan bloqueadas y su ultimo estado), los riegos que estan en marcha y el
# historico de riegos terminados. Gracias a ella la central no pierde nada si se cae
#########################################################################
class Database:
    def __init__(self, ruta):
        self.conn = sqlite3.connect(ruta, check_same_thread=False) #abre el archivo (si no existe lo crea); check_same_thread=False: la usan varios hilos
        self.conn.row_factory = sqlite3.Row #para leer las columnas por nombre: fila["id"]
        self.lock = threading.Lock() #solo un hilo usa la conexion a la vez
        self._crear_tablas() #nada mas abrirla, nos aseguramos de que existen las tablas

    #########################################################################
    # Crea las tablas si no existen
    # operators: operarios dados de alta. stations: estaciones registradas.
    # irrigations: historico de riegos. active_irrigations: riegos en curso, para
    # recuperarlos si la central se cae a mitad de un riego
    #########################################################################
    def _crear_tablas(self):
        with self.lock, self.conn: #with self.conn: si todo va bien guarda los cambios (commit); executescript lanza varias sentencias SQL de golpe
            self.conn.executescript("""
                CREATE TABLE IF NOT EXISTS operators(
                    id TEXT PRIMARY KEY,
                    name TEXT
                );
                CREATE TABLE IF NOT EXISTS stations(
                    id TEXT PRIMARY KEY,
                    location TEXT,
                    blocked INTEGER DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS irrigations(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    station TEXT,
                    operator TEXT,
                    started_at TEXT,
                    duration_s REAL,
                    volume_l REAL,
                    reason TEXT
                );
                CREATE TABLE IF NOT EXISTS active_irrigations(
                    station TEXT PRIMARY KEY,
                    operator TEXT,
                    started_at REAL,
                    duration_s REAL
                );
            """)

            try:
                self.conn.execute("ALTER TABLE stations ADD COLUMN status TEXT DEFAULT 'DESCONECTADA'") #columna nueva para BDs antiguas; si ya existe da error y lo ignoramos
            except sqlite3.OperationalError:
                pass


    #########################################################################
    # Ejecuta una sentencia que cambia datos (INSERT, UPDATE o DELETE) y la guarda
    #########################################################################
    def _ejecutar (self, sql, args=()):
        with self.lock, self.conn: #cogemos el candado y al salir se hace el commit
            self.conn.execute(sql, args)

    #########################################################################
    # Ejecuta un SELECT y devuelve todas las filas
    #########################################################################
    def _consultar (self, sql, args=()):
        with self.lock:
            return self.conn.execute(sql, args).fetchall() #los ? del SQL se rellenan con args, asi no hay problemas con comillas

    # Operadores

    # ---- operarios ----
    def add_operator(self, op_id, name):
        self._ejecutar("INSERT OR IGNORE INTO operators(id, name) VALUES(?, ?)", (op_id, name)) #si ya existe no hace nada

    def operator_exists(self, op_id):
        return len(self._consultar("SELECT 1 FROM operators WHERE id = ?", (op_id,))) > 0 #True si hay alguna fila con ese id

    # ---- estaciones ----
    def add_station(self, st_id, location):
        self._ejecutar("INSERT OR IGNORE INTO stations(id, location) VALUES(?, ?)", (st_id, location)) #si ya estaba registrada no la duplica

    def set_location(self, st_id, location):
        self._ejecutar("UPDATE stations SET location = ? WHERE id = ?", (location, st_id)) #por si el monitor vuelve con otra ubicacion

    def get_stations(self):
        return self._consultar("SELECT id, location, blocked FROM stations ORDER BY id") #las que carga la central al arrancar

    def set_blocked(self, st_id, blocked):
        self._ejecutar("UPDATE stations SET blocked = ? WHERE id = ?", (1 if blocked else 0, st_id)) #en SQLite no hay booleanos: 1 o 0
    # ---- riegos en curso ----
    def add_active(self, station, operator, started_at, duration_s):
        self._ejecutar(
            "INSERT OR REPLACE INTO active_irrigations(station, operator, started_at, duration_s) " #si ya habia uno de esa estacion, lo sustituye
            "VALUES(?, ?, ?, ?)",
            (station, operator, started_at, duration_s))

    def delete_active(self, station):
        self._ejecutar("DELETE FROM active_irrigations WHERE station = ?", (station,)) #el riego ha terminado: ya no esta en curso

    def get_active(self):
        return self._consultar("SELECT station, operator, started_at, duration_s FROM active_irrigations") #los que recupera la central al arrancar

    # ---- histórico de riegos ----
    def add_irrigation(self, station, operator, duration_s, volume_l, reason):
        self._ejecutar(
            "INSERT INTO irrigations(station, operator, started_at, duration_s, volume_l, reason) "
            "VALUES(?, ?, datetime('now'), ?, ?, ?)", #datetime('now') pone la fecha y hora de ahora
            (station, operator, duration_s, volume_l, reason))

    def get_irrigations(self):
        return self._consultar("SELECT * FROM irrigations ORDER BY id") #todo el historico, en orden

    def set_status(self, st_id, status):
        self._ejecutar("UPDATE stations SET status = ? WHERE id = ?", (status, st_id)) #ultimo estado de la estacion, lo guarda la central cuando cambia