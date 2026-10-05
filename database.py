import sqlite3
import threading

class Database:
    def __init__(self, ruta):
        self.conn = sqlite3.connect(ruta, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.Lock()
        self._crear_tablas()

    def _crear_tablas(self):
        with self.lock, self.conn:
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
            """)


    def _ejecutar (self, sql, args=()):
        with self.lock, self.conn:
            self.conn.execute(sql, args)

    def _consultar (self, sql, args=()):
        with self.lock:
            return self.conn.execute(sql, args).fetchall()

    # Operadores

    # ---- operarios ----
    def add_operator(self, op_id, name):
        self._ejecutar("INSERT OR IGNORE INTO operators(id, name) VALUES(?, ?)", (op_id, name))

    def operator_exists(self, op_id):
        return len(self._consultar("SELECT 1 FROM operators WHERE id = ?", (op_id,))) > 0

    # ---- estaciones ----
    def add_station(self, st_id, location):
        self._ejecutar("INSERT OR IGNORE INTO stations(id, location) VALUES(?, ?)", (st_id, location))

    def get_stations(self):
        return self._consultar("SELECT id, location, blocked FROM stations ORDER BY id")

    def set_blocked(self, st_id, blocked):
        self._ejecutar("UPDATE stations SET blocked = ? WHERE id = ?", (1 if blocked else 0, st_id))

    # ---- histórico de riegos ----
    def add_irrigation(self, station, operator, duration_s, volume_l, reason):
        self._ejecutar(
            "INSERT INTO irrigations(station, operator, started_at, duration_s, volume_l, reason) "
            "VALUES(?, ?, datetime('now'), ?, ?, ?)",
            (station, operator, duration_s, volume_l, reason))

    def get_irrigations(self):
        return self._consultar("SELECT * FROM irrigations ORDER BY id")

