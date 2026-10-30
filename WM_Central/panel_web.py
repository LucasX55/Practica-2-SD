import json
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

PAGINA = """<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>WM_Central</title>
<style>
  body { font-family: sans-serif; margin: 20px; }
  table { border-collapse: collapse; }
  th, td { border: 1px solid #999; padding: 6px 12px; }
  .DISPONIBLE { background: #4caf50; }
  .REGANDO { background: #4caf50; animation: parpadeo 1s infinite; }
  .FUGA { background: #e53935; color: white; }
  .FUERA { background: #fb8c00; }
  .DESCONECTADA { background: #9e9e9e; }
  @keyframes parpadeo { 50% { opacity: 0.3; } }
  #mensajes { background: #222; color: #eee; padding: 10px; white-space: pre; font-family: monospace; }
</style>
</head>
<body>
<h1>WM_Central</h1>
<p id="aviso" style="color:#e53935; font-weight:bold"></p>
<table>
  <thead><tr><th>ID</th><th>Ubicación</th><th>Estado</th><th>L/min</th><th>Litros</th><th>Operario</th></tr></thead>
  <tbody id="filas"></tbody>
</table>
<form id="formulario">
  <p><input id="orden" size="60" placeholder="iniciar ST1 30 / iniciar todas / parar ST1 / bloquear todas / activar ST1 / alta OP1 Lucas">
  <button>Enviar orden</button></p>
</form>
<div id="mensajes"></div>
<script>
const NOMBRES = {"DISPONIBLE": "Disponible (Available)", "REGANDO": "Regando (Watering)", "FUGA": "Fuga (Leak)",
                 "FUERA DE SERVICIO": "Out of Service", "DESCONECTADA": "Desconectada (Disconnected)"};
async function refrescar() {
  let datos;
  try {
    const respuesta = await fetch("estado");
    datos = await respuesta.json();
    document.getElementById("aviso").textContent = "";       // todo bien: quita el aviso
  } catch (e) {
    document.getElementById("aviso").textContent = "Imposible conectar con WM_Central";
    return;                                                  // no pinta la tabla
  }
  let html = "";
  for (const s of datos.estaciones) {
    let clase = s.estado;
    if (s.estado == "FUERA DE SERVICIO") { clase = "FUERA"; }
    html += "<tr><td>" + s.id + "</td><td>" + s.ubicacion + "</td><td class='" + clase + "'>" + NOMBRES[s.estado] +
            "</td><td>" + s.caudal + "</td><td>" + s.volumen + "</td><td>" + s.operario + "</td></tr>";
  }
  document.getElementById("filas").innerHTML = html;
  document.getElementById("mensajes").textContent = datos.mensajes.join("\\n");
}
document.getElementById("formulario").onsubmit = async function (e) {
  e.preventDefault();
  const campo = document.getElementById("orden");
  await fetch("orden", { method: "POST", body: campo.value });
  campo.value = "";
  refrescar();
};
setInterval(refrescar, 1000);
refrescar();
</script>
</body>
</html>"""


# Lo que el panel necesita de la Central. Se rellena en arrancar()
foto = None             # funcion que devuelve la lista de estaciones
ejecutar_orden = None   # funcion que ejecuta una orden escrita en la web
mensajes = []           # ultimos mensajes de la Central


class PanelWeb(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/estado":
            cuerpo = json.dumps({"estaciones": foto(), "mensajes": mensajes}).encode("utf-8")
            tipo = "application/json"
        else:
            cuerpo = PAGINA.encode("utf-8")
            tipo = "text/html; charset=utf-8"
        self.send_response(200)
        self.send_header("Content-Type", tipo)
        self.end_headers()
        self.wfile.write(cuerpo)

    def do_POST(self):
        if self.path == "/orden":
            largo = int(self.headers.get("Content-Length", 0))
            ejecutar_orden(self.rfile.read(largo).decode("utf-8"))
        self.send_response(204)
        self.end_headers()

    def log_message(self, *args):
        pass   # para que el servidor web no escriba una linea por cada peticion


def arrancar(puerto, funcion_foto, funcion_orden, lista_mensajes):
    """Se ejecuta en un hilo: guarda lo que le pasa la Central y sirve la pagina"""
    global foto, ejecutar_orden, mensajes
    foto = funcion_foto
    ejecutar_orden = funcion_orden
    mensajes = lista_mensajes
    ThreadingHTTPServer(("0.0.0.0", puerto), PanelWeb).serve_forever()