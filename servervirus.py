import os
from cloudlink import cloudlink

cl = cloudlink()
server = cl.server(logs=True)

# Habilitar TODOS los logs internos de CloudLink 0.1.9.1 (quita los filtros silenciosos)
server.suppressed_logs = []

# También puedes capturar eventos mediante el diccionario server.callbacks
def on_connect(client):
    print(f"--> [CONEXION] Cliente conectado: {client}", flush=True)

def on_disconnect(client):
    print(f"<-- [DESCONEXION] Cliente desconectado: {client}", flush=True)

server.callbacks["on_connect"] = on_connect
server.callbacks["on_disconnect"] = on_disconnect

puerto = int(os.environ.get("PORT", 8080))
print(f"=== SERVIDOR CLOUDLINK 0.1.9.1 LISTO EN PUERTO {puerto} ===", flush=True)
server.run(ip="0.0.0.0", port=puerto)
