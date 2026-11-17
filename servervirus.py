import os
from cloudlink import cloudlink

cl = cloudlink()
server = cl.server(logs=True)

# Funciones para imprimir cada evento en tiempo real
def on_connect(client):
    print(f"--> [CONEXION] Nuevo cliente conectado: {client}", flush=True)

def on_disconnect(client):
    print(f"<-- [DESCONEXION] Cliente desconectado: {client}", flush=True)

def on_packet(client, packet):
    print(f"[PAQUETE] De {client}: {packet}", flush=True)

# Registrar callbacks en cloudlink 0.1.9.1
server.callback("on_connect", on_connect)
server.callback("on_disconnect", on_disconnect)
server.callback("on_packet", on_packet)

puerto = int(os.environ.get("PORT", 8080))
print(f"=== SERVIDOR CLOUDLINK 0.1.9.1 LISTO EN PUERTO {puerto} ===", flush=True)
server.run(ip="0.0.0.0", port=puerto)
