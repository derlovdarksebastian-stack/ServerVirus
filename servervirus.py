import os
from cloudlink import cloudlink

cl = cloudlink()
server = cl.server(logs=True)

@server.on_connect
async def on_connect(client):
    print(f"--> [CONEXION] Nuevo cliente conectado: {client.id}", flush=True)

@server.on_disconnect
async def on_disconnect(client):
    print(f"<-- [DESCONEXION] Cliente desconectado: {client.username}", flush=True)

@server.on_packet
async def on_packet(client, packet):
    print(f"[PAQUETE] De {client.username}: {packet}", flush=True)

puerto = int(os.environ.get("PORT", 8080))
print(f"=== SERVIDOR CLOUDLINK 0.1.9.1 LISTO EN PUERTO {puerto} ===", flush=True)
server.run(ip="0.0.0.0", port=puerto)
