import os
import sys
import logging
from cloudlink import cloudlink

# Obligar a Python a imprimir TODOS los logs internos en la consola de Render en vivo
logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    stream=sys.stdout,
    force=True
)

cl = cloudlink()
server = cl.server(logs=True)
server.suppressed_logs = []

# Interceptar el manejador de paquetes de CloudLink 0.1.9.1 para imprimir cada dato
manejador_original = server.packet_handler

async def log_paquetes(client, message):
    print(f"--> [DATO EN VIVO] Cliente {client}: {message}", flush=True)
    await manejador_original(client, message)

server.packet_handler = log_paquetes

puerto = int(os.environ.get("PORT", 8080))
print(f"=== SERVIDOR CON LOGS EN VIVO LISTO EN PUERTO {puerto} ===", flush=True)
server.run(ip="0.0.0.0", port=puerto)
