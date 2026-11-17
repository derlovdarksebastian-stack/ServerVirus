import os
import sys
import logging
from cloudlink import cloudlink

# Obligar a Python y a la librería websockets a imprimir TODO el tráfico en Render
logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s] %(name)s - %(levelname)s: %(message)s",
    stream=sys.stdout,
    force=True
)

cl = cloudlink()
server = cl.server(logs=True)
server.suppressed_logs = []

puerto = int(os.environ.get("PORT", 8080))
print(f"=== SERVIDOR LISTO EN PUERTO {puerto} ===", flush=True)
server.run(ip="0.0.0.0", port=puerto)
