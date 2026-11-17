import os
from cloudlink import cloudlink

cl = cloudlink()
server = cl.server(logs=True)

# Activar modo detallado para ver cada conexión y paquete en Render
server.debug = True

puerto = int(os.environ.get("PORT", 8080))
print(f"Iniciando servidor CloudLink v4 en puerto {puerto}...", flush=True)
server.run(ip="0.0.0.0", port=puerto)
