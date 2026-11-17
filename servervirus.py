import os
from cloudlink import cloudlink

cl = cloudlink()
server = cl.server(logs=True)
server.suppressed_logs = []

puerto = int(os.environ.get("PORT", 8080))
print(f"=== SERVIDOR LISTO EN PUERTO {puerto} ===", flush=True)
server.run(ip="0.0.0.0", port=puerto)
