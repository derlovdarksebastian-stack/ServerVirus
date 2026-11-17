import os
import sys
import logging
from cloudlink import cloudlink

# Filtro para dejar pasar SOLO los paquetes de texto de los jugadores (< TEXT y > TEXT)
class SoloDatosJuego(logging.Filter):
    def filter(self, record):
        mensaje = record.getMessage()
        return "< TEXT" in mensaje or "> TEXT" in mensaje or "connection open" in mensaje or "connection closed" in mensaje

handler = logging.StreamHandler(sys.stdout)
handler.addFilter(SoloDatosJuego())
handler.setFormatter(logging.Formatter("[%(asctime)s] %(message)s", datefmt="%H:%M:%S"))

logging.basicConfig(
    level=logging.DEBUG,
    handlers=[handler],
    force=True
)

cl = cloudlink()
server = cl.server(logs=True)
server.suppressed_logs = []

puerto = int(os.environ.get("PORT", 8080))
print(f"=== LOGS LIMPIOS ACTIVADOS EN PUERTO {puerto} ===", flush=True)
server.run(ip="0.0.0.0", port=puerto)
