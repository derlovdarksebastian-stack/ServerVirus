import os
import sys
import json
import logging
from pymongo import MongoClient
from cloudlink import cloudlink

# 1. Conectar a MongoDB Atlas
mongo_uri = os.environ.get("MONGO_URI")
cliente_mongo = MongoClient(mongo_uri)
db = cliente_mongo["JuegoVirusDB"]
coleccion_jugadores = db["estado_jugadores"]

# Memoria caché para evitar guardar datos repetidos en la base de datos
cache_estados = {}

# 2. Procesar el array [317, 331, "User"] en tiempo real
def procesar_movimiento(mensaje_crudo):
    try:
        json_str = mensaje_crudo.split("< TEXT ", 1).strip("'")
        paquete = json.loads(json_str)
        
        if paquete.get("cmd") in ["gmsg", "gvar"]:
            contenido = paquete.get("val")
            
            # Si llega como texto JSON "[317,331,'User']", convertirlo a lista de Python
            if isinstance(contenido, str) and contenido.startswith("["):
                contenido = json.loads(contenido)
                
            # Verificar que sea un array con al menos 1 dato y el username al final
            if isinstance(contenido, list) and len(contenido) >= 2:
                usuario = str(contenido[-1])   # Extrae "User" (último elemento)
                cartas = contenido[:-1]        # Extrae [317, 331] (todos los anteriores)
                
                # Solo escribir en MongoDB si el estado de las cartas cambió
                if cache_estados.get(usuario) != cartas:
                    cache_estados[usuario] = cartas
                    coleccion_jugadores.update_one(
                        {"usuario": usuario},
                        {"$set": {"usuario": usuario, "cartas": cartas}},
                        upsert=True
                    )
                    print(f"[DB ACTUALIZADA] {usuario} -> {cartas}", flush=True)
    except Exception:
        pass

# 3. Filtro limpio de logs + disparador de guardado
class LogYBaseDeDatos(logging.Filter):
    def filter(self, record):
        mensaje = record.getMessage()
        if "< TEXT" in mensaje:
            procesar_movimiento(mensaje)
            return True
        return "> TEXT" in mensaje or "connection open" in mensaje or "connection closed" in mensaje

handler = logging.StreamHandler(sys.stdout)
handler.addFilter(LogYBaseDeDatos())
handler.setFormatter(logging.Formatter("[%(asctime)s] %(message)s", datefmt="%H:%M:%S"))

logging.basicConfig(level=logging.DEBUG, handlers=[handler], force=True)

cl = cloudlink()
server = cl.server(logs=True)
server.suppressed_logs = []

puerto = int(os.environ.get("PORT", 8080))
print(f"=== SERVIDOR Y MONGODB LISTOS EN PUERTO {puerto} ===", flush=True)
server.run(ip="0.0.0.0", port=puerto)
