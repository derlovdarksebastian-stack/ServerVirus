import os
import sys
import json
import logging
import threading
import websockets.legacy.server
from pymongo import MongoClient
from cloudlink import cloudlink

# 1. Conectar a MongoDB Atlas y probar conexión al arrancar
mongo_uri = os.environ.get("MONGO_URI")
cliente_mongo = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
db = cliente_mongo["JuegoVirusDB"]
coleccion_jugadores = db["estado_jugadores"]

try:
    cliente_mongo.admin.command("ping")
    print("[OK] CONEXION EXITOSA A MONGODB ATLAS!", flush=True)
except Exception as e:
    print(f"[ERROR CRITICO MONGO] {e}", flush=True)

cache_estados = {}

def guardar_en_mongo(usuario, cartas):
    try:
        coleccion_jugadores.update_one(
            {"usuario": usuario},
            {"$set": {"usuario": usuario, "cartas": cartas}},
            upsert=True
        )
        print(f"[DB ACTUALIZADA] {usuario} -> {cartas}", flush=True)
    except Exception as e:
        print(f"[ERROR GUARDANDO] {e}", flush=True)

# 2. Interceptar mensajes entrantes directamente en WebSockets (100% confiable)
recv_original = websockets.legacy.server.WebSocketServerProtocol.recv

async def recv_con_db(self):
    mensaje = await recv_original(self)
    if isinstance(mensaje, str):
        print(f"[RECIBIDO] {mensaje}", flush=True)
        try:
            paquete = json.loads(mensaje)
            if paquete.get("cmd") in ["gmsg", "gvar"]:
                contenido = str(paquete.get("val", "")).strip()
                if contenido.startswith("[") and contenido.endswith("]"):
                    elementos = [x.strip().strip('"').strip("'") for x in contenido[1:-1].split(",") if x.strip()]
                    if len(elementos) >= 2:
                        usuario = elementos[-1]
                        cartas = [int(c) if c.isdigit() else c for c in elementos[:-1]]
                        if cache_estados.get(usuario) != cartas:
                            cache_estados[usuario] = cartas
                            threading.Thread(target=guardar_en_mongo, args=(usuario, cartas)).start()
        except Exception as e:
            print(f"[ERROR PARSE] {e}", flush=True)
    return mensaje

websockets.legacy.server.WebSocketServerProtocol.recv = recv_con_db

# 3. Silenciar el ruido HTTP de Render y arrancar CloudLink
logging.basicConfig(level=logging.ERROR, stream=sys.stdout, force=True)

cl = cloudlink()
server = cl.server(logs=False)

puerto = int(os.environ.get("PORT", 8080))
print(f"=== SERVIDOR LISTO EN PUERTO {puerto} ===", flush=True)
server.run(ip="0.0.0.0", port=puerto)
