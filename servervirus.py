import os
import sys
import json
import asyncio
import logging
import threading
import websockets.legacy.server
from pymongo import MongoClient
from cloudlink import cloudlink

mongo_uri = os.environ.get("MONGO_URI")
cliente_mongo = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
db = cliente_mongo["JuegoVirusDB"]
coleccion_jugadores = db["estado_jugadores"]
coleccion_partida = db["estado_partida"]  # Nueva colección para el turno actual

try:
    cliente_mongo.admin.command("ping")
    print("[OK] MONGODB CONECTADO", flush=True)
except Exception as e:
    print(f"[ERROR MONGO] {e}", flush=True)

cache_estados = {}
clientes_conectados = set()

async def difundir_a_todos(paquete_str):
    for ws in list(clientes_conectados):
        try:
            await ws.send(paquete_str)
        except Exception:
            clientes_conectados.discard(ws)

async def enviar_a_otros(remitente, paquete_str):
    for ws in list(clientes_conectados):
        if ws != remitente:
            try:
                await ws.send(paquete_str)
            except Exception:
                clientes_conectados.discard(ws)

def guardar_y_retransmitir(remitente, usuario, cartas, texto_original, loop):
    try:
        coleccion_jugadores.update_one(
            {"usuario": usuario},
            {"$set": {"usuario": usuario, "cartas": cartas}},
            upsert=True
        )
        paquete_rival = json.dumps({"cmd": "gmsg", "val": texto_original})
        asyncio.run_coroutine_threadsafe(enviar_a_otros(remitente, paquete_rival), loop)
    except Exception as e:
        print(f"[ERROR GUARDANDO] {e}", flush=True)

# Guardar el nuevo turno en MongoDB y avisar a TODOS los jugadores
def cambiar_turno(siguiente_jugador, loop):
    try:
        coleccion_partida.update_one(
            {"_id": "partida_actual"},
            {"$set": {"turno": siguiente_jugador}},
            upsert=True
        )
        print(f"[TURNO] Ahora juega: {siguiente_jugador}", flush=True)
        paquete_turno = json.dumps({"cmd": "gmsg", "val": f"TURNO|{siguiente_jugador}"})
        asyncio.run_coroutine_threadsafe(difundir_a_todos(paquete_turno), loop)
    except Exception as e:
        print(f"[ERROR TURNO] {e}", flush=True)

# Enviar cartas + turno actual cuando alguien ejecuta GET_DB
def enviar_db_a_turbowarp(ws_cliente, loop, filtro_usuario=None):
    try:
        consulta = {"usuario": filtro_usuario} if filtro_usuario else {}
        documentos = list(coleccion_jugadores.find(consulta))
        lista_paquetes = []

        for doc in documentos:
            u = doc.get("usuario", "")
            c_list = doc.get("cartas", [])
            if u and c_list:
                cache_estados[u] = c_list
                c_str = ",".join(str(c) for c in c_list)
                lista_paquetes.append(f"[{c_str},{u}]")

        respuesta_unificada = "DB_ALL|" + "|".join(lista_paquetes) if lista_paquetes else "DB_ALL|VACIO"
        paquete_cartas = json.dumps({"cmd": "gmsg", "val": respuesta_unificada})
        asyncio.run_coroutine_threadsafe(ws_cliente.send(paquete_cartas), loop)

        # Enviar también quién tiene el turno guardado (si no hay ninguno, empieza quien pidió GET_DB)
        info_partida = coleccion_partida.find_one({"_id": "partida_actual"})
        if info_partida and info_partida.get("turno"):
            turno_guardado = info_partida["turno"]
            paquete_turno = json.dumps({"cmd": "gmsg", "val": f"TURNO|{turno_guardado}"})
            asyncio.run_coroutine_threadsafe(ws_cliente.send(paquete_turno), loop)
    except Exception as e:
        print(f"[ERROR LEYENDO DB] {e}", flush=True)

recv_original = websockets.legacy.server.WebSocketServerProtocol.recv

async def recv_con_db(self):
    clientes_conectados.add(self)
    try:
        mensaje = await recv_original(self)
    except Exception:
        clientes_conectados.discard(self)
        raise

    if isinstance(mensaje, str):
        try:
            paquete = json.loads(mensaje)
            if paquete.get("cmd") in ["gmsg", "gvar"]:
                contenido = str(paquete.get("val", "")).strip()
                loop = asyncio.get_running_loop()

                if contenido == "GET_DB":
                    threading.Thread(target=enviar_db_a_turbowarp, args=(self, loop, None)).start()

                elif contenido.startswith("GET|"):
                    usuario_buscado = contenido.split("|", 1)[1].strip()
                    threading.Thread(target=enviar_db_a_turbowarp, args=(self, loop, usuario_buscado)).start()

                # NUEVO COMANDO: Cambiar turno ("PASAR_TURNO|Test")
                elif contenido.startswith("PASAR_TURNO|"):
                    siguiente = contenido.split("|", 1)[1].strip()
                    if siguiente:
                        threading.Thread(target=cambiar_turno, args=(siguiente, loop)).start()

                elif contenido.startswith("[") and contenido.endswith("]"):
                    elementos = [x.strip().strip('"').strip("'") for x in contenido[1:-1].split(",") if x.strip()]
                    if len(elementos) >= 2:
                        usuario = elementos[-1]
                        cartas = [int(c) if c.isdigit() else c for c in elementos[:-1]]
                        if cache_estados.get(usuario) != cartas:
                            cache_estados[usuario] = cartas
                            threading.Thread(
                                target=guardar_y_retransmitir,
                                args=(self, usuario, cartas, contenido, loop)
                            ).start()
        except Exception:
            pass
    return mensaje

websockets.legacy.server.WebSocketServerProtocol.recv = recv_con_db

logging.basicConfig(level=logging.ERROR, stream=sys.stdout, force=True)
cl = cloudlink()
server = cl.server(logs=False)

puerto = int(os.environ.get("PORT", 8080))
print(f"=== SERVIDOR CON TURNOS LISTO EN PUERTO {puerto} ===", flush=True)
server.run(ip="0.0.0.0", port=puerto)
