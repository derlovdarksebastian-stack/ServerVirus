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
coleccion_partida = db["estado_partida"]

try:
    cliente_mongo.admin.command("ping")
    print("[OK] MONGODB CONECTADO", flush=True)
except Exception as e:
    print(f"[ERROR MONGO] {e}", flush=True)

cache_estados = {}
clientes_conectados = set()
usuarios_por_socket = {}

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

def guardar_y_retransmitir(remitente, usuario, cartas, texto_original, loop): #4.7.1 Pre alpha
    try:
        # 1. Guarda en MongoDB
        coleccion_jugadores.update_one(
            {"usuario": usuario},
            {"$set": {"usuario": usuario, "cartas": cartas}},
            upsert=True
        )
        
        # 2. Arma el DB_ALL actualizado con TODOS los jugadores y el turno
        documentos = list(coleccion_jugadores.find({}))
        info_partida = coleccion_partida.find_one({"_id": "partida_actual"})
        turno_actual = info_partida.get("turno") if info_partida else "LIBRE"
        
        lista_paquetes = [f"TURNO:{turno_actual}"]
        for doc in documentos:
            u = doc.get("usuario", "")
            c_list = doc.get("cartas", [])
            if u and c_list:
                cache_estados[u] = c_list
                c_str = ",".join(str(c) for c in c_list)
                lista_paquetes.append(f"[{c_str},{u}]")

        respuesta_unificada = "DB_ALL|" + "|".join(lista_paquetes)
        paquete_todos = json.dumps({"cmd": "gmsg", "val": respuesta_unificada})
        
        # 3. Lo difunde a TODOS los conectados al instante
        asyncio.run_coroutine_threadsafe(difundir_a_todos(paquete_todos), loop)
        print(f"[SINCRONIZADO A TODOS] {respuesta_unificada}", flush=True)
    except Exception as e:
        print(f"[ERROR GUARDANDO] {e}", flush=True)

# NUEVA FUNCIÓN: Borrar una carta específica o todo el registro de un jugador
def borrar_dato_jugador(usuario, carta_objetivo, loop):
    try:
        doc = coleccion_jugadores.find_one({"usuario": usuario})
        if not doc:
            print(f"[BORRAR] No se encontró al jugador: {usuario}", flush=True)
            return

        # Caso 1: Borrar una carta en específico (ej: BORRAR|Derlo|331)
        if carta_objetivo is not None:
            cartas_actuales = doc.get("cartas", [])
            valor_carta = int(carta_objetivo) if str(carta_objetivo).isdigit() else carta_objetivo

            if valor_carta in cartas_actuales:
                cartas_actuales.remove(valor_carta)  # Elimina solo una copia de esa carta
                cache_estados[usuario] = cartas_actuales

                if cartas_actuales:
                    coleccion_jugadores.update_one(
                        {"usuario": usuario},
                        {"$set": {"cartas": cartas_actuales}}
                    )
                    c_str = ",".join(str(c) for c in cartas_actuales)
                    aviso = f"[{c_str},{usuario}]"
                else:
                    # Si se quedó sin cartas en mesa, borra el documento de la DB
                    coleccion_jugadores.delete_one({"usuario": usuario})
                    cache_estados.pop(usuario, None)
                    aviso = f"[VACIO,{usuario}]"

                print(f"[CARTA BORRADA] {valor_carta} de {usuario} -> Estado: {aviso}", flush=True)
                paquete = json.dumps({"cmd": "gmsg", "val": aviso})
                asyncio.run_coroutine_threadsafe(difundir_a_todos(paquete), loop)
            else:
                print(f"[BORRAR] {usuario} no tiene la carta {valor_carta}", flush=True)

        # Caso 2: Borrar TODOS los datos de ese jugador (ej: BORRAR|Derlo)
        else:
            coleccion_jugadores.delete_one({"usuario": usuario})
            cache_estados.pop(usuario, None)
            aviso = f"[VACIO,{usuario}]"
            print(f"[JUGADOR BORRADO DE DB] {usuario}", flush=True)
            paquete = json.dumps({"cmd": "gmsg", "val": aviso})
            asyncio.run_coroutine_threadsafe(difundir_a_todos(paquete), loop)

    except Exception as e:
        print(f"[ERROR BORRANDO] {e}", flush=True)

def rotar_turno(quien_pasa, jugador_forzado, loop):
    try:
        if jugador_forzado:
            siguiente = jugador_forzado
        else:
            todos = [doc["usuario"] for doc in coleccion_jugadores.find({}) if doc.get("usuario")]
            if not todos:
                siguiente = quien_pasa or "LIBRE"
            elif len(todos) == 1:
                siguiente = todos[0]
            else:
                info = coleccion_partida.find_one({"_id": "partida_actual"})
                actual = info.get("turno") if info else quien_pasa
                if actual in todos:
                    idx = (todos.index(actual) + 1) % len(todos)
                    siguiente = todos[idx]
                else:
                    siguiente = todos[0]

        coleccion_partida.update_one(
            {"_id": "partida_actual"},
            {"$set": {"turno": siguiente}},
            upsert=True
        )
        print(f"[TURNO GUARDADO EN DB] Ahora juega: {siguiente}", flush=True)
        paquete_turno = json.dumps({"cmd": "gmsg", "val": f"TURNO|{siguiente}"})
        asyncio.run_coroutine_threadsafe(difundir_a_todos(paquete_turno), loop)
    except Exception as e:
        print(f"[ERROR TURNO] {e}", flush=True)

def enviar_db_a_turbowarp(ws_cliente, loop, filtro_usuario=None):
    try:
        consulta = {"usuario": filtro_usuario} if filtro_usuario else {}
        documentos = list(coleccion_jugadores.find(consulta))
        
        info_partida = coleccion_partida.find_one({"_id": "partida_actual"})
        turno_actual = info_partida.get("turno") if info_partida else "LIBRE"
        
        lista_paquetes = [f"TURNO:{turno_actual}"]

        for doc in documentos:
            u = doc.get("usuario", "")
            c_list = doc.get("cartas", [])
            if u and c_list:
                cache_estados[u] = c_list
                c_str = ",".join(str(c) for c in c_list)
                lista_paquetes.append(f"[{c_str},{u}]")

        respuesta_unificada = "DB_ALL|" + "|".join(lista_paquetes)
        paquete_unico = json.dumps({"cmd": "gmsg", "val": respuesta_unificada})
        asyncio.run_coroutine_threadsafe(ws_cliente.send(paquete_unico), loop)
        print(f"[DB UNIFICADO ENVIADO] {respuesta_unificada}", flush=True)
    except Exception as e:
        print(f"[ERROR LEYENDO DB] {e}", flush=True)

recv_original = websockets.legacy.server.WebSocketServerProtocol.recv

async def recv_con_db(self):
    clientes_conectados.add(self)
    try:
        mensaje = await recv_original(self)
    except Exception:
        clientes_conectados.discard(self)
        usuarios_por_socket.pop(self, None)
        raise

    if isinstance(mensaje, str):
        try:
            paquete = json.loads(mensaje)
            cmd = paquete.get("cmd")
            val = paquete.get("val")
            loop = asyncio.get_running_loop()

            if cmd == "setid":
                usuarios_por_socket[self] = str(val).strip()
                print(f"[CONECTADO] Usuario: {usuarios_por_socket[self]}", flush=True)

            elif val is not None and "turno" in str(val).lower():
                contenido = str(val).strip()
                print(f"[DETECTADO TURNO] cmd={cmd} | val={contenido}", flush=True)
                forzado = contenido.split("|", 1)[1].strip() if "|" in contenido else None
                remitente_user = usuarios_por_socket.get(self, "")
                threading.Thread(target=rotar_turno, args=(remitente_user, forzado, loop)).start()

            elif cmd in ["gmsg", "gvar"]:
                contenido = str(val or "").strip()

                if contenido == "GET_DB":
                    threading.Thread(target=enviar_db_a_turbowarp, args=(self, loop, None)).start()

                elif contenido.startswith("GET|"):
                    usuario_buscado = contenido.split("|", 1)[1].strip()
                    threading.Thread(target=enviar_db_a_turbowarp, args=(self, loop, usuario_buscado)).start()

                # COMANDO BORRAR: "BORRAR|Derlo|331" o "BORRAR|Derlo"
                elif contenido.upper().startswith("BORRAR|"):
                    partes = [p.strip() for p in contenido.split("|")]
                    if len(partes) >= 2 and partes[1]:
                        usuario_objetivo = partes[1]
                        carta_objetivo = partes[2] if len(partes) >= 3 and partes[2] else None
                        threading.Thread(
                            target=borrar_dato_jugador,
                            args=(usuario_objetivo, carta_objetivo, loop)
                        ).start()

                elif contenido.startswith("[") and contenido.endswith("]"):
                    elementos = [x.strip().strip('"').strip("'") for x in contenido[1:-1].split(",") if x.strip()]
                if len(elementos) >= 2:
                    usuario = elementos[-1]
                    usuarios_por_socket[self] = usuario
                    cartas = [int(c) if c.isdigit() else c for c in elementos[:-1]]
                    cache_estados[usuario] = cartas
                    threading.Thread(
                        target=guardar_y_retransmitir,
                        args=(self, usuario, cartas, contenido, loop)
                    ).start()
        except Exception as e:
            print(f"[ERROR PARSE] {e}", flush=True)
    return mensaje

websockets.legacy.server.WebSocketServerProtocol.recv = recv_con_db

logging.basicConfig(level=logging.ERROR, stream=sys.stdout, force=True)
cl = cloudlink()
server = cl.server(logs=False)

puerto = int(os.environ.get("PORT", 8080))
print(f"=== SERVIDOR LISTO EN PUERTO {puerto} ===", flush=True)
server.run(ip="0.0.0.0", port=puerto)
