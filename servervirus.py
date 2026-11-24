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


def analizar_comando_borrar(contenido):
    """Acepta '[331,Player] | BORRAR' y 'BORRAR | [331,Player]'."""
    partes = [parte.strip() for parte in contenido.split("|", 1)]
    if len(partes) != 2:
        return None

    if partes[0].upper() == "BORRAR":
        payload = partes[1]
    elif partes[1].upper() == "BORRAR":
        payload = partes[0]
    else:
        return None

    if not (payload.startswith("[") and payload.endswith("]")):
        return None

    elementos = [x.strip().strip('"').strip("'")
                 for x in payload[1:-1].split(",")]

    # El comando BORRAR identifica exactamente una carta y un jugador.
    if len(elementos) != 2 or not elementos[0] or not elementos[1]:
        return None

    carta_txt, usuario = elementos
    carta = int(carta_txt) if carta_txt.isdigit() else carta_txt
    return carta, usuario


def borrar_carta_y_retransmitir(remitente, usuario, carta, loop):
    """Elimina una sola aparición de la carta y difunde el cambio."""
    try:
        # Reintento optimista: evita sobrescribir una actualización concurrente.
        for intento in range(5):
            documento = coleccion_jugadores.find_one({"usuario": usuario})

            if documento is None:
                print(f"[BORRAR] Jugador inexistente: {usuario}", flush=True)
                return

            cartas_actuales = list(documento.get("cartas", []))

            if carta not in cartas_actuales:
                print(
                    f"[BORRAR] La carta {carta} no está en la mano de {usuario}",
                    flush=True
                )
                return

            cartas_nuevas = cartas_actuales.copy()
            cartas_nuevas.remove(carta)  # Elimina solo la primera aparición.

            resultado = coleccion_jugadores.update_one(
                {
                    "usuario": usuario,
                    "cartas": cartas_actuales
                },
                {
                    "$set": {"cartas": cartas_nuevas}
                }
            )

            if resultado.modified_count == 1:
                cache_estados[usuario] = cartas_nuevas

                if cartas_nuevas:
                    cartas_txt = ",".join(str(c) for c in cartas_nuevas)
                    estado_actualizado = f"[{cartas_txt},{usuario}]"
                else:
                    # El formato antiguo [cartas,usuario] no representa
                    # inequívocamente una mano vacía. Se difunde el evento
                    # para que el cliente pueda aplicar la eliminación.
                    estado_actualizado = f"BORRAR | [{carta},{usuario}]"

                paquete = json.dumps({
                    "cmd": "gmsg",
                    "val": estado_actualizado
                })
                asyncio.run_coroutine_threadsafe(
                    enviar_a_otros(remitente, paquete),
                    loop
                )

                print(
                    f"[BORRAR OK] Una aparición de {carta} eliminada de "
                    f"{usuario}. Nuevo estado: {cartas_nuevas}",
                    flush=True
                )
                return

            print(
                f"[BORRAR] Conflicto concurrente para {usuario}; "
                f"reintentando ({intento + 1}/5)",
                flush=True
            )

        print(
            f"[BORRAR ERROR] No se pudo actualizar la mano de {usuario} "
            f"tras 5 intentos",
            flush=True
        )

    except Exception as e:
        print(f"[ERROR BORRANDO CARTA] {e}", flush=True)


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

            # Si el mensaje contiene "turno" en cualquier formato o comando, lo atrapa
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

                elif "BORRAR" in contenido.upper():
                    solicitud = analizar_comando_borrar(contenido)

                    if solicitud is None:
                        print(f"[BORRAR] Formato inválido: {contenido}", flush=True)
                    else:
                        carta, usuario_objetivo = solicitud
                        usuario_autenticado = usuarios_por_socket.get(self)

                        # El socket debe haberse identificado previamente con setid
                        # y solo puede modificar las cartas de ese mismo usuario.
                        if not usuario_autenticado:
                            print(
                                f"[BORRAR RECHAZADO] Socket sin setid: {contenido}",
                                flush=True
                            )
                        elif usuario_autenticado != usuario_objetivo:
                            print(
                                f"[BORRAR RECHAZADO] {usuario_autenticado} "
                                f"intentó modificar a {usuario_objetivo}",
                                flush=True
                            )
                        else:
                            threading.Thread(
                                target=borrar_carta_y_retransmitir,
                                args=(self, usuario_objetivo, carta, loop)
                            ).start()

                elif contenido.startswith("[") and contenido.endswith("]"):
                    elementos = [x.strip().strip('"').strip("'") for x in contenido[1:-1].split(",") if x.strip()]
                    if len(elementos) >= 2:
                        usuario = elementos[-1]
                        usuarios_por_socket[self] = usuario
                        cartas = [int(c) if c.isdigit() else c for c in elementos[:-1]]
                        if cache_estados.get(usuario) != cartas:
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
