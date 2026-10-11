"""Chapitre 8 : un serveur qui publie une mise à jour toutes les PERIODE
secondes et la met à disposition de quatre façons :
  GET /etat                 polling : l'état courant, tout de suite
  GET /attendre?apres=N     long polling : répond dès qu'une version > N existe
  GET /flux                 Server-Sent Events
  GET /ws                   WebSocket
Chaque mise à jour porte son horodatage d'émission, pour mesurer le délai de
livraison côté client (client et serveur sur la même machine).
Usage : python3 serveur.py PORT [PERIODE]   (pip install -r requirements.txt)
"""
import asyncio
import json
import sys
import time

from aiohttp import WSMsgType, web

PERIODE = float(sys.argv[2]) if len(sys.argv) > 2 else 10.0
etat = {"version": 0, "emis_a": time.time(), "valeur": 0}
nouvelle = asyncio.Event()
abonnes_sse, abonnes_ws = set(), set()


def message():
    return json.dumps(etat, separators=(",", ":"))


async def publier():
    while True:
        await asyncio.sleep(PERIODE)
        etat.update(version=etat["version"] + 1, emis_a=time.time(), valeur=etat["valeur"] + 7)
        nouvelle.set()
        nouvelle.clear()
        texte = message()
        for r in list(abonnes_sse):
            try:
                await r.write(f"id: {etat['version']}\ndata: {texte}\n\n".encode())
            except ConnectionError:
                abonnes_sse.discard(r)
        for ws in list(abonnes_ws):
            if not ws.closed:
                await ws.send_str(texte)


async def page_etat(request):
    return web.Response(text=message(), content_type="application/json")


async def page_attendre(request):
    apres = int(request.query.get("apres", -1))
    fin = time.monotonic() + 30
    while etat["version"] <= apres and time.monotonic() < fin:
        try:
            await asyncio.wait_for(nouvelle.wait(), fin - time.monotonic())
        except asyncio.TimeoutError:
            break
    if etat["version"] <= apres:
        return web.Response(status=204)  # rien de neuf : le client relance aussitôt
    return web.Response(text=message(), content_type="application/json")


async def page_flux(request):
    r = web.StreamResponse(headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache"})
    await r.prepare(request)
    await r.write(b"retry: 3000\n\n")
    abonnes_sse.add(r)
    try:
        while True:
            await asyncio.sleep(25)
            await r.write(b": battement\n\n")  # commentaire SSE : garde la connexion active
    except ConnectionError:
        pass  # le client est parti
    finally:
        abonnes_sse.discard(r)
    return r


async def page_ws(request):
    ws = web.WebSocketResponse(heartbeat=25)
    await ws.prepare(request)
    abonnes_ws.add(ws)
    try:
        async for msg in ws:
            if msg.type == WSMsgType.ERROR:
                break
    finally:
        abonnes_ws.discard(ws)
    return ws


async def demarrer(app):
    app["publication"] = asyncio.create_task(publier())


app = web.Application()
app.add_routes([web.get("/etat", page_etat), web.get("/attendre", page_attendre),
                web.get("/flux", page_flux), web.get("/ws", page_ws)])
app.on_startup.append(demarrer)
web.run_app(app, host="127.0.0.1", port=int(sys.argv[1]), print=None, access_log=None)
