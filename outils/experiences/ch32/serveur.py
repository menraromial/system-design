"""Chapitre 32 : un service de virements minimal.

POST /virement?lent=S attend S secondes puis compte le virement ;
GET /compte renvoie le nombre de virements exécutés.

    python3 serveur.py --port 11232
"""
import argparse
import asyncio

from aiohttp import web

executes = 0


async def virement(requete):
    global executes
    await asyncio.sleep(float(requete.query.get("lent", 0)))
    executes += 1
    return web.json_response({"ok": True, "n": executes})


async def compte(requete):
    return web.json_response({"executes": executes})


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=11232)
    app = web.Application()
    app.add_routes([web.post("/virement", virement), web.get("/compte", compte)])
    web.run_app(app, host="127.0.0.1", port=p.parse_args().port, print=None,
                access_log=None)
