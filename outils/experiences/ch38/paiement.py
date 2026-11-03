"""Chapitre 38 : un service de paiement, avec ou sans clé d'idempotence.

POST /payer, corps JSON {"compte": 7, "montant": 100, "ref": "..."} ; la
référence « ref » sert seulement à compter les doublons après coup. L'en-tête
Idempotency-Key porte la clé. Le traitement dure 20 à 80 ms, et 1,2 à 2 s une
fois sur dix (pause dans la transaction).

  --mode sans       la clé est ignorée ;
  --mode naive      on cherche la clé ; absente, on paie, puis on enregistre la
                    clé et la réponse, dans une seconde transaction ;
  --mode atomique   la clé est insérée au début de la transaction du paiement
                    (clé primaire) ; une requête répétée attend la fin de la
                    première, puis reçoit la réponse enregistrée ; une même clé
                    avec un autre corps reçoit 422.

    python3 paiement.py --port 11830 --pg 11832 --mode atomique [--rapide]
"""
import argparse
import asyncio
import hashlib
import json
import random

from aiohttp import web
from psycopg_pool import AsyncConnectionPool


def application(args):
    pool = AsyncConnectionPool(f"host=127.0.0.1 port={args.pg} dbname=postgres user=postgres",
                               min_size=40, max_size=40, open=False)

    async def ouvrir(app):
        await pool.open(wait=True)

    async def fermer(app):
        await pool.close()

    def duree():
        if args.rapide:
            return 0.0
        return random.uniform(1.2, 2.0) if random.random() < 0.1 else random.uniform(0.02, 0.08)

    async def payer_dans(cur, corps):
        await cur.execute("update comptes set solde = solde - %s where id = %s",
                          (corps["montant"], corps["compte"]))
        await cur.execute("insert into paiements (compte, montant, ref) values (%s, %s, %s) returning id",
                          (corps["compte"], corps["montant"], corps["ref"]))
        (pid,) = await cur.fetchone()
        await asyncio.sleep(duree())
        return {"paiement": pid}

    async def payer(requete):
        brut = await requete.read()
        corps = json.loads(brut)
        cle = requete.headers.get("Idempotency-Key")
        empreinte = hashlib.sha256(brut).hexdigest()
        async with pool.connection() as conn:
            if args.mode == "sans" or cle is None:
                async with conn.transaction(), conn.cursor() as cur:
                    rep = await payer_dans(cur, corps)
                return web.json_response(rep, status=201)

            if args.mode == "naive":
                async with conn.cursor() as cur:
                    await cur.execute("select code, corps from cles where cle = %s", (cle,))
                    deja = await cur.fetchone()
                if deja:
                    return web.json_response(deja[1], status=deja[0])
                async with conn.transaction(), conn.cursor() as cur:
                    rep = await payer_dans(cur, corps)
                async with conn.transaction(), conn.cursor() as cur:
                    await cur.execute("insert into cles (cle, empreinte, code, corps) values (%s, %s, 201, %s)"
                                      " on conflict do nothing", (cle, empreinte, json.dumps(rep)))
                return web.json_response(rep, status=201)

            # atomique
            async with conn.transaction(), conn.cursor() as cur:
                await cur.execute("insert into cles (cle, empreinte) values (%s, %s)"
                                  " on conflict do nothing returning cle", (cle, empreinte))
                if await cur.fetchone() is None:
                    await cur.execute("select empreinte, code, corps from cles where cle = %s", (cle,))
                    emp, code, rep = await cur.fetchone()
                    if emp != empreinte:
                        return web.json_response({"erreur": "clé déjà employée"}, status=422,
                                                 dumps=lambda o: json.dumps(o, ensure_ascii=False))
                    return web.json_response(rep, status=code, headers={"Idempotent-Replayed": "true"})
                rep = await payer_dans(cur, corps)
                await cur.execute("update cles set code = 201, corps = %s where cle = %s",
                                  (json.dumps(rep), cle))
            return web.json_response(rep, status=201)

    app = web.Application()
    app.on_startup.append(ouvrir)
    app.on_cleanup.append(fermer)
    app.add_routes([web.post("/payer", payer)])
    return app


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=11830)
    p.add_argument("--pg", type=int, default=11832)
    p.add_argument("--mode", choices=["sans", "naive", "atomique"], default="atomique")
    p.add_argument("--rapide", action="store_true")
    args = p.parse_args()
    web.run_app(application(args), host="127.0.0.1", port=args.port, print=None, access_log=None)
