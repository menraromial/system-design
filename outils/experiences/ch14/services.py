"""Chapitre 14 : les deux services placés derrière Caddy.

  origine : GET /catalogue  un catalogue JSON de 1 000 produits (toujours le même)
            GET /api        une petite réponse JSON qui renvoie l'en-tête X-Utilisateur
  auth    : GET /verifier   200 et X-Utilisateur si Authorization vaut « Bearer
                            jeton-valide », 401 sinon (comme un service d'identité)
Usage : python3 services.py origine|auth PORT   (pip install -r requirements.txt)
"""
import json
import random
import sys

from aiohttp import web

ROLE, PORT = sys.argv[1], int(sys.argv[2])
MOTS = ("acier bois coton lin verre cuir laine bambou chêne noyer lampe table chaise "
        "étagère tapis coussin miroir vase bol tasse plateau panier boîte cadre horloge "
        "rouge bleu vert noir blanc gris naturel clair foncé mat brillant petit grand").split()


def catalogue():
    r = random.Random(14)
    produits = [{
        "id": 100000 + i,
        "nom": " ".join(r.choice(MOTS) for _ in range(3)),
        "prix": round(r.uniform(5, 500), 2),
        "categorie": r.choice(("salon", "cuisine", "chambre", "bureau", "jardin")),
        "stock": r.randint(0, 200),
        "description": " ".join(r.choice(MOTS) for _ in range(20)),
        "etiquettes": r.sample(MOTS, 3),
    } for i in range(1000)]
    return json.dumps({"produits": produits}, ensure_ascii=False).encode()


CATALOGUE = catalogue()


async def get_catalogue(request):
    return web.Response(body=CATALOGUE, content_type="application/json")


async def get_api(request):
    return web.json_response({"utilisateur": request.headers.get("X-Utilisateur"), "ok": True})


async def verifier(request):
    if request.headers.get("Authorization") == "Bearer jeton-valide":
        return web.Response(status=200, headers={"X-Utilisateur": "lea"})
    return web.Response(status=401, text="jeton absent ou invalide")


app = web.Application()
if ROLE == "origine":
    app.add_routes([web.get("/catalogue", get_catalogue), web.get("/api", get_api)])
else:
    app.add_routes([web.get("/verifier", verifier)])
web.run_app(app, host="127.0.0.1", port=PORT, access_log=None, print=None)
