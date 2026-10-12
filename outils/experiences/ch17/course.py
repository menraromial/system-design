"""Chapitre 17 : la course entre une lecture et une écriture, en cache-aside.

Un lecteur trouve la clé absente, lit le profil dans la base, puis, après un
délai (de 0 à 20 ms : un serveur lent, une pause du ramasse-miettes), le range
dans le cache. Pendant ce temps, un écrivain modifie le profil dans la base et
supprime la clé du cache. Si le lecteur a lu avant la modification et écrit
dans le cache après la suppression, le cache garde l'ancienne version jusqu'à
l'expiration de la clé. 500 manches par stratégie :
  - naïve : SET par le lecteur, DEL par l'écrivain ;
  - double suppression : l'écrivain supprime la clé, puis encore 10 ms plus tard ;
  - baux (comme le memcache de Facebook) : au moment de l'échec, le lecteur
    reçoit un jeton ; la suppression invalide le jeton ; le SET n'est accepté
    que si le jeton est encore valide (script Lua).
On compte les manches qui finissent avec une version ancienne dans le cache.
Prérequis : docker. Usage : python3 course.py   (pip install -r requirements.txt)
"""
import asyncio
import random
import secrets

import psycopg
import redis.asyncio as aioredis

import commun

MANCHES, CLE, BAIL = 500, "profil:1", "profil:1:bail"
SET_SI_BAIL = """
if redis.call('GET', KEYS[2]) == ARGV[2] then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', 60000)
  redis.call('DEL', KEYS[2])
  return 1
end
return 0
"""


async def lecteur(r, cx, strategie, alea):
    if await r.get(CLE) is not None:
        return
    jeton = secrets.token_hex(8)
    if strategie == "baux":
        await r.set(BAIL, jeton, px=10_000)          # le bail est donné au moment de l'échec
    version = (await (await cx.execute("SELECT version FROM profils WHERE id = 1")).fetchone())[0]
    await asyncio.sleep(alea.uniform(0, 0.020))       # le lecteur est lent à ranger sa valeur
    if strategie == "baux":
        await r.eval(SET_SI_BAIL, 2, CLE, BAIL, version, jeton)
    else:
        await r.set(CLE, version, px=60_000)


async def ecrivain(r, cx, strategie, alea):
    await asyncio.sleep(alea.uniform(0, 0.010))
    await cx.execute("UPDATE profils SET version = version + 1 WHERE id = 1")
    if strategie == "baux":
        await r.delete(CLE, BAIL)                     # la suppression invalide le bail
    else:
        await r.delete(CLE)
    if strategie == "double suppression":
        await asyncio.sleep(0.010)
        await r.delete(CLE)


async def essai(strategie):
    r = aioredis.Redis(port=commun.REDIS)
    cl = await psycopg.AsyncConnection.connect(commun.DSN, autocommit=True)
    ce = await psycopg.AsyncConnection.connect(commun.DSN, autocommit=True)
    await ce.execute("DROP TABLE IF EXISTS profils")
    await ce.execute("CREATE TABLE profils (id int PRIMARY KEY, version int)")
    await ce.execute("INSERT INTO profils VALUES (1, 0)")
    alea = random.Random(17)
    perimees = 0
    for _ in range(MANCHES):
        await r.delete(CLE, BAIL)
        await asyncio.gather(lecteur(r, cl, strategie, alea), ecrivain(r, ce, strategie, alea))
        await asyncio.sleep(0.030)                    # laisser finir la seconde suppression
        en_cache = await r.get(CLE)
        en_base = (await (await ce.execute("SELECT version FROM profils WHERE id = 1")).fetchone())[0]
        if en_cache is not None and int(en_cache) < en_base:
            perimees += 1
    await cl.close()
    await ce.close()
    await r.aclose()
    return perimees


async def main():
    commun.lancer()
    try:
        print(f"{MANCHES} manches : un lecteur lent (0 à 20 ms) et un écrivain sur la même clé")
        print(f"{'stratégie':<22}{'version ancienne laissée dans le cache':>40}")
        for strategie in ("naïve", "double suppression", "baux"):
            n = await essai(strategie)
            print(f"{strategie:<22}{n:>29} manches ({100 * n / MANCHES:.1f} %)")
    finally:
        commun.arreter()


if __name__ == "__main__":
    asyncio.run(main())
