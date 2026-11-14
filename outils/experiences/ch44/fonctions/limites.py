"""Fonction pour éprouver les limites : durée (dort) et mémoire (alloue)."""
import os
import time

APPELS = 0


def handler(event, context):
    global APPELS
    APPELS += 1
    if "dormir" in event:
        time.sleep(event["dormir"])
    if "allouer_mo" in event:
        bloc = bytearray(event["allouer_mo"] << 20)
        bloc[::4096] = b"x" * len(bloc[::4096])
    return {"appels": APPELS, "pid": os.getpid()}
