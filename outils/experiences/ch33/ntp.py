"""Chapitre 33 : interroger des serveurs NTP (SNTP, RFC 4330 et 5905).

Pour chaque serveur, 6 requêtes espacées de 8 s (les serveurs du pool
refusent par « RATE » des requêtes plus rapprochées). Chaque réponse donne les
quatre instants T1 (envoi par le client), T2 (réception par le serveur), T3
(réponse du serveur), T4 (réception par le client), d'où le décalage
θ = ((T2 - T1) + (T3 - T4)) / 2 et le délai δ = (T4 - T1) - (T3 - T2). Le
décalage n'est sûr qu'à ± δ/2 près, puisque rien ne dit comment l'aller et
le retour se partagent le délai.

    python3 ntp.py
"""
import socket
import statistics
import struct
import time

SERVEURS = ["time.cloudflare.com", "time.google.com", "pool.ntp.org",
            "ntp.ubuntu.com"]
EPOQUE_NTP = 2208988800                     # 1900-01-01 → 1970-01-01, en s


def interroger(hote):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(3)
    paquet = bytes([0x23]) + bytes(47)      # LI 0, version 4, mode 3 (client)
    t1 = time.time()
    s.sendto(paquet, (hote, 123))
    d, (adresse, _) = s.recvfrom(48)
    t4 = time.time()
    s.close()

    def instant(o):
        sec, frac = struct.unpack("!II", d[o:o + 8])
        return sec - EPOQUE_NTP + frac / 2 ** 32
    if d[1] == 0 or struct.unpack("!II", d[40:48]) == (0, 0):
        # strate 0 : « kiss-o'-death » (refus, limitation de débit) ou
        # serveur non synchronisé ; la RFC 4330 demande d'ignorer la réponse
        raise ValueError(d[12:16].decode("ascii", "replace"))
    t2, t3 = instant(32), instant(40)
    return {"theta": ((t2 - t1) + (t3 - t4)) / 2, "delta": (t4 - t1) - (t3 - t2),
            "strate": d[1], "adresse": adresse}


def main():
    print("décalage θ de l'horloge locale par rapport au serveur, délai δ")
    print(f"   {'serveur':19} {'str.':>4} {'δ min':>7} {'θ à δ min':>10} "
          f"{'θ min..max':>16} {'± δ/2':>6}")
    for hote in SERVEURS:
        mesures, refus = [], []
        for _ in range(6):
            try:
                mesures.append(interroger(hote))
            except OSError:
                refus.append("sans réponse")
            except ValueError as e:
                refus.append(f"refus {e}")
            time.sleep(8)
        if not mesures:
            print(f"   {hote:19} sans réponse")
            continue
        meilleure = min(mesures, key=lambda m: m["delta"])
        thetas = [m["theta"] * 1000 for m in mesures]
        print(f"   {hote:19} {meilleure['strate']:4} "
              f"{meilleure['delta'] * 1000:5.0f}ms "
              f"{meilleure['theta'] * 1000:+8.1f}ms "
              f"{min(thetas):+7.1f}..{max(thetas):+6.1f}ms "
              f"{meilleure['delta'] * 500:4.0f}ms")
        if refus:
            from collections import Counter
            print(f"   {'':19} écartées : " + ", ".join(
                f"{n} × {r}" for r, n in Counter(refus).items()))
    print("   (6 requêtes par serveur ; δ min = la requête la plus rapide)")


if __name__ == "__main__":
    main()
