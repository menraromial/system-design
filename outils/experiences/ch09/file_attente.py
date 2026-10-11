"""Chapitre 9 : latence d'une file d'attente à un serveur selon son taux
d'utilisation, mesurée avec de vraies attentes (time.sleep), comparée au
modèle M/M/1 et à la loi de Little.

Un fil génère des requêtes à des instants aléatoires (processus de Poisson,
débit λ), un autre les sert une par une avec une durée de service aléatoire
(exponentielle, moyenne S = 2 ms). On mesure le temps passé dans le système
(attente + service) par chaque requête, et le nombre moyen de requêtes dans
le système.
Usage : python3 file_attente.py
"""
import queue
import random
import statistics
import threading
import time

S = 0.002          # durée moyenne de service : 2 ms
N = 20000          # requêtes par niveau de charge
random.seed(7)


def mesurer(rho):
    lam = rho / S
    file, sejours, evenements = queue.Queue(), [], []
    verrou = threading.Lock()

    def generer():
        t = time.perf_counter()
        for _ in range(N):
            t += random.expovariate(lam)
            pause = t - time.perf_counter()
            if pause > 0:
                time.sleep(pause)
            arrivee = time.perf_counter()
            with verrou:
                evenements.append((arrivee, +1))
            file.put(arrivee)
        file.put(None)

    def servir():
        services = []
        while (arrivee := file.get()) is not None:
            debut = time.perf_counter()
            time.sleep(random.expovariate(1 / S))
            fin = time.perf_counter()
            services.append(fin - debut)
            sejours.append(fin - arrivee)
            with verrou:
                evenements.append((fin, -1))
        return services

    resultat = {}
    g = threading.Thread(target=generer)
    w = threading.Thread(target=lambda: resultat.setdefault("s", servir()))
    debut = time.perf_counter()
    g.start(); w.start(); g.join(); w.join()
    duree = time.perf_counter() - debut

    # nombre moyen de requêtes dans le système : intégrale du compteur sur le temps
    evenements.sort()
    dans, aire, t_prec = 0, 0.0, evenements[0][0]
    for t, delta in evenements:
        aire += dans * (t - t_prec)
        dans, t_prec = dans + delta, t
    L = aire / (evenements[-1][0] - evenements[0][0])
    s_reel = statistics.mean(resultat["s"])
    debit = N / duree
    W = statistics.mean(sejours)
    q = statistics.quantiles(sejours, n=1000)
    return rho, debit * s_reel, W, q[499], q[989], q[998], L, debit * W, sejours


def main():
    print("S = 2 ms ; temps de séjour en ms ; ρ réel = débit mesuré × service moyen mesuré")
    print(f"{'ρ visé':>6} {'ρ réel':>6} {'moyen':>7} {'p50':>7} {'p99':>7} {'p99,9':>7} "
          f"{'M/M/1':>7} {'L mesuré':>9} {'λ × W':>7}")
    for rho in (0.5, 0.7, 0.8, 0.9, 0.95):
        r, rr, W, p50, p99, p999, L, lw, _ = mesurer(rho)
        theorie = S / (1 - rr) * 1000
        print(f"{r:6.2f} {rr:6.2f} {W*1000:7.1f} {p50*1000:7.1f} {p99*1000:7.1f} {p999*1000:7.1f} "
              f"{theorie:7.1f} {L:9.2f} {lw:7.2f}")


if __name__ == "__main__":
    main()
