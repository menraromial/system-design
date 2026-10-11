/* Chapitre 9 : quelques latences de référence, mesurées sur la machine.
 *
 *   1. accès mémoire dépendants (chaînage de pointeurs dans une permutation
 *      aléatoire) pour des tableaux de 16 Kio à 1 Gio : cache L1, L2, L3, mémoire,
 *      avec des pages de 4 Kio puis de 2 Mio (madvise) pour isoler l'effet du TLB ;
 *   2. lecture séquentielle de la mémoire (Go/s) ;
 *   3. appel système, verrou non contesté ;
 *   4. aller-retour TCP local (1 octet, TCP_NODELAY) ;
 *   5. lectures aléatoires de 4 Kio sur le SSD, en contournant le cache du
 *      système (O_DIRECT), une à la fois, et lecture séquentielle par blocs de 1 Mio.
 *
 * Compilation : gcc -O2 -o latences latences.c -lpthread
 * Usage : taskset -c 2 ./latences FICHIER_SUR_LE_SSD   (épingler sur un cœur de performance)   (le fichier de 1 Gio est créé puis supprimé)
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

static double maintenant(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec + t.tv_nsec * 1e-9;
}

static uint64_t aleatoire = 88172645463325252ULL;
static uint64_t xorshift(void) {
    aleatoire ^= aleatoire << 13; aleatoire ^= aleatoire >> 7; aleatoire ^= aleatoire << 17;
    return aleatoire;
}

/* une case par ligne de cache (64 octets) ; permutation cyclique de Sattolo */
static double chainage_ns(size_t octets, int grandes_pages) {
    size_t n = octets / 64, aligne = (octets + (2UL << 20) - 1) & ~((2UL << 20) - 1);
    size_t *t = aligned_alloc(2UL << 20, aligne);
    if (grandes_pages) madvise(t, aligne, MADV_HUGEPAGE);  /* pages de 2 Mio : moins de défauts de TLB */
    size_t *ordre = malloc(n * sizeof *ordre);
    for (size_t i = 0; i < n; i++) ordre[i] = i;
    for (size_t i = n - 1; i > 0; i--) {
        size_t j = xorshift() % i, x = ordre[i];
        ordre[i] = ordre[j]; ordre[j] = x;
    }
    for (size_t i = 0; i < n; i++) t[ordre[i] * 8] = ordre[(i + 1) % n] * 8;
    size_t p = 0, pas = 20000000;
    for (size_t i = 0; i < n; i++) p = t[p];            /* préchauffage */
    double d = maintenant();
    for (size_t i = 0; i < pas; i++) p = t[p];
    d = maintenant() - d;
    if (p == (size_t)-1) puts("");                       /* empêche l'optimisation */
    free(t); free(ordre);
    return d / pas * 1e9;
}

static double lecture_sequentielle_gos(size_t octets) {
    uint64_t *t = aligned_alloc(64, octets), somme = 0;
    memset(t, 1, octets);
    double d = maintenant();
    for (int r = 0; r < 4; r++)
        for (size_t i = 0; i < octets / 8; i++) somme += t[i];
    d = maintenant() - d;
    if (somme == 42) puts("");
    free(t);
    return 4.0 * octets / d / 1e9;
}

static int comparer(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}

static void *echo(void *arg) {
    int ecoute = *(int *)arg, c = accept(ecoute, NULL, NULL), un = 1;
    char o;
    setsockopt(c, IPPROTO_TCP, TCP_NODELAY, &un, sizeof un);
    while (read(c, &o, 1) == 1) write(c, &o, 1);
    close(c);
    return NULL;
}

static void aller_retour_tcp(void) {
    int ecoute = socket(AF_INET, SOCK_STREAM, 0), un = 1;
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_LOOPBACK)};
    socklen_t l = sizeof a;
    bind(ecoute, (struct sockaddr *)&a, sizeof a);
    listen(ecoute, 1);
    getsockname(ecoute, (struct sockaddr *)&a, &l);
    pthread_t th;
    pthread_create(&th, NULL, echo, &ecoute);
    int s = socket(AF_INET, SOCK_STREAM, 0);
    connect(s, (struct sockaddr *)&a, sizeof a);
    setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &un, sizeof un);
    enum { N = 50000 };
    static double d[N];
    char o = 'x';
    for (int i = 0; i < N; i++) {
        double t = maintenant();
        write(s, &o, 1); read(s, &o, 1);
        d[i] = (maintenant() - t) * 1e6;
    }
    close(s); pthread_join(th, NULL); close(ecoute);
    qsort(d, N, sizeof *d, comparer);
    printf("aller-retour TCP local          médiane %6.1f µs   p99 %6.1f µs\n", d[N / 2], d[N * 99 / 100]);
}

static void ssd(const char *chemin) {
    const size_t taille = 1UL << 30, bloc = 1 << 20;
    char *tampon = aligned_alloc(4096, bloc);
    for (size_t i = 0; i < bloc; i++) tampon[i] = (char)xorshift();
    int f = open(chemin, O_CREAT | O_TRUNC | O_WRONLY | O_DIRECT, 0600);
    for (size_t o = 0; o < taille; o += bloc) pwrite(f, tampon, bloc, o);
    fsync(f); close(f);

    f = open(chemin, O_RDONLY | O_DIRECT);
    enum { N = 20000 };
    static double d[N];
    for (int i = 0; i < N; i++) {
        off_t o = (off_t)(xorshift() % (taille / 4096)) * 4096;
        double t = maintenant();
        pread(f, tampon, 4096, o);
        d[i] = (maintenant() - t) * 1e6;
    }
    qsort(d, N, sizeof *d, comparer);
    printf("lecture aléatoire 4 Kio, SSD    médiane %6.1f µs   p99 %6.1f µs   p99,9 %6.1f µs\n",
           d[N / 2], d[N * 99 / 100], d[N * 999 / 1000]);
    double t = maintenant();
    for (size_t o = 0; o < taille; o += bloc) pread(f, tampon, bloc, o);
    t = maintenant() - t;
    printf("lecture séquentielle, SSD       %.2f Go/s (1 Mio par lecture)\n", taille / t / 1e9);
    close(f); unlink(chemin); free(tampon);
}

int main(int argc, char **argv) {
    size_t tailles[] = {16 << 10, 512 << 10, 4 << 20, 16 << 20, 256 << 20, 1UL << 30};
    const char *noms[] = {"16 Kio", "512 Kio", "4 Mio", "16 Mio", "256 Mio", "1 Gio"};
    printf("accès dépendant (ns)        pages de 4 Kio   pages de 2 Mio\n");
    for (int i = 0; i < 6; i++)
        printf("  tableau de %-7s           %8.1f         %8.1f\n", noms[i],
               chainage_ns(tailles[i], 0), chainage_ns(tailles[i], 1));
    printf("lecture séquentielle mémoire    %.1f Go/s (tableau de 1 Gio)\n", lecture_sequentielle_gos(1UL << 30));

    enum { M = 2000000 };
    double t = maintenant();
    for (int i = 0; i < M; i++) syscall(SYS_getpid);
    printf("appel système (getpid)          %6.1f ns\n", (maintenant() - t) / M * 1e9);
    pthread_mutex_t verrou = PTHREAD_MUTEX_INITIALIZER;
    t = maintenant();
    for (int i = 0; i < M * 10; i++) { pthread_mutex_lock(&verrou); pthread_mutex_unlock(&verrou); }
    printf("verrou non contesté             %6.1f ns (prise + libération)\n", (maintenant() - t) / (M * 10) * 1e9);

    aller_retour_tcp();
    if (argc > 1) ssd(argv[1]);
    return 0;
}
