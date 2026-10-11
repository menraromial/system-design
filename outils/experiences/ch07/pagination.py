"""Chapitre 7 : pagination par décalage (OFFSET) contre pagination par curseur,
sur une table SQLite d'un million de lignes indexée par sa clé primaire.

Mesure le temps d'obtention d'une page de 20 lignes de plus en plus loin dans
la liste, puis montre ce que voit un client qui pagine par décalage pendant
que de nouvelles lignes arrivent.
Usage : python3 pagination.py
"""
import sqlite3
import time

N, PAGE = 1_000_000, 20


def base():
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE article (id INTEGER PRIMARY KEY, titre TEXT, cree_le INTEGER)")
    db.executemany("INSERT INTO article VALUES (?, ?, ?)",
                   ((i, f"Article {i}", 1_700_000_000 + i) for i in range(1, N + 1)))
    db.commit()
    return db


def chrono(db, sql, params, n=50):
    debut = time.perf_counter()
    for _ in range(n):
        lignes = db.execute(sql, params).fetchall()
    return (time.perf_counter() - debut) / n * 1000, lignes


def main():
    db = base()
    print(f"{'position de la page':>20} {'OFFSET':>10} {'curseur':>10}")
    for position in (0, 10_000, 100_000, 500_000, 990_000):
        t_off, a = chrono(db, "SELECT id, titre FROM article ORDER BY id DESC LIMIT ? OFFSET ?",
                          (PAGE, position))
        dernier_vu = N - position + 1  # id de la dernière ligne de la page précédente
        t_cur, b = chrono(db, "SELECT id, titre FROM article WHERE id < ? ORDER BY id DESC LIMIT ?",
                          (dernier_vu, PAGE))
        assert a == b
        print(f"{position:20,d} {t_off:8.3f} ms {t_cur:8.3f} ms")

    # un client lit la page 1, puis 5 articles sont publiés, puis il lit la page 2
    page1 = db.execute("SELECT id FROM article ORDER BY id DESC LIMIT ? OFFSET 0", (PAGE,)).fetchall()
    db.executemany("INSERT INTO article VALUES (?, ?, ?)",
                   ((N + i, f"Nouvel article {i}", 1_800_000_000 + i) for i in range(1, 6)))
    page2_off = db.execute("SELECT id FROM article ORDER BY id DESC LIMIT ? OFFSET ?", (PAGE, PAGE)).fetchall()
    page2_cur = db.execute("SELECT id FROM article WHERE id < ? ORDER BY id DESC LIMIT ?",
                           (page1[-1][0], PAGE)).fetchall()
    doublons = sorted(set(page1) & set(page2_off))
    print(f"après 5 publications, page 2 par OFFSET : {len(doublons)} articles déjà vus en page 1 "
          f"(ids {doublons[0][0]} à {doublons[-1][0]}) ; par curseur : "
          f"{len(set(page1) & set(page2_cur))} doublon")
    print("plan OFFSET :", db.execute("EXPLAIN QUERY PLAN SELECT id FROM article ORDER BY id DESC "
                                      "LIMIT 20 OFFSET 500000").fetchall()[0][3])
    print("plan curseur :", db.execute("EXPLAIN QUERY PLAN SELECT id FROM article WHERE id < 500000 "
                                       "ORDER BY id DESC LIMIT 20").fetchall()[0][3])


if __name__ == "__main__":
    main()
