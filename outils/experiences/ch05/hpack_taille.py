"""Chapitre 5 : taille des en-têtes d'une suite de requêtes, en texte HTTP/1.1
et compressés par HPACK (HTTP/2), avec la bibliothèque hpack.

Les en-têtes imitent ceux d'un navigateur qui charge les ressources d'une
page : seul le chemin change d'une requête à l'autre.
Usage : python3 hpack_taille.py   (pip install hpack)
"""
import hpack

COMMUNS = [
    ("user-agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/140.0.0.0 Safari/537.36"),
    ("accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,"
               "image/webp,*/*;q=0.8"),
    ("accept-language", "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7"),
    ("accept-encoding", "gzip, deflate, br, zstd"),
    ("referer", "https://boutique.example/catalogue"),
    ("cookie", "session=" + "a1b2c3d4" * 8 + "; panier=17; consentement=oui; ab_test=variante-b"),
]


def texte_http11(chemin):
    lignes = [f"GET {chemin} HTTP/1.1", "Host: boutique.example"]
    lignes += [f"{nom.title()}: {valeur}" for nom, valeur in COMMUNS]
    return ("\r\n".join(lignes) + "\r\n\r\n").encode()


def main():
    codeur = hpack.Encoder()  # garde sa table dynamique d'une requête à l'autre
    total_texte = total_hpack = 0
    for i in range(1, 11):
        chemin = f"/static/image-{i:02d}.webp"
        texte = len(texte_http11(chemin))
        bloc = len(codeur.encode([(":method", "GET"), (":scheme", "https"),
                                  (":authority", "boutique.example"), (":path", chemin)] + COMMUNS))
        total_texte += texte
        total_hpack += bloc
        if i in (1, 2, 10):
            print(f"requête {i:2d} : HTTP/1.1 {texte:4d} o, HPACK {bloc:4d} o")
    print(f"10 requêtes : HTTP/1.1 {total_texte} o, HPACK {total_hpack} o, "
          f"soit {100 * total_hpack / total_texte:.0f} %")


if __name__ == "__main__":
    main()
