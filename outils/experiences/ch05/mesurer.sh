#!/usr/bin/env bash
# Chapitre 5 : télécharger 100 fichiers de 1 Ko en HTTP/1.1 et en HTTP/2, à
# travers un relais qui ajoute 50 ms à chaque aller-retour.
# Prérequis : caddy, curl avec HTTP/2, python3.  Usage : ./mesurer.sh
set -euo pipefail
export LC_NUMERIC=C
cd "$(dirname "$0")"
TMP=$(mktemp -d)
caddy run --config Caddyfile --adapter caddyfile > "$TMP/caddy.log" 2>&1 &
CADDY=$!
python3 relais_latence.py 9443 8443 25 &
RELAIS=$!
trap 'kill $CADDY $RELAIS 2>/dev/null; rm -rf "$TMP"' EXIT
sleep 2

essai() {  # libellé, options curl
  local nom=$1; shift
  local debut fin connexions
  debut=$(date +%s.%N)
  connexions=$(curl -sk "$@" -o "$TMP/#1" -w '%{num_connects}\n' \
    "https://localhost:9443/f[001-100].txt" | paste -sd+ | bc)
  fin=$(date +%s.%N)
  printf '%-42s %6.2f s   %3d connexion(s)\n' "$nom" "$(echo "$fin - $debut" | bc)" "$connexions"
}

echo "100 fichiers de 1 Ko, aller-retour simulé de 50 ms"
essai "HTTP/1.1, une connexion, en série"         --http1.1
essai "HTTP/1.1, 6 connexions en parallèle"       --http1.1 --parallel --parallel-max 6
essai "HTTP/2, une connexion, en série"           --http2
essai "HTTP/2, une connexion, multiplexée"        --http2 --parallel --parallel-max 100
