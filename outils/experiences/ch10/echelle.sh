#!/usr/bin/env bash
# Chapitre 10 : passage à l'échelle horizontal. 1, 2 puis 4 serveurs d'application
# (app.py, environ 2 ms de calcul par requête, un processus Python chacun, épinglé sur
# son propre cœur de performance) derrière Caddy en répartiteur round-robin ; wrk
# envoie la charge depuis d'autres cœurs.
# Usage : ./echelle.sh PYTHON   (python avec aiohttp ; caddy et wrk installés)
set -uo pipefail
export LC_ALL=C
cd "$(dirname "$0")"
PY=${1:-python3}
COEURS=(1 3 6 8)          # un cœur de performance par serveur d'application
TMP=$(mktemp -d); trap 'kill $(jobs -p) 2>/dev/null; rm -rf "$TMP"' EXIT

mesurer() {  # url, libellé
  local s
  s=$(taskset -c 12-15 wrk -t4 -c64 -d10s --latency "$1" 2>&1)
  printf '%-30s %8s req/s   p50 %8s   p99 %8s\n' "$2" \
    "$(awk '/Requests\/sec/ {print int($2)}' <<<"$s")" \
    "$(awk '$1=="50%" {print $2}' <<<"$s")" "$(awk '$1=="99%" {print $2}' <<<"$s")"
}

echo "64 connexions, 10 s par mesure, environ 2 ms de calcul par requête"
for n in 1 2 4; do
  amont=""
  for i in $(seq 0 $((n - 1))); do
    port=$((9101 + i))
    SESSIONS=memoire taskset -c "${COEURS[$i]}" "$PY" app.py "$port" & amont="$amont 127.0.0.1:$port"
  done
  cat > "$TMP/Caddyfile" <<CADDY
{
	admin off
	auto_https off
}
:9100 {
	reverse_proxy$amont {
		lb_policy round_robin
	}
}
CADDY
  taskset -c 10,11 caddy run --config "$TMP/Caddyfile" --adapter caddyfile > "$TMP/caddy.log" 2>&1 &
  sleep 2
  [ "$n" = 1 ] && mesurer "http://127.0.0.1:9101/calcul" "1 serveur, sans répartiteur"
  mesurer "http://127.0.0.1:9100/calcul" "$n serveur(s) derrière Caddy"
  kill $(jobs -p) 2>/dev/null; wait 2>/dev/null; sleep 1
done
