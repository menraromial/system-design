#!/usr/bin/env bash
# Chapitre 6 : une petite infrastructure de certificats locale avec openssl.
#   1. une autorité racine, un certificat serveur valide, un certificat expiré,
#      un certificat client ;
#   2. ce que voit un client face au certificat valide, puis expiré ;
#   3. TLS mutuel : connexion refusée sans certificat client, acceptée avec ;
#   4. poignées de main TLS 1.3 complètes et reprises de session par seconde.
# Prérequis : OpenSSL 3.4 ou plus (options -not_before/-not_after).
set -uo pipefail
export LC_ALL=C
cd "$(dirname "$0")"
D=$(mktemp -d); trap 'kill $(jobs -p) 2>/dev/null; rm -rf "$D"' EXIT
cd "$D"
cle() { openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1" 2>/dev/null; }

# 1. autorité racine (10 ans) et certificats signés par elle
cle ca.key
openssl req -x509 -new -key ca.key -subj "/CN=Racine du cours" -days 3650 -out ca.crt
signer() {  # nom, CN, dates, extension
  cle "$1.key"
  openssl req -new -key "$1.key" -subj "/CN=$2" -out "$1.csr"
  openssl x509 -req -in "$1.csr" -CA ca.crt -CAkey ca.key -out "$1.crt" $3 \
    -extfile <(printf '%s\n' "$4") 2>/dev/null
}
signer serveur localhost "-days 90" "subjectAltName=DNS:localhost"
signer expire localhost "-not_before 20260101000000Z -not_after 20260401000000Z" "subjectAltName=DNS:localhost"
signer client service-commandes "-days 90" "extendedKeyUsage=clientAuth"
echo "== certificats"
for c in serveur expire client; do
  printf '%-8s ' "$c"; openssl x509 -in "$c.crt" -noout -subject -enddate | paste -sd' '
done

client() {  # port, options supplémentaires
  timeout 5 openssl s_client -connect "localhost:$1" -servername localhost -CAfile ca.crt \
    -verify_return_error -brief "${@:2}" </dev/null 2>&1 \
    | grep -E "Verification|verify error|alert|CONNECTION ESTABLISHED|Protocol version" | head -3
}

# 2. certificat valide, puis expiré
openssl s_server -accept 9601 -cert serveur.crt -key serveur.key -www -quiet 2>/dev/null &
openssl s_server -accept 9602 -cert expire.crt -key expire.key -www -quiet 2>/dev/null &
sleep 1
echo "== client face au certificat valide"; client 9601
echo "== client face au certificat expiré"; client 9602

# 3. TLS mutuel : le serveur exige un certificat client signé par la racine
openssl s_server -accept 9603 -cert serveur.crt -key serveur.key -CAfile ca.crt \
  -Verify 1 -verify_return_error -www -quiet 2>/dev/null &
sleep 1
echo "== TLS mutuel, sans certificat client"
# en TLS 1.3, le client se croit connecté avant que le serveur ait vérifié son
# certificat : on garde son entrée ouverte une seconde pour recevoir l'alerte
sleep 1 | timeout 5 openssl s_client -connect localhost:9603 -servername localhost -CAfile ca.crt \
  -brief 2>&1 | grep -Eio "CONNECTION ESTABLISHED|alert certificate required" | head -2
echo "== TLS mutuel, avec le certificat du service-commandes"; client 9603 -cert client.crt -key client.key

# 4. poignées de main par seconde, client et serveur sur la même machine.
# s_time ne sait reprendre une session qu'en TLS 1.2 : on compare donc, en
# TLS 1.2, poignée complète et reprise, avec une clé ECDSA puis RSA.
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out rsa.key 2>/dev/null
openssl req -x509 -new -key rsa.key -subj "/CN=localhost" -days 90 -out rsa.crt
openssl s_server -accept 9604 -cert rsa.crt -key rsa.key -www -quiet 2>/dev/null &
sleep 1
echo "== TLS 1.2, connexions en 6 s de temps réel"
for cas in "9601 ECDSA-P256" "9604 RSA-2048"; do
  set -- $cas
  for mode in -new -reuse; do
    n=$(openssl s_time -connect "localhost:$1" $mode -time 5 -tls1_2 2>/dev/null \
        | grep "real seconds" | awk '{print $1}')
    printf '%-10s %-7s %6s connexions\n' "$2" "$mode" "$n"
  done
done
