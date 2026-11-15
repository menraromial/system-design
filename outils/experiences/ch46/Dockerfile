# Une « région » du chapitre 46 : un espace réseau (ce conteneur) où tournent le
# client Python et le serveur PostgreSQL de la région ; tc ajoute les délais vers
# les autres régions.
FROM python:3.14-alpine
RUN apk add --no-cache iproute2-tc libpq && pip install --no-cache-dir "psycopg[binary]" || pip install --no-cache-dir psycopg
