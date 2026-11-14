# Image des fonctions du chapitre 44 : image de base officielle d'AWS Lambda
# (runtime Python 3.13 et émulateur d'interface d'exécution, RIE), plus les
# bibliothèques que la fonction « lourde » importe à l'initialisation.
FROM public.ecr.aws/lambda/python:3.13
RUN pip install --no-cache-dir numpy pandas "psycopg[binary]"
COPY fonctions/ ${LAMBDA_TASK_ROOT}/
