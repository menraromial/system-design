# Expériences du cours de system design

Scripts des mesures publiées dans le cours, un fichier ou un sous-dossier par
chapitre (`ch04_time_wait.py`, `ch05/` à `ch13/`). Chaque script décrit en tête
ce qu'il mesure, ses prérequis et sa commande de lancement.

La page « Reproduire les expériences » du cours donne la machine des mesures,
l'installation, le tableau des expériences par chapitre et le nettoyage :
https://menraromial.com/system-design/cours/experiences

Installation rapide :

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r ch13/requirements.txt    # le fichier du chapitre voulu
```

Les conteneurs Docker créés par les scripts portent un nom en `cours-sd-` et
sont supprimés à la fin de chaque exécution.
