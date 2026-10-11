# System design

Cours de system design en français, publié avec Docusaurus sur
<https://menraromial.com/system-design/>. Consignes : `PROMPT.md` ; plan détaillé : `PLAN.md`.

## Travailler en local

```bash
npm ci
npm run start          # aperçu avec rechargement
npm run build          # build complet (liens cassés = échec)
```

## Figures

Les schémas sont écrits en TikZ dans `figures/src/*.tex` avec `figures/lib/sd.sty`, qui
charge les deux registres de `~/these/docs/diagramme` (`diagramme.sty` pour les architectures à
icônes, `papier.sty` pour les mécanismes et chronogrammes, installés dans `~/texmf`) et ramène
leurs couleurs à la palette `figures/palette.tsv`.

```bash
make -C figures          # PDF (lualatex) -> SVG (pdftocairo) -> svgpost.py -> src/figures/
make -C figures apercu   # un PNG par figure dans figures/build/
```

`svgpost.py` remplace chaque couleur par une variable CSS (`--fig-*`, générées dans
`src/css/figures.css` par `palette.py`) : les figures suivent le thème clair ou sombre. Une
couleur hors palette, par exemple un mélange `Dblue!30`, fait échouer la conversion. Les SVG
sont versionnés, la CI n'a besoin ni de LaTeX ni de pdftocairo.

Dans une page : `import x from '@site/src/figures/x.svg'` puis
`<Figure svg={x} num="1.1" alt="...">légende</Figure>`.

## Rédaction

Après chaque chapitre : `python3 outils/typo.py docs/.../chapitre.mdx` (espaces insécables,
apostrophes), ajout du chapitre à `sidebars.ts` et de son identifiant dans
`src/components/parties.ts` (le lien s'active sur l'accueil), puis `npm run build`.
`outils/capture.mjs` prend une capture d'une page servie en local, en clair ou en sombre.
