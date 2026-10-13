import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

// Une catégorie par partie ; chaque chapitre terminé y est ajouté aussitôt,
// et reçoit son identifiant de page dans src/components/parties.ts.
const sidebars: SidebarsConfig = {
  cours: [
    'index',
    'experiences',
    {
      type: 'category',
      label: 'Partie 0 · Méthode',
      collapsed: false,
      link: {type: 'doc', id: 'partie-0/index'},
      items: ['partie-0/demarche', 'partie-0/estimation'],
    },
    {
      type: 'category',
      label: 'Partie 1 · Fondamentaux',
      collapsed: false,
      link: {type: 'doc', id: 'partie-1/index'},
      items: ['partie-1/client-serveur-dns', 'partie-1/transport', 'partie-1/http', 'partie-1/tls', 'partie-1/styles-api', 'partie-1/pousser-donnees', 'partie-1/latence-debit', 'partie-1/passer-echelle', 'partie-1/disponibilite', 'partie-1/coherence-cap'],
    },
    {
      type: 'category',
      label: 'Partie 2 · Briques de base',
      collapsed: false,
      link: {type: 'doc', id: 'partie-2/index'},
      items: ['partie-2/repartition-charge', 'partie-2/proxy-passerelle', 'partie-2/cdn-edge', 'partie-2/cache-strategies', 'partie-2/cache-invalidation', 'partie-2/transactions-isolation', 'partie-2/index-modelisation', 'partie-2/moteurs-stockage'],
    },
  ],
};

export default sidebars;
