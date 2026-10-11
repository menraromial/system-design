import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

// Une catégorie par partie ; chaque chapitre terminé y est ajouté aussitôt,
// et reçoit son identifiant de page dans src/components/parties.ts.
const sidebars: SidebarsConfig = {
  cours: [
    'index',
    {
      type: 'category',
      label: 'Partie 0 · Méthode',
      collapsed: false,
      link: {type: 'doc', id: 'partie-0/index'},
      items: ['partie-0/demarche'],
    },
  ],
};

export default sidebars;
