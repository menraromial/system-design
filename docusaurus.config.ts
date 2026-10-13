import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import remarkMath from 'remark-math';
import rehypeKatex from 'rehype-katex';

const config: Config = {
  title: 'System design',
  tagline: 'Concevoir des systèmes qui tiennent la charge, et savoir le justifier',
  favicon: 'img/favicon.svg',

  future: {
    v4: true,
  },

  url: 'https://menraromial.com',
  baseUrl: '/system-design/',
  trailingSlash: false,

  onBrokenLinks: 'throw',
  markdown: {
    // notes de bas de page (sources) : libellés en français
    remarkRehypeOptions: {
      footnoteLabel: 'Sources et notes',
      footnoteBackLabel: 'Retour au texte',
    },
    hooks: {
      onBrokenMarkdownLinks: 'throw',
    },
  },

  i18n: {
    defaultLocale: 'fr',
    locales: ['fr'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          routeBasePath: 'cours',
          sidebarPath: './sidebars.ts',
          breadcrumbs: false,
          // formules en LaTeX : $...$ dans le texte, $$...$$ en bloc (KaTeX)
          remarkPlugins: [remarkMath],
          rehypePlugins: [rehypeKatex],
          admonitions: {
            keywords: ['calcul', 'piege', 'entretien', 'fiche'],
            extendDefaults: true,
          },
        },
        blog: false,
        theme: {
          customCss: ['./src/css/custom.css', './src/css/figures.css'],
        },
        // Les SVG des figures sont déjà préparés par figures/svgpost.py
        // (couleurs en variables CSS, identifiants préfixés) : SVGO les
        // abîmerait en fusionnant les identifiants de deux figures.
        svgr: {
          svgrConfig: {
            svgo: false,
          },
        },
      } satisfies Preset.Options,
    ],
  ],

  themes: [
    [
      '@easyops-cn/docusaurus-search-local',
      {
        hashed: true,
        language: ['fr'],
        indexBlog: false,
        docsRouteBasePath: 'cours',
        highlightSearchTermsOnTargetPage: true,
        searchBarShortcutHint: false,
      },
    ],
  ],

  themeConfig: {
    colorMode: {
      respectPrefersColorScheme: true,
    },
    docs: {
      sidebar: {
        hideable: true,
      },
    },
    navbar: {
      title: 'System design',
      logo: {
        alt: '',
        src: 'img/logo.svg',
        srcDark: 'img/logo-sombre.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'cours',
          position: 'left',
          label: 'Le cours',
        },
      ],
    },
    footer: {
      copyright: 'System design, cours de Romial Menra.',
    },
    prism: {
      theme: prismThemes.oneLight,
      darkTheme: prismThemes.oneDark,
      additionalLanguages: ['bash', 'json', 'python', 'sql', 'lua', 'protobuf', 'http', 'yaml'],
    },
    tableOfContents: {
      minHeadingLevel: 2,
      maxHeadingLevel: 3,
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
