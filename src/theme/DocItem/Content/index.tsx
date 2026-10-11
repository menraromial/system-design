import React, {type ReactNode} from 'react';
import Content from '@theme-original/DocItem/Content';
import type ContentType from '@theme/DocItem/Content';
import type {WrapperProps} from '@docusaurus/types';
import {useDoc} from '@docusaurus/plugin-content-docs/client';
import {partie as trouverPartie, numeroChapitre} from '@site/src/components/parties';

type Props = WrapperProps<typeof ContentType>;

type Entete = {partie?: number; chapitre?: number; cartouche?: string};

// Cartouche posé au-dessus du titre, à la manière du cadre d'un plan
// technique : partie à gauche, numéro de chapitre à droite.
// front matter : partie (0 à 7) et chapitre (1 à 89), ou cartouche pour un
// libellé libre (« Annexe B »).
export default function ContentWrapper(props: Props): ReactNode {
  const {frontMatter} = useDoc();
  const {partie, chapitre, cartouche} = frontMatter as Entete;
  const p = trouverPartie(partie);
  const droite = cartouche ?? (chapitre !== undefined ? `Ch. ${numeroChapitre(chapitre)}` : undefined);
  return (
    <>
      {(p || droite) && (
        <div className="cartouche">
          {p && (
            <span className="cartouche__partie">
              <span className="cartouche__etiquette">Partie {p.num}</span>
              {p.titre}
            </span>
          )}
          {droite && <span className="cartouche__chapitre">{droite}</span>}
        </div>
      )}
      <Content {...props} />
    </>
  );
}
