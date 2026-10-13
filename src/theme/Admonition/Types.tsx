import React, {type ReactNode} from 'react';
import DefaultAdmonitionTypes from '@theme-original/Admonition/Types';
import AdmonitionLayout from '@theme/Admonition/Layout';
import type {Props} from '@theme/Admonition';

// Encadrés propres au cours, en plus de note, tip, info, warning, danger :
//   :::calcul     une estimation menée ligne par ligne
//   :::piege      une erreur fréquente, chez le débutant ou en entretien
//   :::entretien  ce que l'examinateur attend sur ce point
//   :::fiche      la fiche de révision du chapitre
function encadre(type: string, titreParDefaut: string) {
  return function Encadre(props: Props): ReactNode {
    return (
      <AdmonitionLayout
        {...props}
        type={type}
        icon={null}
        title={props.title ?? titreParDefaut}>
        {props.children}
      </AdmonitionLayout>
    );
  };
}

const AdmonitionTypes = {
  ...DefaultAdmonitionTypes,
  calcul: encadre('calcul', 'Calcul d’estimation'),
  piege: encadre('piege', 'Erreur fréquente'),
  entretien: encadre('entretien', 'En entretien'),
  fiche: encadre('fiche', 'Fiche de révision'),
};

export default AdmonitionTypes;
