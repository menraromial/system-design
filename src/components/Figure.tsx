import React, {type ComponentType, type ReactNode, type SVGProps} from 'react';

type Props = {
  // composant importé depuis src/figures (source : figures/src/<nom>.tex) :
  //   import cycle from '@site/src/figures/demarche-cycle.svg';
  //   <Figure svg={cycle} num="1.1" alt="...">légende</Figure>
  svg: ComponentType<SVGProps<SVGSVGElement> & {title?: string}>;
  num: string;
  alt: string;
  // largeur maximale en pixels, pour les petites figures qui seraient
  // démesurées à pleine colonne
  max?: number;
  children: ReactNode;
};

// Les SVG sont insérés dans la page (et non en <img>) pour que leurs couleurs,
// écrites en variables CSS, suivent le thème clair ou sombre.
export default function Figure({svg: Svg, num, alt, max, children}: Props): ReactNode {
  return (
    <figure className="figure">
      <div className="figure__planche">
        <Svg role="img" aria-label={alt} style={max ? {maxWidth: `${max}px`} : undefined} />
      </div>
      <figcaption>
        <span className="figure__num">Figure {num}</span>
        {children}
      </figcaption>
    </figure>
  );
}
