import React, {type ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Layout from '@theme/Layout';
import {PARTIES, numeroChapitre} from '@site/src/components/parties';
import styles from './index.module.css';

function Plan(): ReactNode {
  return (
    <section className={styles.plan}>
      {PARTIES.map((p) => (
        <div key={p.num} className={styles.partie}>
          <div className={styles.entetePartie}>
            <span className={styles.numPartie}>Partie {p.num}</span>
            <h2>{p.titre}</h2>
          </div>
          <p className={styles.resume}>{p.resume}</p>
          <ol className={styles.chapitres}>
            {p.chapitres.map((c) => (
              <li key={c.num} className={c.doc ? undefined : styles.aVenir}>
                <span className={styles.numChapitre}>{numeroChapitre(c.num)}</span>
                {c.doc ? <Link to={`/cours/${c.doc}`}>{c.titre}</Link> : <span>{c.titre}</span>}
              </li>
            ))}
          </ol>
        </div>
      ))}
    </section>
  );
}

export default function Accueil(): ReactNode {
  return (
    <Layout
      title="Concevoir des systèmes à grande échelle"
      description="Cours de system design en français : méthode, briques, systèmes distribués, architectures, production et études de cas.">
      <header className={styles.calque}>
        <div className={styles.cadre}>
          <p className={styles.surtitre}>Cours de system design</p>
          <h1 className={styles.titre}>Concevoir des systèmes qui tiennent la charge</h1>
          <p className={styles.chapeau}>
            Pour qui sait programmer et n’a jamais conçu de système distribué. On part d’un
            serveur et d’une base, on chiffre la charge, on ajoute chaque brique quand elle devient
            nécessaire, et l’on finit par concevoir de bout en bout une messagerie, un service de
            paiement ou une plateforme vidéo, avec les arguments qu’on attend en entretien.
          </p>
          <div className={styles.actions}>
            <Link className={styles.bouton} to="/cours/partie-0/demarche">
              Commencer : la démarche de conception
            </Link>
            <Link className={styles.lien} to="/cours">
              Comment le cours est construit
            </Link>
          </div>
        </div>
      </header>
      <main className={styles.principal}>
        <Plan />
      </main>
    </Layout>
  );
}
