// Capture d'écran d'une page du site servi en local, pour vérifier un rendu.
// Chrome doit tourner avec --remote-debugging-port=9334 (voir plus bas).
// Usage : node outils/capture.mjs URL sortie.png [sélecteur CSS] [sombre]
//   google-chrome --headless=new --remote-debugging-port=9334 --window-size=1400,1800 about:blank &
import {writeFileSync} from 'node:fs';

const [url, sortie, selecteur, theme] = process.argv.slice(2);
const cible = await (await fetch('http://127.0.0.1:9334/json/new?about:blank', {method: 'PUT'})).json();
const ws = new WebSocket(cible.webSocketDebuggerUrl);
let id = 0;
const attente = new Map();
ws.onmessage = (e) => {
  const m = JSON.parse(e.data);
  if (attente.has(m.id)) { attente.get(m.id)(m); attente.delete(m.id); }
};
const appel = (method, params = {}) => new Promise((r) => {
  const i = ++id; attente.set(i, r); ws.send(JSON.stringify({id: i, method, params}));
});
await new Promise((r) => (ws.onopen = r));
await appel('Emulation.setDeviceMetricsOverride', {width: 1400, height: 1800, deviceScaleFactor: 1, mobile: false});
if (theme === 'sombre') {
  await appel('Emulation.setEmulatedMedia', {features: [{name: 'prefers-color-scheme', value: 'dark'}]});
}
await appel('Page.navigate', {url});
await new Promise((r) => setTimeout(r, 3500));
if (selecteur) {
  await appel('Runtime.evaluate', {expression:
    `document.querySelector(${JSON.stringify(selecteur)})?.scrollIntoView({block: 'start'}); window.scrollBy(0, -70);`});
  await new Promise((r) => setTimeout(r, 800));
}
const res = await appel('Page.captureScreenshot', {format: 'png'});
writeFileSync(sortie, Buffer.from(res.result.data, 'base64'));
await fetch('http://127.0.0.1:9334/json/close/' + cible.id);
ws.close();
process.exit(0);
