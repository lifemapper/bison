/*
 * Imports Leaflet, adds plugins along with new controls and reexports it
 */

// eslint-disable-next-line simple-import-sort/imports
import L from 'leaflet';
import { GestureHandling } from 'leaflet-gesture-handling';
import 'leaflet-gesture-handling/dist/leaflet-gesture-handling.css';
// Create sub-layers to selectively toggle markers in clusters
import 'leaflet.featuregroup.subgroup';
import 'leaflet.markercluster/dist/leaflet.markercluster.js';
// Marker Clustering
import 'leaflet.markercluster/dist/MarkerCluster.css';
import 'leaflet.markercluster/dist/MarkerCluster.Default.css';

import 'leaflet/dist/leaflet.css';
import '../css/leaflet.css';

/* This code is needed to properly load the images in the Leaflet's CSS */

// @ts-expect-error
delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: require('leaflet/dist/images/marker-icon-2x.png'),
  iconUrl: require('leaflet/dist/images/marker-icon.png'),
  shadowUrl: require('leaflet/dist/images/marker-shadow.png'),
});

L.Map.addInitHook('addHandler', 'gestureHandling', GestureHandling);

/* Create a "full screen" button */
// @ts-expect-error
L.Control.FullScreen = L.Control.extend({
  onAdd(map: Readonly<L.Map>) {
    const control = L.DomUtil.create('button');
    control.classList.add('leaflet-full-screen-toggle');
    control.setAttribute('type', 'button');
    control.setAttribute('aria-pressed', 'false');
    control.innerHTML = `<img
      src="/static/img/full-screen.svg"
      alt="Toggle full-screen"
    >`;

    let scrollTop = 0;
    L.DomEvent.on(control, 'click', (event) => {
      L.DomEvent.stopPropagation(event);
      L.DomEvent.preventDefault(event);

      // @ts-expect-error
      const container = (map._container as HTMLElement).parentElement!;
      const isFullScreen = container.classList.contains(
        'leaflet-map-full-screen'
      );
      if (isFullScreen) {
        container.classList.remove('leaflet-map-full-screen');
        control.setAttribute('aria-pressed', 'false');
        document.body.classList.remove('full-screen');
        window.scrollTo({
          left: 0,
          top: scrollTop,
          behavior: 'auto',
        });
        // @ts-expect-error GestureHandling plugin has no type definitions
        map.gestureHandling.enable();
      } else {
        scrollTop = window.scrollY;
        container.classList.add('leaflet-map-full-screen');
        control.setAttribute('aria-pressed', 'true');
        document.body.classList.add('full-screen');
        // @ts-expect-error GestureHandling plugin has no type definitions
        map.gestureHandling.disable();
      }

      map.invalidateSize();
    });

    // @ts-expect-error
    this.control = control;

    return control;
  },

  onRemove() {
    // @ts-expect-error Somebody did a really poor job of typing Leaflet
    L.DomEvent.off(this.img);
  },
});

/* Adds a printer icon to print the map */
// @ts-expect-error
L.Control.PrintMap = L.Control.extend({
  onAdd() {
    const button = L.DomUtil.create('span')!;
    button.classList.add('leaflet-print-map');
    button.textContent = '🖨️';

    L.DomEvent.on(button, 'click', (event) => {
      L.DomEvent.stopPropagation(event);
      L.DomEvent.preventDefault(event);
      window.print();
    });

    // @ts-expect-error
    this.button = button;

    return button;
  },
  onRemove() {
    // @ts-expect-error Somebody did a really poor job of typing Leaflet
    L.DomEvent.off(this.button);
  },
});

export function addFullScreenButton(map: L.Map): void {
  // @ts-expect-error
  new L.Control.FullScreen({ position: 'topleft' }).addTo(map);
}

export function addPrintMapButton(map: L.Map): void {
  // @ts-expect-error
  new L.Control.PrintMap({ position: 'topleft' }).addTo(map);
}

export default L;
