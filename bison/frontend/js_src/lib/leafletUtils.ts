import type { LayersControlEventHandlerFn } from 'leaflet';

import * as cache from './cache';
import type { leafletTileServers } from './config';
import { DEFAULT_CENTER, DEFAULT_ZOOM, preferredBaseLayer } from './config';
import type { LeafletOverlays } from './frontend/occurrence';
import { parseLayerFromJson } from './frontend/occurrence';
import L, { addFullScreenButton, addPrintMapButton } from './leaflet';

export function rememberSelectedBaseLayers(
  map: L.Map,
  layerGroup: L.Control.Layers,
  cacheName: string,
  tileLayers: typeof leafletTileServers
): void {
  const layers = tileLayers.baseMaps;
  const currentLayer = cache.get('leafletBaseLayer', cacheName);
  let layerLabel =
    currentLayer !== false && currentLayer in layers
      ? currentLayer
      : preferredBaseLayer;
  if (typeof layers[layerLabel] === 'undefined')
    layerLabel = Object.keys(layers)[0];
  const baseLayer = layers[layerLabel]();
  baseLayer.addTo(map);

  Object.entries(layers).forEach(([label, layer]) =>
    layerGroup.addBaseLayer(label === layerLabel ? baseLayer : layer(), label)
  );

  map.on('baselayerchange', ({ name }: { readonly name: string }) => {
    cache.set('leafletBaseLayer', cacheName, name, { overwrite: true });
  });
}

export const isOverlayDefault = (
  layerName: string,
  defaultValue: boolean
): boolean =>
  cache.get('leafletOverlays', layerName, {
    defaultValue: defaultValue ?? false,
  });

export function rememberSelectedOverlays(map: L.Map): void {
  const handleOverlayEvent: LayersControlEventHandlerFn = ({ name, type }) =>
    cache.set('leafletOverlays', name, type === 'overlayadd', {
      overwrite: true,
    });

  map.on('overlayadd', handleOverlayEvent);
  map.on('overlayremove', handleOverlayEvent);
}

export const addAggregatorOverlays =
  (leafletMap: L.Map, layerGroup: L.Control.Layers, callback?: () => void) =>
  (layers: LeafletOverlays) =>
    Object.entries(layers).forEach(([label, { isDefault, ...options }]) => {
      callback?.();
      const layer = parseLayerFromJson(options, false)();
      layerGroup.addOverlay(layer, label);
      if (isDefault) layer.addTo(leafletMap);
    });

export const legendPoint = (color: string): string => `<span
  aria-hidden="true"
  style="--color: ${color}"
  class="leaflet-legend-point"
></span>`;

export const legendGradient = (
  leftColor: string,
  rightColor: string
): string => `<span
  aria-hidden="true"
  style="--left-color: ${leftColor}; --right-color: ${rightColor}"
  class="leaflet-legend-gradient"
></span>`;

export function showMap(
  mapContainer: HTMLElement,
  cacheName: string,
  tileLayers: typeof leafletTileServers
): Readonly<[L.Map, L.Control.Layers]> {
  const map = L.map(mapContainer, {
    maxZoom: 23,
    /*
    * Workaround for pop-ups not working in Safari. See:
    * https://stackoverflow.com/questions/65369083/popup-does-not-open-when-clicking-on-marker-safari/65369228
    * */
    tap: false,
  }).setView(DEFAULT_CENTER, DEFAULT_ZOOM);

  // @ts-expect-error GestureHandling plugin has no type definitions
  map.gestureHandling.enable();

  const layerGroup = L.control.layers({}, {});
  layerGroup.addTo(map);

  addFullScreenButton(map);
  addPrintMapButton(map);
  rememberSelectedBaseLayers(map, layerGroup, cacheName, tileLayers);
  rememberSelectedOverlays(map);

  Object.entries(tileLayers.overlays).forEach(([label, getLayer]) => {
    const layer = getLayer(false);
    layerGroup.addOverlay(layer, label);
    if (isOverlayDefault(label, true)) layer.addTo(map);
  });

  return [map, layerGroup];
}
