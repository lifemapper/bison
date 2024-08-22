import L from './leaflet';

// Record
export type R<V> = Record<string, V>;
// Immutable record
export type IR<V> = Readonly<Record<string, V>>;
// Immutable record of any type
export type RR<K extends string | number | symbol, V> = Readonly<Record<K, V>>;
// Immutable Array
export type RA<V> = readonly V[];
// JSX Element
export type Component = Readonly<JSX.Element>;

export const DEFAULT_CENTER: [number, number] = [0, 0];
export const DEFAULT_ZOOM = 2;

export let leafletTileServers: RR<
  'baseMaps' | 'overlays',
  IR<(grayscale?: boolean) => L.TileLayer>
> = {
  baseMaps: {
    'Satellite Map (ESRI)': (grayscale = true) =>
      L.tileLayer(
        'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
        {
          className: grayscale ? 'grayscale' : '',
          attribution:
            'Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community',
        }
      ),
  },
  overlays: {
    'Labels and boundaries': () =>
      L.tileLayer(
        'https://server.arcgisonline.com/ArcGIS/rest/services/Reference/World_Reference_Overlay/MapServer/tile/{z}/{y}/{x}',
        {
          attribution:
            'Esri, HERE, Garmin, (c) OpenStreetMap contributors, and the GIS user community',
          className: 'darkened',
        }
      ),
  },
};

export function updateLeafletTileServers(
  tileServers: typeof leafletTileServers
): void {
  leafletTileServers = tileServers;
}

export const preferredBaseLayer = 'Satellite Map (ESRI)';
