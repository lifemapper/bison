import type { RA, RR } from '../config';
import frontEndText from '../localization/frontend';

export const VERSION = '1.0.0';

export const OCC_PROVIDERS = ['specify', 'gbif', 'idb', 'mopho'];
export const NAME_PROVIDERS = ['gbif', 'itis', 'worms'];
export const MIN_COLUMNS = Math.min(
  OCC_PROVIDERS.length,
  NAME_PROVIDERS.length
);

export type LifemapperLayerTypes = 'raster';

export const layersResolveTimeout = 250;
export const maxProjectionLayers = 10;

export const lifemapperLayerVariations: RR<
  LifemapperLayerTypes,
  { layerLabel: string; transparent: boolean; opacity?: number }
> = {
  raster: {
    layerLabel: frontEndText('projectionLayerLabel'),
    transparent: true,
  },
};

// These fields should be present for locality to be mappable
export const requiredLocalityColumns: RA<string> = [
  'locality.latitude1',
  'locality.longitude1',
] as const;

export const mappingLocalityColumns: RA<string> = [
  ...requiredLocalityColumns,
  'locality.latitude2',
  'locality.longitude2',
  'locality.latlongtype',
  'locality.latlongaccuracy',
];
