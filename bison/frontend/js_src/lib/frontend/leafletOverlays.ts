import React from 'react';

import type { RA } from '../config';
import { isOverlayDefault, legendGradient, legendPoint } from '../leafletUtils';
import frontEndText from '../localization/frontend';
import type { BrokerRecord } from './entry';
import type { LeafletOverlays } from './occurrence';
import { extractField } from './utils';

async function getIdbLayer(
  scientificName: string,
  collectionCode: string | undefined,
  layerName: string,
  className?: string
): Promise<LeafletOverlays> {
  let request: Response | undefined;
  try {
    request = await fetch('https://search.idigbio.org/v2/mapping/', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
      },
      body: JSON.stringify({
        rq: {
          scientificname: scientificName,
          ...(typeof collectionCode === 'string'
            ? {
                collectioncode: collectionCode,
              }
            : {}),
        },
        type: 'auto',
        threshold: 100_000,
      }),
    });
  } catch {
    return {};
  }
  const response: {
    readonly itemCount: number;
    readonly tiles: string;
  } = await request.json();

  if (response.itemCount === 0) return {};

  return {
    [layerName]: {
      endpoint: response.tiles,
      serverType: 'tileServer',
      layerOptions: {
        attribution: 'iDigBio and the user community',
        className: className ?? 'hyper-saturated',
      },
      isDefault: isOverlayDefault(layerName, true),
    },
  };
}

export function useIdbLayers(
  occurrence: RA<BrokerRecord> | undefined,
  scientificName: string | undefined
): Readonly<[LeafletOverlays, RA<string>]> {
  const idbScientificName =
    extractField(occurrence, 'idb', 'dwc:scientificName') ?? scientificName;
  const collectionCode = extractField(occurrence, 'idb', 'dwc:collectionCode');

  const [layers, setLayers] = React.useState<LeafletOverlays | undefined>(
    undefined
  );

  React.useEffect(() => {
    if (typeof idbScientificName === 'undefined') return;

    Promise.all([
      getIdbLayer(
        idbScientificName,
        undefined,
        `iDigBio ${legendPoint('#197')}`
      ),
      getIdbLayer(
        idbScientificName,
        collectionCode,
        `iDigBio (${collectionCode ?? 'collection'} points only) ${legendPoint(
          '#e68'
        )}`,
        'hue-rotate'
      ),
    ])
      .then((layers) => layers.flatMap(Object.entries))
      .then(Object.fromEntries)
      .then(setLayers)
      .catch(console.error);
  }, [idbScientificName, collectionCode]);

  if (typeof layers === 'undefined') return [{}, []];

  return [layers, [frontEndText('iDigBioDescription')]];
}

export function useGbifLayers(
  name: RA<BrokerRecord> | undefined
): Readonly<[LeafletOverlays, RA<string>]> {
  const taxonKey =
    typeof name === 'undefined'
      ? undefined
      : extractField(name, 'gbif', 's2n:gbif_taxon_key');
  if (typeof taxonKey === 'undefined') return [{}, []];

  return [
    {
      [`GBIF ${legendGradient('#ee0', '#d11')}`]: {
        endpoint:
          'https://api.gbif.org/v2/map/occurrence/{source}/{z}/{x}/{y}{format}?{params}',
        serverType: 'tileServer',
        layerOptions: {
          attribution: '',
          source: 'density',
          format: '@1x.png',
          className: 'saturated',
          params: Object.entries({
            srs: 'EPSG:3857',
            style: 'classic.poly',
            bin: 'hex',
            hexPerTile: 20,
            taxonKey,
          })
            .map(([key, value]) => `${key}=${value}`)
            .join('&'),
        },
        isDefault: isOverlayDefault('gbif', true),
      },
    },
    [frontEndText('gbifDescription')],
  ];
}
