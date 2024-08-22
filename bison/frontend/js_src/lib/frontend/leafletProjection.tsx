import React from 'react';

import type { Component, IR, R, RA } from '../config';
import { isOverlayDefault } from '../leafletUtils';
import type { LifemapperLayerTypes } from './config';
import { lifemapperLayerVariations, maxProjectionLayers } from './config';
import type { LeafletOverlays } from './occurrence';
import { validateBrokerResponse } from './utils';

export function useProjectionLayers(
  scientificName: string | undefined
): Readonly<[LeafletOverlays, RA<Component>]> {
  const [response, setResponse] = React.useState<
    | undefined
    | {
        readonly errors: IR<unknown>;
        readonly records: [
          {
            readonly errors: IR<unknown>;
            readonly records: {
              readonly 's2n:endpoint': string;
              readonly 's2n:modtime': string;
              // eslint-disable-next-line @typescript-eslint/naming-convention
              readonly 's2n:layer_name': string;
              // eslint-disable-next-line @typescript-eslint/naming-convention
              readonly 's2n:layer_type': LifemapperLayerTypes;
              readonly 's2n:sdm_projection_scenario_code'?: string;
            }[];
          }
        ];
      }
  >(undefined);

  React.useEffect(() => {
    if (typeof scientificName === 'undefined') return;
    fetch(
      `/api/v1/map/?namestr=${scientificName}&scenariocode=worldclim-curr&provider=lm`
    )
      .then(async (response) => response.json())
      .then(setResponse)
      .catch(console.error);
  }, [scientificName]);

  if (
    typeof response === 'undefined' ||
    !validateBrokerResponse(response) ||
    !validateBrokerResponse(response.records[0])
  )
    return [{}, []];

  const layerCounts: R<number> = {};
  let lifemapperLayers: LeafletOverlays = {};
  try {
    lifemapperLayers = Object.fromEntries(
      response.records[0].records
        .filter(
          (record) =>
            record['s2n:sdm_projection_scenario_code'] === 'worldclim-curr' &&
            record['s2n:layer_type'] === 'raster'
        )
        .map<LeafletOverlays>((record) => {
          const layerType = record['s2n:layer_type'];
          layerCounts[layerType] ??= 0;
          layerCounts[layerType] += 1;

          if (layerCounts[layerType] > maxProjectionLayers) return {};

          const showLayerNumber =
            response.records[0].records.filter(
              (record) => record['s2n:layer_type'] === layerType
            ).length !== 1;

          const isFirstOfType = layerCounts[layerType] === 1;
          const label = `${lifemapperLayerVariations[layerType].layerLabel}${
            showLayerNumber ? ` (${layerCounts[layerType]})` : ''
          }`;
          return {
            [label]: {
              isDefault: isOverlayDefault(
                `layerType${isFirstOfType ? '_first' : ''}`,
                isFirstOfType
              ),
              endpoint: record['s2n:endpoint'],
              serverType: 'wms',
              layerOptions: {
                layers: record['s2n:layer_name'],
                opacity: 0.7,
                transparent: true,
                service: 'wms',
                version: '1.0',
                height: '400',
                format: 'image/png',
                request: 'getmap',
                srs: 'epsg:3857',
                width: '800',
              },
            },
          };
        })
        .flatMap(Object.entries)
    );

    const modificationTime = response.records[0].records[0]['s2n:modtime'];
    const dateObject = new Date(modificationTime);
    return [
      lifemapperLayers,
      [
        <>
          It also displays a predicted distribution model from Lifemapper. Model
          computed with default Maxent parameters:{' '}
          <time dateTime={dateObject.toISOString()}>
            {dateObject.toDateString()}
          </time>
        </>,
      ],
    ];
  } catch {
    console.error(
      'Failed to display Lifemapper projection map for this species'
    );
    return [{}, []];
  }
}
