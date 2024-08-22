import type { Action } from 'typesafe-reducer';
import { generateReducer } from 'typesafe-reducer';

import type { IR, leafletTileServers, RA, RR } from '../config';
import type { States } from './leafletState';
import type { LocalityData, OccurrenceData } from './occurrence';
import { parseLayersFromJson } from './occurrence';

type BasicInformationAction = Action<
  'BasicInformationAction',
  {
    systemInfo: IR<unknown>;
    leafletLayers: RR<
      'baseMaps' | 'overlays',
      IR<{
        endpoint: string;
        serverType: 'tileServer' | 'wms';
        layerOptions: IR<unknown>;
      }>
    >;
  }
>;

type ResolveLeafletLayersAction = Action<
  'ResolveLeafletLayersAction',
  {
    tileLayers: typeof leafletTileServers;
  }
>;

type LocalOccurrencesAction = Action<
  'LocalOccurrencesAction',
  {
    occurrences: RA<OccurrenceData>;
  }
>;

type PointDataAction = Action<
  'PointDataAction',
  {
    index: number;
    localityData: LocalityData;
  }
>;

export type IncomingMessage =
  | BasicInformationAction
  | ResolveLeafletLayersAction
  | LocalOccurrencesAction
  | PointDataAction;

export type Actions = IncomingMessage;

export const reducer = generateReducer<States, Actions>({
  BasicInformationAction: ({ action: { leafletLayers }, state }) => ({
    ...state,
    tileLayers: parseLayersFromJson(leafletLayers),
  }),
  ResolveLeafletLayersAction: ({ action: { tileLayers }, state }) => ({
    ...state,
    tileLayers: state.tileLayers ?? tileLayers,
  }),
  LocalOccurrencesAction: ({ action: { occurrences }, state }) => ({
    ...state,
    occurrencePoints: occurrences,
  }),
  PointDataAction: ({ action: { localityData, index }, state }) => ({
    ...state,
    extendedOccurrencePoints: {
      ...state.extendedOccurrencePoints,
      [index]: localityData,
    },
  }),
});
