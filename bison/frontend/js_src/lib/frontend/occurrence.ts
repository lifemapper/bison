import type { Action } from 'typesafe-reducer';

import type { IR, leafletTileServers } from '../config';
import L from '../leaflet';

export type LeafletOverlays = IR<{
  readonly endpoint: string;
  readonly serverType: 'wms' | 'tileServer';
  readonly layerOptions: IR<unknown>;
  readonly isDefault: boolean;
}>;

export type JsonLeafletLayers = IR<
  IR<Omit<LeafletOverlays[string], 'isDefault'>>
>;

export const parseLayerFromJson =
  (
    {
      endpoint,
      serverType,
      layerOptions,
    }: Omit<LeafletOverlays[string], 'isDefault'>,
    grayscale: boolean
  ): (() => L.TileLayer | L.TileLayer.WMS) =>
  () =>
    (serverType === 'wms' ? L.tileLayer.wms : L.tileLayer)(endpoint, {
      className: grayscale ? 'grayscale' : '',
      ...layerOptions,
    });

export const parseLayersFromJson = (json: JsonLeafletLayers) =>
  Object.fromEntries(
    Object.entries(json).map(([layerGroup, layers]) => [
      layerGroup,
      Object.fromEntries(
        Object.entries(layers).map(([layerName, options]) => [
          layerName,
          parseLayerFromJson(
            options,
            layerGroup === 'baseMaps' && layerName.startsWith('Satellite')
          ),
        ])
      ),
    ])
  ) as typeof leafletTileServers;

type LoadedAction = Action<'LoadedAction', { version: string }>;

type GetPinInfoAction = Action<'GetPinInfoAction', { index: number }>;

export type OutgoingMessage = LoadedAction | GetPinInfoAction;

export type Field<T> = { readonly headerName: string; readonly value: T };

export type LocalityData = IR<Field<string | number>>;

export type OccurrenceData = {
  readonly collectionObjectId: number;
  readonly collectingEventId: number;
  readonly localityId: number;
  readonly localityData: LocalityData;
};
