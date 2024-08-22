import React from 'react';

import { leafletTileServers } from '../config';
import type L from '../leaflet';
import { showMap } from '../leafletUtils';

export function useLeaflet({
  mapContainer,
  tileLayers = leafletTileServers,
}: {
  mapContainer: HTMLDivElement | null;
  tileLayers?: typeof leafletTileServers;
}): Readonly<[L.Map | undefined, L.Control.Layers | undefined]> {
  const [leafletMap, setLeafletMap] = React.useState<L.Map | undefined>(
    undefined
  );
  const [layerGroup, setLayerGroup] = React.useState<
    L.Control.Layers | undefined
  >(undefined);

  React.useEffect(() => {
    if (mapContainer === null) return undefined;

    const [leafletMap, layerGroup] = showMap(
      mapContainer,
      'occurrence',
      tileLayers
    );
    setLeafletMap(leafletMap);
    setLayerGroup(layerGroup);

    return (): void => {
      // @ts-expect-error GestureHandling has no type definitions
      leafletMap.gestureHandling.disable();
      leafletMap.off();
      leafletMap.remove();
    };
  }, [mapContainer]);

  return [leafletMap, layerGroup];
}
