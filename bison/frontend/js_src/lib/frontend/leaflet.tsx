import React from 'react';

import { Section } from '../components/common';
import { useLeaflet } from '../components/leaflet';
import type { Component, RA, RR } from '../config';
import { leafletTileServers } from '../config';
import { addAggregatorOverlays } from '../leafletUtils';
import frontEndText from '../localization/frontend';
import { getQueryParameter } from '../utils';
import { layersResolveTimeout, VERSION } from './config';
import type { BrokerRecord } from './entry';
import type { MarkerGroups } from './leafletOccurrence';
import {
  addMarkersToMap,
  formatLocalityData,
  getMarkersFromLocalityData,
} from './leafletOccurrence';
import { useGbifLayers, useIdbLayers } from './leafletOverlays';
import { useProjectionLayers } from './leafletProjection';
import type { IncomingMessage } from './leafletReducer';
import { reducer } from './leafletReducer';
import type {
  LeafletOverlays,
  LocalityData,
  OccurrenceData,
  OutgoingMessage,
} from './occurrence';

export function LeafletContainer({
  occurrence,
  name,
  scientificName,
}: {
  occurrence: RA<BrokerRecord> | undefined;
  name: RA<BrokerRecord> | string | undefined;
  scientificName: string | undefined;
}): Component | null {
  const [state, dispatch] = React.useReducer(reducer, {
    type: 'MainState',
    tileLayers: undefined,
    occurrencePoints: undefined,
    extendedOccurrencePoints: {},
  });

  const origin = getQueryParameter('origin', (origin) =>
    origin.startsWith('http')
  );

  const sendMessage = (action: OutgoingMessage): void =>
    origin ? window.opener?.postMessage(action, origin) : undefined;

  React.useEffect(() => {
    const resolveTileLayers = (): void =>
      dispatch({
        type: 'ResolveLeafletLayersAction',
        tileLayers: leafletTileServers,
      });
    if (!origin || !window.opener) {
      resolveTileLayers();
      return undefined;
    }
    setTimeout(resolveTileLayers, layersResolveTimeout);

    sendMessage({ type: 'LoadedAction', version: VERSION });
    const eventHandler = (event: MessageEvent<IncomingMessage>) => {
      if (
        event.source !== window.opener ||
        event.origin !== origin ||
        typeof event.data?.type !== 'string'
      )
        return;
      dispatch(event.data);
    };
    window.addEventListener('message', eventHandler);

    return (): void => window.removeEventListener('message', eventHandler);
  }, []);

  const [projectionLayers, projectionDetails] =
    useProjectionLayers(scientificName);
  const [gbifLayers, gbifDetails] = useGbifLayers(
    Array.isArray(name) ? name : undefined
  );
  const [idbLayers, idbDetails] = useIdbLayers(occurrence, scientificName);

  const overlays = {
    ...projectionLayers,
    ...gbifLayers,
    ...idbLayers,
  };
  const layerDetails = [
    frontEndText('mapDescription'),
    ...projectionDetails,
    ...gbifDetails,
    ...idbDetails,
  ];

  return typeof state.tileLayers === 'undefined' ||
    (Object.keys(overlays).length === 0 &&
      (typeof state.occurrencePoints === 'undefined' ||
        // eslint-disable-next-line unicorn/no-null
        state.occurrencePoints.length === 0)) ? null : (
    <Section key="map" anchor="map" label={frontEndText('distribution')}>
      {layerDetails.map((value, index) => (
        <p key={index}>{value}</p>
      ))}
      <LeafletMap
        tileLayers={state.tileLayers}
        occurrencePoints={state.occurrencePoints}
        extendedOccurrencePoints={state.extendedOccurrencePoints}
        overlays={overlays}
        origin={origin}
        onMarkerClick={(index): void =>
          sendMessage({
            type: 'GetPinInfoAction',
            index,
          })
        }
      />
    </Section>
  );
}

export function LeafletMap({
  tileLayers,
  occurrencePoints,
  extendedOccurrencePoints,
  overlays,
  onMarkerClick: handleMarkerClick,
  origin,
}: {
  tileLayers: typeof leafletTileServers | undefined;
  occurrencePoints: RA<OccurrenceData> | undefined;
  extendedOccurrencePoints: RR<number, LocalityData>;
  overlays: LeafletOverlays;
  onMarkerClick: (index: number) => void;
  origin: string | undefined;
}) {
  const mapContainer = React.useRef<HTMLDivElement | null>(null);
  const [leafletMap, layerGroup] = useLeaflet({
    mapContainer: mapContainer.current,
    tileLayers,
  });

  const loadedOverlays = React.useRef<Set<string>>(new Set());
  React.useEffect(() => {
    if (typeof leafletMap === 'undefined' || typeof layerGroup === 'undefined')
      return;
    const addedOverlays = Object.keys(overlays).filter(
      (label) => !loadedOverlays.current.has(label)
    );
    const addLayers = addAggregatorOverlays(leafletMap, layerGroup);
    addLayers(
      Object.fromEntries(addedOverlays.map((label) => [label, overlays[label]]))
    );
    addedOverlays.forEach((label) => {
      loadedOverlays.current.add(label);
    });
  }, [overlays]);

  const markers = React.useRef<RA<MarkerGroups>>([]);
  const fetchedMarkers = React.useRef<Set<number>>(new Set());
  React.useEffect(() => {
    if (
      typeof occurrencePoints === 'undefined' ||
      typeof leafletMap === 'undefined' ||
      typeof layerGroup === 'undefined' ||
      typeof origin === 'undefined'
    )
      return;

    markers.current = occurrencePoints.map(
      ({ localityData, collectionObjectId }, index) =>
        getMarkersFromLocalityData({
          localityData,
          markerClickCallback() {
            if (fetchedMarkers.current.has(index)) return;
            handleMarkerClick(index);
          },
          viewUrl: `${origin}/specify/view/collectionobject/${collectionObjectId}/`,
        })
    );
    addMarkersToMap(leafletMap, layerGroup, markers.current);
  }, [occurrencePoints]);

  const loadedExtendedOccurrencePoints = React.useRef<Set<number>>(new Set());
  React.useEffect(() => {
    if (
      typeof markers === 'undefined' ||
      typeof occurrencePoints === 'undefined' ||
      typeof origin === 'undefined'
    )
      return;
    const newOccurrencePoints = Object.keys(extendedOccurrencePoints).filter(
      (index) =>
        !loadedExtendedOccurrencePoints.current.has(Number.parseInt(index))
    );
    newOccurrencePoints.forEach((rawIndex) => {
      const index = Number.parseInt(rawIndex);
      loadedExtendedOccurrencePoints.current.add(index);
      const localityData = extendedOccurrencePoints[index];
      const formattedLocality = formatLocalityData(
        localityData,
        `${origin}/specify/view/collectionobject/${occurrencePoints[index].collectionObjectId}/`,
        false
      );
      Object.values(markers.current[index])
        .flat()
        .map((marker) => marker.getPopup()?.setContent(formattedLocality));
    });
  }, [extendedOccurrencePoints]);

  const availableOverlays = Object.keys(overlays);
  React.useEffect(() => {}, [availableOverlays]);

  return (
    <div className="leaflet-map-container">
      <div className="leaflet-map" ref={mapContainer} />
    </div>
  );
}
