import type { RA, RR } from '../config';
import L from '../leaflet';
import { isOverlayDefault } from '../leafletUtils';
import commonText from '../localization/common';
import frontEndText from '../localization/frontend';
import { splitJoinedMappingPath } from '../utils';
import { mappingLocalityColumns } from './config';
import type { Field, LocalityData } from './occurrence';

const markerLayerName = [
  'marker',
  'polygon',
  'polygonBoundary',
  'errorRadius',
] as const;

export type MarkerLayerName = typeof markerLayerName[number];

const defaultMarkerGroupsState: RR<MarkerLayerName, boolean> = {
  marker: true,
  polygon: true,
  polygonBoundary: false,
  errorRadius: false,
};

export function addMarkersToMap(
  map: L.Map,
  controlLayers: L.Control.Layers,
  markers: RA<MarkerGroups>
): void {
  if (markers.length === 0) return;

  // Initialize layer groups
  const cluster = L.markerClusterGroup({
    iconCreateFunction(cluster) {
      const childCount = cluster.getChildCount();

      const minHue = 10;
      const maxHue = 90;
      const maxValue = 200;
      const hue = Math.max(
        0,
        Math.round((childCount / maxValue) * (minHue - maxHue) + maxHue)
      );

      const iconObject = new L.DivIcon({
        html: `<div
          style="background-color: hsl(${hue}deg, 50%, 50%, 0.7)"
        ><span>${childCount}</span></div>`,
        className: `marker-cluster marker-cluster-${
          childCount < 10 ? 'small' : childCount < 100 ? 'medium' : 'large'
        }`,
        iconSize: new L.Point(40, 40),
      });

      const iconElement = iconObject.createIcon();
      iconElement.classList.add('test');

      return iconObject;
    },
  });
  cluster.addTo(map);

  const layerGroups = Object.fromEntries(
    markerLayerName.map(
      (groupName) =>
        [groupName, L.featureGroup.subGroup(cluster)] as [
          MarkerLayerName,
          L.FeatureGroup
        ]
    )
  ) as RR<MarkerLayerName, L.FeatureGroup>;

  const groupsWithMarkers = new Set<string>();

  // Sort markers by layer groups
  markers.forEach((markers) =>
    Object.entries(markers).forEach(([markerGroupName, markers]) =>
      (markers as Marker[]).forEach((marker) => {
        layerGroups[markerGroupName as MarkerLayerName].addLayer(marker);
        groupsWithMarkers.add(markerGroupName);
      })
    )
  );

  const layerLabels: RR<MarkerLayerName, string> = {
    marker: commonText('markerLayerLabel'),
    polygon: commonText('polygonLayerLabel'),
    polygonBoundary: commonText('polygonBoundaryLayerLabel'),
    errorRadius: commonText('errorRadiusLayerLabel'),
  };

  // Add layer groups' checkboxes to the layer control menu
  Object.entries(layerLabels)
    .filter(([key]) => groupsWithMarkers.has(key))
    .forEach(([key, label]) => {
      const layer = layerGroups[key as MarkerLayerName];
      controlLayers.addOverlay(layer, label);
      if (
        isOverlayDefault(key, defaultMarkerGroupsState[key as MarkerLayerName])
      )
        layer.addTo(map);
    });
}

export function isValidAccuracy(
  latlongaccuracy: string | undefined
): latlongaccuracy is string {
  try {
    if (
      typeof latlongaccuracy === 'undefined' ||
      Number.isNaN(Number.parseFloat(latlongaccuracy)) ||
      Number.parseFloat(latlongaccuracy) < 1
    )
      return false;
  } catch {
    return false;
  }
  return true;
}

export type MarkerGroups = {
  readonly marker: L.Marker[];
  readonly polygon: (L.Polygon | L.Polyline)[];
  readonly polygonBoundary: L.Marker[];
  readonly errorRadius: L.Circle[];
};
type Marker = L.Marker | L.Polygon | L.Polyline | L.Circle;

const createLine = (
  coordinate1: [number, number],
  coordinate2: [number, number]
): L.Polyline =>
  new L.Polyline([coordinate1, coordinate2], {
    weight: 3,
    opacity: 0.5,
    smoothFactor: 1,
  });

export const formatLocalityData = (
  localityData: LocalityData,
  viewUrl: string,
  isLoading: boolean
): string =>
  [
    ...Object.entries(localityData)
      .filter(
        ([fieldName]) =>
          isLoading || !mappingLocalityColumns.includes(fieldName)
      )
      .filter(
        (entry): entry is [string, Field<string | number>] =>
          typeof entry[1] !== 'undefined' && entry[1].value !== ''
      )
      .map(([fieldName, field]) =>
        splitJoinedMappingPath(fieldName).includes('taxon')
          ? `<b>${field.value}</b>`
          : `<b>${field.headerName}</b>: ${field.value}`
      ),
    isLoading ? frontEndText('fetchingInformation') : '',
    `<a
        href="${viewUrl}"
        target="_blank"
      >${frontEndText('viewRecord')}</a>`,
  ]
    .filter((entry) => entry)
    .join('<br>');

export function getMarkersFromLocalityData({
  localityData,
  markerClickCallback,
  iconClass,
  viewUrl,
}: {
  readonly localityData: LocalityData;
  readonly markerClickCallback?: string | L.LeafletEventHandlerFn;
  readonly iconClass?: string;
  readonly viewUrl: string;
}): MarkerGroups {
  const markers: MarkerGroups = {
    marker: [],
    polygon: [],
    polygonBoundary: [],
    errorRadius: [],
  };

  const getNumber = (fieldName: string): number | undefined =>
    typeof localityData[fieldName]?.value === 'number'
      ? (localityData[fieldName].value as number)
      : undefined;

  const getString = (fieldName: string): string | undefined =>
    typeof localityData[fieldName]?.value === 'string'
      ? (localityData[fieldName].value as string)
      : undefined;

  const parsedLocalityData = {
    latitude1: getNumber('locality.latitude1'),
    latitude2: getNumber('locality.latitude2'),
    longitude1: getNumber('locality.longitude1'),
    longitude2: getNumber('locality.longitude2'),
    latlongaccuracy: getString('locality.latlongaccuracy'),
    latlongtype: getString('locality.latlongtype'),
  };

  if (
    typeof parsedLocalityData.latitude1 === 'undefined' ||
    typeof parsedLocalityData.longitude1 === 'undefined'
  )
    return markers;

  const icon = new L.Icon.Default();
  if (typeof iconClass !== 'undefined') icon.options.className = iconClass;

  const createPoint = (latitude1: number, longitude1: number): L.Marker =>
    L.marker([latitude1, longitude1], {
      icon,
    });

  if (
    typeof parsedLocalityData.latitude2 === 'undefined' ||
    typeof parsedLocalityData.longitude2 === 'undefined'
  ) {
    // A circle
    if (isValidAccuracy(parsedLocalityData.latlongaccuracy))
      markers.errorRadius.push(
        L.circle(
          [parsedLocalityData.latitude1, parsedLocalityData.longitude1],
          {
            radius: Number.parseFloat(parsedLocalityData.latlongaccuracy),
          }
        )
      );

    // A point
    markers.marker.push(
      createPoint(parsedLocalityData.latitude1, parsedLocalityData.longitude1)
    );
  } else {
    markers.polygon.push(
      parsedLocalityData.latlongtype?.toLowerCase() === 'line'
        ? createLine(
            [parsedLocalityData.latitude1, parsedLocalityData.longitude1],
            [parsedLocalityData.latitude2, parsedLocalityData.longitude2]
          )
        : L.polygon([
            [parsedLocalityData.latitude1, parsedLocalityData.longitude1],
            [parsedLocalityData.latitude2, parsedLocalityData.longitude1],
            [parsedLocalityData.latitude2, parsedLocalityData.longitude2],
            [parsedLocalityData.latitude1, parsedLocalityData.longitude2],
          ])
    );
    markers.polygonBoundary.push(
      createPoint(parsedLocalityData.latitude1, parsedLocalityData.longitude1),
      createPoint(parsedLocalityData.latitude1, parsedLocalityData.longitude2)
    );
  }

  Object.values(markers)
    .flat(2)
    .forEach((vector) => {
      if (typeof markerClickCallback === 'function')
        vector.on('click', markerClickCallback);
      vector.bindPopup(formatLocalityData(localityData, viewUrl, true));
    });

  return markers;
}
