import * as React from 'react';

import { useLeaflet } from '../components/leaflet';
import type { Component, IR } from '../config';
import { leafletTileServers } from '../config';
import { throttle } from '../helper';
import L from '../leaflet';
import statsText from '../localization/stats';
import { defaultSliderPosition, yearRangeThrottleRate } from './config';
import type { GbifMapData } from './utils';

export function GbifMap({
  mapOptions,
  value,
  getMapData,
}: {
  mapOptions: IR<unknown>;
  value: string | undefined;
  getMapData: (value: string) => Promise<GbifMapData>;
}): Component | null {
  const mapContainer = React.useRef<HTMLDivElement | null>(null);
  const [leafletMap] = useLeaflet({
    mapContainer: mapContainer.current,
  });

  const [mapData, setMapData] = React.useState<
    GbifMapData | 'error' | 'loading' | undefined
  >(undefined);

  const [minValue, setMinValue] = React.useState<number | undefined>(undefined);
  const [maxValue, setMaxValue] = React.useState<number | undefined>(undefined);
  const hasYears =
    typeof minValue !== 'undefined' && typeof maxValue !== 'undefined';

  React.useEffect(() => {
    if (value)
      getMapData(value)
        .then((mapData) => {
          setMapData(mapData);
          if (
            typeof minValue === 'undefined' ||
            typeof maxValue === 'undefined'
          ) {
            setMinValue(
              Math.round(
                (mapData.minYear ?? 0) +
                  ((mapData.maxYear ?? 0) - (mapData.minYear ?? 0)) *
                    defaultSliderPosition
              )
            );
            setMaxValue(mapData.maxYear);
          }
        })
        .catch((error) => {
          console.error(error);
          setMapData('error');
        });
  }, [value]);

  const overlay = React.useRef<L.Layer | undefined>(undefined);
  React.useEffect(() => {
    if (typeof leafletMap === 'undefined') return;
    if (overlay.current) leafletMap.removeLayer(overlay.current);
    overlay.current = L.tileLayer(
      'https://api.gbif.org/v2/map/occurrence/{source}/{z}/{x}/{y}{format}?{params}',
      {
        attribution: '',
        source: 'density',
        format: '@1x.png',
        params: Object.entries({
          srs: 'EPSG:3857',
          style: 'classic.poly',
          bin: 'hex',
          ...(hasYears ? { year: `${minValue ?? ''},${maxValue ?? ''}` } : {}),
          ...mapOptions,
        })
          .map(([key, value]) => `${key}=${value}`)
          .join('&'),
      }
    );
    overlay.current.addTo(leafletMap);
    const labelsLayer = Object.values(leafletTileServers.overlays)[0]();
    labelsLayer.bringToFront();
  }, [leafletMap, minValue, maxValue, mapData]);

  return typeof mapData === 'object' || mapData === 'loading' ? (
    <>
      <YearSlider
        minYear={typeof mapData === 'object' ? mapData.minYear : undefined}
        maxYear={typeof mapData === 'object' ? mapData.maxYear : undefined}
        minValue={minValue ?? 0}
        maxValue={maxValue ?? new Date().getFullYear()}
        onMinValueChange={setMinValue}
        onMaxValueChange={setMaxValue}
      />
      <div className="leaflet-map-container">
        <div className="leaflet-map" ref={mapContainer} />
      </div>
    </>
  ) : mapData === 'error' ? (
    <p className="alert alert-error" role="alert">
      {statsText('noMapForInstitution')}
    </p>
  ) : null;
}

export function YearSlider({
  minYear,
  maxYear,
  minValue,
  maxValue,
  onMinValueChange,
  onMaxValueChange,
}: {
  minYear: number | undefined;
  maxYear: number | undefined;
  minValue: number;
  maxValue: number;
  onMinValueChange: (min: number) => void;
  onMaxValueChange: (max: number) => void;
}): Component {
  const handleMinValueChange = React.useCallback(
    throttle(onMinValueChange, yearRangeThrottleRate),
    []
  );
  const handleMaxValueChange = React.useCallback(
    throttle(onMaxValueChange, yearRangeThrottleRate),
    []
  );

  function handleMinChange({
    target,
  }: React.ChangeEvent<HTMLInputElement>): void {
    const minValue = Number.parseInt(target.value);
    handleMinValueChange(Math.min(minValue, maxValue));
    handleMaxValueChange(Math.max(minValue, maxValue));
  }

  function handleMaxChange({
    target,
  }: React.ChangeEvent<HTMLInputElement>): void {
    const maxValue = Number.parseInt(target.value);
    handleMinValueChange(Math.min(minValue, maxValue));
    handleMaxValueChange(Math.max(minValue, maxValue));
  }

  const isDisabled =
    typeof minYear === 'undefined' || typeof maxYear === 'undefined';

  return (
    <p>
      {isDisabled
        ? statsText('noYearRange')
        : statsText('yearRangeDescription')}
      <br />
      <span className={`slider ${isDisabled ? 'darkened' : ''}`}>
        <input
          type="number"
          className="min"
          aria-label={statsText('startYear')}
          placeholder={statsText('startYear')}
          min={minYear}
          value={minValue}
          max={maxYear}
          disabled={isDisabled}
          onChange={handleMinChange}
        />
        <span className="slider-inputs" aria-hidden="true">
          <input
            type="range"
            className="max"
            min={minYear}
            value={maxValue}
            max={maxYear}
            disabled={isDisabled}
            onChange={handleMaxChange}
          />
          <input
            type="range"
            className="min"
            min={minYear}
            value={minValue}
            max={maxYear}
            disabled={isDisabled}
            onChange={handleMinChange}
          />
        </span>
        <input
          type="number"
          className="max"
          aria-label={statsText('endYear')}
          placeholder={statsText('endYear')}
          min={minYear}
          value={maxValue}
          max={maxYear}
          disabled={isDisabled}
          onChange={handleMaxChange}
        />
      </span>
    </p>
  );
}
