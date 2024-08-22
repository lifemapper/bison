import type { State } from 'typesafe-reducer';

import type { leafletTileServers, RA, RR } from '../config';
import type { LocalityData, OccurrenceData } from './occurrence';

export type MainState = State<
  'MainState',
  {
    tileLayers: undefined | typeof leafletTileServers;
    occurrencePoints: undefined | RA<OccurrenceData>;
    extendedOccurrencePoints: RR<number, LocalityData>;
  }
>;

export type States = MainState;
