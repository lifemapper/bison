import '../../css/frontend.css';

import React from 'react';

import { app } from '../components/main';
import type { IR, RA } from '../config';
import commonText from '../localization/common';
import { getQueryParameter } from '../utils';
import { reducer } from './reducer';
import { stateReducer } from './state';
import { extractField, fetchName, fetchOccurrence } from './utils';

export type BrokerProvider = {
  readonly code: string;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  readonly icon_url: string;
  readonly label: string;
};

export type RawBrokerResponse = {
  readonly errors: IR<unknown>;
  readonly service: string;
  readonly provider: BrokerProvider;
  readonly records: RA<{
    readonly errors: IR<unknown>;
    readonly provider: BrokerProvider;
    readonly records: RA<IR<unknown>>;
  }>;
};

export type BrokerRecord = {
  provider: BrokerProvider;
  service: string;
  record: IR<unknown>;
};

app(function FrontEnd() {
  const [state, dispatch] = React.useReducer(reducer, {
    type: 'MainState',
    occurrence: 'loading',
    name: 'loading',
  });

  React.useEffect(() => {
    const occId = getQueryParameter('occid', (occId) => occId.length > 0);
    const nameString = getQueryParameter(
      'namestr',
      (nameString) => nameString.length > 0
    );

    if (!occId && !nameString) {
      dispatch({
        type: 'ErrorAction',
        title: commonText('invalidRequestErrorTitle'),
        message: commonText('invalidRequestErrorMessage'),
      });
      return;
    }

    fetchOccurrence(occId).then(dispatch).catch(console.error);
    fetchName(nameString, 'invalid').then(dispatch).catch(console.error);
  }, []);

  const occurrence = state.type === 'MainState' ? state.occurrence : undefined;
  const name = state.type === 'MainState' ? state.name : undefined;

  React.useEffect(() => {
    if (name !== 'invalid') return;
    if (typeof occurrence === 'undefined')
      dispatch({
        type: 'LoadedNameAction',
        name: undefined,
      });
    else {
      const nameString = extractField(occurrence, 'gbif', 'dwc:scientificName');
      if (typeof nameString === 'string')
        fetchName(nameString, undefined).then(dispatch).catch(console.error);
    }
  }, [occurrence, name]);

  return stateReducer(undefined, state) ?? <i />;
});
