import type { Action } from 'typesafe-reducer';
import { generateReducer } from 'typesafe-reducer';

import type { Component, RA } from '../config';
import type { BrokerRecord } from './entry';
import type { States } from './state';
import { ensureMainState } from './state';

export type LoadedOccurrenceAction = Action<
  'LoadedOccurrenceAction',
  {
    occurrence: RA<BrokerRecord> | undefined;
  }
>;

export type LoadedNameAction = Action<
  'LoadedNameAction',
  {
    name: RA<BrokerRecord> | 'invalid' | undefined;
  }
>;

type ErrorAction = Action<
  'ErrorAction',
  {
    title: string;
    message: Component | RA<Component>;
  }
>;

export type Actions = LoadedOccurrenceAction | LoadedNameAction | ErrorAction;

export const reducer = generateReducer<States, Actions>({
  LoadedOccurrenceAction: ({ action: { occurrence }, state }) => ({
    ...ensureMainState(state),
    occurrence,
  }),
  LoadedNameAction: ({ action: { name }, state }) => ({
    ...ensureMainState(state),
    name,
  }),
  ErrorAction: ({ action: { title, message } }) => ({
    type: 'ErrorState',
    title,
    message,
  }),
});
