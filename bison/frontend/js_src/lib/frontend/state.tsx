import React from 'react';
import type { State } from 'typesafe-reducer';
import { generateReducer } from 'typesafe-reducer';

import { Section } from '../components/common';
import { ErrorMessage } from '../components/errorBoundary';
import { Loading } from '../components/loading';
import type { Component, RA } from '../config';
import commonText from '../localization/common';
import frontEndText from '../localization/frontend';
import {
  IssuesTable,
  NameTable,
  OccurrenceTable,
  SyftoriumLink,
} from './components';
import type { BrokerRecord } from './entry';
import { LeafletContainer } from './leaflet';
import { extractField } from './utils';

export type MainState = State<
  'MainState',
  {
    occurrence: RA<BrokerRecord> | undefined | 'loading';
    name: RA<BrokerRecord> | undefined | 'invalid' | 'loading';
  }
>;

export type ErrorState = State<
  'ErrorState',
  {
    title: string;
    message: Component | RA<Component>;
  }
>;

export type States = MainState | ErrorState;

export function ensureMainState(state: States): MainState {
  if (state.type === 'MainState') return state;
  else throw new Error(`Reducer called on ${state.type} state`);
}

export const stateReducer = generateReducer<Component | undefined, States>({
  ErrorState({ action: { title, message } }) {
    return <ErrorMessage title={title}>{message}</ErrorMessage>;
  },
  MainState({ action: { occurrence, name } }) {
    if (typeof occurrence === 'undefined' && typeof name === 'undefined')
      return (
        <ErrorMessage title={commonText('noDataErrorTitle')}>
          <p>{commonText('noDataErrorMessage')}</p>
        </ErrorMessage>
      );
    else if (
      occurrence === 'loading' ||
      (typeof occurrence === 'undefined' && name === 'loading')
    )
      return <Loading />;

    const scientificName =
      extractField(name, 'gbif', 's2n:scientific_name') ??
      extractField(occurrence, 'gbif', 'dwc:scientificName');

    return (
      <main>
        <header>
          <h1>
            <img src="/static/img/specify_network_long.svg" alt="" />
            <span className="sr-only">{commonText('specifyNetwork')}</span>
          </h1>
          <h2>{scientificName ?? frontEndText('scientificNameUnknown')}</h2>
        </header>
        <div className="sections">
          {typeof occurrence === 'object' ? (
            <>
              <IssuesTable occurrence={occurrence} />
              <OccurrenceTable occurrence={occurrence} />
            </>
          ) : undefined}
          {typeof name === 'object' ? (
            <>
              <NameTable name={name} />
              <LeafletContainer
                occurrence={occurrence}
                name={name}
                scientificName={scientificName}
              />
            </>
          ) : (
            <Section key="name" anchor="name" label={frontEndText('taxonomy')}>
              <p>{frontEndText('fetchingInformation')}</p>
            </Section>
          )}
          {typeof occurrence === 'object' ? (
            <SyftoriumLink occurrence={occurrence} />
          ) : undefined}
        </div>
      </main>
    );
  },
});
