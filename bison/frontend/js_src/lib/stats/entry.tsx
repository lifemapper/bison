import '../../css/stats.css';

import React from 'react';

import { Section } from '../components/common';
import { ErrorMessage } from '../components/errorBoundary';
import { Loading } from '../components/loading';
import { app } from '../components/main';
import type { IR, RA } from '../config';
import commonText from '../localization/common';
import statsText from '../localization/stats';
import { getQueryParameter } from '../utils';
import { GbifMap } from './components';
import { getCollectionMapData, getInstitutionMapMeta } from './utils';

const reGuid = /^\w{8}-(\w{4}-){3}\w{12}$/;
const isGuid = (guid: string): boolean => reGuid.exec(guid) !== null;

app(function Stats() {
  const [dataSet, setDataSet] = React.useState<string>(
    getQueryParameter('dataset_key', isGuid)
  );
  const [dataSets, setDataSets] = React.useState<
    IR<string> | 'loading' | undefined
  >('loading');

  const publishingOrgKey = getQueryParameter('publishing_org_key', isGuid);
  React.useEffect(() => {
    (publishingOrgKey
      ? fetch(
          `https://api.gbif.org/v1/dataset/search/?publishingOrg=${publishingOrgKey}`
        )
          .then(async (result) => result.json())
          .then(
            ({
              results,
            }: {
              readonly results: RA<{
                readonly key: string;
                readonly title: string;
              }>;
            }) =>
              Object.fromEntries(
                results.map((result) => [result.key, result.title])
              )
          )
      : Promise.resolve({})
    ).then((dataSets) => {
      if (Object.keys(dataSets).length === 0) setDataSets(undefined);
      else {
        if (!(dataSet in dataSets)) setDataSet(Object.keys(dataSets)[0]);
        setDataSets(dataSets);
      }
    });
  }, []);

  const institutionCode = getQueryParameter(
    'institution_code',
    (institutionCode) => institutionCode.length > 0
  );

  return dataSets === 'loading' ? (
    <Loading />
  ) : typeof dataSets === 'undefined' ? (
    <ErrorMessage title={commonText('noDataErrorTitle')}>
      <p>{commonText('noDataErrorMessage')}</p>
    </ErrorMessage>
  ) : (
    <main>
      <header>
        <h1>
          <img src="/static/img/specify_network_long.svg" alt="" />
          <span className="sr-only">{commonText('specifyNetwork')}</span>
        </h1>
        <h2>{statsText('collectionLevelStats')}</h2>
      </header>
      <div className="sections">
        <Section
          key="change-collection"
          anchor="change-collection"
          label={statsText('chooseCollection')}
        >
          <div>
            <select
              value={dataSet}
              onChange={({ target }): void => setDataSet(target.value)}
            >
              {Object.entries(dataSets).map(([dataSet, label]) => (
                <option value={dataSet} key={dataSet}>
                  {label}
                </option>
              ))}
            </select>
          </div>
        </Section>
        <Section
          key="collection-map"
          anchor="collection-map"
          label={statsText('collectionMapHeader')}
        >
          {statsText('collectionMapDescription')}
          <GbifMap
            mapOptions={{ datasetKey: dataSet }}
            value={dataSet}
            getMapData={getCollectionMapData}
          />
        </Section>
        <Section
          key="institution-map"
          anchor="institution-map"
          label={statsText('institutionMapHeader')(institutionCode)}
        >
          {statsText('institutionMapDescription')(institutionCode)}
          <GbifMap
            mapOptions={{ publishingOrg: publishingOrgKey }}
            value={publishingOrgKey}
            getMapData={getInstitutionMapMeta}
          />
        </Section>
      </div>
    </main>
  );
});
