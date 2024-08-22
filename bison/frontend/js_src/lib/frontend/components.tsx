import '../../css/table.css';

import React from 'react';

import { Section } from '../components/common';
import type { Component, IR, RA } from '../config';
import frontEndText from '../localization/frontend';
import type { BrokerRecord } from './entry';
import { Response } from './formatTable';
import { extractField } from './utils';

export function SyftoriumLink({
  occurrence,
}: {
  readonly occurrence: RA<BrokerRecord>;
}): Component | null {
  const fields = [
    {
      source: occurrence,
      aggregator: 'idb',
      field: 'dwc:institutionCode',
      name: 'institution_code',
    },
    {
      source: occurrence,
      aggregator: 'idb',
      field: 'dwc:collectionCode',
      name: 'collection_code',
    },
    {
      source: occurrence,
      aggregator: 'idb',
      field: 'gbif:publishingOrgKey',
      name: 'publishing_org_key',
    },
    {
      source: occurrence,
      aggregator: 'idb',
      field: 'gbif:datasetKey',
      name: 'dataset_key',
    },
  ].map(
    ({ source, aggregator, field, name }) =>
      [name, extractField(source, aggregator, field)] as const
  );

  const hashedFields = Object.fromEntries(fields);
  const queryString = fields
    .filter(
      (field): field is [string, string] => typeof field[0] !== 'undefined'
    )
    .map(([name, value]) => `${name}=${value}`)
    .join('&');

  if (fields.some(([_name, value]) => typeof value === 'undefined'))
    // eslint-disable-next-line unicorn/no-null
    return null;

  return (
    <Section key="stats" anchor="stats" label={frontEndText('syftoriumHeader')}>
      <p>
        {frontEndText('syftoriumMessage')(
          hashedFields.institution_code ?? '',
          hashedFields.collection_code ?? '',
          (text) => (
            <a href={`/api/v1/stats/?${queryString}`}>{text}</a>
          )
        )}
      </p>
    </Section>
  );
}

export function Table({
  className = '',
  header,
  children,
}: {
  readonly className?: string;
  readonly header?: Component | RA<Component>;
  readonly children: RA<Component>;
}): Component {
  return (
    <table className={className}>
      {header && (
        <thead>
          <tr>{header}</tr>
        </thead>
      )}
      <tbody>{children}</tbody>
    </table>
  );
}

export function Row({
  header,
  title,
  cells,
  className = '',
}: {
  readonly header: string;
  readonly title?: string;
  readonly cells: RA<Component | string>;
  readonly className?: string;
}): JSX.Element {
  return (
    <tr className={className}>
      <th scope="row" title={title}>
        {header}
      </th>
      {cells.map((column, index) => (
        <td key={index}>{column}</td>
      ))}
    </tr>
  );
}

export function IssuesTable({
  occurrence,
}: {
  readonly occurrence: RA<BrokerRecord>;
}): Component | null {
  const issues = occurrence.filter(
    ({ record }) => Object.keys(record['s2n:issues'] as IR<string>).length > 0
  );
  // eslint-disable-next-line unicorn/no-null
  return issues.length === 0 ? null : (
    <Section key="issues" anchor="issues" label={frontEndText('dataQuality')}>
      <Table className="issues">
        {issues.map(({ provider, record }) => (
          <Row
            key={provider.code}
            header={frontEndText('reportedBy')(provider.label)}
            cells={[
              <ul key="list">
                {Object.entries(record['s2n:issues'] as IR<string>).map(
                  ([key, value]) => (
                    <li key={key}>
                      {value} ({key})
                    </li>
                  )
                )}
              </ul>,
            ]}
          />
        ))}
      </Table>
    </Section>
  );
}

export function OccurrenceTable({
  occurrence,
}: {
  readonly occurrence: RA<BrokerRecord>;
}): Component | null {
  const formattedSpecimenId =
    extractField(occurrence, 'mopho', 'mopho:specimen.specimen_id', false) ??
    extractField(occurrence, 'mopho', 's2n:view_url', false);

  const alteredResponse = occurrence
    .filter((record) => record.provider.code !== 'mopho')
    .map((response) => ({
      ...response,
      record: {
        ...response.record,
        'mopho:specimen.specimen_id':
          response.record['mopho:specimen.specimen_id'] || formattedSpecimenId,
      },
    }));

  const response = <Response responses={alteredResponse} />;
  return response === null ? null : (
    <Section key="occ" anchor="occ" label={frontEndText('collectionObject')}>
      {response}
    </Section>
  );
}

export function NameTable({
  name,
}: {
  name: RA<BrokerRecord>;
}): Component | null {
  const response = <Response responses={name} />;
  return response === null ? null : (
    <Section key="name" anchor="name" label={frontEndText('taxonomy')}>
      {response}
    </Section>
  );
}
