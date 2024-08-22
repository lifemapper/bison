// Convert field names to field labels

import React from 'react';

import type { Component, IR, RA } from '../config';
import { camelToHuman, capitalize } from '../helper';
import frontEndText from '../localization/frontend';
import { Value } from './formatValue';

// Replace a word with a mapped variant
const fieldPartMapper: IR<string> = {
  gbif: 'GBIF',
  idigbio: 'iDigBio',
  itis: 'ITIS',
  id: 'ID',
  uuid: 'UUID',
  url: 'URL',
  tsn: 'TSN',
  lsid: 'LSID',
  worms: 'WoRMS',
};

// Replace field name with a label
const labelMapper: IR<string> = {
  'idigbio:uuid': 'iDigBio Record UUID',
  'mopho:specimen.specimen_id': 'MorphoSource ID',
  'dwc:stateProvince': 'State/Province',
  'gbif:gbifID': 'GBIF Record ID',
  'gbif:publishingOrgKey': 'GBIF Publisher ID',
  's2n:specify_identifier': 'Specify Record ID',
  'dcterms:modified': 'Modified by Host',
  'dwc:decimalLongitude': 'Longitude',
  'dwc:decimalLatitude': 'Latitude',
  's2n:worms_match_type': 'WoRMS Match Type',
  's2n:hierarchy': 'Classification',
};

const extractMorphosourceId = (link: string): string | undefined =>
  link.startsWith('https://www')
    ? /\/[^/]+$/.exec(link)?.[0].slice(1)
    : undefined;

const extractWormsId = (wormsLsid: string): string | undefined =>
  wormsLsid.startsWith('urn:lsid:marinespecies.org:taxname:')
    ? /\d+$/.exec(wormsLsid)?.[0]
    : undefined;

const linkify =
  (link: string) =>
  (key: string): Component | '' =>
    key ? (
      <a href={`${link}${key}`} target="_blank" rel="noreferrer">
        {key}
      </a>
    ) : (
      ''
    );

const stringGuard =
  (callback: (value: string) => Component | string) =>
  (value: unknown): Component | string =>
    typeof value === 'string' ? callback(value) : '';

// Replace field value with a transformed value
const valueMapper: IR<(value: unknown) => Component | string> = {
  'gbif:publishingOrgKey': stringGuard(
    linkify('https://www.gbif.org/publisher/')
  ),
  'gbif:gbifID': stringGuard(linkify('https://www.gbif.org/occurrence/')),
  'idigbio:uuid': stringGuard(
    linkify('https://www.idigbio.org/portal/records/')
  ),
  'mopho:specimen.specimen_id': stringGuard((specimenViewUrl) => {
    return typeof extractMorphosourceId(specimenViewUrl) === 'string' ? (
      <a href={specimenViewUrl} target="_blank" rel="noreferrer">
        {extractMorphosourceId(specimenViewUrl)}
      </a>
    ) : (
      specimenViewUrl
    );
  }),
  's2n:worms_lsid': stringGuard((wormsLsid) => {
    return typeof extractWormsId(wormsLsid) === 'string' ? (
      <a
        href={`http://www.marinespecies.org/aphia.php?p=taxdetails&id=${extractWormsId(
          wormsLsid
        )}`}
        target="_blank"
        rel="noreferrer"
      >
        {wormsLsid}
      </a>
    ) : (
      wormsLsid
    );
  }),
};

const mergeFields: RA<{
  readonly fieldNames: RA<string>;
  readonly label: string;
  readonly title: string;
  readonly mergeFunction: (values: RA<unknown>) => Component | string;
}> = [
  {
    fieldNames: ['dwc:year', 'dwc:month', 'dwc:day'],
    label: frontEndText('collectionDate'),
    title: 'dwc:month / dwc:day / dwc:year',
    // @ts-expect-error
    mergeFunction: ([year, month, day]: Readonly<[string, string, string]>):
      | Component
      | '' =>
      [year, month, day].every((value) => value) ? (
        <input
          type="date"
          value={`${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`}
          aria-label={frontEndText('collectionDate')}
          tabIndex={-1}
          readOnly
          required
        />
      ) : (
        ''
      ),
  },
];

function labelFromFieldName(fieldName: string): string {
  const strippedFieldName = fieldName.replace(/^\w+:/, '');
  const formattedFieldName = camelToHuman(strippedFieldName);
  const convertedFieldName = formattedFieldName
    .split('_')
    .map((part) => capitalize(part))
    .join(' ');

  const mappedFieldName = convertedFieldName
    .split(' ')
    .map((part) => fieldPartMapper[part.toLowerCase()] ?? part);

  if (mappedFieldName[0].toLowerCase() in labelMapper)
    mappedFieldName[0] = labelMapper[mappedFieldName[0]];

  return capitalize(mappedFieldName.join(' '));
}

function isLink(string: string): boolean {
  try {
    const url = new URL(string);
    return url.protocol === 'http:' || url.protocol === 'https:';
  } catch {
    return false;
  }
}

function DefaultValueFormatter(value: unknown): Component | string {
  if (typeof value === 'undefined') return '';
  else if (typeof value === 'string' && isLink(value))
    return (
      <a href={value} target="_blank" rel="noreferrer">
        {value}
      </a>
    );
  else if (typeof value === 'string' || typeof value === 'number')
    return `${value}`;
  else return <Value>{value}</Value>;
}

const transpose = <T,>(array: RA<RA<T>>): RA<RA<T>> =>
  array[0].map((_, colIndex) => array.map((row) => row[colIndex]));

type TableRow<T> = {
  readonly label: string;
  readonly title: string;
  readonly originalCells: RA<T>;
  readonly cells: RA<Component | string>;
};

export function mapFields<T>(dictionary: IR<RA<T>>): RA<TableRow<T>> {
  const mergedFields = new Set();
  return Object.entries(dictionary)
    .map(([fieldName, values]) => {
      if (mergedFields.has(fieldName)) return undefined;

      const resolvedValueMapper =
        valueMapper[fieldName] ?? DefaultValueFormatter;

      const merge =
        typeof resolvedValueMapper === 'undefined'
          ? mergeFields.find(({ fieldNames }) => fieldNames.includes(fieldName))
          : undefined;
      if (merge) {
        merge.fieldNames.forEach((fieldName) => mergedFields.add(fieldName));
        const cells = transpose(
          merge.fieldNames.map((fieldName) => dictionary[fieldName])
        );
        return {
          label: merge.label,
          title: merge.title,
          originalCells: cells.map((cells) => JSON.stringify(cells)),
          cells: cells.map(merge.mergeFunction),
        };
      } else
        return {
          label: labelMapper[fieldName] ?? labelFromFieldName(fieldName),
          title: fieldName,
          originalCells: values,
          cells: values.map(resolvedValueMapper) as RA<Component | string>,
        };
    })
    .filter((row): row is TableRow<T> => typeof row !== 'undefined');
}
