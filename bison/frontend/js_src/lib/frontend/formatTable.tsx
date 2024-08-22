import React from 'react';

import type { Component, RA } from '../config';
import { Row, Table } from './components';
import { MIN_COLUMNS } from './config';
import type { BrokerRecord } from './entry';
import { mapFields } from './fieldMapper';
import { reorderFields } from './reorderFields';
import { getRecordKeys, getValue } from './responseToTable';

export function Response({
  responses,
}: {
  responses: RA<BrokerRecord>;
}): Component | null {
  const blankColumns = Math.max(0, MIN_COLUMNS - responses.length);
  return responses.length === 0 ? null : (
    <Table
      className="data"
      header={
        <>
          <td />
          {responses.map(({ provider, record }) => (
            <th key={provider.code} scope="col">
              <div>
                <img alt="" src={`${provider.icon_url}&icon_status=active`} />
                <span>
                  {provider.label}{' '}
                  {typeof record['s2n:view_url'] === 'string' ? (
                    <a
                      href={record['s2n:view_url']}
                      target="_blank"
                      rel="noreferrer"
                    >
                      (link)
                    </a>
                  ) : (
                    ''
                  )}
                </span>
              </div>
            </th>
          ))}
          {Array.from({ length: blankColumns }, (_, index) => (
            <th scope="col" key={index} />
          ))}
        </>
      }
    >
      {mapFields(
        reorderFields(
          Object.fromEntries(
            getRecordKeys(responses.map((response) => response.record)).map(
              (key) =>
                [
                  key,
                  responses.map((response) => getValue(response, key)),
                ] as const
            )
          )
        )
      ).map(({ label, title, originalCells, cells }) => (
        <Row
          key={label}
          header={label}
          title={title}
          className={
            new Set(
              originalCells
                .filter((value) => value)
                .map((value) => JSON.stringify(value))
            ).size === 1
              ? 'identical'
              : ''
          }
          cells={[...cells, ...Array.from({ length: blankColumns }, () => '')]}
        />
      ))}
    </Table>
  );
}
