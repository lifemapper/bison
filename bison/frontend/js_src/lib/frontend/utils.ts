import type { IR, RA } from '../config';
import { NAME_PROVIDERS, OCC_PROVIDERS } from './config';
import type { BrokerRecord, RawBrokerResponse } from './entry';
import type { LoadedNameAction, LoadedOccurrenceAction } from './reducer';

export function validateBrokerResponse(response: {
  readonly errors: IR<unknown>;
  readonly records: RA<unknown>;
}): boolean {
  if (Object.keys(response.errors).length === 0)
    return response.records.length > 0;
  else {
    console.error(response);
    return false;
  }
}

function extractResponseRecord(
  response: RawBrokerResponse
): BrokerRecord | undefined {
  if (
    !validateBrokerResponse(response) ||
    !validateBrokerResponse(response.records[0])
  )
    return undefined;
  return {
    record: response.records[0].records[0],
    service: response.service,
    provider: response.records[0].provider,
  };
}

const fetchFromBroker = async (
  requestUrl: string
): Promise<BrokerRecord | undefined> =>
  fetch(requestUrl)
    .then(async (response) => response.json())
    .then((data: RawBrokerResponse) => extractResponseRecord(data))
    .catch((error) => {
      console.error(error);
      return undefined;
    });

const fetchFromEndpoint = async (
  url: string,
  providers: RA<string>
): Promise<RA<BrokerRecord> | undefined> =>
  Promise.all(
    providers.map(async (provider) =>
      fetchFromBroker(`${url}&provider=${provider}`)
    )
  )
    .then((responses) =>
      responses
        .filter(
          (response): response is BrokerRecord => typeof response === 'object'
        )
        .sort(
          (left, right) =>
            providers.indexOf(left.provider.code) -
            providers.indexOf(right.provider.code)
        )
    )
    .then((responses) => (responses.length === 0 ? undefined : responses));

export const fetchOccurrence = async (
  occId: string
): Promise<LoadedOccurrenceAction> =>
  (occId
    ? fetchFromEndpoint(`/api/v1/occ/?occid=${occId}`, OCC_PROVIDERS)
    : Promise.resolve(undefined)
  ).then((occurrence) => ({ type: 'LoadedOccurrenceAction', occurrence }));

export const fetchName = async (
  nameString: string,
  defaultValue: 'invalid' | undefined
): Promise<LoadedNameAction> =>
  (nameString
    ? fetchFromEndpoint(`/api/v1/name/?namestr=${nameString}`, NAME_PROVIDERS)
    : Promise.resolve(undefined)
  ).then((name) => ({ type: 'LoadedNameAction', name: name ?? defaultValue }));

export function extractField<T = string>(
  responses: RA<BrokerRecord> | undefined | string,
  aggregator: string,
  field: string,
  resolveUndefined = true
): T | undefined {
  if (typeof responses !== 'object') return undefined;

  const fields = Object.fromEntries<T | undefined>(
    responses
      .map(
        (response) =>
          [response.provider.code, response.record[field] as T] as const
      )
      .filter(([_aggregator, value]) => value)
  );
  return (
    fields[aggregator] ??
    (resolveUndefined ? Object.values(fields)[0] : undefined)
  );
}
