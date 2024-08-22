import '../css/loader.css';

import type { RA } from './config';

const urlParameters = new URLSearchParams(window.location.search);

export const getQueryParameter = (
  name: string,
  validator: (value: string) => boolean
): string =>
  validator(urlParameters.get(name) ?? '') ? urlParameters.get(name) ?? '' : '';

export const splitJoinedMappingPath = (string: string): RA<string> =>
  string.split('.');
