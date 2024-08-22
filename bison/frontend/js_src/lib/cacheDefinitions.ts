// The types of cached values are defined here
import type { IR } from './config';

export type CacheDefinitions = {
  readonly leafletBaseLayer: IR<string>;
  readonly leafletOverlays: IR<boolean>;
};
