import type { RA } from './config';

export const camelToHuman = (value: string): string =>
  capitalize(value.replace(/([a-z])([A-Z])/g, '$1 $2')).replace(/Dna\b/, 'DNA');

export const capitalize = (string: string): string =>
  string.charAt(0).toUpperCase() + string.slice(1);

/*
 * Copied from:
 * https://underscorejs.org/docs/modules/throttle.html
 *
 * It was then modified to modernize and simplify the code, as well as, to
 * add the types
 */
export function throttle<ARGUMENTS extends RA<unknown>>(
  callback: (...rest: ARGUMENTS) => void,
  wait: number
): (...rest: ARGUMENTS) => void {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  let previousArguments: ARGUMENTS | undefined;
  let previousTimestamp = 0;

  function later(): void {
    previousTimestamp = Date.now();
    timeout = undefined;
    callback(...previousArguments!);
  }

  return (...rest: ARGUMENTS): void => {
    const now = Date.now();
    const remaining = wait - (now - previousTimestamp);
    previousArguments = rest;
    if (remaining <= 0 || remaining > wait) {
      if (typeof timeout !== 'undefined') {
        clearTimeout(timeout);
        timeout = undefined;
      }
      previousTimestamp = now;
      callback(...previousArguments);
    } else if (typeof timeout === 'undefined')
      timeout = setTimeout(later, remaining);
  };
}
