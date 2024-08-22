import React from 'react';

import { createDictionary } from './utils';

// Refer to "Guidelines for Programmers" in ./utils.tsx before editing this file

const commonText = createDictionary({
  specifyNetwork: 'Specify Network',
  errorBoundaryErrorHeader: 'An unexpected error has occurred',
  errorBoundaryErrorMessage: (
    <>
      Please reload the page and try again. If this issue persists, please
      contact{' '}
      <a href="mailto:support@specifysoftware.org">
        support@specifysoftware.org
      </a>
      .
    </>
  ),
  invalidRequestErrorTitle: 'Invalid request URL',
  invalidRequestErrorMessage: (
    <p>Please make sure you entered correct URL and try again.</p>
  ),
  noDataErrorTitle: 'Unable to find any data for this request',
  noDataErrorMessage: 'Please try searching for a different record',
  markerLayerLabel: 'Specify 7 Pins',
  polygonLayerLabel: 'Specify 7 Polygons',
  polygonBoundaryLayerLabel: 'Specify 7 Polygon Boundaries',
  errorRadiusLayerLabel: 'Specify 7 Pin Error Radius',
});

export default commonText;
