import React from 'react';

import { createDictionary } from './utils';

// Refer to "Guidelines for Programmers" in ./utils.tsx before editing this file

const frontEndText = createDictionary({
  scientificNameUnknown: 'Scientific Name Unknown',
  dataQuality: 'Data Quality',
  reportedBy: (provider: string) => `Reported by ${provider}`,
  collectionObject: 'Collection Object',
  taxonomy: 'Taxonomy',
  distribution: 'Distribution',
  projectionLayerLabel: 'Lifemapper Distribution Model',
  syftoriumHeader: 'Institution and Collection Analyses',
  syftoriumMessage: (
    institutionCode: string,
    collectionCode: string,
    linkWrapper: (text: string) => JSX.Element
  ): JSX.Element => (
    <>
      Distribution maps of all of the digitized specimens curated in the
      Collection &quot;{collectionCode}&quot; and in the Institution &quot;
      {institutionCode}&quot; are available {linkWrapper('here')}.
    </>
  ),
  collectionDate: 'Collection Date',
  mapDescription:
    'This map shows all occurrences of this taxon from iDigBio and GBIF.',
  iDigBioDescription: `
    iDigBio points are represented as green dots on the map. Of those,
    the occurrences published to iDigBio from the current collection are red.
  `,
  gbifDescription: `
    For GBIF data, individual points and clusters of points are shown as
    hexagons of different shading ranging from yellow to orange to red
    with the dark red hexagons corresponding to densest distributions of
    points.
  `,
  viewRecord: 'View Record',
  fetchingInformation: 'Fetching information...',
});

export default frontEndText;
