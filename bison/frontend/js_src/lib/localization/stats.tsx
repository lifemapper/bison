import { createDictionary } from './utils';

// Refer to "Guidelines for Programmers" in ./utils.tsx before editing this file

const statsText = createDictionary({
  collectionLevelStats: 'Collection and Institution analyses',
  chooseCollection: 'Choose collection:',
  collectionMapHeader:
    'Geographic distribution of all species in the collection based on GBIF',
  collectionMapDescription: `
    Geographic distribution of all digitized specimen localities for all species
    in the collection based on data in GBIF
  `,
  institutionMapHeader: (institutionCode: string): string => `
    Geographic distribution of all species from ${
      institutionCode || 'this institution'
    } based on GBIF
  `,
  institutionMapDescription: (institutionCode: string): string => `
    Geographic distribution of all digitized specimens for all species for all
    collections at ${institutionCode || 'this institution'} based on data in
    GBIF
  `,
  noMapForCollection: 'Unable to find a map for this collection',
  noMapForInstitution: 'Unable to find a map for this institution',
  yearRangeDescription: 'Showing occurrences collected between these years:',
  noYearRange: 'Year-by-year map is not available.',
  startYear: 'Start Year',
  endYear: 'End Year',
});

export default statsText;
