import '../../css/slider.css';

export type GbifMapData = {
  readonly minYear?: number;
  readonly maxYear?: number;
};

export async function getInstitutionMapMeta(
  publishingOrgKey: string
): Promise<GbifMapData> {
  const request = await fetch(
    `https://api.gbif.org/v2/map/occurrence/density/capabilities.json?publishingOrg=${publishingOrgKey}`
  );
  return request.json();
}

export async function getCollectionMapData(
  datasetKey: string
): Promise<GbifMapData> {
  const request = await fetch(
    `https://api.gbif.org/v2/map/occurrence/density/capabilities.json?datasetKey=${datasetKey}`
  );
  return request.json();
}
