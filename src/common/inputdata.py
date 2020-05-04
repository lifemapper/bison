from common.constants import ProviderActions, BISON_IPT_PREFIX

ANCILLARY_FILES = {
    # Modified from original to merge US and Canada, split into simple
    # (no multi) polygons, and add centroids
    'terrestrial': {
        'file': 'us_can_boundaries_centroids.shp',
        'fields': (('B_FIPS', 'calculated_fips'), 
                   ('B_COUNTY', 'calculated_county_name'),
                   ('B_STATE', 'calculated_state_name'))},
    # Modified from original to split into simple polygons and 
    # intersected with a 5/10 degree grid
    'marine': {
#         'file': 'World_EEZ_v8_20140228_splitpolygons/World_EEZ_v8_2014_HR.shp',
#         'file': 'eez_gridded_boundaries_10.shp',
#         'file': 'eez_gridded_boundaries_5.shp',
        'file': 'eez_gridded_boundaries_2.5.shp',
        'fields': (('EEZ', 'calculated_waterbody'), 
                   ('MRGID', 'mrgid'))},
    # From Annie Simpson
    'establishment_means': {'file': 'NonNativesIndex20190912.txt'},
    # From ITIS developers
    'itis': {'file': 'itis_lookup.csv'},
    # From existing database
    'resource': {'file': 'resource.csv'},
    'provider': {'file': 'provider.csv'}}

BISON_PROVIDER = {
    
    # X my.usgs.gov/jira/browse/BISON-402
    'nplichens':
    {'action': ProviderActions.add,
     'resource': 'NPS - US National Park Lichens - 2013 (NPLichens)',
     'resource_url': BISON_IPT_PREFIX + 'nplichens',
     'filename': 'FINAL-NPLichens03Dec2019.txt',
    },
    
    # X my.usgs.gov/jira/browse/BISON-832
    '440,100012': 
    {'action': ProviderActions.replace, 
     'resource': 'USGS PWRC - Native Bee Inventory and Monitoring Lab (BIML)',
     'resource_url': BISON_IPT_PREFIX + 'usgs-pwrc-biml',
     'filename': 'FINALusgspwrc-nativebeeinventoryandmonitoringlab-25Nov2019.txt',
    },

    # X my.usgs.gov/jira/browse/BISON-1035, verify centroid symbols and missing field values?
    
    # (?? ticket gone or no permission) my.usgs.gov/jira/browse/BISON-895
    'usgs-pwrc-amphibian-research-monitoring-initiative':
    {'action': ProviderActions.wait, # add,
     'resource': 'USGS PWRC - Amphibian Research and Monitoring Initiative (ARMI) - NEast Region',
     'resource_url': BISON_IPT_PREFIX + 'usgs-pwrc-amphibian-research-monitoring-initiative',
     'filename': None
     },
    
    # X my.usgs.gov/jira/browse/BISON-976
    'csu-nrel-co-riverbasin-tamarix':
    {'action': ProviderActions.add,
     'resource': 'CO State University - Natural Resource Ecology Lab - CO River Basin - Tamarix - 2014 and 2017',
     'resource_url': BISON_IPT_PREFIX + 'csu-nrel-co-riverbasin-tamarix',
     'filename': 'FINAL-CSU-NREL-CO-Riverbasin-Tamarix_20191126es.txt'
     },
    
    # X my.usgs.gov/jira/browse/BISON-978
    '440,100061':
    {'action': ProviderActions.replace,
     'resource': 'BugGuide',
     'resource_url': BISON_IPT_PREFIX + 'bugguide',
     'filename': 'FINAL-bugguide19Dec2019.txt'},
    
    # X my.usgs.gov/jira/browse/BISON-979
    '440,100068':
    {'action': ProviderActions.replace,
     'resource': 'Xerces Society - Bumble Bee Watch',
     'resource_url': BISON_IPT_PREFIX + 'xerces-bumblebeewatch',
     'filename': 'xerces-bumblebeewatch_FINAL14Aug2019.txt'},
    
    # X my.usgs.gov/jira/browse/BISON-986
    '440,100071':
    {'action': ProviderActions.replace,
     'resource': 'Monarch Watch',
     'resource_url': BISON_IPT_PREFIX + 'monarchwatch',
     'filename': 'FINALmonarchwatch_2019Nov19.txt'},
    
    # X my.usgs.gov/jira/browse/BISON-987
    '440,100028':
    {'action': ProviderActions.replace,
     'resource': 'USFS - Forest Inventory and Analysis - Trees (Public Lands)',
     'resource_url': BISON_IPT_PREFIX + 'usfs-fia-trees-public-lands',
     'filename': 'bison_fiapub_2019-03-25.txt'}, 
    
    # X my.usgs.gov/jira/browse/BISON-988
    '440,100042':
    {'action': ProviderActions.replace,
     'resource': 'USFS - Forest Inventory and Analysis - Trees (Private Lands)',
     'resource_url': BISON_IPT_PREFIX + 'usfs-fia-trees-private-lands',
     'filename': 'bison_fiapriv_2019-03-25.txt'},
    
    # X my.usgs.gov/jira/browse/BISON-989 (changed from replace/rename)
    'nycity-tree-census-2015':
    {'action': ProviderActions.add,
     'resource': 'New York City Tree Census - 2015',
     'resource_url': 'http://bison.ornl.gov/ipt/resource.do?r=nycity-tree-census-2015',
     'filename': 'FINAL-nycitytreecensus-2015-05Dec2019.txt'},
    
    # X my.usgs.gov/jira/browse/BISON-993
    'usgs-warc-suwanneemoccasinshell':
    {'action': ProviderActions.add,
     'resource': 'USGS WARC - Florida and Georgia - Suwannee moccasinshell - 1916-2015',
     'resource_url': BISON_IPT_PREFIX + 'usgs-warc-suwanneemoccasinshell',
     'filename': 'revised_BISON_USGS_WARC_suwanneemoccasinshell.txt'},
    
    # X my.usgs.gov/jira/browse/BISON-994
    '440,100046':
    {'action': ProviderActions.replace,
     'resource': 'USGS PWRC - Bird Banding Lab - US State Centroid',
     'resource_url': BISON_IPT_PREFIX + 'usgs-pwrc-bbl-us-state',
     # DL at https://drive.google.com/drive/folders/1Fxy2s3_kFnAGMVNN_HyRLzuBSnKFmsQv
     'filename': 'bison_bbl_state_ordered_final_2019-02-22.txt'},
    
    # X my.usgs.gov/jira/browse/BISON-995
    '440,100008':
    {'action': ProviderActions.replace, 
     'resource': 'USGS PWRC - Bird Banding Lab - US 10min Block',
     'resource_url': BISON_IPT_PREFIX + 'usgs-pwrc-bbl-us-10minblock',
     # DL https://drive.google.com/drive/folders/1Fxy2s3_kFnAGMVNN_HyRLzuBSnKFmsQv
     'filename': 'bison_bbl_10min_ordered_final_2019-02-21.txt'},
    
    # X my.usgs.gov/jira/browse/BISON-996
    '440,100052':
    {'action': ProviderActions.replace,  
     'resource': 'USGS PWRC - Bird Banding Lab - Canada Province Centroid',
     'resource_url': BISON_IPT_PREFIX + 'usgs-pwrc-bbl-canada-province',
     'filename': 'bison_bbl_ca_province_ordered_final_2019-02-26.txt'},
    
    # X my.usgs.gov/jira/browse/BISON-998
    # No IPT url?
    '440,1066':
    {'action': ProviderActions.replace,
     'resource': 'USDA - PLANTS Database',
     'resource_url': 'http://plants.usda.gov/java/citePlants',
     'filename': 'bison_USDA_Plants_2019-12-19.txt'},
    
    # X my.usgs.gov/jira/browse/BISON-1001
    'nps-nisims':
    {'action': ProviderActions.add,
     'resource': 'NPS - National Invasive Species Information Management System - Plants',
     'resource_url': BISON_IPT_PREFIX + 'nps-nisims',
     'filename': 'NPS-NISIMS-Plants_20190325.txt'},
    
    # X my.usgs.gov/jira/browse/BISON-1002
    '440,100032':
    {'action': ProviderActions.replace_rename, 
     'resource': 'BLM - National Invasive Species Information Management System - Plants',
     'resource_url': BISON_IPT_PREFIX + 'blm_nisims',
     'filename': 'BLM-NISIMS_20190325.txt'},
    
    # X my.usgs.gov/jira/browse/BISON-1003
    '440,100060':
    {'action': ProviderActions.replace, 
     'resource': 'iMapInvasives - NatureServe - New York',
     'resource_url': BISON_IPT_PREFIX + 'imapinvasives-natureserve-ny',
     'filename': 'iMap_NY_plants-replace20190307.txt'},
    
    # X my.usgs.gov/jira/browse/BISON-1005
    '440,100043':
    {'action': ProviderActions.replace_rename, 
     'resource': 'USGS PWRC - North American Breeding Bird Survey',
     'resource_url': BISON_IPT_PREFIX + 'usgs_pwrc_north_american_breeding_bird_survey',
     'filename': 'bbsfifty_processed_2019-03-14.txt'},
    
    # X my.usgs.gov/jira/browse/BISON-1012
    'cdc-arbonet-mosquitoes':
    {'action': ProviderActions.add, 
     'resource': 'CDC - ArboNET - Mosquitoes',
     'resource_url': BISON_IPT_PREFIX + 'cdc-arbonet-mosquitoes',
     'filename': 'CDC-ArboNET-Mosquitoes_20190315.txt'},
    
    # X my.usgs.gov/jira/browse/BISON-1018
    'emammal':
    {'action': ProviderActions.add, 
     'resource': 'eMammal',
     'resource_url': BISON_IPT_PREFIX + 'emammal',
     'filename': 'bison_emammal_2019-09-18.txt'},
    
    # X my.usgs.gov/jira/browse/BISON-1051
    'usfws-waterfowl-survey':
    {'action': ProviderActions.add,
     'resource': '     USFWS - Waterfowl Breeding Population and Habitat Survey',
     'resource_url': BISON_IPT_PREFIX + 'usfws-waterfowl-survey',
     'filename': 'USFWS - Waterfowl Breeding Population and Habitat Survey 11Jul2019.txt'},
    
    # X my.usgs.gov/jira/browse/BISON-1053
    # old name = USGS PIERC - Hawaii Forest Bird Survey Database 
    # rename resource in records in existing dataset
    '440,100050':
    {'action': ProviderActions.rename, 
     'resource': 'USGS PIERC - Hawaii Forest Bird Survey - Plants',
     'resource_url': BISON_IPT_PREFIX + 'usgs_pierc_hfbs1',
     'filename': None},
    
    # X my.usgs.gov/jira/browse/BISON-1075
    # old name = USGS PIERC - Hawaii Forest Bird Survey Database 
    '440,100075':
    {'action': ProviderActions.rename, 
     'resource': 'USGS PIERC - Hawaii Forest Bird Survey - Birds',
     'resource_url': BISON_IPT_PREFIX + 'usgs-pierc-hfbs-birds',
     'filename': None},
    
    # X my.usgs.gov/jira/browse/BISON-1067
    'nps-nisims':
    {'action': ProviderActions.add, 
     'resource': 'National Invasive Species Information Management System - Plants',
     'resource_url': BISON_IPT_PREFIX + 'nps-nisims',
     'filename': 'NPS-NISIMS-plants_20200325.txt'},

    # X my.usgs.gov/jira/browse/BISON-1088
    '440,100011':
    {'action': ProviderActions.replace, 
     'resource': 'USGS WERC - SDFS Fisher Lab - Vertebrates - 1999-2009',
     'resource_url': BISON_IPT_PREFIX + 'usgs-werc-sdfs-carnivore_cam',
     'filename': 'USGS-WERCcarnivoreCam-20200318.txt'},

#     # my.usgs.gov/jira/browse/BISON-
#     '':
#     {'action': ProviderActions.wait, 
#      'resource': '',
#      'resource_url': BISON_IPT_PREFIX + '',
#      'filename': None},
}
