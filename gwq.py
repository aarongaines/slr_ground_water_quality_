import pandas as pd
import numpy as np
import geopandas as gpd
import dask_geopandas as dgpd
import scipy.stats as stats
import urllib.request
import urllib.error
from pathlib import Path
import os

bp = Path(os.getcwd())


def weight_proxy(row):

    if row.values < 250:
        adj_wgt = wells['mag'][row.name] * 1

    elif 250 <= row.values < 500:
        adj_wgt = wells['mag'][row.name] * 0.5

    elif 500 <= row.values < 750:
        adj_wgt = wells['mag'][row.name] * 0.25

    elif 750 <= row.values <= 1000:
        adj_wgt = wells['mag'][row.name] * 0.1

    return adj_wgt


def calculate_distance(row):

    distL = wells.distance(row)
    distL = distL[distL <= 1000]

    if distL.empty:
        score = 0

    else:
        frame = {'dist': distL}
        distdf = gpd.GeoDataFrame(frame)

        distdf['adj_wgt'] = distdf.apply(axis=1, func=weight_proxy)
        distdf['adj_wgt'] = distdf['adj_wgt'].astype(float)
        score = round(distdf['adj_wgt'].mean(),4)

    print(score)

    return score


if __name__ == '__main__':

    import pandas as pd
    from pathlib import Path

    def mkdir_except(folder_name):  # function to create folders and ignore if folder exists

        try:
            os.mkdir(folder_name)
            #print("Folder {} created. ".format(folder_name))

        except:
            print("Folder {} already exists. \n".format(folder_name))

    def dlSave_zip(url, folder_path):  # function for downloading url to a path

        fName = url.split('/')[-1]
        sp = folder_path / fName

        if os.path.isfile(sp):
            print("{} already downloaded \n".format(sp))

        else:
            try:
                req = urllib.request.urlopen(url)
                if not [i for i in req.getheaders() if 'text/html' in i]:
                    print('Downloading: {} '.format(url))
                    data = req.read()
                    req.close()

                    local = open(sp, 'wb')
                    local.write(data)
                    local.close()

            except urllib.error.HTTPError:
                print("HTTPError for {} ".format(url))

            except urllib.error.URLError:
                print("URLError for {} ".format(url))

# list of counties for scores to be run on. Below are counties within the 263m screening area
    county_names = [
    'Alameda',
    'ContraCosta',
    'DelNorte',
    'Humboldt',
    'LosAngeles',
    'Marin',
    'Mendocino',
    'Monterey',
    'Napa',
    'Orange',
    'Riverside',
    'Sacramento',
    'SanBenito',
    'SanBernardino',
    'SanDiego',
    'SanFrancisco',
    'SanLuisObispo',
    'SanMateo',
    'SantaBarbara',
    'SantaClara',
    'SantaCruz',
    'Siskiyou',
    'Solano',
    'Sonoma',
    'Trinity',
    'Ventura'
    ]
    date = '2012-01-01'
    edf_name = 'EDF.zip'
    xy_name = 'GeoXY.zip'

# set base geotracker urls and create the path for geotracker downloads (edf AND xy)
    geotracker_edf_url = "https://geotracker.waterboards.ca.gov/data_download/edf_by_county/"
    geo_edf_path = bp / 'geotracker_edf_results'
    mkdir_except(geo_edf_path)

    geotracker_xy_url = "https://geotracker.waterboards.ca.gov/data_download/geo_by_county/"
    geo_xy_path = bp / 'geotracker_xy'
    mkdir_except(geo_xy_path)

    def dl_geotracker(url_start, clist, url_alt, folder_path):

        urlList = []

        for i in clist:
            url = url_start + i + url_alt
            urlList.append(url)

        for j in urlList:
            dlSave_zip(j, folder_path)

# runs download for geotracker sample resutls (edf) and xy locations
    print('Downloading GeoTracker Data: \n')
    dl_geotracker(geotracker_edf_url, county_names, edf_name, geo_edf_path)
    dl_geotracker(geotracker_xy_url, county_names, xy_name, geo_xy_path)

# set base gama results url and creates a path for them to be downloaded to
    gama_base_url = 'https://gamagroundwater.waterboards.ca.gov/gama/data_download/'
    gama_res_path = bp / 'gama_results'
    mkdir_except(gama_res_path)

# alternate url substrings for each dataset from GAMA
    gama_alt_urls = [
        'ddw_',
        'dpr_',
        'dwr_',
        'gama_dom_',
        'gama_sp-study_',
        'gama_usgs_',
        'localgw_',
        'usgs_nwis_',
        'wb_cleanup_',
        'wb_ilrp_',
        'wrd_'
    ]

    # gama results with dl_Save_zip()
    def dl_gama_results(start_url, clist, alt_urls, dl_path):

        url_list = []

        pref = 'gama_'
        suf = '_v2.zip'

        for c in clist:
            c = c.lower()

            for au in alt_urls:
                url = start_url + pref + au + c + suf
                url_list.append(url)

        for url in url_list:
            dlSave_zip(url, dl_path)


# Runs downloads for GAMA sample results
    print('Downloading GAMA sample results: \n')
    dl_gama_results(gama_base_url, county_names, gama_alt_urls, gama_res_path)

# set base GAMA xy url and path for downloads
    gama_xy_url = 'https://gamagroundwater.waterboards.ca.gov/gama/data_download/gama_location_construction_v2.zip'
    gama_xy_path = bp / 'gama_xy'
    mkdir_except(gama_xy_path)

# runs download for GAMA xy data
    print('Downloading GAMA XY data: \n')
    gama_xy = dlSave_zip(gama_xy_url, gama_xy_path)


# creation of sample ID column (SID)

    def sid_col(df):
        df['SID'] = df['WID'].astype(
            str) + ' ' + df['LOGDATE'].astype(str) + ' ' + df['PARLABEL'].astype(str)


# loads gama results data to dataframe

    def create_gama_table(p):
        print('Loading: {} '.format(p))
        try:
            column_list = ['GM_WELL_ID', 'GM_CHEMICAL_VVL',
                           'GM_RESULT', 'GM_RESULT_UNITS', 'GM_SAMP_COLLECTION_DATE']

            df = pd.read_csv(p, sep='\t', lineterminator='\n', encoding='unicode_escape',
                             usecols=column_list, low_memory=False, parse_dates=['GM_SAMP_COLLECTION_DATE'])
            df = df[df['GM_RESULT'] != 0]
            df['GM_RESULT'] = pd.to_numeric(df['GM_RESULT'], errors='coerce')
            df['GM_RESULT'] = df['GM_RESULT'].notna()
            df['GM_RESULT'] = df['GM_RESULT'].astype(float)
            df = df[df['GM_SAMP_COLLECTION_DATE'] >= date]

            return df

        except:
            try:
                df = pd.read_csv(p, sep='\t', lineterminator='\n', usecols=column_list,
                                 low_memory=False, parse_dates=['GM_SAMP_COLLECTION_DATE'])
                df = df[df['GM_RESULT'] != 0]
                df['GM_RESULT'] = pd.to_numeric(df['GM_RESULT'], errors='coerce')
                df['GM_RESULT'] = df['GM_RESULT'].notna()
                df['GM_RESULT'] = df['GM_RESULT'].astype(float)
                df = df[df['GM_SAMP_COLLECTION_DATE'] >= date]

            except:
                print('{} corrupt. Data does not exist \n'.format(p))


# function to concat gama result datasets

    def concat_gama_data(files):

        df_list = []

        for i in files:
            j = create_gama_table(i)
            if j is not None:
                df_list.append(j)
        print('')
        print('Combining GAMA sample results: \n')
        concatDF = pd.concat(df_list, axis=0)

        for df in df_list:
            del df

        return concatDF


# collect gama results files and concat them
    print('Loading GAMA sample results: \n')
    gama_files = gama_res_path.glob('**/*.zip')
    gama_results = concat_gama_data(gama_files)


# rename columns to their Geotracker counterparts and add SID column
    print('Renaming GAMA columns and creating SID: \n')
    column_dict = {'GM_WELL_ID': 'WID', 'GM_CHEMICAL_VVL': 'PARLABEL', 'GM_RESULT': 'PARVAL',
                   'GM_SAMP_COLLECTION_DATE': 'LOGDATE', 'GM_RESULT_UNITS': 'UNITS'}
    
    gama_results.rename(columns=column_dict, inplace=True)
    sid_col(gama_results)


# function for loading geotradcker edf tables
    def create_edf_table(p):
        print('Loading: {} '.format(p))
        try:
            column_list = ['GLOBAL_ID', 'FIELD_PT_NAME',
                        'LOGDATE', 'PARLABEL', 'PARVAL', 'UNITS']

            df = pd.read_csv(p, sep='\t', lineterminator='\n', encoding='unicode_escape', usecols=column_list, parse_dates=[
                'LOGDATE'])
            df = df[df['PARVAL'] != 0]
            df['PARVAL'] = pd.to_numeric(df['PARVAL'], errors='coerce')
            df['PARVAL'] = df['PARVAL'].notna()
            df['PARVAL'] = df['PARVAL'].astype(float)
            df = df[df['LOGDATE'] >= date]
            df['WID'] = df['GLOBAL_ID'] + '-' + df['FIELD_PT_NAME']
            df = df.drop(columns=['GLOBAL_ID', 'FIELD_PT_NAME'])

            return df

        except:
            try:
                df = pd.read_csv(p, sep='\t', lineterminator='\n', usecols=column_list, parse_dates=[
                    'LOGDATE'])
                df = df[df['PARVAL'] != 0]
                df['PARVAL'] = pd.to_numeric(df['PARVAL'], errors='coerce')
                df['PARVAL'] = df['PARVAL'].notna()
                df['PARVAL'] = df['PARVAL'].astype(float)
                df = df[df['LOGDATE'] >= date]
                df['WID'] = df['GLOBAL_ID'] + '-' + df['FIELD_PT_NAME']
                df = df.drop(columns=['GLOBAL_ID', 'FIELD_PT_NAME'])

                return df

            except:
                print('{} corrupt. Data does not exist \n'.format(p))


# function to concat gama result datasets

    def concat_geo_data(files):

        df_list = []

        for i in files:
            j = create_edf_table(i)

            if j is not None:
                df_list.append(j)
        print('Combining Geotracker EDF sample results: \n')
        concatDF = pd.concat(df_list, axis=0)

        for df in df_list:
            del df

        return concatDF

# Load and combine Geotracker EDF results
    print('Loading Geotracker EDF results \n')
    edf_files = geo_edf_path.glob('**/*.zip')
    edf_results = concat_geo_data(edf_files)
    sid_col(edf_results)

# Combines GAMA and Geotracker EDF results into one dataset
    print('Combining GAMA and Geotracker EDF results \n')
    samples = pd.concat([edf_results, gama_results], ignore_index=True)

    for i in [edf_results, gama_results]:
        del i

    samples['SID'] = samples['SID'].astype(str)

# selects desired samples and drops duplicates in SID, grabbing highest results value of dupes
    print('Selecting desired samples and dropping duplicates \n')
    samples = samples.sort_values(by=['SID'])
    samples = samples.sort_values(by='PARVAL', ascending=False)
    samples = samples.drop_duplicates(subset='SID', keep='first')

# load MCL table into memory
    import scipy.stats as stats

    print('Loading MCL table \n')
    mcl_path = bp / 'MCL_list_1.xlsx'
    mcl = pd.read_excel(mcl_path, engine='openpyxl')
    mcl.set_index('chem_abrv', inplace=True)

# join MCL values to sample results
    print('Joining MCL values to samples \n')
    samples_mcl = pd.merge(samples, mcl, left_on='PARLABEL', right_index=True)
    del samples
    samples_mcl = samples_mcl[pd.notna(samples_mcl['PARVAL'])]

    samples_mcl.reset_index(drop=True)

# set of MCL exceedences and magnitudes. Removes outliers with z-score
    print('Selecting MCL Exceedances \n')
    samples_mcl = samples_mcl[samples_mcl['PARVAL']
                              >= samples_mcl['comp_conc_val']]

    print('Calculating magnitudes of MCL exceedances \n')
    samples_mcl['mag'] = samples_mcl['PARVAL'] / samples_mcl['comp_conc_val']

    print('Calculating z-scores and removing outliers \n')
    samples_mcl['zScore'] = stats.zscore(samples_mcl['mag'])
    samples_mcl = samples_mcl[(samples_mcl['zScore'] <= 3) & (
        samples_mcl['zScore'] >= -3)]

# LOAD GEO XY PATH
    geo_xy_path = bp / 'geotracker_xy'

    def create_geo_xy(p):  # simple function for loading gama tables

        try:

            df = pd.read_csv(p, sep='\t', lineterminator='\n', encoding='unicode_escape',
                             quotechar='"',  quoting=3,  on_bad_lines='warn')

            df['WID'] = df['GLOBAL_ID'] + '-' + df['FIELD_PT_NAME']
            columns = ['WID', 'LATITUDE', 'LONGITUDE']
            df = df[columns]

            return df

        except:
            print('Exception, no such file.')

    def concat_geo_xy(files):  # function to concat gama result datasets

        df_list = []

        for i in files:
            j = create_geo_xy(i)
            if j is not None:
                df_list.append(j)

        concatDF = pd.concat(df_list, axis=0)

        for df in df_list:
            del df

        return concatDF

    geo_xy_files = geo_xy_path.glob('**/*.zip')

    geo_xy = concat_geo_xy(geo_xy_files)

# load GAMA XY
    gama_xy_path = bp / "gama_xy\gama_location_construction_v2.zip"
    gama_xy = pd.read_table(gama_xy_path, sep='\t', encoding='unicode_escape')

    gama_xy.rename(columns={'GM_WELL_ID': 'WID', 'GM_LATITUDE': 'LATITUDE',
                   'GM_LONGITUDE': 'LONGITUDE'}, inplace=True)
    gama_xy_columns = ['WID', 'LATITUDE', 'LONGITUDE']
    gama_xy = gama_xy[gama_xy_columns]

# combine well location data into singular dataset
    wells = pd.concat([gama_xy, geo_xy], ignore_index=True)
    wells = wells.drop_duplicates(subset='WID')

# calculates the sum of magnitudes for each WID in the exceedences dataframe
    mags = samples_mcl.groupby('WID').mean()
    wells = wells.merge(mags, how='inner', left_on='WID', right_index=True)
    wells = wells.set_index('WID').sort_index()

# block scoring and proximity adjustment
    import geopandas as gpd

    wells = gpd.GeoDataFrame(wells, geometry=gpd.points_from_xy(
        x=wells.LONGITUDE, y=wells.LATITUDE, crs='EPSG:4269'))

    blocksPath = bp / r"blocks\blocks2020_screen.shp"
    blocks = gpd.read_file(blocksPath)
    blocks = blocks.set_index('GEOID20')
    blocks = blocks.sort_index(axis='index')

# reproject geometry into planar coords for accurate measurement. Maybe look for better distance preserving projection
    print('Reprojecting geometry')

    # reprojection to cartesian projection (PCS) required for distance values
    def cart_prj_geom(g):
        # EPSG:26911, cartesian projection NAD83 UTM 11N meters
        g['geometry'] = g['geometry'].to_crs(26911)

    for geo in [wells, blocks]:
        cart_prj_geom(geo)

    print("Running scores")
    import multiprocessing as mp

    parts = mp.cpu_count()

    blocksDGDF = dgpd.from_geopandas(blocks, npartitions=parts)

    res = blocksDGDF['geometry'].apply(func=calculate_distance).compute()
    blocks = blocks.merge(res.rename('scores'),
                          left_index=True, right_index=True)

    scores_dir = bp / 'scores'
    mkdir_except(scores_dir)
    scores_name = input('Type filename: ') + '.shp'
    scores_path = scores_dir / scores_name
    print('Saving scores to: {}'.format(scores_path))
    blocks.to_file(scores_path)

    wells_dir = bp / 'wells'
    mkdir_except(wells_dir)
    wells_name = 'wells' + scores_name
    wells_path = wells_dir / wells_name
    print('Saving wells used to: {}'.format(wells_path))
    wells.to_file(wells_path)

# written by Aaron Gaines from the Center for Geospatial Science and Technology at the California State University of Northridge