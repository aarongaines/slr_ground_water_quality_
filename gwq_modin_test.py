import modin.pandas as pd
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
        try:
            adj_wgt = wells['mag'].loc[row.name] * 1
        except:
            print("Sample WID not found in location data")
            adj_wgt = None

    elif 250 <= row.values < 500:
        try:
            adj_wgt = wells['mag'].loc[row.name] * 0.5
        except:
            print("Sample WID not found in location data")
            adj_wgt = None

    elif 500 <= row.values < 750:
        try:
            adj_wgt = wells['mag'].loc[row.name] * 0.25
        except:
            print("Sample WID not found in location data")
            adj_wgt = None

    elif 750 <= row.values <= 1000:
        try:
            adj_wgt = wells['mag'].loc[row.name] * 0.1
        except:
            print("Sample WID not found in location data")
            adj_wgt = None

    return adj_wgt


def calculate_distance(row):

    dist_list = wells.distance(row)
    dist_list = dist_list[dist_list <= 1000]

    if dist_list.empty:
        score = 0

    else:
        frame = {'dist': dist_list}
        dist_df = gpd.GeoDataFrame(frame)

        dist_df['adj_wgt'] = dist_df.apply(axis=1, func=weight_proxy)
        dist_df['adj_wgt'] = dist_df['adj_wgt'].astype(float)
        score = dist_df['adj_wgt'].mean()

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

    def download_save_zip(url, folder_path):  # function for downloading url to a path

        file_name = url.split('/')[-1]
        sp = folder_path / file_name

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

    date = input("Enter desired start date (YYYY-MM-DD): ")
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
            download_save_zip(j, folder_path)

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
            download_save_zip(url, dl_path)


# Runs downloads for GAMA sample results
    print('Downloading GAMA sample results: \n')
    dl_gama_results(gama_base_url, county_names, gama_alt_urls, gama_res_path)

# set base GAMA xy url and path for downloads
    gama_xy_url = 'https://gamagroundwater.waterboards.ca.gov/gama/data_download/gama_location_construction_v2.zip'
    gama_xy_path = bp / 'gama_xy'
    mkdir_except(gama_xy_path)

# runs download for GAMA xy data
    print('Downloading GAMA XY data: \n')
    gama_xy = download_save_zip(gama_xy_url, gama_xy_path)


    # gama sample loading
    def sid_col(df):
        df['SID'] = df['WID'].astype(
            str) + ' ' + df['LOGDATE'].astype(str) + ' ' + df['PARLABEL'].astype(str)

    def clean_filter_gama_resutls(df):

        column_dict = {'GM_WELL_ID': 'WID', 'GM_CHEMICAL_VVL': 'PARLABEL','GM_RESULT_MODIFIER': 'PARVQ','GM_RESULT': 'PARVAL','GM_REPORTING_LIMIT':'REPDL',
                'GM_SAMP_COLLECTION_DATE': 'LOGDATE', 'GM_RESULT_UNITS': 'UNITS'}

        df = df.rename(columns=column_dict)

        columns = ['WID', 'PARLABEL', 'PARVQ', 'PARVAL', 'REPDL', 'LOGDATE', 'UNITS', 'SID']

        df = df[
            (df['LOGDATE'] >= date) &
            (df['PARVQ'] != '<') &
            (df['PARVAL'] != 0) &
            (df['PARVAL'] > df['REPDL'])
        ]

        sid_col(df)
        df = df[columns]
        return df

    def create_gama_table(p): # loads gama results data to dataframe

        print('Loading: {} '.format(p))

        try:
            df = pd.read_csv(p, sep='\t', lineterminator='\n', encoding='unicode_escape', low_memory=False, parse_dates=['GM_SAMP_COLLECTION_DATE'])
            print('Renaming columns and creating SIDs: \n')
            df = clean_filter_gama_resutls(df)
            
        except:
            try:
                df = pd.read_csv(p, sep='\t', lineterminator='\n', low_memory=False, parse_dates=['GM_SAMP_COLLECTION_DATE'])
                print('Renaming columns and creating SIDs: \n')
                df = clean_filter_gama_resutls(df)

            except:
                print('Corrupt: {} '.format(p))
                df = None

        return df


    def concat_gama_data(files): # function to concat gama result datasets

        df_list = []

        for i in files:
            j = create_gama_table(i)
            if j is not None:
                df_list.append(j)

        print('\nCombining GAMA sample results: \n')

        concatDF = pd.concat(df_list, axis=0)
        for df in df_list:
            del df
        return concatDF


    # collect gama results files and concat them
    print('Loading GAMA sample results: \n')
    gama_files = gama_res_path.glob('**/*.zip')
    gama_results = concat_gama_data(gama_files)


    # edf sample loading

    def clean_filter_edf_resutls(df):

        columns = ['WID', 'PARLABEL', 'PARVQ', 'PARVAL', 'REPDL', 'LOGDATE', 'UNITS', 'SID']

        df = df[
            (df['LOGDATE'] >= date) &
            (df['PARVQ'] != '<') &
            (df['PARVAL'] != 0) &
            (df['PARVAL'] > df['REPDL'])
        ]
        df['WID'] = df['GLOBAL_ID'] + '-' + df['FIELD_PT_NAME']
        sid_col(df)
        df = df[columns]
        return df

    def create_edf_table(p):

        print('Loading: {} '.format(p))

        try:
            df = pd.read_csv(p, sep='\t', lineterminator='\n', encoding='unicode_escape', parse_dates=[
                'LOGDATE'])
            print('Renaming columns and creating SIDs: \n')
            df = clean_filter_edf_resutls(df)

        except:
            try:
                df = pd.read_csv(p, sep='\t', lineterminator='\n', parse_dates=[
                    'LOGDATE'])
                print('Renaming columns and creating SIDs: \n')
                df = clean_filter_edf_resutls(df)

            except:
                print('Corrupt: {} '.format(p))
                df = None

        return df


    def concat_geo_data(files): # function to concat gama result datasets

        df_list = []

        for i in files:
            j = create_edf_table(i)

            if j is not None:
                df_list.append(j)

        print('Combining Geotracker EDF sample results: \n')

        concat_df = pd.concat(df_list, axis=0)

        for df in df_list:
            del df

        return concat_df


    # Load and combine Geotracker EDF results
    print('Loading Geotracker EDF results \n')

    edf_files = geo_edf_path.glob('**/*.zip')
    edf_results = concat_geo_data(edf_files)


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

    print('Loading MCL table \n')
    mcl_path = bp / 'MCL_list_1.xlsx'
    mcl = pd.read_excel(mcl_path, engine='openpyxl')
    mcl = mcl.set_index('chem_abrv')

# join MCL values to sample results
    print('Joining MCL values to samples \n')
    samples_mcl = pd.merge(samples, mcl, left_on='PARLABEL', right_index=True)
    del samples

    samples_mcl = samples_mcl.reset_index(drop=True)

# set of MCL exceedences and magnitudes. Removes outliers with z-score
    print('Selecting MCL Exceedances \n')
    samples_mcl = samples_mcl[samples_mcl['PARVAL']
                              > samples_mcl['comp_conc_val']]

    print('Calculating magnitudes of MCL exceedances \n')
    samples_mcl['mag'] = samples_mcl['PARVAL'] / samples_mcl['comp_conc_val']


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
    print('Loading Geotracker XY \n')
    geo_xy = concat_geo_xy(geo_xy_files)

# load GAMA XY
    print('Loading GAMA XY \n')
    gama_xy_path = bp / "gama_xy\gama_location_construction_v2.zip"
    gama_xy = pd.read_table(gama_xy_path, sep='\t', encoding='unicode_escape')

    gama_xy.rename(columns={'GM_WELL_ID': 'WID', 'GM_LATITUDE': 'LATITUDE',
                   'GM_LONGITUDE': 'LONGITUDE'}, inplace=True)
    gama_xy_columns = ['WID', 'LATITUDE', 'LONGITUDE']
    gama_xy = gama_xy[gama_xy_columns]

# combine well location data into singular dataset
    print('Combining GAMA and Geotracker XY \n')
    wells = pd.concat([gama_xy, geo_xy], ignore_index=True)
    wells = wells.drop_duplicates(subset='WID')

# calculates the sum of magnitudes for each WID in the exceedences dataframe
    print('Calculating magnitudes for each WID \n')
    print(samples_mcl.head())
    mags = samples_mcl.groupby(['WID']).mag.apply(stats.gmean)
    print('Merging geometric mean magnitudes to wells \n')
    wells = wells.merge(mags, how='inner', left_on='WID', right_index=True)
    wells = wells.set_index('WID').sort_index()

# block scoring and proximity adjustment
    import geopandas as gpd
    print('Converting wells to GeoDataFrame \n')
    wells = gpd.GeoDataFrame(wells, geometry=gpd.points_from_xy(
        x=wells.LONGITUDE, y=wells.LATITUDE, crs='EPSG:4269'))
    print('Loading block data \n')
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
    scores_name = input('Type file_name: ') + '.shp'
    #scores_name = 'test_3_236m_mcl19.shp'
    scores_path = scores_dir / scores_name
    print('Saving scores to: {}'.format(scores_path))
    blocks.to_file(scores_path)

    wells_dir = bp / 'wells'
    mkdir_except(wells_dir)
    wells_name = 'wells' + scores_name
    wells_path = wells_dir / wells_name
    print('Saving wells used to: {}'.format(wells_path))
    wells.to_file(wells_path)
    print(res)

# written by Aaron Gaines from the Center for Geospatial Science and Technology at California State University of Northridge