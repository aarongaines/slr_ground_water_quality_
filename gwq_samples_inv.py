import pandas as pd
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
        score = (stats.gmean(dist_df['adj_wgt']))

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