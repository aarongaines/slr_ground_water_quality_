import modin.pandas as pd

date = input("Enter desired start date (YYYY-MM-DD): ")

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

