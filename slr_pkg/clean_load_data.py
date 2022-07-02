# Module file for slr_groud_water_quality repository data cleaning.
import pandas as pd


# Function to open data file.
def open_table(p, dtypes, date_cols, cols):

    """
    open_table() is a function that opens a csv file and returns a dataframe. 
    Will try to open the file with the default encoding, if that fails, will try with the unicode_escape encoding.

    ---------------------------------------------------------------------------------------------------------------------
    Args:
        p: path to file
        dtypes: dictionary of data types
        date_cols: list of columns to parse as dates
        cols: list of columns to use
    """
    try:
        df = pd.read_csv(p, sep='\t', dtype=dtypes, on_bad_lines='warn', parse_dates=date_cols, usecols=cols)
        return df
        
    except:
        try:
            df = pd.read_csv(p, sep='\t', dtype=dtypes, on_bad_lines='warn', encoding='unicode_escape', parse_dates=date_cols, usecols=cols)
            return df
            
        except:
            df = pd.read_csv(p, sep='\t', dtype=dtypes, engine='python', encoding='unicode_escape', on_bad_lines='warn', parse_dates=date_cols, usecols=cols)
            return df


# Class for opening and cleaning sample data.
class Sample_Data:

    # initialize class
    def __init__():
        pass


    # Function to open a geotracker edf file and return a dataframe.    
    def geotracker_df(p):

        # Dictionary of data types for columns in the geotracker data.
        geotracker_dtypes = {
        'GLOBAL_ID' : 'string',
        'FIELD_PT_NAME' : 'string',
        'PARLABEL' : 'string',
        'PARVAL' : 'Float64',
        'PARVQ' : 'string',
        'REPDL' : 'Float64',
        'UNITS' : 'string',
        }

        # Date column of geotracker data for open_table().
        geotracker_date = ['LOGDATE']

        # Columns of geotracker data for open_table().
        geotracker_cols = list(geotracker_dtypes.keys()) + geotracker_date

        # Returns dataframe from open_table() using parameters above.
        print('Loading Geotracker file. {}'.format(p))
        df = open_table(p, geotracker_dtypes, geotracker_date, geotracker_cols)
        # Create WID column.

        df['WID'] = df['GLOBAL_ID'] + '-' + df['FIELD_PT_NAME']
        df['WID'] = df['WID'].str.replace(' ','')

        # Drop unnecessary columns.
        df = df.drop(columns=['GLOBAL_ID', 'FIELD_PT_NAME'])


        return df


    # Function to open a gama sample file and return a dataframe.
    def gama_df(p):

        # Dictionary of data types for columns in the gama data.
        gama_dtypes = {
            'GM_WELL_ID' : 'string',
            'GM_CHEMICAL_VVL' : 'string',
            'GM_RESULT_MODIFIER' : 'string',
            'GM_RESULT' : 'Float64',
            'GM_RESULT_UNITS' : 'string',
            'GM_REPORTING_LIMIT' : 'Float64',
            }

        # Date column of gama data for open_table().
        gama_date = ['GM_SAMP_COLLECTION_DATE']

        # Columns of gama data for open_table().
        gama_cols = list(gama_dtypes.keys()) + gama_date

        # Returns dataframe from open_table() using parameters above.
        print('Loading GAMA file. {}'.format(p))
        df = open_table(p, gama_dtypes, gama_date, gama_cols)

        # Dictionary to rename gama columns to match df1.
        gama_to_edf_dict = {
            'GM_WELL_ID' : 'WID',
            'GM_CHEMICAL_VVL' : 'PARLABEL',
            'GM_RESULT_MODIFIER' : 'PARVQ',
            'GM_RESULT' : 'PARVAL',
            'GM_RESULT_UNITS' : 'UNITS',
            'GM_REPORTING_LIMIT' : 'REPDL',
            'GM_SAMP_COLLECTION_DATE' : 'LOGDATE',
        }

        # Rename gama columns to match df1.
        df = df.rename(columns=gama_to_edf_dict)

        df['WID'] = df['WID'].str.replace(' ','')

        return df


    # Function to concatenate gama and geotracker dataframes, then add a GID column.
    def concat_samples(df1, df2):

        # Concatenate gama_results and df1.
        print("Concatenating GAMA and Geotracker dataframes. \n")
        samples = pd.concat([df1, df2])

        # List of columns that require a value.
        samples_req_cols = ['WID','LOGDATE', 'PARLABEL', 'PARVAL']

        # Drops rows with missing values in required columns.
        print("Checking for missing values. \n")
        samples = samples.dropna(subset=samples_req_cols)

        # Set group ID with WID and LOGDATE.
        print("Creating group ID (GID). \n")
        samples['LOGDATE'] = samples['LOGDATE'].astype(str)
        samples['GID'] = list(zip(samples['WID'], samples['LOGDATE']))
        samples = samples.reset_index(drop=True)

        return samples


# Class for opening and cleaning well location data.
class Location_Data:

    # initialize class
    def __init__():
        pass