# Module file for slr_groud_water_quality repository
import pandas as pd

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

# Function to open data file.
def open_table(p, dtypes, date_cols, cols):

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


class Samples:


    def __init__():
        pass


    # Function to open the geotracker edf file and return a dataframe.    
    def geotracker_df(p):

        geotracker_dtypes = {
        'GLOBAL_ID' : 'string',
        'FIELD_PT_NAME' : 'string',
        'PARLABEL' : 'string',
        'PARVAL' : 'Float64',
        'PARVQ' : 'string',
        'REPDL' : 'Float64',
        'UNITS' : 'string',
        }

        # Date column of geotracker edf_results for open_table().
        geotracker_date = ['LOGDATE']

        # Columns of geotracker edf_results for open_table().
        geotracker_cols = list(geotracker_dtypes.keys()) + geotracker_date

        # Returns dataframe from open_table() using parameters above.
        return open_table(p, geotracker_dtypes, geotracker_date, geotracker_cols)

    def gama_df(p):

        # Dictionary of data types for gama_results for open_table().
        gama_dtypes = {
            'GM_WELL_ID' : 'string',
            'GM_CHEMICAL_VVL' : 'string',
            'GM_RESULT_MODIFIER' : 'string',
            'GM_RESULT' : 'Float64',
            'GM_RESULT_UNITS' : 'string',
            'GM_REPORTING_LIMIT' : 'Float64',
            }

        # Date column of gama_results for open_table().
        gama_date = ['GM_SAMP_COLLECTION_DATE']

        # Columns of gama_results for open_table().
        gama_cols = list(gama_dtypes.keys()) + gama_date

        return open_table(p, gama_dtypes, gama_date, gama_cols)