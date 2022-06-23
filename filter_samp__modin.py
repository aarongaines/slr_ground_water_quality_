import modin.pandas as mpd
import pandas as pd
from pathlib import Path
from itertools import combinations
from distributed import Client


bp = Path(r"E:\work\projects\coast_slr\scripts\ground_water_quality")

# Function to get select samples
"""
get_select_samples() is a function that looks counts how many sample groups have results for all contaminants in the input list. 

---------------------------------------------------------------------------------------------------------------------
Args:
    row: A row from the dataframe being applied to the function.
    contaminants: A list of contaminants to look for in the row.
"""

def get_select_samples(row, contaminants):

    count = 0

    for values in row:
        if all(item in values for item in contaminants):
            count += 1
    return count


if __name__ == "__main__":

    client = Client()

    # List of contaminants from CES Drinking Water Quality index plux BTEX and MTBE.
    contaminants_2 = [
        'AS',
        'BZ',
        'BZME',
        'CD',
        'CR6',
        'DBCP',
        'EBZ',
        'EDB',
        'NO3N',
        'MTBE',
        'PB',
        'PCATE',
        'PCE',
        'TCE',
        'TCPR123',
        'THM',
        'XYLENES',
        ]

    # Ask for county to gather data for.
    county = input('Enter county: ')

    # Function to open data file.
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


    def open_table(p, dtypes, date_cols, cols):

        try:
            df = pd.read_table(p, sep='\t', dtype=dtypes, parse_dates=date_cols,  usecols=cols)
            return df
            
        except:
            df = pd.read_table(p, sep='\t', dtype=dtypes, parse_dates=date_cols, usecols=cols, encoding='unicode_escape')
            return df


    # Create geotracker path.
    edf_path = bp / 'geotracker_edf_results'

    # Dictionary of data types for geotracker edf_results for open_table().
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

    print('Loading Geotracker EDF results \n')

    # create list of files to open
    edf_files = edf_path.glob('**/*{}*.txt'.format(county))

    # Use list comprehension to create a list of dataframes from the files list. Uses open_table() to open the files.
    edf_results_list = [open_table(i,geotracker_dtypes,geotracker_date,geotracker_cols) for i in edf_files]

    # Concatenate the list of dataframes into one dataframe if there are more than one.
    if len(edf_results_list) > 1:
        edf_results = pd.concat(edf_results_list)

    else:
        edf_results = edf_results_list[0]

    # Create WID column.
    edf_results['WID'] = edf_results['GLOBAL_ID'] + '-' + edf_results['FIELD_PT_NAME']

    # Drop unnecessary columns.
    edf_results = edf_results.drop(columns=['GLOBAL_ID', 'FIELD_PT_NAME'])

    # Set path of gama_results.
    gama_path = bp / 'gama_results'

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

    print('Loading GAMA results \n')

    # Create list of files to open.
    gama_files = gama_path.glob('**/*{}*.txt'.format(county.lower()))

    # Use list comprehension to create a list of dataframes from the files list. Uses open_table() to open the files.
    gama_results_list = [open_table(i,gama_dtypes,gama_date,gama_cols) for i in gama_files]

    # Concatenate the list of dataframes into one dataframe.
    gama_results = pd.concat(gama_results_list)

    # Dictionary to rename gama columns to match edf_results.
    gama_to_edf_dict = {
        'GM_WELL_ID' : 'WID',
        'GM_CHEMICAL_VVL' : 'PARLABEL',
        'GM_RESULT_MODIFIER' : 'PARVQ',
        'GM_RESULT' : 'PARVAL',
        'GM_RESULT_UNITS' : 'UNITS',
        'GM_REPORTING_LIMIT' : 'REPDL',
        'GM_SAMP_COLLECTION_DATE' : 'LOGDATE',
        }

    # Rename gama columns to match edf_results.
    print('Renaming GAMA columns \n')
    gama_results = gama_results.rename(columns=gama_to_edf_dict)

    # Concatenate gama_results and edf_results.
    print('Concatenating GAMA and EDF results \n')
    samples = pd.concat([edf_results, gama_results])

    # List of columns that require a value.
    samples_req_cols = ['LOGDATE', 'PARLABEL', 'PARVAL']

    # Drops rows with missing values in required columns.
    print('Dropping rows with missing values in required columns \n')
    samples = samples.dropna(subset=samples_req_cols)

    # Set multi index on WID and LOGDATE.
    print('Setting multi index on WID and LOGDATE \n')
    samples = samples.set_index(['WID', 'LOGDATE'])

    # Group samples by WID and LOGDATE apply list function to get list of PARLABELS for each group.
    print('Grouping samples by WID and LOGDATE \n')
    sample_groups = samples.groupby(['WID', 'LOGDATE'])['PARLABEL'].apply(list)
    
    print(sample_groups.head(), '\n')

    # Turn sample groups into a modin object.
    sample_groups_modin = mpd.DataFrame(sample_groups)

    #while len(contaminants_2) >= 10:

    contaminant_num = len(contaminants_2) - 1

    # Create list of contaminant combinations.
    combinations_list = list(combinations(contaminants_2, contaminant_num))

    # Create empty dictionary to store results.
    res_dict = {}

    # Get number of combinations.
    total = len(combinations_list)

    print('Number of combinations: ', total)

    # Create zero counter for progress.
    count = 0

    # Loop through combinations of contaminants.
    for contaminants in combinations_list:

        # Apply get_select_samples() function to sample groups. Use contaminants as argument to check against. Returns the number of times the contaminants are in a sample group.
        res = sample_groups_modin.apply(get_select_samples, contaminants=contaminants)

        # Add the number of times the contaminants are in a sample group to the dictionary with the contaminants as the key.
        res_dict[contaminants] = res[0]

        # Increment counter.
        count += 1

        # Print progress.
        percent = int((count/total)*100)

        # Prints progress if percent is divisible by 5.
        if percent % 5 == 0:
            print('{}%'.format(percent))

    print(res_dict, '\n')

    # Get max value of res_dict.
    max_value = max(res_dict.values())

    # Get key of res_dict with max value.
    max_key = max(res_dict, key=res_dict.get)

    # Print max value and key.
    print(max_key, max_value)

    contaminants_2 = max_key