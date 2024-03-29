# Module file for slr_groud_water_quality repository data cleaning.
import pandas as pd
import geopandas as gpd


# Function to open data file.
def open_table(p, dtypes, cols, date_cols = None):

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
            try:
                df = pd.read_csv(p, sep='\t', dtype=dtypes, engine='python', encoding='unicode_escape', quotechar='"', quoting=3, on_bad_lines='warn', parse_dates=date_cols, usecols=cols)
                return df

            except:
                try:
                    df = pd.read_csv(p, sep='\t', dtype=dtypes, engine='python', encoding='unicode_escape', encoding_errors='backslashreplace', on_bad_lines='warn', parse_dates=date_cols, usecols=cols)
                    return df

                except:
                    try:
                        df = pd.read_csv(p, sep='\t', dtype=dtypes, usecols= cols, lineterminator='\n', encoding='unicode_escape',
                                quotechar='"',  quoting=3,  on_bad_lines='warn')
                        return df

                    except:
                        df = pd.read_table(p, sep='\t', dtype=dtypes, usecols= cols, encoding='unicode_escape')
                        return df


# Class for opening and cleaning sample data as pandas dataframe.
class Sample_Data:

    # initialize class
    def __init__():
        pass


    # Function to open a geotracker edf file and return a dataframe.    
    def geotracker_df(p):

        # Dictionary of data types for columns in the geotracker data.
        dtypes = {
        'GLOBAL_ID' : 'string',
        'FIELD_PT_NAME' : 'string',
        'PARLABEL' : 'string',
        'PARVAL' : 'Float64',
        'PARVQ' : 'string',
        'REPDL' : 'Float64',
        'UNITS' : 'string',
        }

        # Date column of geotracker data for open_table().
        date = ['LOGDATE']

        # Columns of geotracker data for open_table().
        cols = list(dtypes.keys()) + date

        # Returns dataframe from open_table() using parameters above.
        print('Loading Geotracker file: {} '.format(p))
        df = open_table(p, dtypes=dtypes, date_cols=date, cols=cols)

        # Create WID column.
        print('Creating WID column... ')
        df['WID'] = df['GLOBAL_ID'] + '-' + df['FIELD_PT_NAME']
        df['WID'] = df['WID'].str.replace(' ','')

        # Drop unnecessary columns.
        df = df.drop(columns=['GLOBAL_ID', 'FIELD_PT_NAME'])

        return df


    # Function to open a gama sample file and return a dataframe.
    def gama_df(p):

        # Dictionary of data types for columns in the gama data.
        dtypes = {
            'GM_WELL_ID' : 'string',
            'GM_CHEMICAL_VVL' : 'string',
            'GM_RESULT_MODIFIER' : 'string',
            'GM_RESULT' : 'Float64',
            'GM_RESULT_UNITS' : 'string',
            'GM_REPORTING_LIMIT' : 'Float64',
            }

        # Date column of gama data for open_table().
        date = ['GM_SAMP_COLLECTION_DATE']

        # Columns of gama data for open_table().
        cols = list(dtypes.keys()) + date

        # Returns dataframe from open_table() using parameters above.
        print('Loading GAMA file: {} '.format(p))
        df = open_table(p, dtypes = dtypes, cols = cols, date_cols = date)

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
        print('Renaming GAMA columns... ')
        df = df.rename(columns=gama_to_edf_dict)

        df['WID'] = df['WID'].str.replace(' ','')

        return df


    # Function to concatenate gama and geotracker dataframes, then add a GID column.
    def concat_samples(df1, df2):

        # Concatenate gama_results and df1.
        print("Concatenating GAMA and Geotracker dataframes...") 
        samples = pd.concat([df1, df2])

        # List of columns that require a value.
        samples_req_cols = ['WID','LOGDATE', 'PARLABEL', 'PARVAL']

        # Drops rows with missing values in required columns.
        print("Checking for missing values... ")
        samples = samples.dropna(subset=samples_req_cols)

        # Set group ID with WID and LOGDATE.
        print("Creating group ID (GID)... ")
        samples['LOGDATE'] = pd.to_datetime(samples['LOGDATE'].astype(str), errors='coerce', format='%Y-%m-%d')
        samples['GID'] = list(zip(samples['WID'], samples['LOGDATE']))

        # Set sample ID with GID and PARLABEL.
        print("Creating sample ID (SID)... ")
        samples['SID'] = list(zip(samples['GID'], samples['PARLABEL']))

        print("Sorting samples... ")
        samples = samples.sort_values(by=['PARVAL'], ascending=False)

        print("Dropping duplicate samples... \n")
        samples = samples.drop_duplicates(subset=['SID'], keep='first')
        samples = samples.dropna(subset=['LOGDATE'])
        samples = samples.reset_index(drop=True)



        return samples


    # Function to return a dataframe of all samples. Takes list of sample files as input.
    def full_dataset(list_geotracker, list_gama):
        """
        Function to open a list of files and return a dataframe.
        """
        # Initialize dataframe.
        geo = pd.DataFrame()
        gama = pd.DataFrame()

        # Loop through list of files.
        for p in list_geotracker:
            # Open file.
            geo_temp = Sample_Data.geotracker_df(p)
            # Concatenate dataframes.
            geo = pd.concat([geo, geo_temp])


        for p in list_gama:
            # Open file.
            gama_temp = Sample_Data.gama_df(p)
            # Concatenate dataframes.
            gama = pd.concat([gama, gama_temp])


        # Concatenate gama and geotracker dataframes.
        samples = Sample_Data.concat_samples(geo, gama)
        return samples


# Class for opening and cleaning well location data as pandas dataframe.
class Location_Data:

    # initialize class
    def __init__():
        pass
    

    # Function to open a geotracker well location file and return a dataframe.
    def geotracker_df(p):
        
        dtypes = {
        'GLOBAL_ID' : 'string',
        'FIELD_PT_NAME' : 'string',
        'FIELD_PT_CLASS' : 'string',
        'LATITUDE' : 'Float64',
        'LONGITUDE' : 'Float64',
        }

        cols = list(dtypes.keys())

        print('Loading Geotracker file: {} '.format(p))
        df = open_table(p, dtypes = dtypes, cols = cols)

        df['WID'] = df['GLOBAL_ID'] + '-' + df['FIELD_PT_NAME']
        df['WID'] = df['WID'].str.replace(' ','')

        df = df.drop(columns=['GLOBAL_ID', 'FIELD_PT_NAME'])

        return df


    # Function to open a gama well location file and return a dataframe.
    def gama_df(p):

        dtypes = {
            'GM_WELL_ID' : 'string',
            'GM_WELL_CATEGORY' : 'string',
            'GM_LATITUDE' : 'Float64',
            'GM_LONGITUDE' : 'Float64',
        }

        print('Loading GAMA file: {} '.format(p))
        gama_geo_dict = {
            'GM_WELL_ID' : 'WID',
            'GM_WELL_CATEGORY' : 'FIELD_PT_CLASS',
            'GM_LATITUDE' : 'LATITUDE',
            'GM_LONGITUDE' : 'LONGITUDE',
        }

        cols = list(dtypes.keys())
        df = open_table(p, dtypes = dtypes, cols = cols)
        df = df.rename(columns=gama_geo_dict)

        return df


    # Function to concatenate gama and geotracker dataframes.
    def concat_df(df1, df2):
            
            # Concatenate gama_results and df1.
            print("Concatenating GAMA and Geotracker dataframes...")
            df = pd.concat([df1, df2])
    
            # List of columns that require a value.
            req_cols = ['WID','LATITUDE', 'LONGITUDE']
    
            # Drops rows with missing values in required columns.
            print("Checking for missing values... ")
            df = df.dropna(subset=req_cols)

            # Drop duplicate WIDs and reset index.
            print("Removing duplicate WIDs...")
            df = df.drop_duplicates(subset=['WID'], keep='first')
            df = df.reset_index(drop=True)

            return df


    # Function to return a dataframe of well locations. Takes list of well location files as input.
    def full_dataset(list_geotracker, list_gama):
        """
        Function to open a list of files and return a dataframe.
        """
        # Initialize dataframe.
        geo = pd.DataFrame()
        gama = pd.DataFrame()

        # Loop through list of files.
        for p in list_geotracker:
            # Open file.
            geo_temp = Location_Data.geotracker_df(p)
            # Concatenate dataframes.
            geo = pd.concat([geo, geo_temp])

        print('\n')

        for p in list_gama:
            # Open file.
            gama_temp = Location_Data.gama_df(p)
            # Concatenate dataframes.
            gama = pd.concat([gama, gama_temp])

        print('\n')
        
        # Concatenate gama and geotracker dataframes.
        df = Location_Data.concat_df(geo, gama)
        return df


# Class for opening and cleaning well depth to water data as pandas dataframe.
class Depth_Data:

    def __init__():
        pass


# Class for openening and cleaning UST data as geopandas geodataframe.
class UST_Data:

    def __init__():
        pass


    # Function to open geotracker UST data.
    def geotracker(p):

        # Dictionary of data types for columns in the geotracker data.
        geo_dtypes = {
            "FACILITY_ID" : "string",
            "BUSINESS_NAME" : "string",
            "ADDRESS" : "string",
            "CITY" : "string",
            "ZIP" : "string",
            "COUNTY" : "string",
            "LATITUDE" : "float64",
            "LONGITUDE" : "float64",
        }
        geo_cols = list(geo_dtypes.keys())

        # Returns dataframe from open_table() using parameters above.
        print('Loading Geotracker file. {}'.format(p))
        df = open_table(p, geo_dtypes, geo_cols)

        # Convert column names to match usepa.
        geo_to_usepa = {
            "FACILITY_ID" : "Facility_I",
            "BUSINESS_NAME" : "Name",
            "ADDRESS" : "Address",
            "CITY" : "City",
            "ZIP" : "Zip_Code",
            "COUNTY" : "County",
            "LATITUDE" : "Latitude",
            "LONGITUDE" : "Longitude",
        }

        df = df.rename(columns=geo_to_usepa)

        df['Open_USTs'] = 1

        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude), crs='EPSG:4326')

        return gdf


    # Function to open USEPA UST data.
    def usepa(p):

        # Dictionary of data types for columns in the usepa UST data.
        usepa_dtypes = {
            'Facility_I' : 'string',
            'Name' : 'string',
            'Address' : 'string',
            'City' : 'string',
            'County' : 'string',
            'Zip_Code' : 'string',
            'Latitude' : 'float64',
            'Longitude' : 'float64',
            'Open_USTs' : 'float64',
            'geometry' : 'geometry',
        }

        usepa_cols = list(usepa_dtypes.keys())

        # Returns dataframe from open_table() using parameters above.
        print('Loading USEPA UST file. {}'.format(p))
        
        gdf = gpd.read_file(p, dtypes = usepa_dtypes,usecols = usepa_cols)

        # Drops all columns not needed.
        for column in gdf.columns:
            if column not in usepa_cols:
                gdf = gdf.drop(column, axis=1)

        return gdf


    # Function to concat geotracker and USEPA geodataframes.
    def concat_usts(gdf1, gdf2):
            
        # Concatenate gama_results and df1.
        print("Concatenating UST dataframes. \n")
        ust = pd.concat([gdf1, gdf2])

        ust = ust.sort_values(by=['Open_USTs'], ascending=False)
        ust = ust.drop_duplicates(subset=['Address'], keep='first')
        ust = ust.reset_index(drop=True)

        return ust


# Class for opening and cleaning cleanup data as geopandas geodataframe.
class Cleanup_Data:

    def __init__():
        pass
    
    # Function to open geotracker cleanup data.
    def geotracker(p):

        cleanups_dtypes = {
            'GLOBAL_ID' : 'string',
            'BUSINESS_NAME' : 'string',
            'STREET_NUMBER' : 'string',
            'STREET_NAME' : 'string',
            'CITY' : 'string',
            'ZIP' : 'string',
            'CASE_TYPE' : 'string',
            'STATUS' : 'string',
        }

        cleanups_columns = list(cleanups_dtypes.keys())

        # Returns dataframe from open_table() using parameters above.
        print('Loading Cleanup file. {}'.format(p))
        df = open_table(p, dtypes = cleanups_dtypes, cols = cleanups_columns)

        return df


class Merged_Data:

    def __init__():
        pass

# 