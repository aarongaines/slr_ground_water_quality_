import urllib.request
import urllib.error
from pathlib import Path
import os

# sets current working directory as working directory
bp = Path(os.getcwd())
print(bp)
#bp = Path(r'E:\work\projects\coast_slr\scripts\ground_water_quality')


# function to create folders and ignore if folder exists
def mkdir_except(folder_name):

    # try to make directory (folder_name), if it exists, ignore
    try:
        # create a directory and print the path
        os.mkdir(folder_name)
        print("Directory {} created".format(folder_name))

    except:
        print("Directory {} already exists".format(folder_name))


# Defines a function for downloading a url to a path.
def download_save_zip(url, folder_path):

    # Create a filename from the passed url.
    file_name = url.split('/')[-1]

    # Create path to save file.
    sp = folder_path / file_name

    # Skips file if it exists and prints message.
    if os.path.isfile(sp):
        print("{} already downloaded \n".format(sp))

    # Downloads file if it doesn't exist and prints message.
    else:
        # Tries to download file, if it fails, prints error message.
        try:
            # request url
            req = urllib.request.urlopen(url)

            # Checks if request is not a text file.
            # If not it will download the file, otherwise it will print an error message.
            # This if statement does not seem to be working and downloads regardless.
            if not [i for i in req.getheaders() if 'text/html' in i]:
                print('Downloading: {} '.format(url))
                # Reads the request and saves it as a variable
                data = req.read()
                req.close()

                # Saves the files to the specified path created above.
                local = open(sp, 'wb')
                local.write(data)
                local.close()

        except urllib.error.HTTPError:
            print("HTTPError for {} ".format(url))

        except urllib.error.URLError:
            print("URLError for {} ".format(url))


# List of counties for data to be downloaded. Below are counties within the 263m screening area.
"""county_names = [
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
    'Ventura',
]"""

county_names = [
    'Imperial',
    'Kern',
    'SantaBarbara',
    'LosAngeles',
]

# Variables for beginning of geotracker urls.
geotracker_edf_url = "https://geotracker.waterboards.ca.gov/data_download/edf_by_county/"
geotracker_xy_url = "https://geotracker.waterboards.ca.gov/data_download/geo_by_county/"
geotracker_z_url = "https://geotracker.waterboards.ca.gov/data_download/geo_by_county/"

# Variables for the end of GeoTracker urls.
edf_name = 'EDF.zip'
xy_name = 'GeoXY.zip'
z_name = 'GeoZ.zip'

# Create the path for geotracker downloads (edf AND xy)
geo_edf_path = bp / 'geotracker_edf_results'
geo_xy_path = bp / 'geotracker_xy'
geo_z_path = bp / 'geotracker_z'

# Call function to create directories for geotracker downloads.
mkdir_except(geo_edf_path)
mkdir_except(geo_xy_path)
mkdir_except(geo_z_path)


"""
The download_geotracker() function below creates a list of urls for GeoTracker downloads. It uses the 
county_names list to create the urls. The urls are create by appending the county name to the beginning of the urls. 
Then the ending of the url is appened to the url depending on the type of download. These urls are
appended to the urlList. The urlList is then looped through and the files are downloaded by calling the
download_save_zip() function.

----------------------------------------------------------------------------------------------------

Variables:
    url_start: The beginning of the url.
    clist: The list of counties.
    url_alt: The ending of the url.
    folder_path: The path to save the files.
"""


def download_geotracker(url_start, clist, url_alt, folder_path):
    # Create empty list for urls.
    urlList = []

    # Create urls for geotracker downloads and append to urlList.
    for i in clist:

        # url is the start of the url + the county name + the end of the url.
        url = url_start + i + url_alt

        # Append url to urlList.
        urlList.append(url)

    # Loop through urlList and download files.
    for j in urlList:
        # Call function to download files. j is the url parameter,
        # folder_path is the fodler_path parameter.
        download_save_zip(j, folder_path)


# Prints message and calls function to download geotracker sample resutls (edf)
print('Downloading GeoTracker EDF Results Data: \n')
download_geotracker(geotracker_edf_url, county_names, edf_name, geo_edf_path)

# Prints message and calls fuction to geotracker sample locations (xy)
print('Downloading GeoTracker XY Data: \n')
download_geotracker(geotracker_xy_url, county_names, xy_name, geo_xy_path)

print('Downloading GeoTracker Z Data: \n')
download_save_zip(geotracker_z_url, county_names, z_name, geo_z_path)

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
    'wrd_',
]


"""
download_gama_results() function downloads the GAMA results data.  It uses the gama_alt_urls list 
and county_names list to create the urls.  The urls are create by a prefix, starting url, alt url, 
county name and suffix.  The urls are appended to the urlList. The urlList is then looped through 
and the files are downloaded by calling the download_save_zip() function. 

----------------------------------------------------------------------------------------------------

Variables:
    url_start: The beginning of the url.
    clist: The list of counties.
    alt_urls: The list of alternate urls for different datasets.
    dl_path: The path to save the files to.
"""


def download_gama_results(start_url, clist, alt_urls, dl_path):
    # Create empty list for urls.
    url_list = []
    # Prefix and suffix for urls.
    pref = 'gama_'
    suf = '_v2.zip'

    # Create urls for gama downloads and append to urlList.
    # Loop through county list.
    for c in clist:
        # set county name to lowercase
        c = c.lower()

        # Loop through alt_urls list.
        for au in alt_urls:
            # url is the start of the url + the prefix + the alt url + the county name + the suffix.
            url = start_url + pref + au + c + suf

            # Append url to urlList.
            url_list.append(url)

    # Loop through urlList and download files using download_save_zip() function. url is the url parameter,
    # dl_path is the fodler_path parameter.
    for url in url_list:
        download_save_zip(url, dl_path)


# Prints message and runs function to download gama sample results data.
print('Downloading GAMA sample results: \n')
download_gama_results(gama_base_url, county_names,
                      gama_alt_urls, gama_res_path)

# Set base GAMA XY url and path for downloads.
gama_xy_path = bp / 'gama_xy'
mkdir_except(gama_xy_path)

# URL for GAMA XY data
gama_xy_url = 'https://gamagroundwater.waterboards.ca.gov/gama/data_download/gama_location_construction_v2.zip'

# Prints message and runs download function for GAMA xy data.
print('Downloading GAMA XY data: \n')
gama_xy = download_save_zip(gama_xy_url, gama_xy_path)