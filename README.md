# Data Links:

## 1. Data Portal Links

    a. GeoTracker

        - https://geotracker.waterboards.ca.gov/data_download_by_county

## 1. Samples

    a. GeoTracker

        - "https://geotracker.waterboards.ca.gov/data_download/edf_by_county/{}EDF.zip".format(CountyName)

    b. GAMA

        - 

# Script Descriptions

filter_samples.ipynb
----------------------------------------------------------------------------------------------------
This is a Jupyter notebook used to filter sample data based on changing parameters. This sample data is currently used to explore methods of interpolation between monitoring wells to determine groundwater quality along the coast.

download_samples_xy.py
----------------------------------------------------------------------------------------------------
When the script is ran, it will create folders for downloads, and download results and location data for Geotracker and GAMA. The GAMA website stores sample results for different departments separately and some counties do not have data for every department. The GAMA website does not throw HTTP errors when accessing a file that does not exist (i.e. department data for county doesnt exist), so python saves a corrupted zip file. This throws errors in the script, but is intended behavior and not a concern.

gwq_1.01.py (deprecated)
----------------------------------------------------------------------------------------------------
This is a ground water quality (GWQ) indicator scoring script. It looks at sample results from monitoring wells, comparing them to their maximum contaminant level (MCL) in order to determine the average quality of groundwater at a monitoring well. The file "MCLs.xlsx" is the MCL table that will be used by the script for running GWQ scores. Currently it only contains those contaminants that were used in the CES ground water qulity indicator, and the BTEX contaminants. Within the script itself you will find two variables that can be changed to alter the counties you wish to score and what sample dates you wish to use in the calculation.

The first variable is named 'county_names' and is a list of county names. The names must be in CamelCase and have no spaces. This will select what files are downloaded. However the script will use all files within the created folders for scoring. If something was previously downloaded but you do not want to use it in calculations it should be deleted first. This may be changed in the future to not be an issue. Currently the script is looking at all counties within the 263 meter screening area.

The second variable is named 'date' and is simply a string in YYYY-MM-DD format. This is the start date and will tell the script to only look at samples from that day to present. Currently the script is looking at samples from 2012-01-01 to present.

Sample results are loaded into memory. At this point samples are filtered by date. Sample results from the different datasets are then combined into one. A sample ID(SID) is created using well ID, date sample was taken, and chemical. This SID is used to remove duplicate sample results. The MCL table is joined to the sample results, keeping only samples of contaminant that have an MCL in the table. Then magnitudes of MCL exceedences are calculated for every sample result, dividing the results value by the MCL of the contaminant. A z-score is calculated for the magnitudes and is used to remove outliers. These samples are then grouped by well ID, taking the mean of the magnitudes of MCL exceedances. This is then joined to the well location data as the base weight for wells. 

Blocks are scored by all wells within 1000 meters. Base weights for wells are adjusted by their distance to a block by multiplying the base weight by 1, 0.5, 0.25, and 0.1 for distances within 250, 500, 750 and 1000 meters respectively. The mean of all adjusted weights for wells within 1000 meters of a block is the ground water quality indicator score for said block.
