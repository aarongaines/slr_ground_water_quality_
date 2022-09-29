import os, pandas as pd, slr_pkg.clean_load_data as cld, slr_pkg.para as para
from collections import Counter
from pathlib import Path

# Get current working directory
bp = Path(os.getcwd())

# Set sample data directory.
edf_path = bp / 'geotracker_edf_results'
gama_path = bp / 'gama_results'

# Set location data directory.
geo_xy_path = bp / 'geotracker_xy'
gama_xy_path = bp / "gama_xy"

# Set results directory
results_path = bp / "results"

# Create path to mcl table.
mcl_path = bp / 'MCLs.xlsx'

# Ask for county to gather data for.
# area = input('Enter county: ')
# areas = ['Ventura','SanDiego', 'Kern', 'Imperial','SantaBarbara', 'LosAngeles']
area = 'Kern'

# List of contaminants.
chems = para.conts11

# Create lists of files to load.
edf_files = edf_path.glob('**/*{}*.zip'.format(area))
gama_files = gama_path.glob('**/*{}*.zip'.format(area.lower()))

# Loads sample data using clean_load_data.py Sample_Data class.
samples = cld.Sample_Data.full_dataset(edf_files, gama_files)

# Create list of files to load.
geo_xy_files = geo_xy_path.glob('**/*{}*.zip'.format(area))
gama_xy_files = gama_xy_path.glob('**/*.zip')

# Loads location data using clean_load_data.py Location_Data class.
locations = cld.Location_Data.full_dataset(geo_xy_files, gama_xy_files)

# Join well location data to sample results.
samples = samples.merge(locations, left_on='WID', right_on='WID', how='inner')

# Open mcl table.
print('Loading MCL table \n')
mcl = pd.read_excel(mcl_path,sheet_name='MCL', engine='openpyxl')

# join MCL values to sample results
print('Joining MCL values to samples \n')
samples = samples.merge(mcl, left_on='PARLABEL', right_on='chem_abrv', how='inner')

# Load conversion tables.
metric_conversion = pd.read_excel(bp / 'unit_conversion.xlsx', sheet_name='metric')

# join coversion factors to samples based on sample unit.
samples = samples.merge(metric_conversion, how='inner', left_on='UNITS', right_on='start_unit')

# Create mask for samples with MCL units in UG/L and converts sample result units to UG/L.
mask = samples['UNITS'] != samples['units']

# Multiply sample results by conversion factor.
samples.loc[mask, 'PARVAL'] = samples['PARVAL'] * samples['coef']
samples['UNITS'] = 'UG/L'

# Drop columns that are not needed.
samples.drop(columns=['REPDL','GID', 'SID','chem_abrv', 'units','comp_conc_type','start_unit', 'coef'], inplace=True)

# Create exceedence attribute, true if sample result exceeds reporting limit.
samples['exceedence'] = samples['PARVAL'] > samples['comp_conc_val']

# Create magnitude attribute. Sample result value divided by the comparison concentration value (MCL or Action level) minus 1.
samples['magnitude'] = (samples['PARVAL'] / samples['comp_conc_val']) - 1

# subset of specific samples meeting parameters.
spec_samples = samples

# Select spec_samples taken since 2010.
spec_samples = spec_samples.loc[spec_samples['LOGDATE'] >= '2012-01-01']

# Select samples with contaminants of interest.
spec_samples = spec_samples.loc[spec_samples['PARLABEL'].isin(chems)]

# Create groups of spec_samples based on WID and PARLABEL(contaminant label).
sample_groups = spec_samples.groupby(['WID'])['PARLABEL'].apply(list).reset_index()


def select_wells(row, para1, para2):

    wid = row['WID']
    counter = Counter(row['PARLABEL'])

    if len(counter) >= para1:
        if all(i >= para2 for i in counter.values()):
            return  wid


para1 = len(chems)/2
para2 = 4

# Create mask of sample groups meeting parameter requirements.
res = sample_groups.apply(select_wells, axis=1, para1=para1, para2=para2)

# Use mask to select sample results from wells that meet parameter requirements.
spec_samples = spec_samples[spec_samples['WID'].isin(res)]
spec_samples.reset_index(drop=True, inplace=True)

print(len(spec_samples['WID'].unique()))
print(len(spec_samples))
# Save sample results to csv.
#spec_samples.to_csv(results_path / '{}_dd_spec_samples_{}.csv'.format(area.lower(), str(len(chems))))