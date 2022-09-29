import os, pandas as pd, slr_pkg.clean_load_data as cld, slr_pkg.para as para
from pathlib import Path


# Get current working directory
bp = Path(os.getcwd())

# Set base data directory.
dp  = bp / 'data'

# Set sample data directory.
edf_path = dp / 'geotracker_edf_results'
gama_path = dp / 'gama_results'

# Set location data directory.
geo_xy_path = dp / 'geotracker_xy'
gama_xy_path = dp / "gama_xy"

# Set results directory
results_path = bp / "results"

# Ask for county to gather data for.
# area = input('Enter county: ')
areas = ['Ventura','SanDiego', 'Kern', 'Imperial','SantaBarbara','LosAngeles']
# areas = ['LosAngeles']

for area in areas:

    # List of contaminants.
    chems = para.conts11

    samples = pd.read_csv(dp / '{}_clean_samples_elev_dtw.csv'.format(area))
    # subset of specific samples meeting parameters.
    spec_samples = samples.copy()

    # Select spec_samples taken since 2012.
    spec_samples = spec_samples.loc[spec_samples['LOGDATE'] >= '2012-01-01']

    # Select spec_samples with wells of "monitoring well" type.
    #spec_samples = spec_samples[(spec_samples['FIELD_PT_CLASS'] == 'MW') | (spec_samples['FIELD_PT_CLASS'] == 'MONITORING')].copy()
    

    # Select samples with contaminants of interest.
    spec_samples = spec_samples.loc[spec_samples['PARLABEL'].isin(chems)]

    # Create groups of spec_samples based on WID and PARLABEL(contaminant label).
    sample_groups = spec_samples.groupby(['WID'])['PARLABEL'].apply(list).reset_index()

    from collections import Counter


    def select_wells(row):
        wid = row['WID']
        counter = Counter(row['PARLABEL'])
        if len(counter) == len(chems):
            if all(i >= 4 for i in counter.values()):
                return  wid


    # Create mask of sample groups meeting parameter requirements.
    res = sample_groups.apply(select_wells, axis=1)

    # Use mask to select sample results from wells that meet parameter requirements.
    spec_samples = spec_samples[spec_samples['WID'].isin(res)]

    # Save sample results to csv.
    spec_samples.to_csv(results_path / '{}_MW_2012_spec_samples_{}.csv'.format(area.lower(), str(len(chems))))