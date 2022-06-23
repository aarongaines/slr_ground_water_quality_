from os import getcwd
from multiprocessing import cpu_count
from pathlib import Path
import multiprocessing as mp


bp = Path(getcwd())

# block scoring and proximity adjustment
import geopandas as gpd
print('Converting wells to GeoDataFrame \n')
wells = gpd.GeoDataFrame(wells, geometry=gpd.points_from_xy(
    x=wells.LONGITUDE, y=wells.LATITUDE, crs='EPSG:4269'))
print('Loading block data \n')
blocksPath = bp / r"blocks\blocks2020_screen.shp"
blocks = gpd.read_file(blocksPath)
blocks = blocks.set_index('GEOID20')
blocks = blocks.sort_index(axis='index')

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
        score = dist_df['adj_wgt'].mean()

    print(score)

    return score

# reproject geometry into planar coords for accurate measurement. Maybe look for better distance preserving projection
print('Reprojecting geometry')

# reprojection to cartesian projection (PCS) required for distance values
def cart_prj_geom(g):
    # EPSG:26911, cartesian projection NAD83 UTM 11N meters
    g['geometry'] = g['geometry'].to_crs(26911)

for geo in [wells, blocks]:
    cart_prj_geom(geo)

print("Running scores")
import multiprocessing as mp

parts = cpu_count()

blocksDGDF = dgpd.from_geopandas(blocks, npartitions=parts)

res = blocksDGDF['geometry'].apply(func=calculate_distance).compute()
blocks = blocks.merge(res.rename('scores'),
                        left_index=True, right_index=True)

scores_dir = bp / 'scores'
mkdir_except(scores_dir)
scores_name = input('Type file_name: ') + '.shp'
#scores_name = 'test_3_236m_mcl19.shp'
scores_path = scores_dir / scores_name
print('Saving scores to: {}'.format(scores_path))
blocks.to_file(scores_path)

wells_dir = bp / 'wells'
mkdir_except(wells_dir)
wells_name = 'wells' + scores_name
wells_path = wells_dir / wells_name
print('Saving wells used to: {}'.format(wells_path))
wells.to_file(wells_path)
print(res)