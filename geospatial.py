import geopandas as gpd
import sys
import os

def intersect_regions(data_path):
    """Associate reaches with VT biophysical regions.

    Args:
        data_path (str): Path to data folder.  Could do this more 
        directly, but keeping consistent format with the other 
        functions of this module.
    """
    # Load the reaches and VT biophysical regions
    print('Loading reach data...')
    reach_path = os.path.join(os.path.dirname(data_path), 'merged_data', 'geospatial', 'reach_metadata.gpkg')
    reach_gdf = gpd.read_file(reach_path)

    print('Loading biophysical regions...')
    regions_path = os.path.join(os.path.dirname(data_path), 'merged_data', 'geospatial', 'Vermont_Biophysical_Regions.shp')
    regions_gdf = gpd.read_file(regions_path)[['NAME', 'geometry']]
    regions_gdf = regions_gdf.to_crs(reach_gdf.crs)

    # Perform the intersection
    reach_gdf = gpd.overlay(reach_gdf, regions_gdf, how='intersection')
    reach_gdf['clip_len'] = reach_gdf.length
    reach_gdf = reach_gdf.sort_values(by='clip_len', ascending=False)
    reach_gdf = reach_gdf.drop_duplicates(subset='Code', keep='first')
    reach_gdf = reach_gdf[['Code', 'NAME']]
    
    # Save the results
    out_path = os.path.join(os.path.dirname(data_path), 'merged_data', 'phys_regions.csv')
    reach_gdf.to_csv(out_path, index=False)

if __name__ == '__main__':
    data_path = sys.argv[1]
    intersect_regions(data_path)