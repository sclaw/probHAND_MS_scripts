import sys
import os
import pandas as pd


def make_dataset(data_path):
    """Merges all mini datasets into a single file.

    Args:
        data_path (str): path to the data folder.  Slightly roundabout
        way to find merged files, but retaining for consistency.
    """
    # Load the mini datasets
    working_dir = os.path.join(os.path.dirname(data_path), 'merged_data')
    metadata_path = os.path.join(working_dir, 'reach_metadata.csv')
    interpolated_path = os.path.join(working_dir, 'interpolated_variables.csv')
    drainage_areas_path = os.path.join(working_dir, 'drainage_areas.csv')
    phys_regions_path = os.path.join(working_dir, 'phys_regions.csv')
    bankfull_path = os.path.join(working_dir, 'bankfull_depths.csv')

    metadata = pd.read_csv(metadata_path)
    metadata['Code'] = metadata['Code'].astype(int).astype(str)
    metadata = metadata.set_index('Code')
    metadata = metadata.drop(columns=['OBJECTID', 'F50AEP_cfs', 'F20AEP_cfs', 'F10AEP_cfs', 'F4AEP_cfs_', 'F2AEP_cfs_', 'F1AEP_cfs_', 'F0_5AEP_cf', 'F0_2AEP_cf', 'RunDate', 'lengthchck', 'Shape__Length'])
    interpolated = pd.read_csv(interpolated_path)
    interpolated['REACH'] = interpolated['REACH'].astype(int).astype(str)
    interpolated = interpolated.set_index('REACH')
    drainage_areas = pd.read_csv(drainage_areas_path)
    drainage_areas['Name'] = drainage_areas['Name'].astype(int).astype(str)
    drainage_areas = drainage_areas.set_index('Name')
    phys_regions = pd.read_csv(phys_regions_path)
    phys_regions['Code'] = phys_regions['Code'].astype(int).astype(str)
    phys_regions = phys_regions.set_index('Code')
    bankfull = pd.read_csv(bankfull_path)
    bankfull['REACH'] = bankfull['REACH'].astype(int).astype(str)
    bankfull = bankfull.set_index('REACH')

    # Modify some fields
    drainage_areas['DrainArea(sqkm)'] = drainage_areas['AreaSqMi'] * 2.58999
    drainage_areas = drainage_areas.drop(columns=['AreaSqMi'])

    phys_regions['Region'] = phys_regions['NAME']
    phys_regions = phys_regions.drop(columns=['NAME'])

    # Merge the datasets
    merged = metadata.join(drainage_areas)
    merged = merged.join(phys_regions)
    merged = merged.join(bankfull)
    merged = merged.join(interpolated)

    # Save the merged dataset
    out_path = os.path.join(working_dir, 'all_data.csv')
    merged.to_csv(out_path, index_label='Reach')



if __name__ == '__main__':
    data_path = sys.argv[1]
    make_dataset(data_path)