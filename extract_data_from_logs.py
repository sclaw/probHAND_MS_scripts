import sys
import os
import shutil
import subprocess
import pandas as pd


PATH_TO_7ZIP = r"C:\Program Files\7-Zip\7z.exe"  # change this to the path to 7z.exe on your system


def unzip_logbooks(data_path, out_dir):
    """Unzip data from logs
    
    Iterates through all available directories and subdirectories 
    looking for rc_logbook.csv files to unzip.

    Args:
        data_path (str): path to the folder containing zipped data files from probHAND run
        out_dir (str): path to the folder where files will be unzipped to
    """
    # search all subdirectories for "model_files.7z"
    print('Scanning for model_files.7z files...')
    zipped_file_list = list()
    for root, dirs, files in os.walk(data_path):
        if 'model_files.7z' in files:
            zipped_file_list.append(os.path.join(root, 'model_files.7z'))
    
    # Extract Output_Logbooks folder from each zipped file
    print('Unzipping files...')
    counter = 1
    for zipped_file in zipped_file_list:
        print(f'{counter}/{len(zipped_file_list)}', end='\r')
        basin_name = os.path.split(os.path.dirname(os.path.dirname(zipped_file)))[-1]
        tmp_out_dir = os.path.join(out_dir, basin_name)
        os.makedirs(tmp_out_dir, exist_ok=True)
        subprocess.run([PATH_TO_7ZIP, 'x', zipped_file, f'-o{tmp_out_dir}', 'Output_Logbooks/*', '-aos'])


def unzip_drainage_areas(data_path, out_dir):
    """Unzip drainage areas from Stream_Stats_NHD
    
    Iterates through all available directories and subdirectories 
    looking for Stream_Stats_NHD.csv files to unzip.

    Args:
        data_path (str): path to the folder containing zipped data files from probHAND run
        out_dir (str): path to the folder where files will be unzipped to
    """
    # search all subdirectories for "model_files.7z"
    print('Scanning for model_files.7z files...')
    zipped_file_list = list()
    for root, dirs, files in os.walk(data_path):
        if 'model_files.7z' in files:
            zipped_file_list.append(os.path.join(root, 'model_files.7z'))
    
    # Extract Output_Logbooks folder from each zipped file
    print('Unzipping files...')
    counter = 1
    for zipped_file in zipped_file_list:
        print(f'{counter}/{len(zipped_file_list)}', end='\r')
        basin_name = os.path.split(os.path.dirname(os.path.dirname(zipped_file)))[-1]
        tmp_out_dir = os.path.join(out_dir, basin_name)
        os.makedirs(tmp_out_dir, exist_ok=True)
        subprocess.run([PATH_TO_7ZIP, 'x', zipped_file, f'-o{tmp_out_dir}', 'Stream_Stats/Stream_Stats_NHD.csv', '-aos'])


def merge_rating_curves(data_path, out_path):
    """Merge all rc_logbook.csv files

    Iterates through all available directories and subdirectories to 
    find rc_logbook.csv files, filters them, and saves the merged data

    Args:
        data_path (str): directory containing all rc_logbook.csv files to merge
        out_path (str): path to save merged data to
    """
    # Search all subdirectories for rc_logbook.csv files
    print('Scanning for rc_logbook.csv files...')
    logbook_file_list = list()
    for root, dirs, files in os.walk(data_path):
        if 'rc_logbook.csv' in files:
            logbook_file_list.append(os.path.join(root, 'rc_logbook.csv'))
    
    # Load each file and filter it
    print('Filtering files...')
    counter = 1
    for logbook_file in logbook_file_list:
        print(f'{counter}/{len(logbook_file_list)}', end='\r')
        counter += 1
        tmp_filtered_path = logbook_file.replace('rc_logbook.csv', 'filtered_logbook.csv')
        if os.path.exists(tmp_filtered_path):
            continue

        tmp_data = pd.read_csv(logbook_file)
        tmp_data['TOPWIDTH'] = tmp_data['SA'] / tmp_data['LENGTH']
        tmp_data['XSAREA'] = tmp_data['VOLUME'] / tmp_data['LENGTH']
        tmp_data = tmp_data.drop(columns=['RESOLUTION', 'N_SIM', 'LENGTH', 'XS_AREA', 'H_RADIUS', 'DISCHARGE', 'SA', 'VOLUME'])
        aggregated = tmp_data.groupby(['REACH', 'STAGE']).agg({'TOPWIDTH': 'median', 'XSAREA': 'median', 'MANNINGS': 'median', 'SLOPE': 'first'}).reset_index()
        aggregated['H_RADIUS'] = aggregated['XSAREA'] / aggregated['TOPWIDTH']
        aggregated['DISCHARGE'] = (1 / aggregated['MANNINGS']) * aggregated['XSAREA'] * (aggregated['H_RADIUS'] ** (2/3)) * (aggregated['SLOPE'] ** (1/2))
        aggregated.to_csv(tmp_filtered_path, index=False)
    print()

    # Merge all filtered files
    print('Merging files...')
    merged_data = pd.DataFrame()
    counter = 1
    for logbook_file in logbook_file_list:
        print(f'{counter}/{len(logbook_file_list)}', end='\r')
        counter += 1
        tmp_filtered_path = logbook_file.replace('rc_logbook.csv', 'filtered_logbook.csv')
        tmp_data = pd.read_csv(tmp_filtered_path)
        merged_data = pd.concat([merged_data, tmp_data])
    merged_data.to_csv(out_path, index=False)
    print()


def merge_ri_stages(data_path, out_path):
    """Merge all RIstage_logbook.csv files

    Iterates through all available directories and subdirectories to
    find RIstage_logbook.csv files, filters them, and saves the merged data

    Args:
        data_path (str): directory containing all RIstage_logbook.csv files to merge
        out_path (str): path to save merged data to
    """
    # Search all subdirectories for RIstage_logbook.csv files
    print('Scanning for RIstage_logbook.csv files...')
    logbook_file_list = list()
    for root, dirs, files in os.walk(data_path):
        if 'RIstage_logbook.csv' in files and os.path.split(root)[-1] != 'archive':
            logbook_file_list.append(os.path.join(root, 'RIstage_logbook.csv'))
    
    # Load each file and filter it
    print('Filtering files...')
    counter = 1
    for logbook_file in logbook_file_list:
        print(f'{counter}/{len(logbook_file_list)}', end='\r')
        counter += 1
        tmp_filtered_path = logbook_file.replace('RIstage_logbook.csv', 'filtered_stages.csv')
        # if os.path.exists(tmp_filtered_path):
        #     continue

        tmp_data = pd.read_csv(logbook_file)
        tmp_data = tmp_data.astype({'REACH': 'int', 'RI': 'int', 'STAGE': 'float', 'Q': 'float'})
        tmp_data = tmp_data.drop(columns=['RESOLUTION', 'N_SIM'])
        aggregated = tmp_data.groupby(['REACH', 'RI']).agg({'Q': 'median', 'STAGE': 'median'}).reset_index()
        aggregated.to_csv(tmp_filtered_path, index=False)
    print()

    # Merge all filtered files
    print('Merging files...')
    merged_data = pd.DataFrame()
    counter = 1
    for logbook_file in logbook_file_list:
        print(f'{counter}/{len(logbook_file_list)}', end='\r')
        counter += 1
        tmp_filtered_path = logbook_file.replace('RIstage_logbook.csv', 'filtered_stages.csv')
        tmp_data = pd.read_csv(tmp_filtered_path)
        merged_data = pd.concat([merged_data, tmp_data])
    merged_data.to_csv(out_path, index=False)
    print()


def merge_das(data_path, out_path):
    """Merge all drainage areas from Stream_Stats_NHD.csv files

    Args:
        data_path (str): Directory containing all Stream_Stats_NHD.csv files to merge
        out_path (str): Path to save merged data to
    """
    # Search all subdirectories for Stream_Stats_NHD.csv files
    print('Scanning for Stream_Stats_NHD.csv files...')
    logbook_file_list = list()
    for root, dirs, files in os.walk(data_path):
        if 'Stream_Stats_NHD.csv' in files:
            logbook_file_list.append(os.path.join(root, 'Stream_Stats_NHD.csv'))

    # Load each file and join them
    print('Merging files...')
    merged_data = pd.DataFrame()
    counter = 1
    for logbook_file in logbook_file_list:
        print(f'{counter}/{len(logbook_file_list)}', end='\r')
        counter += 1
        tmp_data = pd.read_csv(logbook_file)[['Name', 'AreaSqMi']].drop_duplicates()
        merged_data = pd.concat([merged_data, tmp_data])
    merged_data.to_csv(out_path, index=False)
    print()

def extract_data(data_path):
    """Extracts data from probHAND zipped run logs
    
    Runs all functions necessary to 1) unzip run logs, 2) filter data
    for median values of monte-carlo simulations, and 3) merge data for
    all basins

    Args:
        data_path (str): path to the folder containing zipped data files from probHAND run
    """
    # Make a working directory
    working_dir = os.path.join(os.path.dirname(data_path), 'merged_data')

    # Unzip all files
    unzip_dir = os.path.join(working_dir, 'unzipped_logs')
    os.makedirs(unzip_dir, exist_ok=True)
    # unzip_logbooks(data_path, unzip_dir)
    # unzip_drainage_areas(data_path, unzip_dir)

    # Filter and merge all logbooks
    merged_rc_path = os.path.join(working_dir, 'merged_rating_curves.csv')
    # merge_rating_curves(unzip_dir, merged_rc_path)
    merge_ri_path = os.path.join(working_dir, 'merged_ri_stages.csv')
    merge_ri_stages(unzip_dir, merge_ri_path)
    da_path = os.path.join(working_dir, 'drainage_areas.csv')
    # merge_das(unzip_dir, da_path)

    # Clean up workspace
    print('Cleaning up...')
    # shutil.rmtree(unzip_dir)

    print(f'Finished processing data.')
    print(f'Rating curve data saved to {merged_rc_path}')
    print(f'RI stage data saved to {merge_ri_path}')
    print(f'Drainage area data saved to {da_path}')

if __name__ == '__main__':
    data_path = sys.argv[1]
    extract_data(data_path)
