import os
import sys
import pandas as pd
import numpy as np
from scipy.interpolate import interp1d
import matplotlib.pyplot as plt


def derive_variables(rc_path, ri_path, bkf_path, out_path):
    """Derive various hydraulic characteristics

    Derives flow characteristics at various discharge levels, such as
    floodplain width, channel and overbank volumes, and channel and 
    overbank SSPs

    Args:
        rc_path (str): path to the merged rating curve data
        ri_path (str): path to the merged RI stage data
        out_path (str): path to save the derived data to
    """
    # Load data
    rc_data = pd.read_csv(rc_path)
    rc_data = rc_data.set_index('REACH')
    ri_data = pd.read_csv(ri_path)
    ri_data = ri_data.astype({'REACH': 'int', 'STAGE': 'float', 'Q': 'float', 'RI': 'int'})
    ri_data = ri_data.set_index('REACH')
    
    bkf_data = pd.read_csv(bkf_path)
    bkf_data = bkf_data.set_index('REACH')

    # Get unique reaches
    reaches = list(set(rc_data.index.unique()).intersection(ri_data.index.unique()).intersection(bkf_data.index.unique()))

    # Initialize output data
    ris = np.sort(ri_data['RI'].unique())
    section_width_cols = [f'Q{x}_Total_Width' for x in ris]
    channel_width_cols = [f'Q{x}_Ch_Width' for x in ris]
    overbank_width_cols = [f'Q{x}_OB_Width' for x in ris]
    section_vol_cols = [f'Q{x}_Total_Area' for x in ris]
    channel_vol_cols = [f'Q{x}_Ch_Area' for x in ris]
    overbank_vol_cols = [f'Q{x}_OB_Area' for x in ris]
    section_ssp_cols = [f'Q{x}_Total_SSP' for x in ris]
    channel_ssp_cols = [f'Q{x}_Ch_SSP' for x in ris]
    overbank_ssp_cols = [f'Q{x}_OB_SSP' for x in ris]
    all_cols = channel_width_cols + overbank_width_cols + channel_vol_cols + overbank_vol_cols + channel_ssp_cols + overbank_ssp_cols
    out_data = pd.DataFrame(index=reaches, columns=all_cols)

    # Run
    counter = 1
    for r in reaches:
        print(f'{counter}/{len(reaches)}', end='\r')
        counter += 1
        rc_subset = pd.DataFrame(rc_data.loc[r])
        ri_subset = pd.DataFrame(ri_data.loc[r]).sort_values('RI')
        tmp_bkf_stage = bkf_data.loc[r, 'BankfullDepth']
        tmp_slope = rc_subset['SLOPE'].values[0]

        # Separate channel and overbank series
        bkf_tw = np.interp(tmp_bkf_stage, rc_subset['STAGE'], rc_subset['TOPWIDTH'])
        bkf_area = np.interp(tmp_bkf_stage, rc_subset['STAGE'], rc_subset['XSAREA'])

        rc_subset['CH_TW'] = np.minimum(bkf_tw, rc_subset['TOPWIDTH'])
        rc_subset['OB_TW'] = rc_subset['TOPWIDTH'] - rc_subset['CH_TW']

        rc_subset['CH_AREA'] = np.minimum(bkf_area, rc_subset['XSAREA'])
        add_ch_area = (rc_subset['XSAREA'] - tmp_bkf_stage) * bkf_tw
        add_ch_area = np.maximum(0, add_ch_area)
        rc_subset['CH_AREA'] += add_ch_area
        rc_subset['OB_AREA'] = rc_subset['XSAREA'] - rc_subset['CH_AREA']

        # Interpolate values
        # Widths
        total_widths = np.interp(ri_subset['STAGE'], rc_subset['STAGE'], rc_subset['TOPWIDTH'])
        out_data.loc[r, section_width_cols] = total_widths

        ch_widths = np.interp(ri_subset['STAGE'], rc_subset['STAGE'], rc_subset['CH_TW'])
        out_data.loc[r, channel_width_cols] = ch_widths

        ob_widths = np.interp(ri_subset['STAGE'], rc_subset['STAGE'], rc_subset['OB_TW'])
        out_data.loc[r, overbank_width_cols] = ob_widths

        # Areas
        total_areas = np.interp(ri_subset['STAGE'], rc_subset['STAGE'], rc_subset['XSAREA'])
        out_data.loc[r, section_vol_cols] = total_areas

        ch_areas = np.interp(ri_subset['STAGE'], rc_subset['STAGE'], rc_subset['CH_AREA'])
        out_data.loc[r, channel_vol_cols] = ch_areas

        ob_areas = np.interp(ri_subset['STAGE'], rc_subset['STAGE'], rc_subset['OB_AREA'])
        out_data.loc[r, overbank_vol_cols] = ob_areas

        # SSP
        total_velocities = ri_subset['Q'].values / total_areas

        total_ssp = ri_subset['Q'].values * tmp_slope * 9810 / total_widths
        out_data.loc[r, section_ssp_cols] = total_ssp

        ch_ssp = total_velocities * ch_areas * tmp_slope * 9810 / ch_widths
        out_data.loc[r, channel_ssp_cols] = ch_ssp

        ob_mask = ob_widths > 0
        ob_ssp = total_velocities[ob_mask] * ob_areas[ob_mask] * tmp_slope * 9810 / ob_widths[ob_mask]
        out_data.loc[r, np.array(overbank_ssp_cols)[ob_mask]] = ob_ssp
    print()
    out_data.to_csv(out_path, index_label='REACH')

        
def generate_bankfull_depths(working_dir, out_path, method='static'):
    """Generate bankfull depths for each reach

    Generates bankfull depths using fixed thresholds or regional regression

    Args:
        data_path (str): path to working directory.  should contain file
        called drainage_areas.csv
        method (str): method to use for bankfull depth calculation
    """
    # Load reach drainage areas
    meta_path = os.path.join(working_dir, 'drainage_areas.csv')
    meta_data = pd.read_csv(meta_path)

    # Calculate bankfull depths using the specified method
    if method == 'static':
        bkf_eq = lambda x: 0.5 if x < 775 else 1.0  # 2,000sqkm break
    elif method == 'regression':
        # placeholder.  Add new methods here.
        pass
    meta_data['BankfullDepth'] = meta_data['AreaSqMi'].apply(bkf_eq)

    # Reformat
    meta_data['REACH'] = meta_data['Name']
    meta_data = meta_data[['REACH', 'BankfullDepth']]

    # Save the calculated bankfull depths
    meta_data.to_csv(out_path, index=False)

def generate_dataset(data_path):
    """Runs all steps to process data and derive variables

    Args:
        data_path (str): Folder containing all data files
    """
    # Make a working directory
    working_dir = os.path.join(os.path.dirname(data_path), 'merged_data')

    # Generate bankfull depths
    bkf_path = os.path.join(working_dir, 'bankfull_depths.csv')
    # generate_bankfull_depths(working_dir, bkf_path)

    # Derive hydraulic characteristics
    rc_path = os.path.join(working_dir, 'merged_rating_curves.csv')
    ri_path = os.path.join(working_dir, 'merged_ri_stages.csv')
    out_path = os.path.join(working_dir, 'interpolated_variables.csv')
    derive_variables(rc_path, ri_path, bkf_path, out_path)


if __name__ == '__main__':
    data_path = sys.argv[1]
    generate_dataset(data_path)