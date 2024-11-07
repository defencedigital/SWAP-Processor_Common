import pytest
import yaml
import pandas as pd

from os import path


@pytest.mark.parametrize(
    'output',
    (
        pytest.param('pq', id='pq output'),
        pytest.param('csv', id='csv output'),
        pytest.param('both', id='pq+csv output'),
    )
)
@pytest.mark.parametrize(
    'save_load',
    (
        pytest.param(False, id=''),
        pytest.param(True, id='save-load dataset state'),
    )
)
@pytest.mark.parametrize(
    'test_folder',
    (
        pytest.param('basic_test', id='basic test'),
        pytest.param('basic_test_split_files', id='basic test, split output files'),
        pytest.param('basic_test_ent_tbl', id='basic test, entities from table'),
        pytest.param('basic_test_drp_evn', id='basic test, drop events'),
        pytest.param('complex_test', id='complex test (inc. edge cases)'),
        pytest.param('complex_test_split_files', id='complex test (inc. edge cases), split output files'),
        pytest.param('complex_test_ent_tbl', id='complex test (inc. edge cases), entities from table'),
        pytest.param('complex_test_drp_evn', id='complex test (inc. edge cases), drop events'),
    )
)
def test_end_to_end(test_utils, test_folder, output, save_load):
    """
    Test the CDF processor end-to-end process:
        Create a Dataset instance, add entities, events and metadata, finalise data and export files
        Verify exported files against reference files
        Check Dataset instance log for any error events

    the output_csv and output_parquet parameters are used to test generating the CDF outputs in just csv format,
    just parquet format and in both formats (Note that a parquet engine needs to be installed to successfully
    generate parquet outputs - if one is not installed all tests with output_parquet as '1' will fail)

    if the save_load parameter is True then saving and reloading of dataset state will be tested

    Notes -
    Test is parametrized to run a series of tests with each individual test contained in a sub folder of
    reference_data/end_to_end_test, Each sub folder contains:
        test_data.yaml  - configuration and input data
        reference files* - CDF_Cbt_Pwr_exp.csv, CDF_Entity_table_exp.csv, CDF_Events_exp.csv
        entity table file - if the test configuration is set up to read entity_data_from_table then the
                            test folder must contain an entity table file and the filename must be specified
                            as the 'entity_table_file' parameter in the test_config section of test_data.yaml

    To add a test: create a new subfolder in reference_data/end_to_end_test with the ste of files described
    above (use an existing set as a template) and add the name of the folder as a new pytest.param for the
    test_folder parameter
    """
    fail_msg_ls = []
    # use a dataset log file list in case the saved and reloaded dataset ends up with a different log file
    dataset_log_file_ls = []

    test_path = path.join('reference_data', 'end_to_end_test', test_folder)
    with open(path.join(test_path, 'test_data.yaml'), 'r') as test_data_file:
        test_data_dict = yaml.safe_load(test_data_file)
    test_data_dict['test_config']['output_location'] = path.join('Output', 'EndToEndTest', test_folder)
    test_data_dict['test_config']['input_location'] = test_path

    output_csv = '0'
    output_parquet = '0'

    if output == 'csv':
        output_csv = '1'
    elif output == 'pq':
        output_parquet = '1'
    elif output == 'both':
        output_csv = '1'
        output_parquet = '1'

    test_data_dict['test_config']['output_csv'] = output_csv
    test_data_dict['test_config']['output_parquet'] = output_parquet

    config_dict = test_data_dict['test_config']
    input_location = config_dict['input_location']

    # set up the Dataset instance, add entities, events and metadata, finalise data and export files
    test_dataset = test_utils.make_dataset(log_file=True, dataset_config=config_dict)
    # get the log file name now so that the date-time part of the file name will match
    dataset_log_file_ls.append(f"Dataset_log_S{test_dataset.serial}_{test_dataset.init_date_time_str}.log")

    if 'entity_data_from_table' not in config_dict.keys() or str(config_dict['entity_data_from_table']) != '1':
        for test_ent_dict_name in test_data_dict['test_ent_dict_ls']:
            test_ent_dict = test_data_dict[test_ent_dict_name]
            test_utils.add_entities(dataset=test_dataset, ent_dict=test_ent_dict)

    for add_event_dict_name in test_data_dict['add_event_dict_ls']:
        add_event_dict = test_data_dict[add_event_dict_name]
        test_utils.add_single_events(dataset=test_dataset, event_dict=add_event_dict)
    for append_event_dict_name in test_data_dict['append_event_dict_ls']:
        append_event_dict = test_data_dict[append_event_dict_name]
        test_utils.append_events(dataset=test_dataset, append_event_dict=append_event_dict)

    for meta_item in test_data_dict['metadata_dict'].items():
        meta_key = meta_item[0]
        meta_val = meta_item[1]
        test_dataset.add_metadata(meta_key=meta_key, meta_value=meta_val)

    if save_load:
        save_file_name = f'{test_dataset.case}-{test_dataset.replication}_dataset_save.yaml'
        save_file_name = save_file_name.replace(' ', '_')

        test_dataset = test_utils.dataset_save_load(dataset=test_dataset,
                                                    save_location='Output/EndToEndTest/dataset_saves',
                                                    save_file=save_file_name)
        # add the log file name to the list
        dataset_log_file_ls.append(f"Dataset_log_S{test_dataset.serial}_{test_dataset.init_date_time_str}.log")

    test_dataset.finalise_data()
    test_dataset.export_data()

    # get the reference data, need to apply column types to use as reference for pq too
    df_name_ls = ['cdf entity table', 'cdf events', 'cdf cbt pwr']
    ref_cdf_ent_tbl_csv_df = pd.read_csv(path.join(input_location, 'CDF_EntityTable_exp.csv'),
                                         dtype=test_dataset.ent_tbl_col_types_dict)
    ref_cdf_events_csv_df = pd.read_csv(path.join(input_location, 'CDF_Events_exp.csv'),
                                        dtype=test_dataset.evn_tbl_col_types_dict)
    ref_cdf_cbt_pwr_csv_df = pd.read_csv(path.join(input_location, 'CDF_Cbt_Pwr_exp.csv'),
                                         dtype=test_dataset.cbt_tbl_col_types_dict)
    ref_csv_df_ls = [ref_cdf_ent_tbl_csv_df, ref_cdf_events_csv_df, ref_cdf_cbt_pwr_csv_df]
    with open(path.join(input_location, 'CDF_Metadata_exp.yaml'), 'r') as file:
        ref_cdf_meta_dict = yaml.safe_load(file)

    # check if the output files are being split by type
    if 'split_files_by_type' in config_dict.keys() and str(config_dict['split_files_by_type']) == '1':
        split_files_by_type = True
    else:
        split_files_by_type = False

    act_file_name_ls = [test_dataset.entity_filename, test_dataset.events_filename, test_dataset.cbt_filename]
    col_types_ls = [test_dataset.ent_tbl_col_types_dict, test_dataset.evn_tbl_col_types_dict,
                    test_dataset.cbt_tbl_col_types_dict]
    act_folder_ls = [test_dataset.entity_folder_name, test_dataset.events_folder_name,
                     test_dataset.cbt_folder_name]

    # if outputting in csv format get the csv data and compare to csv reference
    if output_csv == '1':
        act_df_csv_ls = []
        for idx, act_file_name in enumerate(act_file_name_ls):
            try:
                if split_files_by_type:
                    folder = act_folder_ls[idx]
                else:
                    folder = ''
                act_df_csv_ls.append(pd.read_csv(path.join(config_dict['output_location'], folder, act_file_name),
                                                 dtype=col_types_ls[idx]))
            except FileNotFoundError:
                fail_msg_ls.append(f"{act_file_name} not found")
                act_df_csv_ls.append(pd.DataFrame())

        # compare output csv data to csv reference
        for idx, ref_df in enumerate(ref_csv_df_ls):
            df_name = df_name_ls[idx]
            act_df = act_df_csv_ls[idx]
            if not ref_df.equals(act_df):
                fail_msg_ls.append(f'differences in {df_name} dataframe in .csv output: \n'
                                   f'{test_utils.get_dataframe_diff(df_act=act_df, df_exp=ref_df)}')

    # if outputting in parquet format get the parquet data and compare to parquet reference
    if output_parquet == '1':
        try:
            act_df_pq_ls = []
            for idx, act_file_name in enumerate(act_file_name_ls):
                try:
                    if split_files_by_type:
                        folder = act_folder_ls[idx]
                    else:
                        folder = ''
                    pq_file_name = act_file_name.replace('.csv', '.parquet')
                    act_df_pq_ls.append(pd.read_parquet(path.join(config_dict['output_location'], folder, pq_file_name)))
                except FileNotFoundError:
                    fail_msg_ls.append(f"{act_file_name} not found")
                    act_df_pq_ls.append(pd.DataFrame())

            # generate parquet reference from csv reference
            ref_pq_df_ls = []
            for ref_csv_df in ref_csv_df_ls:
                ref_pq_df_ls.append(ref_csv_df.copy())

            # read csv reads a blank string as na so need to replace with '' to compare to parquet
            for ref_pq_df in ref_pq_df_ls:
                for column in ref_pq_df.columns:
                    if ref_pq_df[column].dtype == object:
                        ref_pq_df[column].fillna(value='', inplace=True)

            # compare output parquet data to parquet reference
            for idx, ref_df in enumerate(ref_pq_df_ls):
                df_name = df_name_ls[idx]
                act_df = act_df_pq_ls[idx]
                if not ref_df.equals(act_df):
                    fail_msg_ls.append(f'differences in {df_name} dataframe in .parquet output: \n'
                                       f'{test_utils.get_dataframe_diff(df_act=act_df, df_exp=ref_df)}')
        except ImportError:
            fail_msg_ls.append('testing with parquet output but no parquet engine installed')

    # get the output metadata file
    try:
        if split_files_by_type:
            meta_folder = test_dataset.meta_folder_name
        else:
            meta_folder = ''
        with open(path.join(config_dict['output_location'], meta_folder, test_dataset.metadata_filename)) as file:
            act_meta_data_dict = yaml.safe_load(file)
    except FileNotFoundError:
        fail_msg_ls.append(f"{test_dataset.metadata_filename} not found")
        act_meta_data_dict = {}

    # compare output metadata file to reference
    for ref_key in ref_cdf_meta_dict.keys():
        if ref_key not in act_meta_data_dict.keys():
            fail_msg_ls.append(f'key {ref_key} missing from {test_dataset.metadata_filename}')
    for act_key in act_meta_data_dict.keys():
        if act_key not in ref_cdf_meta_dict.keys():
            if act_key not in vars(test_dataset):
                fail_msg_ls.append(f'{test_dataset.metadata_filename} has additional key {act_key} which is not a '
                                   f'dataset variable (value: {act_meta_data_dict[act_key]})')
            else:
                var_val = vars(test_dataset)[act_key]
                act_val = act_meta_data_dict[act_key]
                if act_val != var_val:
                    fail_msg_ls.append(f"dataset variable key {act_key} in {test_dataset.metadata_filename} "
                                       f"has value {act_val} but dataset variable set to {var_val}")
    for ref_item in ref_cdf_meta_dict.items():
        ref_key = ref_item[0]
        ref_val = ref_item[1]
        if ref_key in act_meta_data_dict.keys():
            if ref_val != act_meta_data_dict[ref_key]:
                fail_msg_ls.append(f'value for {ref_key} in {test_dataset.metadata_filename} '
                                   f'was {act_meta_data_dict[ref_key]} but expected {ref_val}')

    # check the dataset log file for any error events
    for dataset_log_file_name in dataset_log_file_ls:
        try:
            if split_files_by_type:
                log_folder = test_dataset.dataset_log_folder
            else:
                log_folder = ''
            with open(path.join(config_dict['output_location'], log_folder, dataset_log_file_name)) as dataset_log_file:
                for log_entry in dataset_log_file:
                    if 'ERROR' in log_entry:
                        fail_msg_ls.append(f'ERROR event in dataset log file {dataset_log_file_name} {log_entry}')
        except FileNotFoundError:
            fail_msg_ls.append(f'dataset log file {dataset_log_file_name} not found')

    test_utils.check_fail_ls(fail_msg_ls)
