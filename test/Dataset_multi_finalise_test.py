import pytest

test_ent_dict = {'uid': ['t-b', 't-r'],
                 'unit_name': ['blue inf', 'red inf'],
                 'unit_type': ['infantry', 'infantry'],
                 'affiliation': ['blue', 'red'],
                 'init_comps': [10, 10],
                 'force': ['blue force', 'red force']}

location_events_dict = {'event_type': 'location',
                        'uid': ['t-b', 't-b', 't-b', 't-r', 't-r', 't-r'],
                        'time': [0.0, 5.0, 12.0, 0, 8, 13],
                        'x': ['8', '9', '7', '19', '21', '22'],
                        'y': ['12', '15', '18', '17', '18', '15'],
                        'entity': [None] * 6,
                        'detail_keys': [],
                        'detail_vals': [[None]] * 6}

shot_events_dict = {'event_type': 'shot',
                    'uid': ['t-r', 't-b', 't-b', 't-b', 't-r', 't-r', 't-r', 't-b'],
                    'time': [15.1, 3.1, 7, 13, 3, 9, 15, 7],
                    'x': ['22', '8', '9', '7', '19', '21', '22', '9'],
                    'y': ['15', '12', '15', '18', '17', '18', '15', '15'],
                    'entity': [None] * 8,
                    'detail_keys': [],
                    'detail_vals': [[None]] * 8}

kill_events_dict = {'event_type': 'kill',
                    'uid': ['t-b', 't-r', 't-b', 't-r'],
                    'time': [13, 14, 8, 7],
                    'x': ['7', '22', '9', '19'],
                    'y': ['18', '15', '15', '17'],
                    'entity': ['t-r', 't-b', 't-r', 't-b'],
                    'detail_keys': [],
                    'detail_vals': [[None]] * 4}

loss_events_dict = {'event_type': 'loss',
                    'uid': ['t-r', 't-r', 't-b', 't-b'],
                    'time': [13, 8, 14, 7],
                    'x': ['22', '21', '7', '9'],
                    'y': ['15', '18', '18', '15'],
                    'entity': ['t-b', 't-b', 't-r', 't-r'],
                    'detail_keys': [],
                    'detail_vals': [[None]] * 4}

seen_events_dict = {'event_type': 'seen',
                    'uid': ['t-r', 't-r', 't-r', 't-b', 't-b', 't-b'],
                    'time': [15, 9, 1, 3, 38, 6],
                    'x': ['22', '21', '19', '8', '7', '9'],
                    'y': ['15', '18', '17', '12', '18', '15'],
                    'entity': ['t-b', 't-b', 't-b', 't-r', 't-r', 't-r'],
                    'detail_keys': [],
                    'detail_vals': [[None]] * 6}

spot_events_dict = {'event_type': 'spot',
                    'uid': ['t-b', 't-b', 't-b', 't-r', 't-r', 't-r'],
                    'time': [15, 9, 1, 3, 38, 6],
                    'x': ['7', '9', '8', '19', '22', '19'],
                    'y': ['18', '15', '12', '17', '15', '17'],
                    'entity': ['t-r', 't-r', 't-r', 't-b', 't-b', 't-b'],
                    'detail_keys': [],
                    'detail_vals': [[None]] * 6}

status_event_dict = {'event_type': 'status',
                     'uid': ['t-b', 't-b', 't-r', 't-r'],
                     'time': [0.0, 12.0, 0.0, 13.0],
                     'x': ['8', '7', '19', '22'],
                     'y': ['12', '18', '17', '15'],
                     'entity': [None]*4,
                     'detail_keys': [],
                     'detail_vals': [[None]]*4}

stop_event_dict = {'event_type': 'stop',
                   'uid': ['t-b', 't-r'],
                   'time': [6.3, 8.4],
                   'x': ['9', '21'],
                   'y': ['15', '18'],
                   'entity': [None]*2,
                   'detail_keys': [],
                   'detail_vals': [[None]]*2}


event_dict_ls = [location_events_dict, shot_events_dict, kill_events_dict, loss_events_dict,
                 seen_events_dict, spot_events_dict, status_event_dict, stop_event_dict]

modified_config_dict = dict(serial='0_update', case='case_name_update', replication='rep_num_update',
                            input_location='Input_update', output_location='Output/MultiFinaliseTest/update',
                            output_csv='0', output_parquet='1',
                            model_name='model_update', data_date='data_date_update', data_name='data_name_update',
                            data_details='data_details_update', time_unit='time_update',
                            distance_unit='distance_update',
                            cbt_pwr_unit='cbt_pwr_update',
                            entity_data_from_table='1', entity_table_file='entity_data_table_update.csv',
                            force_unique_unit_names='0', split_files_by_type='1',
                            drop_location_events='1', drop_shot_events='1',
                            drop_seen_events='1', drop_spot_events='1')


@pytest.mark.parametrize(
    'finalise_reps',
    (
            pytest.param(1, id='x1'),
            pytest.param(2, id='x2'),
            pytest.param(5, id='5x'),
    )
)
@pytest.mark.parametrize(
    'export_import',
    (
            pytest.param(False, id=''),
            pytest.param(True, id='export-import entity data'),
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
    'config_dict',
    (
            pytest.param({'output_location': 'Output/MultiFinaliseTest'}, id='default dataset config'),
            pytest.param(modified_config_dict, id='modified dataset config'),
    )
)
def test_multi_finalise(test_utils, export_import, save_load, finalise_reps, config_dict):
    """
    Create Dataset instance, populate with entities and add events of all types
    Finalise dataset and extract reference versions of CDF dataframes and metadata dict
    Finalise dataset x times
    Check CDF dataframes and metadata dict in dataset instance are identical to reference versions (test1)
        Note - init_date_time_str is expected to change when saving and loading dataset state
    Add additional events and metadata, Finalise dataset
    Check that CDF dataframes and metadata dict in dataset instance are different to reference versions (test2)

    Repeat both Tests with default dataset configuration and a modified dataset configuration

    if the save_load parameter is True then saving and reloading of dataset state will be tested each time the
    dataset instance is finalised (this will cause init_date_time_str to change).
    if the export_import parameter is True then the entity_export_dict and entity_import_dict
    functions will be tested before and after saving and reloading the dataset state in each finalisation rep.
    """
    fail_msg_ls = []

    test_dataset = test_utils.make_dataset(dataset_config=config_dict)
    test_utils.add_entities(dataset=test_dataset, ent_dict=test_ent_dict)
    for event_dict in event_dict_ls:
        test_utils.add_single_events(dataset=test_dataset, event_dict=event_dict)
    test_dataset.finalise_data()

    df_name_ls = ['entity table', 'events', 'combat power']

    ref_df_ls = [test_dataset.CDF_entity_table_df.copy(),
                 test_dataset.CDF_events_df.copy(),
                 test_dataset.CDF_combat_power_DF.copy()]

    ref_metadata_dict = test_dataset.metadata_dict.copy()

    # finalise the dataset the specified number of times
    for i in range(0, finalise_reps):
        if export_import:
            test_utils.dataset_export_import_entities(dataset=test_dataset, finalise=False)
        if save_load:
            test_dataset = test_utils.dataset_save_load(dataset=test_dataset)
        if export_import:
            test_utils.dataset_export_import_entities(dataset=test_dataset, finalise=False)
        test_dataset.finalise_data()

    # check that the dfs and metadata dict have not changed compared to reference versions - test1
    test1_df_ls = [test_dataset.CDF_entity_table_df.copy(),
                   test_dataset.CDF_events_df.copy(),
                   test_dataset.CDF_combat_power_DF.copy()]

    test1_metadata_dict = test_dataset.metadata_dict.copy()

    for idx, ref_df in enumerate(ref_df_ls):
        test1_df = test1_df_ls[idx]
        if not ref_df.equals(test1_df):
            fail_msg_ls.append(f"changes in {df_name_ls[idx]} dataframe after finalise_data {finalise_reps}x called")
            fail_msg_ls.append(test_utils.get_dataframe_diff(df_act=test1_df, df_exp=ref_df))

    for key in ref_metadata_dict.keys():
        if key == 'init_date_time_str':
            if not save_load and test1_metadata_dict[key] != ref_metadata_dict[key]:
                fail_msg_ls.append("value for init_date_time_str changed in metadata dict without dataset save-load")
        elif test1_metadata_dict[key] != ref_metadata_dict[key]:
            fail_msg_ls.append(f"metadata dict key {key} changed after finalise_data {finalise_reps}x called")

    # add the events again, add an item of metadata and finalise dataset the specified number of times
    for event_dict in event_dict_ls:
        test_utils.add_single_events(dataset=test_dataset, event_dict=event_dict)
    test_dataset.add_metadata(meta_key='test', meta_value="added after finalise")

    for i in range(0, finalise_reps):
        if export_import:
            test_utils.dataset_export_import_entities(dataset=test_dataset, finalise=False)
        if save_load:
            test_dataset = test_utils.dataset_save_load(dataset=test_dataset)
        if export_import:
            test_utils.dataset_export_import_entities(dataset=test_dataset, finalise=False)
        test_dataset.finalise_data()

    # check that the dfs and metadata dict have changed compared to reference versions - test2
    test2_df_ls = [test_dataset.CDF_entity_table_df.copy(),
                   test_dataset.CDF_events_df.copy(),
                   test_dataset.CDF_combat_power_DF.copy()]

    test2_metadata_dict = test_dataset.metadata_dict.copy()

    for idx, ref_df in enumerate(ref_df_ls):
        test2_df = test2_df_ls[idx]
        if ref_df.equals(test2_df):
            fail_msg_ls.append(f"no changes in {df_name_ls[idx]} dataframe "
                               f"on adding events after {finalise_reps}x previous calls to finalise_data")

    if ref_metadata_dict == test2_metadata_dict:
        fail_msg_ls.append(f"no changes in metadata dict "
                           f"on adding metadata item after {finalise_reps}x previous calls to finalise_data")

    test_utils.check_fail_ls(fail_msg_ls)
