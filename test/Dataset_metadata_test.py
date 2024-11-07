import pytest

meta_default_dict = dict(serial='0', case='case_name', replication='rep_num', input_location='Input',
                         output_location='Output', output_csv=True, output_parquet=False,
                         model_name='not defined', data_date='not defined',
                         data_name='not defined', data_details='not defined', time_unit='not defined',
                         distance_unit='not defined', cbt_pwr_unit='not defined',
                         entity_data_from_table=False, entity_table_file='entity_data_table.csv',
                         force_unique_unit_names=True, split_files_by_type=False,
                         drop_location_events=False, drop_shot_events=False, drop_seen_events=False,
                         drop_spot_events=False, total_entities=0, total_events=0, total_forces_and_affiliations=0,
                         first_event='no events', last_event='no events')


@pytest.mark.parametrize(
    'save_load',
    (
        pytest.param(False, id=''),
        pytest.param(True, id='save-load dataset state'),
    )
)
@pytest.mark.parametrize(
    ('config_keys_ls', 'config_vals_ls', 'exp_vals_ls'),
    (
            pytest.param([], [], [], id='default configuration'),
            pytest.param(['drop_location_events', 'drop_shot_events', 'drop_seen_events', 'drop_spot_events'],
                         ['1', '1', '1', '1'],
                         [True, True, True, True],
                         id='drop event configuration settings'),
            pytest.param(['output_csv', 'output_parquet'],
                         ['0', '1'],
                         [False, True],
                         id='output format settings'),
            pytest.param(['entity_data_from_table', 'entity_table_file'],
                         ['1', 'input_entity_table_file.csv'],
                         [True, 'input_entity_table_file.csv'],
                         id='entity data from table configuration settings'),
            pytest.param(['force_unique_unit_names', 'split_files_by_type'],
                         ['1', '1'],
                         [True, True],
                         id='force unique unit names and split files bool configuration settings'),
            pytest.param(['serial', 'case', 'replication'],
                         ['metadata_test', 'init_metadata_test', 'metadata_test_1'],
                         ['metadata_test', 'init_metadata_test', 'metadata_test_1'],
                         id='serial, case and replication configuration settings'),
            pytest.param(['input_location', 'output_location'],
                         ['Input/MetaTest', 'Output/MetaTest/update'],
                         [r'Input\MetaTest', r'Output\MetaTest\update'],
                         id='io configuration settings'),
            pytest.param(['time_unit', 'distance_unit', 'cbt_pwr_unit'],
                         ['millennia', 'fathoms', 'cbt pwr'],
                         ['millennia', 'fathoms', 'cbt pwr'],
                         id='units configuration settings'),
            pytest.param(['model_name', 'data_name', 'data_date', 'data_details'],
                         ['test model', 'test data', '1/1/1970', 'data for meta test'],
                         ['test model', 'test data', '1/1/1970', 'data for meta test'],
                         id='model and data info configuration settings'),
    )
)
def test_init_metadata(test_utils, save_load, config_keys_ls, config_vals_ls, exp_vals_ls):
    """
    Create Dataset instance with defined configuration
    Check that all expected metadata keys are present in the Dataset metadata dict
    Check that no additional metadata keys (that are not dataset variable keys) are present in the Dataset metadata dict
    Check that each value in the Dataset metadata dict is either the expected value from the configuration
    or the expected default value
    Check that the values for dataset variable keys in the metadata dict match the corresponding dataset variable

    if the save_load parameter is True then saving and reloading of dataset state will be tested
    """
    fail_msg_ls = []
    config_dict = {}
    exp_dict = {}

    for idx, key in enumerate(config_keys_ls):
        config_dict[key] = config_vals_ls[idx]
        exp_dict[key] = exp_vals_ls[idx]

    if 'output_location' not in config_dict.keys():
        config_dict['output_location'] = 'Output/MetaTest'

    test_dataset = test_utils.make_dataset(dataset_config=config_dict)

    if save_load:
        test_dataset = test_utils.dataset_save_load(dataset=test_dataset)
    test_dataset.finalise_data()

    for key in meta_default_dict.keys():
        if key not in test_dataset.metadata_dict.keys():
            fail_msg_ls.append(f"expected key {key} not present in Dataset metadata dict")

    for key in test_dataset.metadata_dict.keys():
        if key not in meta_default_dict.keys() and key not in vars(test_dataset):
            fail_msg_ls.append(f"unexpected non dataset variable key {key} in Dataset metadata dict "
                               f"(value {test_dataset.metadata_dict[key]})")
        if key in vars(test_dataset):
            metadata_var_val = test_dataset.metadata_dict[key]
            dataset_var_val = vars(test_dataset)[key]
            if metadata_var_val != dataset_var_val:
                fail_msg_ls.append(f"dataset variable key {key} has value {metadata_var_val} in metadata dict "
                                   f"but dataset setting is {dataset_var_val}")

    for item in meta_default_dict.items():
        key = item[0]
        if key not in vars(test_dataset):
            if key in config_dict.keys():
                exp_val = exp_dict[key]
            else:
                exp_val = item[1]
            if key in test_dataset.metadata_dict.keys():
                act_val = test_dataset.metadata_dict[key]
                if act_val != exp_val:
                    fail_msg_ls.append(f"value for {key} was {act_val} but expected {exp_val}")

    test_utils.check_fail_ls(fail_msg_ls)


meta_additional_dict = {'status': 'online and processing',
                        'total_entities': 42,
                        'config complete': True,
                        'process time': 123.4,
                        'serial': None,
                        'output_location': None,
                        'case': None,
                        'output_csv': False,
                        'output_parquet': True}


@pytest.mark.parametrize(
    'save_load',
    (
        pytest.param(False, id=''),
        pytest.param(True, id='save-load dataset state'),
    )
)
def test_add_metadata(test_utils, save_load):
    """
    Create dataset instance
    Add multiple items of metadata from the meta_additional_dict
    Confirm that keys have been added to the Dataset metadata dict and that values are as expected
    Note -  items in meta_additional_dict with a key that matches a dataset variable will be checked against
            the dataset variable rather than the value from meta_additional_dict to check they have not been
            overwritten

    if the save_load parameter is True then saving and reloading of dataset state will be tested
    """
    fail_msg_ls = []
    test_dataset = test_utils.make_dataset(dataset_config={'output_location': 'Output/MetaTest'})

    for item in meta_additional_dict.items():
        meta_key = item[0]
        meta_val = item[1]
        test_dataset.add_metadata(meta_key=meta_key, meta_value=meta_val)

    if save_load:
        test_dataset = test_utils.dataset_save_load(dataset=test_dataset)

    for item in meta_additional_dict.items():
        key = item[0]
        exp_val = item[1]
        if key not in vars(test_dataset):
            if key in test_dataset.metadata_dict.keys():
                act_val = test_dataset.metadata_dict[key]
                if act_val != exp_val:
                    fail_msg_ls.append(f"value for key {key} was {act_val} but expected {exp_val}")
            else:
                fail_msg_ls.append(f"key {key} not in Dataset metadata dict")
        else:
            metadata_var_val = test_dataset.metadata_dict[key]
            dataset_var_val = vars(test_dataset)[key]
            if metadata_var_val != dataset_var_val:
                fail_msg_ls.append(f"dataset variable key {key} has value {metadata_var_val} in metadata dict "
                                   f"but dataset setting is {dataset_var_val}")

    test_utils.check_fail_ls(fail_msg_ls)


@pytest.mark.parametrize(
    'save_load',
    (
        pytest.param(False, id=''),
        pytest.param(True, id='save-load dataset state'),
    )
)
@pytest.mark.parametrize(
    'replace',
    (
            pytest.param(True, id='replace True'),
            pytest.param(False, id='replace False'),
    )
)
def test_readonly_metadata(test_utils, save_load, replace):
    """
    Create a Dataset instance, add additional metadata from the meta_additional_dict
    Get a reference value for each metadata key in the Dataset instance metadata dict
    Attempt to overwrite the value for every key in the metadata dict of the entity instance
    by calling the add_metadata_function
    check that each metadata value either still corresponds to its reference value (replace False) or
    has changed (replace True)
    Note -  this will also test that dataset variable keys in the metadata dict cannot be overwritten by
            add_metadata with Replace True or False

    if the save_load parameter is True then saving and reloading of dataset state will be tested
    """
    fail_msg_ls = []
    test_dataset = test_utils.make_dataset(dataset_config={'output_location': 'Output/MetaTest'})

    for item in meta_additional_dict.items():
        meta_key = item[0]
        meta_val = item[1]
        test_dataset.add_metadata(meta_key=meta_key, meta_value=meta_val)

    if save_load:
        test_dataset = test_utils.dataset_save_load(dataset=test_dataset)

    og_vals_dict = {}
    for item in test_dataset.metadata_dict.items():
        og_vals_dict[item[0]] = item[1]

    for key in test_dataset.metadata_dict.keys():
        test_str = str(test_dataset.metadata_dict[key]) + "_updated"
        test_dataset.add_metadata(meta_key=key, meta_value=test_str, replace=replace)

    for item in test_dataset.metadata_dict.items():
        if replace:
            if item[0] in vars(test_dataset) and item[1] != og_vals_dict[item[0]]:
                fail_msg_ls.append(f"value for dataset variable key {item[0]} was changed by calling add_metadata "
                                   f"with dataset variable key and replace True")
            elif item[0] not in vars(test_dataset) and item[1] == og_vals_dict[item[0]]:
                fail_msg_ls.append(f"value for metadata key {item[0]} was not changed by calling add_metadata "
                                   f"with existing key and replace True")

        elif not replace:
            if item[1] != og_vals_dict[item[0]]:
                if item[0] in vars(test_dataset):
                    fail_msg_ls.append(f"value for dataset variable key {item[0]} was changed by calling add_metadata "
                                       f"with dataset variable key and replace False")
                else:
                    fail_msg_ls.append(f"value for metadata key {item[0]} was changed by calling add_metadata "
                                       f"with existing key and replace False")

    test_utils.check_fail_ls(fail_msg_ls)
