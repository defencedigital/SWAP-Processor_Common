import pytest


@pytest.mark.parametrize(
    'finalise_dataset',
    (
        pytest.param(False, id=''),
        pytest.param(True, id='finalise dataset'),
    )
)
@pytest.mark.parametrize(
    ('setting_name', 'config_input_ls', 'expected_ls'),
    (
            ('input_location', ['Input/init_test'], [r'Input\init_test']),
            ('output_location', ['Output/init_test'], [r'Output\init_test']),
            ('output_csv', ['0', '1'], [False, True]),
            ('output_parquet', ['0', '1'], [False, True]),
            ('force_unique_unit_names', ['0', '1'], [False, True]),
            ('entity_data_from_table', ['0', '1'], [False, True]),
            ('split_files_by_type', ['0', '1'], [False, True]),
            ('drop_location_events', ['0', '1'], [False, True]),
            ('drop_spot_events', ['0', '1'], [False, True]),
            ('drop_seen_events', ['0', '1'], [False, True]),
            ('drop_shot_events', ['0', '1'], [False, True]),
    )
)
def test_dataset_init(test_utils, finalise_dataset, setting_name, config_input_ls, expected_ls):
    """
    Test all Dataset settings that are processed from the config_dict
    (i.e. anything not just read in directly as a string)
    Set setting_name using each of the items in the config_input_ls
    check that the dataset var has the value of the corresponding item in expected_ls
    """
    fail_msg_ls = []
    for idx, config_val in enumerate(config_input_ls):
        test_dataset = test_utils.make_dataset(dataset_config={setting_name: config_val})

        if finalise_dataset:
            test_dataset.finalise_data()

        dataset_setting = vars(test_dataset)[setting_name]
        expected_setting = expected_ls[idx]
        if dataset_setting != expected_setting:
            fail_msg_ls.append(f"{setting_name} set as {dataset_setting} but expected {expected_setting}")

    test_utils.check_fail_ls(fail_msg_ls)

