import pytest

update_config_dict = dict(serial='0_update', case='case_name_update', replication='rep_num_update',
                          input_location='Input_update', output_location='Output/UpdateConfigTest',
                          output_csv=False, output_parquet=True,
                          model_name='model_update', data_date='data_date_update', data_name='data_name_update',
                          data_details='data_details_update', time_unit='time_update', distance_unit='distance_update',
                          cbt_pwr_unit='cbt_pwr_update',
                          entity_data_from_table=True, entity_table_file='entity_data_table_update.csv',
                          force_unique_unit_names=False, split_files_by_type=True,
                          drop_location_events=True, drop_shot_events=True,
                          drop_seen_events=True, drop_spot_events=True)


@pytest.mark.parametrize(
    'save_load',
    (
        pytest.param(False, id=''),
        pytest.param(True, id='save-load dataset state'),
    )
)
def test_update_config(test_utils, save_load):
    """
    create a dataset instance with default config
    update configuration using the update config dict
    check that the updated configuration is reflected in the dataset variables and the metadata dict

    if the save_load parameter is True then saving and reloading of dataset state will be tested
    """
    fail_msg_ls = []
    test_dataset = test_utils.make_dataset()
    for item in update_config_dict.items():
        test_dataset.update_config(setting=item[0], value=item[1])

    if save_load:
        test_dataset = test_utils.dataset_save_load(dataset=test_dataset)

    for item in update_config_dict.items():
        if vars(test_dataset)[item[0]] != item[1]:
            fail_msg_ls.append(f"update of dataset variable {item[0]} using update_config failed")
        if test_dataset.metadata_dict[item[0]] != item[1]:
            fail_msg_ls.append(f"update of dataset variable {item[0]} using update_config "
                               f"not reflected in metadata dict")

    test_utils.check_fail_ls(fail_msg_ls)
