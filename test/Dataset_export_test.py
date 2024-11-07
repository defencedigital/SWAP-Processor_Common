import pytest
from datetime import datetime
from os import path, listdir, makedirs
test_date_time_str = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")


@pytest.mark.parametrize(
    'save_load',
    (
        pytest.param(False, id=''),
        pytest.param(True, id='save-load dataset state'),
    )
)
@pytest.mark.parametrize(
    'output_format',
    (
        pytest.param('pq', id='pq format'),
        pytest.param('csv', id='csv format'),
        pytest.param('both', id='pq and csv format'),
    )
)
@pytest.mark.parametrize(
    'split_files',
    (
        pytest.param('0', id=''),
        pytest.param('1', id='split files'),
    )
)
@pytest.mark.parametrize(
    'new_location',
    (
        pytest.param(False, id='existing location'),
        pytest.param(True, id='new location'),
    )
)
@pytest.mark.parametrize(
    ('serial', 'case', 'rep'),
    (
            pytest.param('1', 'exp_test_case', 'exp_test_rep', id=''),
            pytest.param('2', 'exp test case', 'exp test rep', id='handle space characters in case and rep strings'),
    )
)
def test_export_files(test_utils, save_load, output_format, split_files, new_location, serial, case, rep):
    """
    Create dataset instance, add an entity, finalise and then export the data
    Verify that all expected files have been written to expected locations
    and that filenames reflect the dataset configuration

    Parametrization tests combinations of:
        creating a new location / exporting to an existing location,
        splitting files by type
        parquet and / or csv output format
        (note that unless pyarrow is installed all tests that output parquet files will fail)
        replacing spaces in file names (from case and rep) with underscores

    if the save_load parameter is True then saving and reloading of dataset state will be tested
    """
    fail_msg_ls = []
    output_path = path.join("Output", "ExportTest", test_date_time_str)
    if not path.isdir(output_path):
        makedirs(output_path)
    if new_location:
        output_path = path.join(output_path, 'S' + str(serial))

    output_csv = '0'
    output_parquet = '0'

    if output_format == 'csv':
        output_csv = '1'
    elif output_format == 'pq':
        output_parquet = '1'
    elif output_format == 'both':
        output_csv = '1'
        output_parquet = '1'

    config_dict = {'output_location': output_path,
                   'output_csv': output_csv,
                   'output_parquet': output_parquet,
                   'split_files_by_type': split_files,
                   'serial': serial,
                   'case': case,
                   'replication': rep}

    test_dataset = test_utils.make_dataset(dataset_config=config_dict, log_file=True)
    if save_load:
        test_dataset = test_utils.dataset_save_load(dataset=test_dataset)
    test_dataset.finalise_data()
    if save_load:
        test_dataset = test_utils.dataset_save_load(dataset=test_dataset)

    test_dataset.export_data()
    file_date_time_str = test_dataset.cdf_file_date_time_str

    # put expected file names together manually based on what they should be for the config dict
    output_name_str = f"{config_dict['case']}-{config_dict['replication']}" \
                      f"_S{config_dict['serial']}_{file_date_time_str}"
    output_name_str = output_name_str.replace(' ', '_')
    meta_filename = f"CDF_Metadata_{output_name_str}.yaml"
    entity_filename = f"CDF_EntityTable_{output_name_str}"
    events_filename = f"CDF_Events_{output_name_str}"
    cbt_filename = f"CDF_Cbt_Pwr_{output_name_str}"
    log_filename = f"Dataset_log_S{config_dict['serial']}_{test_dataset.init_date_time_str}.log"

    # set up an expected file dict where the keys are the expected file names and the values are the expected location
    exp_file_dict = {}

    if split_files == '1':
        meta_folder = test_dataset.meta_folder_name
        entity_folder = test_dataset.entity_folder_name
        events_folder = test_dataset.events_folder_name
        cbt_folder = test_dataset.cbt_folder_name
        log_folder = test_dataset.dataset_log_folder
    else:
        meta_folder = ""
        entity_folder = ""
        events_folder = ""
        cbt_folder = ""
        log_folder = ""

    # add the metadata file
    exp_file_dict[meta_filename] = path.join(output_path, meta_folder)

    # add the dataset log file
    exp_file_dict[log_filename] = path.join(output_path, log_folder)

    # if outputting csv add .csv format cdf files
    if output_csv == '1':
        exp_file_dict[entity_filename + ".csv"] = path.join(output_path, entity_folder)
        exp_file_dict[events_filename + ".csv"] = path.join(output_path, events_folder)
        exp_file_dict[cbt_filename + ".csv"] = path.join(output_path, cbt_folder)
    # if outputting parquet add .parquet format cdf files
    if output_parquet == '1':
        exp_file_dict[entity_filename + ".parquet"] = path.join(output_path, entity_folder)
        exp_file_dict[events_filename + ".parquet"] = path.join(output_path, events_folder)
        exp_file_dict[cbt_filename + ".parquet"] = path.join(output_path, cbt_folder)

    # check that the file names in the exp_file_dict (dict keys) are present in the expected locations (dict values)
    for exp_file in exp_file_dict.keys():
        file_ls = listdir(exp_file_dict[exp_file])
        if exp_file not in file_ls:
            fail_msg_ls.append(f"expected file: {exp_file} not found in {exp_file_dict[exp_file]}, "
                               f"files present: {file_ls}")

    test_utils.check_fail_ls(fail_msg_ls)
