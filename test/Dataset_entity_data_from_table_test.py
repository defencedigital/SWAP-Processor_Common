import pandas as pd
import pytest
from os import path


@pytest.mark.parametrize(
    'export_import',
    (
            pytest.param(False, id=''),
            pytest.param(True, id='export-import entity data'),
    )
)
@pytest.mark.parametrize(
    ('input_entity_table_file', 'expected_entity_table_file'),
    (
            pytest.param('ent_tbl_a_input', 'ent_tbl_a_expected', id='copy entity data'),
            pytest.param('ent_tbl_b_input', 'ent_tbl_b_expected', id='deduce levels, populate commander names'),
            pytest.param('ent_tbl_c_input', 'ent_tbl_c_expected', id='calculate cbt pwr'),
            pytest.param('ent_tbl_d_input', 'ent_tbl_d_expected', id='repeat unit name handling'),
            pytest.param('ent_tbl_e_input', 'ent_tbl_e_expected', id='affil matching force name handling'),
            pytest.param('ent_tbl_f_input', 'ent_tbl_f_expected', id='start_entity and system_entity read'),
    )
)
def test_entity_data_from_table(test_utils, input_entity_table_file, expected_entity_table_file, export_import):
    """
    Test generates a dataset instance that reads entity data from a specified entity table file (.._input)
    The dataset instance is then finalised and its CDF_entity_table_df dataframe verified against a reference
    entity_table file (..._expected)

    if the export_import parameter is true the then export_entity_dict and import_entity_dict functions will be tested
    """
    fail_msg_ls = []
    input_entity_table_file += '.csv'
    expected_entity_table_file += '.csv'
    input_location = path.join('reference_data', 'entity_table_test')
    test_dataset = test_utils.make_dataset(dataset_config={'entity_data_from_table': '1',
                                                           'input_location': input_location,
                                                           'entity_table_file': input_entity_table_file})
    test_dataset.finalise_data()

    if export_import:
        test_utils.dataset_export_import_entities(dataset=test_dataset)

    ref_df_path = path.join(input_location, expected_entity_table_file)
    ref_df = pd.read_csv(ref_df_path, dtype=test_dataset.ent_tbl_col_types_dict)
    test_df = test_dataset.CDF_entity_table_df.copy()
    if not ref_df.equals(test_df):
        fail_msg_ls.append(test_utils.get_dataframe_diff(df_act=test_df, df_exp=ref_df))

    test_utils.check_fail_ls(fail_msg_ls)
