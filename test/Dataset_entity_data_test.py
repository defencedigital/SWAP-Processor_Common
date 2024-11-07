import pytest

basic_ent_dict = {'uid': ['t-0'],
                  'unit_name': ['alpha'],
                  'unit_type': ['operator'],
                  'affiliation': ['blue'],
                  'commander': ['t-0'],
                  'force': ['blue force'],
                  'init_comps': [1],
                  'cbt_per_comp': [1],
                  'start_entity': [False],
                  'system_entity': [False],
                  'add_time': [0.1],
                  'uid_exp': ['t-0'],
                  'unit_name_exp': ['alpha'],
                  'unit_type_exp': ['operator'],
                  'affiliation_exp': ['blue'],
                  'commander_exp': ['t-0'],
                  'force_exp': ['blue force'],
                  'init_comps_exp': [1],
                  'cbt_per_comp_exp': [1],
                  'system_entity_exp': [False],
                  'start_entity_exp': [False],
                  'add_time_exp': [0.1],
                  'level_exp': [1]}

default_ent_dict = {'uid': ['t-0', 't-1'],
                    'unit_name_exp': ['not set', 'not set-2'],
                    'unit_type_exp': ['not set', 'not set'],
                    'affiliation_exp': ['not set', 'not set'],
                    'commander_exp': ['not set', 'not set'],
                    'force_exp': ['not set - Force', 'not set - Force'],
                    'init_comps_exp': [1, 1],
                    'cbt_per_comp_exp': [1, 1]}

duplicate_names_ent_dict = {'uid': ['t-1', 't-2', 't-3'],
                            'unit_name': ['alpha', 'alpha', 'alpha'],
                            'unit_name_exp': ['alpha', 'alpha-2', 'alpha-3']}

force_names_ent_dict = {'uid': ['t-1', 't-2'],
                        'affiliation': ['blue', 'blue'],
                        'force': ['blue', 'blue'],
                        'force_exp': ['blue - Force', 'blue - Force']}

level_ent_dict = {'uid': ['t-1', 't-2', 't-3', 't-4'],
                  'unit_name': ['alpha', 'bravo', 'charlie', 'delta'],
                  'commander': ['t-1', 't-1', 't-2', 't-3'],
                  'level_exp': [1, 2, 3, 4]}

start_ent_dict = {'uid': ['t-1', 't-2', 't-3', 't-4', 't-5'],
                  'start_entity': [True, False, True, False, False]}

add_time_ent_dict = {'uid': ['t-1', 't-2', 't-3', 't-4', 't-5'],
                     'add_time': [0.1, 0.2, 1.3, 22.6, 0.5]}

system_entity_ent_dict = {'uid': ['t-1', 't-2'],
                          'system_entity': [True, False]}


@pytest.mark.parametrize(
    'export_import',
    (
            pytest.param(False, id=''),
            pytest.param(True, id='export-import entity data'),
    )
)
@pytest.mark.parametrize(
    'ent_dict',
    (
            pytest.param(basic_ent_dict, id='basic entity'),
            pytest.param(default_ent_dict, id='default arguments'),
            pytest.param(duplicate_names_ent_dict, id='duplicate unit names test'),
            pytest.param(force_names_ent_dict, id='force names duplicating affiliation names'),
            pytest.param(level_ent_dict, id='entity level deduction'),
            pytest.param(start_ent_dict, id='start entity parameter'),
            pytest.param(add_time_ent_dict, id='add_time parameter'),
            pytest.param(system_entity_ent_dict, id='system_entity parameter'),
    )
)
def test_set_entity_data(test_utils, ent_dict, export_import):
    """
    Check that input entity data has been set correctly using the set entity data function - verify vs. the ent_dict
    Call finalise_data on the Dataset instance
    Check any processed entity data - verify vs. any ent_dict keys ending '_exp'
    where the export_import parameter is True the export_entity_dict and import_entity_dict functions will be tested
    """
    fail_msg_ls = []
    ent_idx_ls = list(range(0, len(ent_dict['uid'])))
    data_name_ls = list(ent_dict.keys())
    test_dataset = test_utils.make_dataset()
    test_utils.add_entities(dataset=test_dataset, ent_dict=ent_dict)

    if export_import:
        test_utils.dataset_export_import_entities(dataset=test_dataset, finalise=False)

    for ent_idx in ent_idx_ls:
        for data_name in data_name_ls:
            if data_name in vars(test_dataset.entities[ent_idx]).keys():
                ent_val = vars(test_dataset.entities[ent_idx])[data_name]
                exp_val = ent_dict[data_name][ent_idx]
                if ent_val != exp_val:
                    fail_msg_ls.append(f"{data_name} not set correctly for entity {ent_idx}, "
                                       f"set as {ent_val} - expected {exp_val}")

    test_dataset.finalise_data()

    for ent_idx in ent_idx_ls:
        for data_name in data_name_ls:
            if data_name[-4:] == '_exp':
                if data_name[0:-4] in vars(test_dataset.entities[ent_idx]).keys():
                    ent_val = vars(test_dataset.entities[ent_idx])[data_name[0:-4]]
                    exp_val = ent_dict[data_name][ent_idx]
                    if ent_val != exp_val:
                        fail_msg_ls.append(f"{data_name} not finalised correctly for entity {ent_idx}, "
                                           f"set as {ent_val} - expected {exp_val}")

    test_utils.check_fail_ls(fail_msg_ls)
