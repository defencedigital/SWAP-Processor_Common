import pytest

ent_dict = {'uid': ['t-1']}


@pytest.mark.parametrize(
    'export_import',
    (
            pytest.param(False, id=''),
            pytest.param(True, id='export-import entity data'),
    )
)
@pytest.mark.parametrize('event_type', ('location', 'shot', 'kill', 'loss', 'seen', 'spot', 'status', 'stop'))
@pytest.mark.parametrize(
    ('detail_keys', 'detail_vals', 'encode_expected'),
    (
            pytest.param(['key1', 'key2'], ['val1'],
                         '{"key1":"val1", "key2":"no_val"}',
                         id='2 x key, 1 x val'),
            pytest.param(['key1', 'key2', 'key3'], ['val1'],
                         '{"key1":"val1", "key2":"no_val", "key3":"no_val"}',
                         id='3 x key, 1 x val'),
            pytest.param(['key1', 'key2'], ['val1', 'val2', 'val3'],
                         '{"key1":"val1", "key2":"val2", "no_key":"val3"}',
                         id='2 x key, 3 x val'),
            pytest.param(['key1'], ['val1', 'val2', 'val3'],
                         '{"key1":"val1", "no_key":"val2", "no_key":"val3"}',
                         id='1 x key, 3 x val'),
            pytest.param(['key 1', 'key,2', '"key":3'], ['val:1', '"val" 2', 'val,3'],
                         '{"key_1":"val_1", "key_2":"_val__2", "_key__3":"val_3"}',
                         id='space, comma, double quote and separator chars in keys and vals'),
            pytest.param(['key{1', 'key}2', 'key{3'], ['val}1', 'val{2', 'val}3'],
                         '{"key_1":"val_1", "key_2":"val_2", "key_3":"val_3"}',
                         id='brace chars in keys and vals'),
            pytest.param(['key{1', 'key}2', 'key{3', 'k,e:y 4'], ['v al}1', 'v,a:l{2', 'val}3'],
                         '{"key_1":"v_al_1", "key_2":"v_a_l_2", "key_3":"val_3", "k_e_y_4":"no_val"}',
                         id='combined test'),
    )
)
def test_add_event_detail_encode(test_utils, event_type, detail_keys, detail_vals, encode_expected, export_import):
    """
    Add a single event to an entity via add_event function and check the encoded event detail is as expected
    if export_import parameter is true the export_entity_dict and import entity_dict functions will be tested
    """
    fail_msg_ls = []
    test_dataset = test_utils.make_dataset()
    test_utils.add_entities(dataset=test_dataset, ent_dict=ent_dict)
    event_dict = {'event_type': event_type,
                  'uid': ['t-1'],
                  'time': [0.0],
                  'x': ['0'],
                  'y': ['0'],
                  'entity': [None],
                  'detail_keys': detail_keys,
                  'detail_vals': [detail_vals]}
    test_utils.add_single_events(dataset=test_dataset, event_dict=event_dict)
    test_dataset.finalise_data()

    if export_import:
        test_utils.dataset_export_import_entities(dataset=test_dataset)

    cdf_events_df = test_dataset.CDF_events_df.copy()
    detail_str = cdf_events_df.loc[0, test_dataset.evn_tbl_event_detail_col_lbl]

    if detail_str != encode_expected:
        fail_msg_ls.append(f"detail encode for {event_type} with keys:{detail_keys} and vals:{detail_vals} "
                           f"was {detail_str} but expected {encode_expected}")

    test_utils.check_fail_ls(fail_msg_ls)


@pytest.mark.parametrize(
    'export_import',
    (
            pytest.param(False, id=''),
            pytest.param(True, id='export-import entity data'),
    )
)
@pytest.mark.parametrize(
    ('target_list_set', 'detail_list'),
    (
            pytest.param(['location_time', 'location_x', 'location_y'], 'location_detail',
                         id='location event lists'),
            pytest.param(['shots_time'], 'shots_detail',
                         id='shot event lists'),
            pytest.param(['kills_time', 'kills_victim'], 'kills_detail',
                         id='kill event lists'),
            pytest.param(['losses_time', 'losses_killer'], 'losses_detail',
                         id='loss event lists'),
            pytest.param(['spot_time', 'spot_entity'], 'spot_detail',
                         id='spot event lists'),
            pytest.param(['seen_time', 'seen_entity'], 'seen_detail',
                         id='seen event lists'),
    )
)
@pytest.mark.parametrize(
    ('detail_keys', 'detail_val_lists', 'encode_expected_ls'),
    (
            pytest.param(['key1'], [['k1-val1', 'k1-val2', 'k1-val3']],
                         ['{"key1":"k1-val1"}',
                          '{"key1":"k1-val2"}',
                          '{"key1":"k1-val3"}'],
                         id='single key, 3 events'),
            pytest.param(['key1', 'key2'], [['k1-val1', 'k1-val2'], ['k2-val1', 'k2-val2']],
                         ['{"key1":"k1-val1", "key2":"k2-val1"}',
                          '{"key1":"k1-val2", "key2":"k2-val2"}'],
                         id='2 x keys, 2 x events'),
            pytest.param(['key1'], [['k1-val1', 'k1-val2'], ['k2-val1', 'k2-val2']],
                         ['{"key1":"k1-val1", "no_key":"k2-val1"}',
                          '{"key1":"k1-val2", "no_key":"k2-val2"}'],
                         id='extra list of vals'),
            pytest.param(['key1', 'key2'], [['k1-val1', 'k1-val2']],
                         ['{"key1":"k1-val1", "key2":"no_val"}',
                          '{"key1":"k1-val2", "key2":"no_val"}'],
                         id='missing list of vals'),
            pytest.param(['ke{}y: "1"'], [['k1:val{}1', 'k1-,,val2', 'k1-val"3']],
                         ['{"ke__y___1_":"k1_val__1"}',
                          '{"ke__y___1_":"k1-__val2"}',
                          '{"ke__y___1_":"k1-val_3"}'],
                         id='char replacement (comma, space, separator, braces, double quote'),
    )

)
def test_append_event_detail_encode(test_utils, target_list_set, detail_list,
                                    detail_keys, detail_val_lists, encode_expected_ls, export_import):
    """
    Add multiple events via the append_to_list function and check event detail encoding is as expected
    if the export_import parameter is true the entity_export_dict and entity_import_dict functions will be tested
    """
    fail_msg_ls = []
    test_dataset = test_utils.make_dataset()
    test_utils.add_entities(dataset=test_dataset, ent_dict=ent_dict)

    # first item in data lists is a list of 0.0 for the event time list
    data_lists = [[0.0] * len(encode_expected_ls)]

    # add further lists with None for the remaining data lists
    while len(data_lists) < len(target_list_set):
        data_lists.append([None] * len(encode_expected_ls))

    append_event_dict = {'uid': 't-1',
                         'target_list': target_list_set,
                         'data_vals': data_lists,
                         'detail_list': detail_list,
                         'detail_keys': detail_keys,
                         'detail_vals': detail_val_lists}

    test_utils.append_events(dataset=test_dataset, append_event_dict=append_event_dict)
    test_dataset.finalise_data()

    if export_import:
        test_utils.dataset_export_import_entities(dataset=test_dataset)

    detail_col_ls = test_dataset.CDF_events_df[test_dataset.evn_tbl_event_detail_col_lbl].to_list()
    if len(detail_col_ls) == 0:
        fail_msg_ls.append("no encoded detail strings to check!")
    else:
        for idx, detail_str in enumerate(detail_col_ls):
            encode_expected = encode_expected_ls[idx]
            if detail_str != encode_expected:
                fail_msg_ls.append(f"detail encode was {detail_str} but expected {encode_expected}")

    test_utils.check_fail_ls(fail_msg_ls)
