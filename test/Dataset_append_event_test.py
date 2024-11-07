import pytest

test_ent_dict = {'uid': ['t-1', 't-2', 't-3', 't-4'],
                 'unit_name': ['blue inf 1', 'blue inf 2', 'red inf 1', 'red inf 2'],
                 'unit_type': ['infantry', 'infantry', 'infantry', 'infantry'],
                 'affiliation': ['blue', 'blue', 'red', 'red'],
                 'init_comps': [10, 10, 10, 10],
                 'force': ['blue force', 'blue force', 'red force', 'red force']}

t1_location_dict = {'event_type': 'location',
                    'uid': 't-1',
                    'target_list': ['location_time', 'location_x', 'location_y'],
                    'data_vals': [[0, 5, 11, 22],
                                  [10, 12, 14, 12],
                                  [5, 8, 9, 10]],
                    'detail_list': 'location_detail',
                    'detail_keys': [None],
                    'detail_vals': [[None] * 4],
                    'exp_x': [10, 12, 14, 12],
                    'exp_y': [5, 8, 9, 10],
                    'exp_ent': [''] * 4}

t2_location_dict = {'event_type': 'location',
                    'uid': 't-2',
                    'target_list': ['location_time', 'location_x', 'location_y'],
                    'data_vals': [[0, 10, 15, 20],
                                  [22, 18, 15, 10],
                                  [31, 32, 36, 40]],
                    'detail_list': 'location_detail',
                    'detail_keys': [None],
                    'detail_vals': [[None] * 4],
                    'exp_x': [22, 18, 15, 10],
                    'exp_y': [31, 32, 36, 40],
                    'exp_ent': [''] * 4}

t3_location_dict = {'event_type': 'location',
                    'uid': 't-3',
                    'target_list': ['location_time', 'location_x', 'location_y'],
                    'data_vals': [[0, 20, 30],
                                  [41, 35, 38],
                                  [10, 8, 14]],
                    'detail_list': 'location_detail',
                    'detail_keys': [None],
                    'detail_vals': [[None] * 3],
                    'exp_x': [41, 35, 38],
                    'exp_y': [10, 8, 14],
                    'exp_ent': [''] * 3}

t4_location_dict = {'event_type': 'location',
                    'uid': 't-4',
                    'target_list': ['location_time', 'location_x', 'location_y'],
                    'data_vals': [[0, 15, 35],
                                  [15, 18, 12],
                                  [67, 72, 63]],
                    'detail_list': 'location_detail',
                    'detail_keys': [None],
                    'detail_vals': [[None] * 3],
                    'exp_x': [15, 18, 12],
                    'exp_y': [67, 72, 63],
                    'exp_ent': [''] * 3}

t1_shots_dict = {'event_type': 'shot',
                 'uid': 't-1',
                 'target_list': ['shots_time'],
                 'data_vals': [[2, 7, 12, 14, 25]],
                 'detail_list': 'shots_detail',
                 'detail_keys': [None],
                 'detail_vals': [[None] * 5],
                 'exp_x': [10, 12, 14, 14, 12],
                 'exp_y': [5, 8, 9, 9, 10],
                 'exp_ent': [''] * 5}

t3_shots_dict = {'event_type': 'shot',
                 'uid': 't-3',
                 'target_list': ['shots_time'],
                 'data_vals': [[1, 19, 20, 25, 31, 67]],
                 'detail_list': 'shots_detail',
                 'detail_keys': [None],
                 'detail_vals': [[None] * 6],
                 'exp_x': [41, 41, 35, 35, 38, 38],
                 'exp_y': [10, 10, 8, 8, 14, 14],
                 'exp_ent': [''] * 6}

t2_kills_dict = {'event_type': 'kill',
                 'uid': 't-2',
                 'target_list': ['kills_time', 'kills_victim'],
                 'data_vals': [[0, 12, 14, 16, 25],
                               ['t-3', 't-4', 't-3', 't-4', 't-4']],
                 'detail_list': 'kills_detail',
                 'detail_keys': [None],
                 'detail_vals': [[None] * 5],
                 'exp_x': [22, 18, 18, 15, 10],
                 'exp_y': [31, 32, 32, 36, 40],
                 'exp_ent': ['t-3', 't-4', 't-3', 't-4', 't-4']}

t4_kills_dict = {'event_type': 'kill',
                 'uid': 't-4',
                 'target_list': ['kills_time', 'kills_victim'],
                 'data_vals': [[0, 12, 17, 40],
                               ['t-1', 't-2', 't-1', 't-2']],
                 'detail_list': 'kills_detail',
                 'detail_keys': [None],
                 'detail_vals': [[None] * 4],
                 'exp_x': [15, 15, 18, 12],
                 'exp_y': [67, 67, 72, 63],
                 'exp_ent': ['t-1', 't-2', 't-1', 't-2']}

t1_loss_dict = {'event_type': 'loss',
                'uid': 't-1',
                'target_list': ['losses_time', 'losses_killer'],
                'data_vals': [[0, 17],
                              ['t-4', 't-4']],
                'detail_list': 'losses_detail',
                'detail_keys': [None],
                'detail_vals': [[None] * 2],
                'exp_x': [10, 14],
                'exp_y': [5, 9],
                'exp_ent': ['t-4', 't-4']}

t2_loss_dict = {'event_type': 'loss',
                'uid': 't-2',
                'target_list': ['losses_time', 'losses_killer'],
                'data_vals': [[12, 40],
                              ['t-4', 't-4']],
                'detail_list': 'losses_detail',
                'detail_keys': [None],
                'detail_vals': [[None] * 2],
                'exp_x': [18, 10],
                'exp_y': [32, 40],
                'exp_ent': ['t-4', 't-4']}

t3_loss_dict = {'event_type': 'loss',
                'uid': 't-3',
                'target_list': ['losses_time', 'losses_killer'],
                'data_vals': [[0, 14],
                              ['t-1', 't-1']],
                'detail_list': 'losses_detail',
                'detail_keys': [None],
                'detail_vals': [[None] * 2],
                'exp_x': [41, 41],
                'exp_y': [10, 10],
                'exp_ent': ['t-1', 't-1']}

t4_loss_dict = {'event_type': 'loss',
                'uid': 't-4',
                'target_list': ['losses_time', 'losses_killer'],
                'data_vals': [[12, 16, 25],
                              ['t-1', 't-1', 't-1']],
                'detail_list': 'losses_detail',
                'detail_keys': [None],
                'detail_vals': [[None] * 3],
                'exp_x': [15, 18, 18],
                'exp_y': [67, 72, 72],
                'exp_ent': ['t-1', 't-1', 't-1']}

t1_spot_dict = {'event_type': 'spot',
                'uid': 't-1',
                'target_list': ['spot_time', 'spot_entity'],
                'data_vals': [[0, 3, 4, 7, 8, 15, 25],
                              ['t-3', 't-2', 't-4', 't-2', 't-3', 't-2', 't-4']],
                'detail_list': 'spot_detail',
                'detail_keys': [None],
                'detail_vals': [[None] * 7],
                'exp_x': [10, 10, 10, 12, 12, 14, 12],
                'exp_y': [5, 5, 5, 8, 8, 9, 10],
                'exp_ent': ['t-3', 't-2', 't-4', 't-2', 't-3', 't-2', 't-4']}

t3_spot_dict = {'event_type': 'spot',
                'uid': 't-3',
                'target_list': ['spot_time', 'spot_entity'],
                'data_vals': [[0, 15, 23, 35, 55],
                              ['t-1', 't-2', 't-4', 't-1', 't-2']],
                'detail_list': 'spot_detail',
                'detail_keys': [None],
                'detail_vals': [[None] * 5],
                'exp_x': [41, 41, 35, 38, 38],
                'exp_y': [10, 10, 8, 14, 14],
                'exp_ent': ['t-1', 't-2', 't-4', 't-1', 't-2']}

t1_seen_dict = {'event_type': 'seen',
                'uid': 't-1',
                'target_list': ['seen_time', 'seen_entity'],
                'data_vals': [[0, 35],
                              ['t-3', 't-3']],
                'detail_list': 'seen_detail',
                'detail_keys': [None],
                'detail_vals': [[None] * 2],
                'exp_x': [10, 12],
                'exp_y': [5, 10],
                'exp_ent': ['t-3', 't-3']}

t2_seen_dict = {'event_type': 'seen',
                'uid': 't-2',
                'target_list': ['seen_time', 'seen_entity'],
                'data_vals': [[3, 7, 15, 3, 7, 15],
                              ['t-3', 't-3', 't-3', 't-1', 't-1', 't-1']],
                'detail_list': 'seen_detail',
                'detail_keys': [None],
                'detail_vals': [[None] * 6],
                'exp_x': [22, 22, 15, 22, 22, 15],
                'exp_y': [31, 31, 36, 31, 31, 36],
                'exp_ent': ['t-3', 't-3', 't-3', 't-1', 't-1', 't-1']}

t3_seen_dict = {'event_type': 'seen',
                'uid': 't-3',
                'target_list': ['seen_time', 'seen_entity'],
                'data_vals': [[0, 8],
                              ['t-1', 't-1']],
                'detail_list': 'seen_detail',
                'detail_keys': [None],
                'detail_vals': [[None] * 2],
                'exp_x': [41, 41],
                'exp_y': [10, 10],
                'exp_ent': ['t-1', 't-1']}

t4_seen_dict = {'event_type': 'seen',
                'uid': 't-4',
                'target_list': ['seen_time', 'seen_entity'],
                'data_vals': [[4, 25, 23],
                              ['t-1', 't-1', 't-3']],
                'detail_list': 'seen_detail',
                'detail_keys': [None],
                'detail_vals': [[None] * 3],
                'exp_x': [15, 18, 18],
                'exp_y': [67, 72, 72],
                'exp_ent': ['t-1', 't-1', 't-3']}

t1_status_dict = {'event_type': 'status',
                  'uid': 't-1',
                  'target_list': ['state_time'],
                  'data_vals': [[0, 23]],
                  'detail_list': 'state_detail',
                  'detail_keys': [None],
                  'detail_vals': [[None] * 2],
                  'exp_x': [10, 12],
                  'exp_y': [5, 10],
                  'exp_ent': [''] * 2}

t2_status_dict = {'event_type': 'status',
                  'uid': 't-2',
                  'target_list': ['state_time'],
                  'data_vals': [[0, 21]],
                  'detail_list': 'state_detail',
                  'detail_keys': [None],
                  'detail_vals': [[None] * 2],
                  'exp_x': [22, 10],
                  'exp_y': [31, 40],
                  'exp_ent': [''] * 2}

t3_stop_dict = {'event_type': 'stop',
                'uid': 't-3',
                'target_list': ['stop_time', 'stop_entity'],
                'data_vals': [[0, 20, 31],
                              ['t-1', 't-2', 't-4']],
                'detail_list': 'stop_detail',
                'detail_keys': [None],
                'detail_vals': [[None] * 3],
                'exp_x': [41, 35, 38],
                'exp_y': [10, 8, 14],
                'exp_ent': ['t-1', 't-2', 't-4']}

t4_stop_dict = {'event_type': 'stop',
                'uid': 't-4',
                'target_list': ['stop_time', 'stop_entity'],
                'data_vals': [[0, 15, 36],
                              ['t-1', 't-2', 't-3']],
                'detail_list': 'stop_detail',
                'detail_keys': [None],
                'detail_vals': [[None] * 3],
                'exp_x': [15, 18, 12],
                'exp_y': [67, 72, 63],
                'exp_ent': ['t-1', 't-2', 't-3']}

append_event_dict_ls = [t1_location_dict, t2_location_dict, t3_location_dict, t4_location_dict,
                        t1_shots_dict, t3_shots_dict,
                        t2_kills_dict, t4_kills_dict,
                        t1_loss_dict, t2_loss_dict, t3_loss_dict, t4_loss_dict,
                        t1_spot_dict, t3_spot_dict,
                        t1_seen_dict, t2_seen_dict, t3_seen_dict, t4_seen_dict,
                        t1_status_dict, t2_status_dict, t3_stop_dict, t4_stop_dict]


@pytest.fixture(params=append_event_dict_ls)
def get_append_event_dict(request):
    return request.param


@pytest.mark.parametrize(
    'export_import',
    (
            pytest.param(False, id=''),
            pytest.param(True, id='export-import entity data'),
    )
)
def test_append_event(test_utils, get_append_event_dict, export_import):
    """
    Add multiple events of different types to a Dataset instance using the Dataset append to list function
    Finalise Dataset instance
    For each append_event_dict:
        Confirm that the total number of events of that type for the entity they were appended to
        in the Dataset CDF events dataframe is correct
        Confirm that events have parameters as specified in the corresponding append_event_dict
        (using combination of primary id, time, x, y and secondary id as appropriate)

    if the export_import parameter is true then the entity_export_dict and entity_import_dict functions will be tested
    """
    fail_msg_ls = []
    test_dataset = test_utils.make_dataset()
    test_utils.add_entities(dataset=test_dataset, ent_dict=test_ent_dict)
    for append_event_dict in append_event_dict_ls:
        test_utils.append_events(dataset=test_dataset, append_event_dict=append_event_dict)
    test_dataset.finalise_data()

    if export_import:
        test_utils.dataset_export_import_entities(dataset=test_dataset)

    event_lbl_dict = {'location': test_dataset.loc_event_lbl,
                      'shot': test_dataset.shot_event_lbl,
                      'kill': test_dataset.kill_event_lbl,
                      'loss': test_dataset.loss_event_lbl,
                      'seen': test_dataset.seen_event_lbl,
                      'spot': test_dataset.spot_event_lbl,
                      'status': test_dataset.status_event_lbl,
                      'stop': test_dataset.stop_event_lbl}

    event_check_dict = get_append_event_dict
    event_check_lbl = event_lbl_dict[event_check_dict['event_type']]

    cdf_events_dict = test_dataset.CDF_events_df.loc[
        (test_dataset.CDF_events_df[test_dataset.evn_tbl_event_type_col_lbl] == event_check_lbl) &
        (test_dataset.CDF_events_df[test_dataset.evn_tbl_prim_id_col_lbl] == event_check_dict['uid']),
        [test_dataset.evn_tbl_prim_id_col_lbl,
         test_dataset.evn_tbl_time_col_lbl,
         test_dataset.evn_tbl_prim_x_col_lbl,
         test_dataset.evn_tbl_prim_y_col_lbl,
         test_dataset.evn_tbl_sec_id_col_lbl]].to_dict(orient='records')

    num_events = len(cdf_events_dict)
    exp_events = len(event_check_dict['data_vals'][0])
    if num_events != exp_events:
        fail_msg_ls.append(f"expected {exp_events} events of type {event_check_dict['event_type']} "
                           f"but {num_events} in CDF events DataFrame")

    for idx, exp_time in enumerate(event_check_dict['data_vals'][0]):
        event_record = {test_dataset.evn_tbl_prim_id_col_lbl: event_check_dict['uid'],
                        test_dataset.evn_tbl_time_col_lbl: exp_time,
                        test_dataset.evn_tbl_prim_x_col_lbl: event_check_dict['exp_x'][idx],
                        test_dataset.evn_tbl_prim_y_col_lbl: event_check_dict['exp_y'][idx],
                        test_dataset.evn_tbl_sec_id_col_lbl: event_check_dict['exp_ent'][idx]}

        if event_record not in cdf_events_dict:
            fail_msg_ls.append(f"{event_check_dict['event_type']} event "
                               f"({event_record}) not present in CDF events Dataframe")

    test_utils.check_fail_ls(fail_msg_ls)
