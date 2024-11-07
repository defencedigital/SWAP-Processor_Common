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
                        'x': [8, 9, 7, 19, 21, 22],
                        'y': [12, 15, 18, 17, 18, 15],
                        'entity': [''] * 6,
                        'detail_keys': [],
                        'detail_vals': [[None]] * 6}

shot_events_dict = {'event_type': 'shot',
                    'uid': ['t-r', 't-b', 't-b', 't-b', 't-r', 't-r', 't-r', 't-b'],
                    'time': [15.1, 3.1, 7, 13, 3, 9, 15, 7],
                    'x': [22, 8, 9, 7, 19, 21, 22, 9],
                    'y': [15, 12, 15, 18, 17, 18, 15, 15],
                    'entity': [''] * 8,
                    'detail_keys': [],
                    'detail_vals': [[None]] * 8}

kill_events_dict = {'event_type': 'kill',
                    'uid': ['t-b', 't-r', 't-b', 't-r'],
                    'time': [13, 14, 8, 7],
                    'x': [7, 22, 9, 19],
                    'y': [18, 15, 15, 17],
                    'entity': ['t-r', 't-b', 't-r', 't-b'],
                    'detail_keys': [],
                    'detail_vals': [[None]] * 4}

loss_events_dict = {'event_type': 'loss',
                    'uid': ['t-r', 't-r', 't-b', 't-b'],
                    'time': [13, 8, 14, 7],
                    'x': [22, 21, 7, 9],
                    'y': [15, 18, 18, 15],
                    'entity': ['t-b', 't-b', 't-r', 't-r'],
                    'detail_keys': [],
                    'detail_vals': [[None]] * 4}

seen_events_dict = {'event_type': 'seen',
                    'uid': ['t-r', 't-r', 't-r', 't-b', 't-b', 't-b'],
                    'time': [15, 9, 1, 3, 38, 6],
                    'x': [22, 21, 19, 8, 7, 9],
                    'y': [15, 18, 17, 12, 18, 15],
                    'entity': ['t-b', 't-b', 't-b', 't-r', 't-r', 't-r'],
                    'detail_keys': [],
                    'detail_vals': [[None]] * 6}

spot_events_dict = {'event_type': 'spot',
                    'uid': ['t-b', 't-b', 't-b', 't-r', 't-r', 't-r'],
                    'time': [15, 9, 1, 3, 38, 6],
                    'x': [7, 9, 8, 19, 22, 19],
                    'y': [18, 15, 12, 17, 15, 17],
                    'entity': ['t-r', 't-r', 't-r', 't-b', 't-b', 't-b'],
                    'detail_keys': [],
                    'detail_vals': [[None]] * 6}

status_event_dict = {'event_type': 'status',
                     'uid': ['t-b', 't-b', 't-r', 't-r'],
                     'time': [0.0, 12.0, 0.0, 13.0],
                     'x': [8, 7, 19, 22],
                     'y': [12, 18, 17, 15],
                     'entity': ['']*4,
                     'detail_keys': [],
                     'detail_vals': [[None]]*4}

stop_event_dict = {'event_type': 'stop',
                   'uid': ['t-b', 't-r'],
                   'time': [6.3, 8.4],
                   'x': [9, 21],
                   'y': [15, 18],
                   'entity': ['']*2,
                   'detail_keys': [],
                   'detail_vals': [[None]]*2}

event_dict_ls = [location_events_dict, shot_events_dict, kill_events_dict, loss_events_dict,
                 seen_events_dict, spot_events_dict, status_event_dict, stop_event_dict]


@pytest.fixture(params=event_dict_ls)
def get_event_dict(request):
    return request.param


@pytest.mark.parametrize(
    'export_import',
    (
            pytest.param(False, id=''),
            pytest.param(True, id='export-import entity data'),
    )
)
def test_add_events(test_utils, get_event_dict, export_import):
    """
    Add multiple events of different types to a Dataset instance using the add single event functions
    Finalise Dataset instance
    For each event type:
        Confirm that the total number of events of that type in the Dataset CDF events dataframe is correct
        Confirm that events of that type have parameters as specified in the corresponding event_dict
        (using combination of primary id, time, x, y and secondary id)

    if the export_import parameter is true then the entity_export_dict and entity_import_dict functions will be tested
    """
    fail_msg_ls = []
    test_dataset = test_utils.make_dataset()
    test_utils.add_entities(dataset=test_dataset, ent_dict=test_ent_dict)
    for event_dict in event_dict_ls:
        test_utils.add_single_events(dataset=test_dataset, event_dict=event_dict)
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

    event_check_dict = get_event_dict
    event_check_lbl = event_lbl_dict[event_check_dict['event_type']]

    cdf_events_dict = test_dataset.CDF_events_df.loc[
        test_dataset.CDF_events_df[test_dataset.evn_tbl_event_type_col_lbl] == event_check_lbl,
        [test_dataset.evn_tbl_prim_id_col_lbl,
         test_dataset.evn_tbl_time_col_lbl,
         test_dataset.evn_tbl_prim_x_col_lbl,
         test_dataset.evn_tbl_prim_y_col_lbl,
         test_dataset.evn_tbl_sec_id_col_lbl]].to_dict(orient='records')
    print(cdf_events_dict)

    num_events = len(cdf_events_dict)
    exp_events = len(event_check_dict['uid'])
    if num_events != exp_events:
        fail_msg_ls.append(f"expected {exp_events} events of type {event_check_dict['event_type']} "
                           f"but {num_events} in CDF events DataFrame")

    for idx, uid in enumerate(event_check_dict['uid']):
        event_record = {test_dataset.evn_tbl_prim_id_col_lbl: uid,
                        test_dataset.evn_tbl_time_col_lbl: event_check_dict['time'][idx],
                        test_dataset.evn_tbl_prim_x_col_lbl: event_check_dict['x'][idx],
                        test_dataset.evn_tbl_prim_y_col_lbl: event_check_dict['y'][idx],
                        test_dataset.evn_tbl_sec_id_col_lbl: event_check_dict['entity'][idx]}

        if event_record not in cdf_events_dict:
            fail_msg_ls.append(f"{event_check_dict['event_type']} event "
                               f"({event_record}) not present in CDF events Dataframe")

    test_utils.check_fail_ls(fail_msg_ls)
