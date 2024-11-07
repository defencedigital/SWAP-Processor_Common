import pytest

test_ent_dict = {'uid': ['t-b1', 't-b2', 't-r1', 't-r2', 't-z']}

location_events_dict = {'event_type': 'location',
                        'uid': ['t-b1', 't-b2', 't-b1', 't-r2', 't-r1', 't-r1'],
                        'time': [0.0, 5.0, 12.0, 0, 8, 13],
                        'x': ['8', '9', '7', '19', '21', '22'],
                        'y': ['12', '15', '18', '17', '18', '15'],
                        'entity': [None] * 6,
                        'detail_keys': ['terrain', 'status'],
                        'detail_vals': [['urban', 'moving'],
                                        ['hilly', 'halted'],
                                        ['forest', 'advancing'],
                                        ['urban', 'halted'],
                                        ['open', 'advancing'],
                                        ['open', 'moving']],
                        'exp_detail': ['{"terrain":"urban", "status":"moving"}',
                                       '{"terrain":"hilly", "status":"halted"}',
                                       '{"terrain":"forest", "status":"advancing"}',
                                       '{"terrain":"urban", "status":"halted"}',
                                       '{"terrain":"open", "status":"advancing"}',
                                       '{"terrain":"open", "status":"moving"}'],
                        'exp_id': ['loc-1', 'loc-2', 'loc-3', 'loc-4', 'loc-5', 'loc-6']}

shot_events_dict = {'event_type': 'shot',
                    'uid': ['t-r1', 't-b1', 't-b2', 't-b1', 't-r2', 't-r2', 't-r2', 't-b1'],
                    'time': [15.1, 3.1, 7, 13, 3, 9, 15, 7],
                    'entity': [None] * 8,
                    'detail_keys': ['weapon'],
                    'detail_vals': [['rifle'], ['sniper'], ['mg'], ['cannon'],
                                    ['rocket'], ['grenade'], ['railgun'], ['chaingun']],
                    'exp_detail': ['{"weapon":"rifle"}', '{"weapon":"sniper"}',
                                   '{"weapon":"mg"}', '{"weapon":"cannon"}',
                                   '{"weapon":"rocket"}', '{"weapon":"grenade"}',
                                   '{"weapon":"railgun"}', '{"weapon":"chaingun"}'],
                    'exp_id': ['shot-1', 'shot-2', 'shot-3', 'shot-4', 'shot-5', 'shot-6', 'shot-7', 'shot-8']}

kill_events_dict = {'event_type': 'kill',
                    'uid': ['t-b1', 't-r1', 't-b2', 't-r1'],
                    'time': [13, 14, 8, 7],
                    'entity': ['t-r2', 't-b2', 't-r1', 't-b1'],
                    'detail_keys': ['kill-id'],
                    'detail_vals': [['1'], ['2'], ['3'], ['4']],
                    'exp_detail': ['{"kill-id":"1"}', '{"kill-id":"2"}', '{"kill-id":"3"}', '{"kill-id":"4"}'],
                    'exp_id': ['kill-1', 'kill-2', 'kill-3', 'kill-4']}

loss_events_dict = {'event_type': 'loss',
                    'uid': ['t-r2', 't-r1', 't-b1', 't-b2'],
                    'time': [13, 8, 14, 7],
                    'entity': ['t-b1', 't-b2', 't-r1', 't-r2'],
                    'detail_keys': ['loss-id'],
                    'detail_vals': [['1'], ['2'], ['3'], ['4']] * 4,
                    'exp_detail': ['{"loss-id":"1"}', '{"loss-id":"2"}', '{"loss-id":"3"}', '{"loss-id":"4"}'],
                    'exp_id': ['loss-1', 'loss-2', 'loss-3', 'loss-4']}

seen_events_dict = {'event_type': 'seen',
                    'uid': ['t-r1', 't-r2', 't-r1', 't-b1', 't-b2', 't-b1'],
                    'time': [15, 9, 1, 3, 38, 6],
                    'entity': ['t-b1', 't-b2', 't-b2', 't-r2', 't-r2', 't-r1'],
                    'detail_keys': ['seen-id'],
                    'detail_vals': [['1'], ['2'], ['3'], ['4'], ['5'], ['6']] * 6,
                    'exp_detail': ['{"seen-id":"1"}', '{"seen-id":"2"}', '{"seen-id":"3"}',
                                   '{"seen-id":"4"}', '{"seen-id":"5"}', '{"seen-id":"6"}'],
                    'exp_id': ['seen-1', 'seen-2', 'seen-3', 'seen-4', 'seen-5', 'seen-6']}

spot_events_dict = {'event_type': 'spot',
                    'uid': ['t-b1', 't-b1', 't-b2', 't-r2', 't-r1', 't-r2'],
                    'time': [15, 9, 1, 3, 38, 6],
                    'entity': ['t-r1', 't-r2', 't-r1', 't-b1', 't-b2', 't-b1'],
                    'detail_keys': ['level'],
                    'detail_vals': [['id'], ['detect'], ['recog'], ['confirm'], ['none'], ['verify']],
                    'exp_detail': ['{"level":"id"}', '{"level":"detect"}', '{"level":"recog"}',
                                   '{"level":"confirm"}', '{"level":"none"}', '{"level":"verify"}'],
                    'exp_id': ['spot-1', 'spot-2', 'spot-3', 'spot-4', 'spot-5', 'spot-6']}

stop_events_dict = {'event_type': 'stop',
                    'uid': ['t-b1', 't-b2'],
                    'time': [15.2, 1.5],
                    'entity': ['t-r1', 't-r1'],
                    'detail_keys': ['cause'],
                    'detail_vals': [['lost_sight'], ['forgot']],
                    'exp_detail': ['{"cause":"lost_sight"}', '{"cause":"forgot"}'],
                    'exp_id': ['stop-1', 'stop-2']}

status_events_dict = {'event_type': 'status',
                      'uid': ['t-b1', 't-r1'],
                      'time': [0.5, 8.5],
                      'entity': [None] * 2,
                      'detail_keys': ['status'],
                      'detail_vals': [['planning'], ['arriving']],
                      'exp_detail': ['{"status":"planning"}', '{"status":"arriving"}'],
                      'exp_id': ['status-1', 'status-2']}

add_event_dict_ls = [location_events_dict, shot_events_dict,
                     spot_events_dict, seen_events_dict,
                     kill_events_dict, loss_events_dict,
                     stop_events_dict, status_events_dict]

tz_location_dict = {'event_type': 'location',
                    'uid': 't-z',
                    'target_list': ['location_time', 'location_x', 'location_y'],
                    'data_vals': [[0, 5, 11, 22],
                                  ['10', '12', '14', '12'],
                                  ['5', '8', '9', '10']],
                    'detail_list': 'location_detail',
                    'detail_keys': ['terrain'],
                    'detail_vals': [['open', 'hilly', 'forest', 'urban']],
                    'exp_x': ['10', '12', '14', '12'],
                    'exp_y': ['5', '8', '9', '10'],
                    'exp_ent': [None, None, None, None],
                    'exp_detail': ['{"terrain":"open"}', '{"terrain":"hilly"}',
                                   '{"terrain":"forest"}', '{"terrain":"urban"}'],
                    'exp_id': ['loc-7', 'loc-8', 'loc-9', 'loc-10']}

tz_shots_dict = {'event_type': 'shot',
                 'uid': 't-z',
                 'target_list': ['shots_time'],
                 'data_vals': [[2, 7, 12, 14, 25]],
                 'detail_list': 'shots_detail',
                 'detail_keys': ['weapon'],
                 'detail_vals': [['assault rifle', 'battle rifle', 'marksman rifle', 'sniper rifle', 'rail rifle']],
                 'exp_ent': [None, None, None, None, None],
                 'exp_detail': ['{"weapon":"assault_rifle"}', '{"weapon":"battle_rifle"}',
                                '{"weapon":"marksman_rifle"}', '{"weapon":"sniper_rifle"}',
                                '{"weapon":"rail_rifle"}'],
                 'exp_id': ['shot-9', 'shot-10', 'shot-11', 'shot-12', 'shot-13']}

tz_spot_dict = {'event_type': 'spot',
                'uid': 't-z',
                'target_list': ['spot_time', 'spot_entity'],
                'data_vals': [[0, 15, 23, 35, 55],
                              ['t-b1', 't-b2', 't-b1', 't-r1', 't-r2']],
                'detail_list': 'spot_detail',
                'detail_keys': ['spot-id'],
                'detail_vals': [['7', '8', '9', '10', '11']],
                'exp_ent': ['t-b1', 't-b2', 't-b1', 't-r1', 't-r2'],
                'exp_detail': ['{"spot-id":"7"}', '{"spot-id":"8"}', '{"spot-id":"9"}',
                               '{"spot-id":"10"}', '{"spot-id":"11"}'],
                'exp_id': ['spot-7', 'spot-8', 'spot-9', 'spot-10', 'spot-11']}

tz_seen_dict = {'event_type': 'seen',
                'uid': 't-z',
                'target_list': ['seen_time', 'seen_entity'],
                'data_vals': [[0, 8],
                              ['t-r1', 't-b2']],
                'detail_list': 'seen_detail',
                'detail_keys': ['range'],
                'detail_vals': [['100m', '250m']],
                'exp_ent': ['t-r1', 't-b2'],
                'exp_detail': ['{"range":"100m"}', '{"range":"250m"}'],
                'exp_id': ['seen-7', 'seen-8']}

tz_kills_dict = {'event_type': 'kill',
                 'uid': 't-z',
                 'target_list': ['kills_time', 'kills_victim'],
                 'data_vals': [[0, 12, 14, 16, 25],
                               ['t-b1', 't-b2', 't-r1', 't-r2', 't-b1']],
                 'detail_list': 'kills_detail',
                 'detail_keys': ['kill-id'],
                 'detail_vals': [['5', '6', '7', '8', '9']],
                 'exp_ent': ['t-b1', 't-b2', 't-r1', 't-r2', 't-b1'],
                 'exp_detail': ['{"kill-id":"5"}', '{"kill-id":"6"}', '{"kill-id":"7"}',
                                '{"kill-id":"8"}', '{"kill-id":"9"}'],
                 'exp_id': ['kill-5', 'kill-6', 'kill-7', 'kill-8', 'kill-9']}

tz_loss_dict = {'event_type': 'loss',
                'uid': 't-z',
                'target_list': ['losses_time', 'losses_killer'],
                'data_vals': [[0, 17],
                              ['t-b1', 't-r2']],
                'detail_list': 'losses_detail',
                'detail_keys': ['loss-id'],
                'detail_vals': [['5', '6']],
                'exp_ent': ['t-b1', 't-r2'],
                'exp_detail': ['{"loss-id":"5"}', '{"loss-id":"6"}'],
                'exp_id': ['loss-5', 'loss-6']}

tz_stop_dict = {'event_type': 'stop',
                'uid': 't-z',
                'target_list': ['stop_time', 'stop_entity'],
                'data_vals': [[0.8, 15.6],
                              ['t-b1', 't-b2']],
                'detail_list': 'stop_detail',
                'detail_keys': ['reason'],
                'detail_vals': [['lost_line_of_sight', 'target_hiding']],
                'exp_ent': ['t-b1', 't-b2'],
                'exp_detail': ['{"reason":"lost_line_of_sight"}', '{"reason":"target_hiding"}'],
                'exp_id': ['stop-3', 'stop-4']}

tz_status_dict = {'event_type': 'status',
                  'uid': 't-z',
                  'target_list': ['state_time'],
                  'data_vals': [[1.0, 25, 57]],
                  'detail_list': 'state_detail',
                  'detail_keys': ['status'],
                  'detail_vals': [['getting_ready', 'on_mission', 'standing_down']],
                  'exp_ent': [None, None, None],
                  'exp_detail': ['{"status":"getting_ready"}', '{"status":"on_mission"}', '{"status":"standing_down"}'],
                  'exp_id': ['status-3', 'status-4', 'status-5']}

append_event_dict_ls = [tz_location_dict, tz_shots_dict,
                        tz_spot_dict, tz_seen_dict,
                        tz_kills_dict, tz_loss_dict,
                        tz_stop_dict, tz_status_dict]


def get_exp_dict(event_id: str) -> dict:
    """
    take an event id as input and return the append_event_dict or add_event_dict for that event_id
    """
    exp_dict = None
    if event_id in location_events_dict['exp_id']:
        exp_dict = location_events_dict
    elif event_id in tz_location_dict['exp_id']:
        exp_dict = tz_location_dict
    elif event_id in shot_events_dict['exp_id']:
        exp_dict = shot_events_dict
    elif event_id in tz_shots_dict['exp_id']:
        exp_dict = tz_shots_dict
    elif event_id in spot_events_dict['exp_id']:
        exp_dict = spot_events_dict
    elif event_id in tz_spot_dict['exp_id']:
        exp_dict = tz_spot_dict
    elif event_id in seen_events_dict['exp_id']:
        exp_dict = seen_events_dict
    elif event_id in tz_seen_dict['exp_id']:
        exp_dict = tz_seen_dict
    elif event_id in kill_events_dict['exp_id']:
        exp_dict = kill_events_dict
    elif event_id in tz_kills_dict['exp_id']:
        exp_dict = tz_kills_dict
    elif event_id in loss_events_dict['exp_id']:
        exp_dict = loss_events_dict
    elif event_id in tz_loss_dict['exp_id']:
        exp_dict = tz_loss_dict
    elif event_id in stop_events_dict['exp_id']:
        exp_dict = stop_events_dict
    elif event_id in tz_stop_dict['exp_id']:
        exp_dict = tz_stop_dict
    elif event_id in status_events_dict['exp_id']:
        exp_dict = status_events_dict
    elif event_id in tz_status_dict['exp_id']:
        exp_dict = tz_status_dict

    return exp_dict


remove_event_params = [
    [],
    ['loc-5'],  # remove single event
    ['loc-1', 'shot-1', 'spot-1', 'seen-1', 'kill-1', 'loss-1', 'stop-1', 'status-1'],  # first of each type
    ['loc-10', 'shot-13', 'spot-11', 'seen-8', 'kill-9', 'loss-6', 'stop-4', 'status-5'],  # last of each type
    ['loc-3', 'loc-6', 'shot-3', 'shot-7', 'shot-9', 'spot-2', 'spot-7', 'seen-2', 'seen-4',
     'kill-3', 'kill-8', 'loss-2', 'loss-5', 'stop-2', 'stop-4', 'status-2', 'status-3'],  # multiple of each type
    ['loc-54', 'shot-14', 'spot-12', 'seen-357', 'kill-91', 'loss-17', 'stop-35', 'status-117'],  # ids not in dataset
    # mix of event ids not in dataset and event ids in dataset
    ['loc-2', 'loc-30', 'shot-3', 'shot-44', 'spot-4', 'spot-52', 'seen-2', 'seen-34',
     'kill-3', 'kill-18', 'loss-4', 'loss-51', 'stop-2', 'stop-22', 'status-3', 'status-33']]


@pytest.fixture(params=remove_event_params)
def get_remove_event_ls(request):
    return request.param


@pytest.mark.parametrize(
    'export_import',
    (
            pytest.param(False, id=''),
            pytest.param(True, id='export-import entity data'),
    )
)
@pytest.mark.parametrize(
    'finalise',
    (
            pytest.param(False, id=''),
            pytest.param(True, id='finalise dataset'),
    )
)
def test_get_event_id_ls(test_utils, export_import, finalise, get_remove_event_ls):
    """
    Create a dataset instance, extract the list of event ids using get_event_id_ls and check it is empty
    Add multiple events of different types to multiple entities using the add event and append to list functions
    Extract the list of event ids from the Dataset instance using get_event_id_ls and test:
        - Number of event ids is correct
        - No duplicate event ids are present
        - all expected event ids are present in the list
        - no unexpected event ids are present in the list

    The 'evn_id' key from the get_event_id_dict function will also be extracted and tested in the same way.

    The remove_event function is also tested via the remove_id_ls parameter - event ids in this list are removed
    using the dataset remove_event function before checking the list of event ids

    If the finalise parameter is True then the dataset will be finalised before extracting and checking the list
    of event ids (event ids present in the event id column of the cdf events dataframe will also be checked)

    If the export_import parameter is True then the entity_export_dict and entity_import_dict functions will be tested
    """
    fail_msg_ls = []
    exp_event_id_ls = []

    test_dataset = test_utils.make_dataset()

    # check the event id list is empty
    if len(test_dataset.get_event_id_ls()) > 0:
        fail_msg_ls.append("dataset instance initialised with non-empty event id list")
    if len(test_dataset.get_event_id_dict()['evn_id']) > 0:
        fail_msg_ls.append("dataset instance initialised with non-empty event id list")

    test_utils.add_entities(dataset=test_dataset, ent_dict=test_ent_dict)
    for add_event_dict in add_event_dict_ls:
        test_utils.add_single_events(dataset=test_dataset, event_dict=add_event_dict)
        exp_event_id_ls.extend(add_event_dict['exp_id'])
    for append_event_dict in append_event_dict_ls:
        test_utils.append_events(dataset=test_dataset, append_event_dict=append_event_dict)
        exp_event_id_ls.extend(append_event_dict['exp_id'])

    remove_ls = get_remove_event_ls
    for event_id in remove_ls:
        test_dataset.remove_event(remove_id=event_id)
        if event_id in exp_event_id_ls:
            exp_event_id_ls.remove(event_id)

    if export_import:
        test_utils.dataset_export_import_entities(dataset=test_dataset)

    test_event_id_ls = []
    if finalise:
        test_dataset.finalise_data()
        test_event_id_ls.append(test_dataset.CDF_events_df[test_dataset.evn_tbl_event_id_col_lbl].to_list())

    test_event_id_ls.append(test_dataset.get_event_id_ls())
    test_event_id_ls.append(test_dataset.get_event_id_dict()['evn_id'])

    for event_id_ls in test_event_id_ls:
        # check that the event id list has the right number of events
        if len(event_id_ls) != len(exp_event_id_ls):
            fail_msg_ls.append(f"number of events different from expected, "
                               f"delta to expected: {len(event_id_ls) - len(exp_event_id_ls)} event(s)")

        # check that there are no unexpected ids are in the event id list
        for event_id in event_id_ls:
            if event_id not in exp_event_id_ls:
                fail_msg_ls.append(f"unexpected id {event_id} present in event id list")
        # check that all expected event ids are present without duplicates
        for event_id in exp_event_id_ls:
            if event_id not in event_id_ls:
                fail_msg_ls.append(f"expected event id {event_id} not present in event id list")
            elif event_id_ls.count(event_id) > 1:
                fail_msg_ls.append(f"{event_id_ls.count(event_id)} occurrences of {event_id} in event id list")

    test_utils.check_fail_ls(fail_msg_ls)


@pytest.mark.parametrize(
    'export_import',
    (
            pytest.param(False, id=''),
            pytest.param(True, id='export-import entity data'),
    )
)
@pytest.mark.parametrize(
    'finalise',
    (
            pytest.param(False, id=''),
            pytest.param(True, id='finalise dataset'),
    )
)
def test_get_event_id_dict(test_utils, export_import, finalise, get_remove_event_ls):
    """
    Create a dataset instance
    Add multiple events of different types to multiple entities using the add event and append to list functions
    Extract the event id dict from the Dataset instance using get_event_id_dict:
    for each expected event id check the event type, primary uid and secondary uid for the event are correct
    Note - the list of event ids from the evn_id key of the event_id_dict is tested in test_get_event_id_ls

    The remove_event function is also tested via the remove_id_ls parameter - event ids in this list are removed
    using the dataset remove_event function before checking the list of event ids

    If the finalise parameter is True then the dataset will be finalised before extracting and checking the
    event id dict

    If the export_import parameter is True then the entity_export_dict and entity_import_dict functions will be tested
    """
    fail_msg_ls = []
    exp_event_id_ls = []

    test_dataset = test_utils.make_dataset()

    test_utils.add_entities(dataset=test_dataset, ent_dict=test_ent_dict)
    for add_event_dict in add_event_dict_ls:
        test_utils.add_single_events(dataset=test_dataset, event_dict=add_event_dict)
        exp_event_id_ls.extend(add_event_dict['exp_id'])
    for append_event_dict in append_event_dict_ls:
        test_utils.append_events(dataset=test_dataset, append_event_dict=append_event_dict)
        exp_event_id_ls.extend(append_event_dict['exp_id'])

    remove_ls = get_remove_event_ls
    for event_id in remove_ls:
        test_dataset.remove_event(remove_id=event_id)
        if event_id in exp_event_id_ls:
            exp_event_id_ls.remove(event_id)

    if export_import:
        test_utils.dataset_export_import_entities(dataset=test_dataset)

    if finalise:
        test_dataset.finalise_data()

    event_id_dict = test_dataset.get_event_id_dict()

    for idx, event_id in enumerate(event_id_dict['evn_id']):
        if event_id not in exp_event_id_ls:
            fail_msg_ls.append(f"unexpected event id {event_id} in event id dict")
        else:
            exp_dict = get_exp_dict(event_id=event_id)

            exp_type = exp_dict['event_type']
            type_map = {'location': test_dataset.loc_event_lbl,
                        'shot': test_dataset.shot_event_lbl,
                        'spot': test_dataset.spot_event_lbl,
                        'seen': test_dataset.seen_event_lbl,
                        'kill': test_dataset.kill_event_lbl,
                        'loss': test_dataset.loss_event_lbl,
                        'stop': test_dataset.stop_event_lbl,
                        'status': test_dataset.status_event_lbl}

            for item in type_map.items():
                if exp_type == item[0]:
                    exp_type = item[1]

            exp_prim_uid = None
            exp_sec_uid = None
            exp_idx = exp_dict['exp_id'].index(event_id)
            if exp_dict in add_event_dict_ls:
                exp_prim_uid = exp_dict['uid'][exp_idx]
                exp_sec_uid = exp_dict['entity'][exp_idx]
            elif exp_dict in append_event_dict_ls:
                exp_prim_uid = exp_dict['uid']
                exp_sec_uid = exp_dict['exp_ent'][exp_idx]

            if exp_type != event_id_dict['type'][idx]:
                fail_msg_ls.append(f"event type for event {event_id} was {event_id_dict['type'][idx]} "
                                   f"but expected {exp_type}")
            if exp_prim_uid != event_id_dict['prim_uid'][idx]:
                fail_msg_ls.append(f"primary uid for event {event_id} was {event_id_dict['prim_uid'][idx]} "
                                   f"but expected {exp_prim_uid}")
            if exp_sec_uid != event_id_dict['sec_uid'][idx]:
                fail_msg_ls.append(f"secondary uid for event {event_id} was {event_id_dict['sec_uid'][idx]} "
                                   f"but expected {exp_sec_uid}")

    test_utils.check_fail_ls(fail_msg_ls)


@pytest.mark.parametrize(
    'export_import',
    (
            pytest.param(False, id=''),
            pytest.param(True, id='export-import entity data'),
    )
)
@pytest.mark.parametrize(
    'finalise',
    (
            pytest.param(False, id=''),
            pytest.param(True, id='finalise dataset'),
    )
)
def test_get_event_data(test_utils, export_import, finalise, get_remove_event_ls):
    """
    Create a dataset instance
    Add multiple events of different types to multiple entities using the add event and append to list functions
    Get the event data for each expected event id using the get_event_data function and compare
    the data to the source dictionary
    Note - for this test event ids in the remove_ls are not removed from the expected event id list as the
    get_event_data function returning an empty dictionary for these ids is also tested.

    The remove_event function is also tested via the remove_id_ls parameter - event ids in this list are removed
    using the dataset remove_event function before checking the list of event ids

    If the finalise parameter is True then the dataset will be finalised before extracting and checking the
    event id dict

    If the export_import parameter is True then the entity_export_dict and entity_import_dict functions will be tested

    """
    fail_msg_ls = []
    exp_event_id_ls = []

    test_dataset = test_utils.make_dataset()

    test_utils.add_entities(dataset=test_dataset, ent_dict=test_ent_dict)
    for add_event_dict in add_event_dict_ls:
        test_utils.add_single_events(dataset=test_dataset, event_dict=add_event_dict)
        exp_event_id_ls.extend(add_event_dict['exp_id'])
    for append_event_dict in append_event_dict_ls:
        test_utils.append_events(dataset=test_dataset, append_event_dict=append_event_dict)
        exp_event_id_ls.extend(append_event_dict['exp_id'])

    remove_ls = get_remove_event_ls
    for event_id in remove_ls:
        test_dataset.remove_event(remove_id=event_id)

    if export_import:
        test_utils.dataset_export_import_entities(dataset=test_dataset)

    if finalise:
        test_dataset.finalise_data()

    event_id_dict = test_dataset.get_event_id_dict()
    for event_id in exp_event_id_ls:
        if event_id not in remove_ls and event_id not in event_id_dict['evn_id']:
            fail_msg_ls.append(f"expected event id {event_id} not in remove_ls and not in event id dict ")
        else:
            event_data_dict = test_dataset.get_event_data(event_id)
            if event_id in remove_ls and len(event_data_dict) != 0:
                fail_msg_ls.append(f"non empty event data dict returned for removed event {event_id}")
            elif event_id not in remove_ls and len(event_data_dict) == 0:
                fail_msg_ls.append(f"empty event data dict returned for event {event_id}")
            elif len(event_data_dict) != 0:
                exp_dict = get_exp_dict(event_id)
                exp_idx = exp_dict['exp_id'].index(event_id)

                exp_detail = exp_dict['exp_detail'][exp_idx]
                exp_type = exp_dict['event_type']
                type_map = {'location': test_dataset.loc_event_lbl,
                            'shot': test_dataset.shot_event_lbl,
                            'spot': test_dataset.spot_event_lbl,
                            'seen': test_dataset.seen_event_lbl,
                            'kill': test_dataset.kill_event_lbl,
                            'loss': test_dataset.loss_event_lbl,
                            'stop': test_dataset.stop_event_lbl,
                            'status': test_dataset.status_event_lbl}

                for item in type_map.items():
                    if exp_type == item[0]:
                        exp_type = item[1]

                exp_prim_uid = None
                exp_sec_uid = None
                exp_time = None
                exp_x = None
                exp_y = None

                if exp_dict in add_event_dict_ls:
                    exp_prim_uid = exp_dict['uid'][exp_idx]
                    exp_sec_uid = exp_dict['entity'][exp_idx]
                    exp_time = exp_dict['time'][exp_idx]

                    if exp_type == test_dataset.loc_event_lbl:
                        exp_x = exp_dict['x'][exp_idx]
                        exp_y = exp_dict['y'][exp_idx]

                elif exp_dict in append_event_dict_ls:
                    exp_prim_uid = exp_dict['uid']
                    exp_sec_uid = exp_dict['exp_ent'][exp_idx]
                    exp_time = exp_dict['data_vals'][0][exp_idx]

                    if exp_type == test_dataset.loc_event_lbl:
                        exp_x = exp_dict['exp_x'][exp_idx]
                        exp_y = exp_dict['exp_y'][exp_idx]

                if event_data_dict['event_id'] != event_id:
                    fail_msg_ls.append(f"event id for event {event_id} was {event_data_dict['event_id']} "
                                       f"but expected {event_id}")
                if event_data_dict['event_type'] != exp_type:
                    fail_msg_ls.append(f"event type for event {event_id} was {event_data_dict['event_type']} "
                                       f"but expected {exp_type}")
                if event_data_dict['time'] != exp_time:
                    fail_msg_ls.append(f"event time for event {event_id} was {event_data_dict['time']} "
                                       f"but expected {exp_time}")
                if event_data_dict['prim_uid'] != exp_prim_uid:
                    fail_msg_ls.append(f"primary uid for event {event_id} was {event_data_dict['prim_uid']} "
                                       f"but expected {exp_prim_uid}")
                if event_data_dict['sec_uid'] != exp_sec_uid:
                    fail_msg_ls.append(f"secondary uid for event {event_id} was {event_data_dict['sec_uid']} "
                                       f"but expected {exp_sec_uid}")
                if event_data_dict['detail'] != exp_detail:
                    fail_msg_ls.append(f"detail for event {event_id} was {event_data_dict['detail']} "
                                       f"but expected {exp_detail}")
                if exp_type == test_dataset.loc_event_lbl:
                    if 'x' not in event_data_dict.keys() or 'y' not in event_data_dict.keys():
                        fail_msg_ls.append(f"x/y keys missing from data dict for location event {event_id}")
                    else:
                        if event_data_dict['x'] != exp_x:
                            fail_msg_ls.append(f"x for location event {event_id} was {event_data_dict['x']} "
                                               f"but expected {exp_x}")
                        if event_data_dict['y'] != exp_y:
                            fail_msg_ls.append(f"y for location event {event_id} was {event_data_dict['y']} "
                                               f"but expected {exp_y}")

    test_utils.check_fail_ls(fail_msg_ls)
