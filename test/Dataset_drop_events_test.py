import pytest

ent_dict = {'uid': ['t-1']}

event_type_ls = ['location', 'shot', 'kill', 'loss', 'spot', 'seen']


@pytest.mark.parametrize(
    'save_load',
    (
            pytest.param(False, id=''),
            pytest.param(True, id='save-load dataset state'),
    )
)
@pytest.mark.parametrize(
    'num_events',
    (
            pytest.param(1, id='1 event / type'),
            pytest.param(5, id='5 events / type'),
            pytest.param(10, id='10 events / type'),
    )
)
@pytest.mark.parametrize('drop_location_events', (pytest.param('0'), pytest.param('1', id='drop location')))
@pytest.mark.parametrize('drop_shot_events', (pytest.param('0'), pytest.param('1', id='drop shot')))
@pytest.mark.parametrize('drop_spot_events', (pytest.param('0'), pytest.param('1', id='drop spot')))
@pytest.mark.parametrize('drop_seen_events', (pytest.param('0'), pytest.param('1', id='drop seen')))
def test_event_type_drop(test_utils, save_load, num_events,
                         drop_location_events, drop_shot_events, drop_spot_events, drop_seen_events):
    """
    Create a dataset instance configured to drop a combination of event types (parametrize)
    Add events of all types to the dataset instance
    Finalise the dataset instance
    Confirm that event types specified have been dropped and all other event types remain

    if the save_load parameter is True then saving and reloading of dataset state will be tested
    """
    fail_msg_ls = []

    config_dict = {'output_location': 'Output/DropEventsTest',
                   'drop_location_events': drop_location_events,
                   'drop_shot_events': drop_shot_events,
                   'drop_spot_events': drop_spot_events,
                   'drop_seen_events': drop_seen_events}

    test_dataset = test_utils.make_dataset(dataset_config=config_dict)
    test_utils.add_entities(dataset=test_dataset, ent_dict=ent_dict)

    for event_type in event_type_ls:
        event_dict = {'event_type': event_type,
                      'uid': ['t-1'],
                      'time': [0.0],
                      'x': ['0'],
                      'y': ['0'],
                      'entity': [None],
                      'detail_keys': [None],
                      'detail_vals': [[None]]}
        for i in range(0, num_events):
            test_utils.add_single_events(dataset=test_dataset, event_dict=event_dict)

    if save_load:
        test_dataset = test_utils.dataset_save_load(dataset=test_dataset)

    test_dataset.finalise_data()

    dropped_event_lbl_ls = []
    if drop_location_events == '1':
        dropped_event_lbl_ls.append(test_dataset.loc_event_lbl)
    if drop_shot_events == '1':
        dropped_event_lbl_ls.append(test_dataset.shot_event_lbl)
    if drop_spot_events == '1':
        dropped_event_lbl_ls.append(test_dataset.spot_event_lbl)
    if drop_seen_events == '1':
        dropped_event_lbl_ls.append(test_dataset.seen_event_lbl)

    cdf_event_ls = test_dataset.CDF_events_df[test_dataset.evn_tbl_event_type_col_lbl].to_list()
    for dropped_event_lbl in dropped_event_lbl_ls:
        if dropped_event_lbl in cdf_event_ls:
            fail_msg_ls.append(f"{dropped_event_lbl} event type not dropped")

    total_events_expected = num_events * (len(event_type_ls) - len(dropped_event_lbl_ls))
    if len(cdf_event_ls) != total_events_expected:
        fail_msg_ls.append(f"expected {total_events_expected} events but CDF events Dataframe had {len(cdf_event_ls)}")

    test_utils.check_fail_ls(fail_msg_ls)
