import pandas as pd
import pytest
import sys

sys.path.append('..')
from processor_core.Dataset import DataSet
from processor_core.CDF_Func import CDFfunc


@pytest.fixture
def check_fail_ls():
    def check(fail_msg_ls):
        assert not fail_msg_ls, print("\nFail messages:\n{}".format("\n".join(fail_msg_ls)))

    return check


@pytest.fixture
def get_dataframe_diff():
    def get_diff(df_act: pd.DataFrame, df_exp: pd.DataFrame):
        return_str = ''

        if df_act.shape != df_exp.shape:
            rows_diff = abs(df_act.shape[0] - df_exp.shape[0])
            cols_diff = abs(df_act.shape[1] - df_exp.shape[1])
            return_str += f"dataframe shapes different - cols diff {cols_diff}, rows diff {rows_diff} \n"
        else:
            act_col_ls = df_act.columns.to_list()
            exp_col_ls = df_exp.columns.to_list()
            if act_col_ls != exp_col_ls:
                return_str += "differences in column headers:  "
                col_name_diff_ls = []
                for idx, exp_col_name in enumerate(exp_col_ls):
                    act_col_name = act_col_ls[idx]
                    if act_col_name != exp_col_name:
                        col_name_diff_ls.append(f"actual: {act_col_name} / expected: {exp_col_name}")
                return_str += str(col_name_diff_ls)
            else:
                diff_df = df_act.compare(df_exp, align_axis=0).rename(index={'self': 'actual', 'other': 'expected'})
                if not diff_df.empty:
                    return_str += "differences in dataframe content: \n "
                    return_str += str(diff_df)

        return return_str

    return get_diff


@pytest.fixture()
def get_cdf_func():
    """
    Factory fixture for making an instance of CDFfunc class
    """

    def make():
        func = CDFfunc()

        return func

    return make


@pytest.fixture()
def make_dataset():
    """
    Factory fixture for Making a Dataset instance
    Default dataset_config to None if not supplied as argument and default log_stream and log_file to False
    """

    def make(dataset_config=None, log_stream=False, log_file=False):
        if dataset_config is None:
            dataset_config = {}
        dataset = DataSet(dataset_config=dataset_config, log_stream=log_stream, log_file=log_file)

        return dataset

    return make


@pytest.fixture
def add_entities():
    """
    Function fixture to add entities to a Dataset instance using an ent_dict
    """

    def add(dataset: DataSet, ent_dict: dict):
        for idx, uid in enumerate(ent_dict['uid']):
            dataset.add_entity(uid=uid)

            # these must match the default parameters for entity
            args_dict = {'unit_name': None, 'unit_type': None, 'affiliation': None, 'commander': None,
                         'force': None, 'init_comps': None, 'cbt_per_comp': None,
                         'system_entity': False, 'start_entity': True, 'add_time': 0.0}

            for arg in args_dict.keys():
                if arg in ent_dict:
                    args_dict[arg] = ent_dict[arg][idx]

            dataset.set_entity_data(uid=uid, unit_name=args_dict['unit_name'], unit_type=args_dict['unit_type'],
                                    affiliation=args_dict['affiliation'], commander=args_dict['commander'],
                                    force=args_dict['force'], init_comps=args_dict['init_comps'],
                                    cbt_per_comp=args_dict['cbt_per_comp'], system_entity=args_dict['system_entity'],
                                    start_entity=args_dict['start_entity'], add_time=args_dict['add_time'])

    return add


@pytest.fixture
def add_location_events():
    """
    Function fixture to add location events from location_event_dict to a dataset via the add_location function
    """

    def add_events(dataset: DataSet, event_dict: dict):
        for idx, uid in enumerate(event_dict['uid']):
            dataset.add_location(uid=uid,
                                 time=event_dict['time'][idx],
                                 x=event_dict['x'][idx],
                                 y=event_dict['y'][idx],
                                 detail_keys=event_dict['detail_keys'],
                                 detail_vals=event_dict['detail_vals'][idx])

    return add_events


@pytest.fixture
def add_shot_events():
    """
    Function fixture to add shot events from shot_event_dict to a dataset via the add_shot function
    """

    def add_events(dataset: DataSet, event_dict: dict):
        for idx, uid in enumerate(event_dict['uid']):
            dataset.add_shot(uid=uid,
                             time=event_dict['time'][idx],
                             detail_keys=event_dict['detail_keys'],
                             detail_vals=event_dict['detail_vals'][idx])

    return add_events


@pytest.fixture
def add_kill_events():
    """
    Function fixture to add kill events from kill_event_dict to a dataset via the add_kill function
    """

    def add_events(dataset: DataSet, event_dict: dict):
        for idx, uid in enumerate(event_dict['uid']):
            dataset.add_kill(uid=uid,
                             time=event_dict['time'][idx],
                             victim=event_dict['entity'][idx],
                             detail_keys=event_dict['detail_keys'],
                             detail_vals=event_dict['detail_vals'][idx])

    return add_events


@pytest.fixture
def add_loss_events():
    """
    Function fixture to add loss events from loss_event_dict to a dataset via the add_loss function
    """

    def add_events(dataset: DataSet, event_dict: dict):
        for idx, uid in enumerate(event_dict['uid']):
            dataset.add_loss(uid=uid,
                             time=event_dict['time'][idx],
                             killer=event_dict['entity'][idx],
                             detail_keys=event_dict['detail_keys'],
                             detail_vals=event_dict['detail_vals'][idx])

    return add_events


@pytest.fixture
def add_spot_events():
    """
    Function fixture to add spot events from spot_event_dict to a dataset via the add_spot function
    """

    def add_events(dataset: DataSet, event_dict: dict):
        for idx, uid in enumerate(event_dict['uid']):
            dataset.add_spot(uid=uid,
                             time=event_dict['time'][idx],
                             entity=event_dict['entity'][idx],
                             detail_keys=event_dict['detail_keys'],
                             detail_vals=event_dict['detail_vals'][idx])

    return add_events


@pytest.fixture
def add_seen_events():
    """
    Function fixture to add seen events from seen_event_dict to a dataset via the add_seen function
    """

    def add_events(dataset: DataSet, event_dict: dict):
        for idx, uid in enumerate(event_dict['uid']):
            dataset.add_seen(uid=uid,
                             time=event_dict['time'][idx],
                             entity=event_dict['entity'][idx],
                             detail_keys=event_dict['detail_keys'],
                             detail_vals=event_dict['detail_vals'][idx])

    return add_events


@pytest.fixture
def add_stop_events():
    """
    Function fixture to add stop events from stop_event_dict to a dataset via the add_stop function
    """

    def add_events(dataset: DataSet, event_dict: dict):
        for idx, uid in enumerate(event_dict['uid']):
            dataset.add_stop(uid=uid,
                             time=event_dict['time'][idx],
                             entity=event_dict['entity'][idx],
                             detail_keys=event_dict['detail_keys'],
                             detail_vals=event_dict['detail_vals'][idx])

    return add_events


@pytest.fixture
def add_status_events():
    """
    Function fixture to add status events from status_event_dict to a dataset via the add_status function
    """

    def add_events(dataset: DataSet, event_dict: dict):
        for idx, uid in enumerate(event_dict['uid']):
            dataset.add_status(uid=uid,
                               time=event_dict['time'][idx],
                               detail_keys=event_dict['detail_keys'],
                               detail_vals=event_dict['detail_vals'][idx])

    return add_events


@pytest.fixture
def append_events():
    """
    Function fixture to add a series of events for an entity via the append_to_list function
    """

    def add_events(dataset: DataSet, append_event_dict):
        uid = append_event_dict['uid']
        for idx, target_list in enumerate(append_event_dict['target_list']):
            data_list = append_event_dict['data_vals'][idx]
            dataset.append_to_list(uid=uid, target_list=target_list, data_list=data_list)

        detail_ls = CDFfunc.encode_event_detail_list(*append_event_dict['detail_vals'],
                                                     detail_keys=append_event_dict['detail_keys'])
        dataset.append_to_list(uid=uid, target_list=append_event_dict['detail_list'], data_list=detail_ls)

    return add_events


@pytest.fixture
def dataset_export_import_entities():
    """
    function fixture to -
        export data from all entity instances within a dataset instance
        clear the entity array
        add entities and import data from the exported dicts
        finalise dataset instance if finalise is True (default)
    """

    def export_import(dataset: DataSet, finalise=True):
        # export the entity_dicts from the dataset instance into a list
        entity_dict_ls = []
        for entity in dataset.entities:
            entity_dict_ls.append(entity.export_entity_dict())
        # empty the dataset instance entity array
        dataset.entities = []
        # add the entities back to the dataset instance using the exported dicts and import the entity dict data
        for entity_dict in entity_dict_ls:
            dataset.add_entity(uid=entity_dict['uid'])
            ent_idx = dataset.get_entity_index(entity_dict['uid'])
            dataset.entities[ent_idx].import_entity_dict(load_vars_dict=entity_dict)
        if finalise:
            dataset.finalise_data()

    return export_import


@pytest.fixture
def dataset_save_load():
    """
    function fixture to -
        save state of dataset instance to file (optionally specify location and file name)
        set up new dataset instance
        load state from saved file into new dataset instance

        return the new dataset instance

    Note -
        Function does not update dataset in place and so return must be assigned to a variable
        The log_file and log_stream parameters are transferred across as these must be supplied at init
        output_location, serial and split_files_by_type are also transferred so that the log file is saved in the
        right place and can be checked for errors (these are tested by the dataset_update_config_test). This will also
        ensure the load_dataset function looks for save_file in the right place if save_location is not specified
    """

    def save_load(dataset: DataSet, save_location=None, save_file=None):
        dataset.save_dataset(save_location=save_location, save_file=save_file)

        log_file = dataset.log_file
        log_stream = dataset.log_stream
        serial = dataset.serial
        output_location = dataset.output_location
        if dataset.split_files_by_type:
            split_files = '1'
        else:
            split_files = '0'

        return_dataset = DataSet(dataset_config={'output_location': output_location,
                                                 'serial': serial,
                                                 'split_files_by_type': split_files},
                                 log_file=log_file, log_stream=log_stream)
        return_dataset.load_dataset(load_location=save_location, load_file=save_file)

        return return_dataset

    return save_load


@pytest.fixture
def test_utils(check_fail_ls, get_dataframe_diff, make_dataset, add_entities,
               add_status_events, add_location_events,
               add_shot_events, add_kill_events, add_loss_events,
               add_seen_events, add_spot_events, add_stop_events,
               append_events,
               get_cdf_func, dataset_export_import_entities, dataset_save_load):
    """
    Class fixture to gather all the other fixtures together
    """

    class Utils:

        def __init__(self):
            self.check_fail_ls = check_fail_ls
            self.get_dataframe_diff = get_dataframe_diff
            self.make_dataset = make_dataset
            self.add_entities = add_entities
            self.add_status = add_status_events
            self.add_location_events = add_location_events
            self.add_shot_events = add_shot_events
            self.add_kill_events = add_kill_events
            self.add_loss_events = add_loss_events
            self.add_seen_events = add_seen_events
            self.add_spot_events = add_spot_events
            self.add_stop_events = add_stop_events
            self.append_events = append_events
            self.encode_detail_list = CDFfunc.encode_event_detail_list
            self.get_cdf_func = get_cdf_func
            self.dataset_export_import_entities = dataset_export_import_entities
            self.dataset_save_load = dataset_save_load

        def add_single_events(self, dataset: DataSet, event_dict: dict):
            if event_dict['event_type'] == 'status':
                self.add_status(dataset=dataset, event_dict=event_dict)
            elif event_dict['event_type'] == 'location':
                self.add_location_events(dataset=dataset, event_dict=event_dict)
            elif event_dict['event_type'] == 'shot':
                self.add_shot_events(dataset=dataset, event_dict=event_dict)
            elif event_dict['event_type'] == 'kill':
                self.add_kill_events(dataset=dataset, event_dict=event_dict)
            elif event_dict['event_type'] == 'loss':
                self.add_loss_events(dataset=dataset, event_dict=event_dict)
            elif event_dict['event_type'] == 'spot':
                self.add_spot_events(dataset=dataset, event_dict=event_dict)
            elif event_dict['event_type'] == 'seen':
                self.add_seen_events(dataset=dataset, event_dict=event_dict)
            elif event_dict['event_type'] == 'stop':
                self.add_stop_events(dataset=dataset, event_dict=event_dict)

    return Utils()
