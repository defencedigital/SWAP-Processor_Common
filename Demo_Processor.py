from processor_core.Dataset import DataSet
from processor_core.CDF_Func import CDFfunc
from os import listdir, path

import pandas as pd

def demo_processor(process_config: dict) -> str:
    # phase 0 - setup the Dataset instance, parameters and options using the configuration dict, check the configuration is valid =====
    
    script_name = "Demo_processor"
    script_version = "1.0.0"

    demo_data = DataSet(dataset_config=process_config)

    # read in the variables from the processor config dictionary
    run_serial = demo_data.serial
    input_location = demo_data.input_location
    output_location = demo_data.output_location

    # get input file names from the configuration
    units_file = str(process_config['units_file'])
    locations_file = str(process_config['locations_file'])
    shots_file = str(process_config['shots_file'])
    sightings_file = str(process_config['sightings_file'])
    kills_file = str(process_config['kills_file'])
    system_events_file = str(process_config['system_events_file'])

    # form these into a list of input file names
    input_file_ls = [units_file, locations_file, shots_file, sightings_file, kills_file, system_events_file]

    # get the zero hour parameter
    try:
        zero_hour = float(process_config['zero_hour'])
    except ValueError:
        zero_hour = 0.0
    if pd.isna(zero_hour):
        zero_hour = 0.0

    # check that the specified configuration is valid and can be processed
    issues_ls = []

    if not path.isdir(input_location):
        issues_ls.append(f"input location {input_location} not found")
    else:
        file_ls = listdir(input_location)
        for input_file in input_file_ls:
            if input_file not in file_ls:
                issues_ls.append(f"{input_file} missing")

    if len(issues_ls) > 0:
        return_val = f"failed - no files generated {len(issues_ls)} issues:"
        for issue in issues_ls:
            return_val = return_val + f" {issue}"
        return return_val

    # set up the script log
    logger = CDFfunc.setup_logger(f"{script_name}_log_S{run_serial}", output_folder=output_location)
    logger.info(f"Script logger started, saving log file to {output_location}")
    logger.info(f"{script_name} version {script_version}")
    logger.info(f"Using Dataset version {DataSet.version} and CDF functions version {CDFfunc.version}")

    # write input location, input files and output location to script log
    logger.info(f"Input location - {input_location}")
    logger.info(f"Input file names - {input_file_ls}")
    logger.info(f"Zero hour parameter - {zero_hour}")
    logger.info(f"Output location - {output_location}")

    # phase 1 - (no longer used - Dataset initialised in phase 0) ==================================================

    # phase 2 - read input files and generate source dataframes ====================================================
    
    # this phase reads the input files and generates source dataframes that will be processed in phase 3 to generate 
    # CDF enties and in phase 4 to add CDF events to those entities
    #
    # the vast majority of the data processing of the model outputs are handled in this section and consqeuently 
    # it has the greatest variation bewteen model processors

    logger.info("Generating source dataframes for unit data")

    source_file = path.join(input_location, units_file)

    # col_maps structure -  {source file column: unit_data_df column}
    col_maps = {'callsign': 'id', 'name': 'name', 'type': 'type', 'components': 'comps',
                'commander': 'commander', 'force colour': 'affiliation'}

    logger.debug(f"Extracting data from {source_file} for unit data dataframe")
    for mapping in col_maps.items():
        logger.debug(f"{mapping[0]} column mapped to {mapping[1]}")

    # read the source csv file, only using the target columns
    unit_data_df = pd.read_csv(source_file, usecols=col_maps.keys())
    # make sure the dataframe columns are in the same order as the col_map keys
    unit_data_df = unit_data_df[list(col_maps.keys())]
    # rename the columns to the col_map values
    unit_data_df.columns = col_maps.values()

    logger.info("Generating source dataframes for event data")

    '''
    df_dict_structure:
        df_name: name of the df for logging
        source_file: model output file to read data from
        source_file_avail: read if True, if not make an empty df (True for mandatory files otherwise check config and create a variable)
        col_maps: {col name in input file: col name in df}
        col_types: {col name in input file: data type to read column as} - explicitly define data type where needed
    
    add all df_dicts to the df_dict_ls variable
    '''

    # aim here is to convert the model output columns into the standard columns used in phase 4 i.e. id, x, y etc.
    # columns that will be used for detail values should have the form key_detail in the source dataframes i.e. terrain_detail etc.
    #
    # note that the demo inputs have a very simple structure and so this is straight forward, some models can have much more complex structures
    # note that df columns that will be used for detail values should have the form key_detail i.e. terrain_detail etc.

    locations_df_dict = {'df_name': 'locations_df',
                         'source_file': path.join(input_location, locations_file),
                         'source_file_avail': True,
                         'col_maps': {'time': 'time',
                                      'callsign': 'id',
                                      'x coord': 'x',
                                      'y coord': 'y',
                                      'terrain': 'terrain_detail',
                                      'status': 'status_detail',
                                      'speed': 'speed_detail'},
                         'col_types': {}}

    spots_df_dict = {'df_name': 'spots_df',
                    'source_file': path.join(input_location, sightings_file),
                    'source_file_avail': True,
                    'col_maps': {'time': 'time',
                                'spotting callsign': 'spotter_id',
                                'spotted callsign': 'spotted_id',
                                'range': 'range_detail',
                                'type': 'level_detail'},
                    'col_types': {}}
    
    shots_df_dict = {'df_name': 'shots_df',
                    'source_file': path.join(input_location, shots_file),
                    'source_file_avail': True,
                    'col_maps': {'time': 'time',
                                'firing callsign': 'id',
                                'rds fired': 'rds_detail',
                                'rd type': 'type_detail'},
                    'col_types': {}}

    kills_df_dict = {'df_name': 'kills_df',
                    'source_file': path.join(input_location, kills_file),
                    'source_file_avail': True,
                    'col_maps': {'time': 'time',
                                'killer callsign': 'killer_id',
                                'tgt callsign': 'tgt_id',
                                'tgt losses': 'kills',
                                'range': 'range_detail',
                                'wpn type': 'wpn_detail'},
                    'col_types': {}}

    # this dict just reads the system events file as is, dict structure used for consistency
    system_events_df_dict = {'df_name': 'system_events_df',
                            'source_file': path.join(input_location, system_events_file),
                            'source_file_avail': True,
                            'col_maps': {'time': 'time',
                                        'type': 'type',
                                        'message': 'message'},
                            'col_types': {}}

    df_dict_ls = [locations_df_dict, spots_df_dict, shots_df_dict, kills_df_dict, system_events_df_dict]

    # this part processes the df_dict objects to generate dataframes and add them to a list
    event_df_ls = []
    for df_dict in df_dict_ls:
        df_name = df_dict['df_name']
        source_file = df_dict['source_file']
        source_file_avail = df_dict['source_file_avail']
        col_maps = df_dict['col_maps']
        col_types = df_dict['col_types']
        if source_file_avail:
            logger.info(f"Extracting data from {source_file} for {df_name}")
            for mapping in col_maps.items():
                logger.debug(f"{mapping[0]} column mapped to {mapping[1]}")
            event_df_ls.append(pd.read_csv(source_file, usecols=list(col_maps.keys()), dtype=col_types))
            event_df_ls[-1] = event_df_ls[-1][list(col_maps.keys())]
            event_df_ls[-1].columns = col_maps.values()
        else:
            logger.warning(f"{source_file} not available - generating empty dataframe for {df_name}")
            event_df_ls.append(pd.DataFrame(columns=col_maps.values()))

    # extract the individual data frames from the list into variables
    locations_df = event_df_ls.pop(0)
    spots_df = event_df_ls.pop(0)
    shots_df = event_df_ls.pop(0)
    kills_df = event_df_ls.pop(0)
    system_events_df = event_df_ls.pop(0)

    # model processors will further filter and manipulate the individual event dfs as needed at this stage
    # (this is generally the most complex part of the model processor with the greatest variability from model to model due to different output formats)
    # for example a kills or losses df may need to be filtered to only include rows where the number of kills / losses > 0
    # in some cases these may generate new df's, for example a shots_df might be filtered to target losses > 0 to generate kills_df

    # in this case the stopped_seeing_df is generated by filtering spots_df to level_detail is 'lost' 
    # and then spots_df needs to be filtered to level_detail is not 'lost'
    logger.info("Generating stopped_seeing_df")
    stopped_seeing_df = spots_df.copy()
    stopped_seeing_df = stopped_seeing_df.loc[stopped_seeing_df['level_detail']=='lost']
    logger.info("Filtering spots_df")
    spots_df = spots_df.loc[spots_df['level_detail'] != 'lost']

    # a row per loss or kill event is also needed so the row_per_event function is used to expand the kills_df based on the kills column
    logger.info("Adding rows per kill / loss to kills_df")
    kills_df = CDFfunc.row_per_event(input_df=kills_df, event_count_col='kills')

    # phase 3 - generate the entities within the dataset instance and set their properties using unit_data_df ========
    
    # note that if the entity data from table option is used this is handled when the instance of the dataset class is initiated
    # hence this bit only needs to be done if that option either isn't being used or hasn't successfully generated any entities
    if not demo_data.entity_data_from_table or demo_data.get_num_entities() == 0:
        # unit_data_map - {entity property: unit_data_df column to get data from}, these should be matched to the column names in phase 2
        unit_data_map = {'uid': 'id',
                         'unit_name': 'name',
                         'unit_type': 'type',
                         'affiliation': 'affiliation',
                         'commander': 'commander',
                         'init_comps': 'comps'}

        # this gets all of the unique values from the uid column specified by the unit_data_map
        logger.info(f"Getting entity UIDs from {unit_data_map['uid']} of unit_data_df")
        uid_list = CDFfunc.get_unique_list(unit_data_df[unit_data_map['uid']].to_list())
        logger.info(f"{len(uid_list)} unique ids found in unit_data_df")

        for mapping in unit_data_map.items():
            logger.debug(f"entity {mapping[0]} from {mapping[1]} column of unit_data_df")

        # this part gets all the values from each column for each unique value in the uid column
        for uid in uid_list:
            logger.debug(f"Adding entity {uid} and populating data from unit_data_df")
            name_ls = CDFfunc.get_col_slice(unit_data_df, uid, unit_data_map['uid'], unit_data_map['unit_name'])
            type_ls = CDFfunc.get_col_slice(unit_data_df, uid, unit_data_map['uid'], unit_data_map['unit_type'])
            affil_ls = CDFfunc.get_col_slice(unit_data_df, uid, unit_data_map['uid'], unit_data_map['affiliation'])
            commander_ls = CDFfunc.get_col_slice(unit_data_df, uid, unit_data_map['uid'], unit_data_map['commander'])
            comp_ls = CDFfunc.get_col_slice(unit_data_df, uid, unit_data_map['uid'], unit_data_map['init_comps'])

            # this part pre-checks if there are multiple different values for any data item and generates a warning that the first value will be used
            unit_data_ls = [name_ls, type_ls, affil_ls, commander_ls, comp_ls]
            for idx, data_list in enumerate(unit_data_ls):
                if len(CDFfunc.get_unique_list(data_list)) > 1:
                    logger.warning(f"multiple values of {list(unit_data_map.keys())[idx+1]} for entity {uid}, ")
                    f"{data_list}, {data_list[0]} used"

            # this part adds the entity and sets its properties to the first value in each list
            # (note that model processors will typically only set a subset of parameters depending on the information available from the raw output)
            demo_data.add_entity(uid)
            demo_data.set_entity_data(uid=uid, unit_name=name_ls[0], unit_type=type_ls[0], commander=commander_ls[0],
                                      affiliation=affil_ls[0], init_comps=comp_ls[0], cbt_per_comp=1)

    # next need to add a system entity to capture system events 
    demo_data.add_entity("system")
    demo_data.set_entity_data(uid="system", unit_name="demo simulation", system_entity=True, init_comps=0, cbt_per_comp=0, unit_type='system entity', commander="system")

    # phase 4 - read the event data into the entities ===============================================================

    '''
    event_map structure:
        df: the dataframe to pull data from
        df_name: name of the df for logging
        mask_col: column to mask on using the uid
        data_maps: [[data column in df, target list for append to list]]
        detail_keys: [keys for the detail key-value pairs]
        detail_cols: [columns in the df with the values for the key-value pairs]
        detail_list: detail target list for the append to list
    
    add all event maps to the event_map_ls
    '''
    # not that 'append to list' refers to the append_to_list function in the Dataset class

    # at this stage the df column names should be standard i.e. id, time, x, y so the data_maps and this phase are usually very similar across model processors 
    # detail_keys, detail_cols and detail_list will vary depending on the particular details available from the model
    # Remember that in phase 2 column names with detail values were set to have an _detail suffix 
    # WARNING - at least one detail key and column must be specified to populate the detail list for the event type, otherwise list lengths will be mismatched - WARNING

    location_event_map = {'df': locations_df,
                          'df_name': 'locations_df',
                          'mask_col': 'id',
                          'data_maps': [['time', 'location_time'],
                                        ['x', 'location_x'],
                                        ['y', 'location_y']],
                          'detail_keys': ['terrain', 'speed'],
                          'detail_cols': ['terrain_detail', 'speed_detail'],
                          'detail_list': 'location_detail'}


    # note how spot and seen events use the same df but reverse the callsign ids and utilise different details
    spots_event_map = {'df': spots_df,
                          'df_name': 'spots_df',
                          'mask_col': 'spotter_id',
                          'data_maps': [['time', 'spot_time'],
                                        ['spotted_id', 'spot_entity']],
                          'detail_keys': ['range', 'level'],
                          'detail_cols': ['range_detail', 'level_detail'],
                          'detail_list': 'spot_detail'}

    seen_event_map = {'df': spots_df,
                          'df_name': 'spots_df',
                          'mask_col': 'spotted_id',
                          'data_maps': [['time', 'seen_time'],
                                        ['spotter_id', 'seen_entity']],
                          'detail_keys': ['range'],
                          'detail_cols': ['range_detail'],
                          'detail_list': 'seen_detail'}
    
    stopped_event_map = {'df': stopped_seeing_df,
                          'df_name': 'stopped_seeing_df',
                          'mask_col': 'spotter_id',
                          'data_maps': [['time', 'stop_time'],
                                        ['spotter_id', 'stop_entity']],
                          'detail_keys': ['range'],
                          'detail_cols': ['range_detail'],
                          'detail_list': 'stop_detail'}
    
    # note here how just the id, time and status columns of locations_df are used for status update events
    status_event_map = {'df': locations_df,
                          'df_name': 'locations_df',
                          'mask_col': 'id',
                          'data_maps': [['time', 'state_time']],
                          'detail_keys': ['status'],
                          'detail_cols': ['status_detail'],
                          'detail_list': 'state_detail'}

    shots_event_map = {'df': shots_df,
                          'df_name': 'shots_df',
                          'mask_col': 'id',
                          'data_maps': [['time', 'shots_time']],
                          'detail_keys': ['rds fired', 'rd type'],
                          'detail_cols': ['rds_detail', 'type_detail'],
                          'detail_list': 'shots_detail'}

    # note how the kills and losses event maps both use kills_df but with the mask col switched
    kills_event_map = {'df': kills_df,
                          'df_name': 'kills_df',
                          'mask_col': 'killer_id',
                          'data_maps': [['time', 'kills_time'],
                                        ['tgt_id', 'kills_victim']],
                          'detail_keys': ['range', 'weapon'],
                          'detail_cols': ['range_detail', 'wpn_detail'],
                          'detail_list': 'kills_detail'}

    losses_event_map = {'df': kills_df,
                          'df_name': 'kills_df',
                          'mask_col': 'tgt_id',
                          'data_maps': [['time', 'losses_time'],
                                        ['killer_id', 'losses_killer']],
                          'detail_keys': ['weapon'],
                          'detail_cols': ['wpn_detail'],
                          'detail_list': 'losses_detail'}

    event_map_ls = [location_event_map, spots_event_map, seen_event_map, stopped_event_map, status_event_map,
                    shots_event_map, kills_event_map, losses_event_map]

    # this part processes the event_map objects to read the data into the CDF entities within the Dataset instance
    # cycle through the event maps
    for event_map in event_map_ls:
        # set variables based on the current event map
        event_df = event_map['df']
        df_name = event_map['df_name']
        mask_col = event_map['mask_col']
        data_maps = event_map['data_maps']
        detail_keys = event_map['detail_keys']
        detail_cols = event_map['detail_cols']
        detail_list = event_map['detail_list']

        logger.info(f"loading event data from {df_name} into entities, masking on {mask_col}, data maps: {data_maps},"
                    f" detail keys: {detail_keys}, detail columns: {detail_cols}")

        # cycle through the entities in the Dataset instance
        for entity in demo_data.entities:
            uid = entity.uid

            logger.debug(f"reading event data from {df_name} for entity {uid}")
            # cycle through the mappings in data_maps
            for mapping in data_maps:
                data_col = mapping[0]
                tgt_list = mapping[1]
                
                # for each mapping get a data list for the entity from the columns of the specified dataframe 
                # and add it to the target list for the entity using the append to list function
                data_ls = CDFfunc.get_col_slice(df=event_df, uid=uid, mask_col=mask_col, tgt_col=data_col)
                if len(data_ls) > 0:
                    demo_data.append_to_list(uid=uid, target_list=tgt_list, data_list=data_ls)
                else:
                    logger.debug(f"no data for {tgt_list} from {df_name} for entity {uid}")

            # next get the event detail key value pairs
            logger.debug(f"adding encoded event detail for entity {uid}")
            detail_val_ls = []
            # for each required key value pair add a list of values to the detail_val_ls
            for detail_col in detail_cols:
                detail_val_ls.append(CDFfunc.get_col_slice(df=event_df, uid=uid, mask_col=mask_col, tgt_col=detail_col))

            # use the encode_event_detail_list helper function to encode this into a list of key - value pairs
            # and add it to the target detail list of the entity using the append to list function
            detail_data_encoded = CDFfunc.encode_event_detail_list(*detail_val_ls, detail_keys=detail_keys)
            if len(detail_data_encoded) > 0:
                demo_data.append_to_list(uid=uid, target_list=detail_list, data_list=detail_data_encoded)
            else:
                logger.debug(f"no data for {detail_list} from {df_name} for entity {uid}")

    # Some model processors will add additional events to entities here using the single event functions of the Dataset class (add_location, add_shot etc)
    # This can be used to handle events that require more complex filtering of dataframes (this should be kept to a minimum and handled in phase 2 if possible)

    # in this case just need to read the system events into the system entity
    demo_data.append_to_list(uid="system", target_list='state_time', data_list=system_events_df['time'].to_list()) 
    system_event_detail_encoded = CDFfunc.encode_event_detail_list(system_events_df['type'], system_events_df['message'], detail_keys=['type', 'message'])   
    demo_data.append_to_list(uid="system", target_list='state_detail', data_list=system_event_detail_encoded)

    # phase 5 - finalise the data in the dataset instance and export the files =======================================

    logger.info("Finalising data and saving output files (see dataset instance log for details)")
    demo_data.finalise_data()
    demo_data.export_data()
    return_val = "complete"
    return return_val

# call the batch_run_processor function with the processor function defined above and the configuration file
# multiprocessing via concurrent.futures can be disabled by setting multiprocess to False if it causes issues
if __name__ == "__main__":
    CDFfunc.batch_run_processor(model_processor=demo_processor, config_file="Demo_config.csv", multiprocess=True)
