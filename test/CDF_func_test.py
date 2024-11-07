import pytest
import pandas as pd


@pytest.mark.parametrize(
    ('in_ls', 'exp_ls'),
    (
            ([['one', 'one', 'one', 'one']], ['one']),
            ([['one', 'one'], ['two', 'two']], ['one', 'two']),
            ([['one', 'two'], ['one', 'three'], ['one', 'four']], ['one', 'two', 'three', 'four']),
            ([[1, 1, 1, 1]], [1]),
            ([[1, 2], [1, 3], [1, 4], [1, 5]], [1, 2, 3, 4, 5]),
    )
)
def test_get_unique_list(test_utils, in_ls, exp_ls):
    fail_msg_ls = []
    func = test_utils.get_cdf_func()

    # in_ls is a list of lists so need to unpack with * when sending to get_unique_list
    out_ls = func.get_unique_list(*in_ls)
    if out_ls != exp_ls:
        fail_msg_ls.append(f"get_unique_list output for {in_ls} was {out_ls} but expected {exp_ls}")

    test_utils.check_fail_ls(fail_msg_ls)


@pytest.mark.parametrize(
    ('in_ls', 'exp'),
    (
            ([[1]], True),
            ([[1], [1], [1], [1]], True),
            ([['one', 'two'], ['three', 'four'], ['five', 'six']], True),
            ([[1, 2], [3, 4], [5, 6]], True),
            ([[1], [2, 3], [4, 5, 6]], False),
            ([['one'], ['two', 'three'], ['four', 'five', 'six']], False),
    )

)
def test_compare_list_lengths(test_utils, in_ls, exp):
    fail_msg_ls = []
    func = test_utils.get_cdf_func()

    # in_ls is a list of lists so need to unpack with * when sending to compare_list_lengths
    result = func.compare_list_lengths(*in_ls)
    if result != exp:
        fail_msg_ls.append(f"compare_list_lengths with {in_ls} returned {result} but expected {exp}")

    test_utils.check_fail_ls(fail_msg_ls)


@pytest.mark.parametrize(
    ('in_ls', 'exp_ls'),
    (
            ([1, 2, 3, 4], [1, 2, 3, 4]),
            ([1, None, 2], [1, 2]),
            (['one', None, 'two', None, 'three'], ['one', 'two', 'three']),
    )
)
def test_remove_nan(test_utils, in_ls, exp_ls):
    fail_msg_ls = []
    func = test_utils.get_cdf_func()

    out_ls = func.remove_nan(in_ls)
    if out_ls != exp_ls:
        fail_msg_ls.append(f"remove_nan with {in_ls} returned {out_ls} but expected {exp_ls}")

    test_utils.check_fail_ls(fail_msg_ls)


test_slices_dict = {'one': ['a', 'b', 'b', 'c', 'd', 'd'],
                    'two': [1, 2, 3, 4, 5, 6],
                    'three': [True, True, False, False, True, True],
                    'four': [0.34, 5.78, 6.2, 1.3, 7.4, 4.3]}


@pytest.mark.parametrize(
    ('uid', 'mask_col', 'tgt_col', 'exp_slice'),
    (
            ('a', 'one', 'one', ['a']),
            ('a', 'one', 'two', [1]),
            ('a', 'one', 'three', [True]),
            ('a', 'one', 'four', [0.34]),
            ('b', 'one', 'one', ['b', 'b']),
            ('b', 'one', 'two', [2, 3]),
            ('b', 'one', 'three', [True, False]),
            ('b', 'one', 'four', [5.78, 6.2]),
            (True, 'three', 'one', ['a', 'b', 'd', 'd']),
            (5, 'two', 'two', [5]),
            ('3', 'two', 'four', [6.2])
    )

)
def test_get_col_slice(test_utils, uid, mask_col, tgt_col, exp_slice):
    fail_msg_ls = []
    func = test_utils.get_cdf_func()

    test_df = pd.DataFrame(data=test_slices_dict)
    col_slice = func.get_col_slice(df=test_df, uid=uid, mask_col=mask_col, tgt_col=tgt_col)
    if col_slice != exp_slice:
        fail_msg_ls.append(f"get_col_slice with {uid}, {mask_col}, {tgt_col} "
                           f"returned {col_slice} but expected {exp_slice}")

    test_utils.check_fail_ls(fail_msg_ls)


@pytest.mark.parametrize(
    ('slice_col', 'slice_val', 'rtn_cols', 'exp_dict'),
    (
            ('one', 'a', ['one'], {'one': ['a']}),
            ('one', 'a', ['two'], {'two': [1]}),
            ('one', 'a', ['three'], {'three': [True]}),
            ('one', 'a', ['four'], {'four': [0.34]}),
            ('two', 2, ['four'], {'four': [5.78]}),
            ('two', '5', ['three', 'four'], {'three': [True], 'four': [7.4]}),
            ('one', 'd', ['two', 'three', 'four'], {'two': [5, 6], 'three': [True, True], 'four': [7.4, 4.3]}),
            ('three', True, ['one', 'two', 'four'], {'one': ['a', 'b', 'd', 'd'],
                                                    'two': [1, 2, 5, 6],
                                                    'four': [0.34, 5.78, 7.4, 4.3]}),
    )
)
def test_get_df_slice(test_utils, slice_col, slice_val, rtn_cols, exp_dict):
    fail_msg_ls = []
    func = test_utils.get_cdf_func()

    exp_df = pd.DataFrame(data=exp_dict)
    test_df = pd.DataFrame(data=test_slices_dict)
    out_df = func.get_df_slice(df=test_df, slice_col=slice_col, slice_val=slice_val, rtn_cols=rtn_cols)
    # reset and drop index so that .equals can be used - may want to put this in to the actual function
    out_df.reset_index(inplace=True, drop=True)

    if not out_df.equals(exp_df):
        fail_msg_ls.append(f"get_df_slice returned dataframe different to expected, differences: "
                           f"\n {test_utils.get_dataframe_diff(df_exp=exp_df, df_act=out_df)}")

    test_utils.check_fail_ls(fail_msg_ls)


@pytest.mark.parametrize(
    ('input_str', 'exp_flt'),
    (
            # days:hh:mm:ss or hh:mm:ss or mm:ss, delimiter can be : . or a mix
            ('12:22', 742),
            ('12.22', 742),
            ('02:04:43', 7483),
            ('02.04:43', 7483),
            ('3:10:34:22', 297262),
            ('3.10:34.22', 297262),
    )
)
@pytest.mark.parametrize('unit', ('hrs', 'mins', 'secs'))
@pytest.mark.parametrize('zero_hr', (0, 6, 12, 3.5, 4.5))
def test_get_time_val(test_utils, input_str, zero_hr, unit, exp_flt):
    fail_msg_ls = []
    func = test_utils.get_cdf_func()

    out_flt = func.get_time_val(input_time_str=input_str, zero_hr=zero_hr, unit=unit)
    out_flt = round(out_flt, 2)

    if unit == 'secs':
        exp_flt = exp_flt - (zero_hr * 60 * 60)
    elif unit == 'mins':
        exp_flt = (exp_flt / 60)
        exp_flt = exp_flt - (zero_hr * 60)
    elif unit == 'hrs':
        exp_flt = (exp_flt / (60 * 60))
        exp_flt = exp_flt - zero_hr

    exp_flt = round(exp_flt, 2)

    if out_flt != exp_flt:
        fail_msg_ls.append(f"get_time_val with {input_str}, {unit} "
                           f"zero hr {zero_hr}, returned {out_flt} but expected {exp_flt}")

    test_utils.check_fail_ls(fail_msg_ls)


test_row_per_event_dict = {'uid': ['a', 'b', 'c', 'd', 'z'],
                           'events': [1, 2, 5, 10, 0]}


@pytest.mark.parametrize('drop_zero', (True, False))
@pytest.mark.parametrize(
    ('uid', 'exp_rows'),
    (
            ('a', 1),
            ('b', 2),
            ('c', 5),
            ('d', 10),
            ('z', 0),
    )
)
def test_row_per_event(test_utils, drop_zero, uid, exp_rows):
    fail_msg_ls = []
    func = test_utils.get_cdf_func()

    test_df = pd.DataFrame(data=test_row_per_event_dict)
    out_df = func.row_per_event(input_df=test_df, event_count_col='events', drop_zero=drop_zero)

    if exp_rows == 0 and not drop_zero:
        exp_rows = 1

    act_rows = len(out_df.loc[out_df['uid'] == uid, 'uid'].to_list())

    if act_rows != exp_rows:
        fail_msg_ls.append(f"row_per_event returned df with {act_rows} rows for uid {uid} with drop_zero {drop_zero} "
                           f"but expected {exp_rows}")

    test_utils.check_fail_ls(fail_msg_ls)


@pytest.mark.parametrize(
    ('input_str', 'exp_ls_group', 'exp_ls_pos'),
    (
            ('a|b', [['a', 'b']], [['a'], ['b']]),
            ('a|b|c,d|e|f', [['a', 'b', 'c'], ['d', 'e', 'f']], [['a', 'd'], ['b', 'e'], ['c', 'f']]),
            ('a|b,c|d,e', [['a', 'b'], ['c', 'd'], ['e']], None),
            ('a|b,c|d,e|f', [['a', 'b'], ['c', 'd'], ['e', 'f']], [['a', 'c', 'e'], ['b', 'd', 'f']]),
    )
)
@pytest.mark.parametrize('output_by_group', (True, False))
def test_flashpoint_str_extract(test_utils, input_str, output_by_group, exp_ls_group, exp_ls_pos):
    fail_msg_ls = []
    func = test_utils.get_cdf_func()

    if output_by_group:
        exp_ls = exp_ls_group
    else:
        exp_ls = exp_ls_pos

    act_ls = func.flashpoint_str_extract(input_str=input_str, output_by_group=output_by_group)

    if act_ls != exp_ls:
        fail_msg_ls.append(f"flashpoint_str_extract returned {act_ls} for {input_str} "
                           f"with output_by_group {output_by_group} but expected {exp_ls}")

    test_utils.check_fail_ls(fail_msg_ls)


test_pivot_dict = {'Key': ['event', 'event', 'event', 'time', 'detail',
                           'event', 'detail', 'time', 'detail', 'add_key_c', 'time',
                           'time', 'event', 'detail', 'time', 'add_key_c', 'detail'],
                   'Value': ['c', 'a', 'c', '1.0', 'event_a1_detail',
                             'b', 'event_c2_detail', '2.5', 'event_b1_detail', 'add_val_c1', '4.0',
                             '4.5', 'b', 'event_b2_detail', '3.0', 'add_val_c2', 'event_c1_detail'],
                   'EventID': ['c1', 'a1', 'c2', 'a1', 'a1',
                               'b1', 'c2', 'b1', 'b1', 'c1', 'c1',
                               'c2', 'b2', 'b2', 'b2', 'c2', 'c1']}


@pytest.mark.parametrize(
    ('event_type', 'exp_dict'),
    (
            ('a', {'EventID': ['a1'],
                   'detail': ['event_a1_detail'],
                   'time': ['1.0']}),
            ('b', {'EventID': ['b1', 'b2'],
                   'detail': ['event_b1_detail', 'event_b2_detail'],
                   'time': ['2.5', '3.0']}),
            ('c', {'EventID': ['c1', 'c2'],
                   'add_key_c': ['add_val_c1', 'add_val_c2'],
                   'detail': ['event_c1_detail', 'event_c2_detail'],
                   'time': ['4.0', '4.5']}),
    )
)
def test_flashpoint_event_pivot(test_utils, event_type, exp_dict):
    fail_msg_ls = []
    func = test_utils.get_cdf_func()

    test_df = pd.DataFrame(data=test_pivot_dict)

    key_ls = list(exp_dict.keys())
    key_ls.remove('EventID')

    out_df = func.flashpoint_event_pivot(all_event_df=test_df, event_type=event_type, key_ls=key_ls)

    exp_df = pd.DataFrame(data=exp_dict)
    exp_df.rename_axis('Key', axis=1, inplace=True)

    if not out_df.equals(exp_df):
        fail_msg_ls.append(f"flashpoint_event_pivot returned dataframe different to expected, differences: "
                           f"\n {test_utils.get_dataframe_diff(df_exp=exp_df, df_act=out_df)}")

    test_utils.check_fail_ls(fail_msg_ls)


add_coords_loc_dict = {1: [27.6, 51.4],
                       2: [8.0, 51.2],
                       3: [88.9, 65.8],
                       4: [91.9, 85.1],
                       5: [99.4, 86.8],
                       6: [17.3, 2.0],
                       7: [70.2, 55.4],
                       8: [28.6, 35.5],
                       9: [97.3, 22.1],
                       10: [73.5, 83.0],
                       15: [66.4, 74.4],
                       17: [23.6, 78.3],
                       42: [39.6, 31.0]}


@pytest.mark.parametrize(
    ('evn_ls', 'hx_ls'),
    (
            (['event 11', 'event 22', 'event 33', 'event 44'], [1, 2, 3, 4]),
            (['evn-1', 'evn-2', 'evn-3', 'evn-4', 'evn-5'], [42, 1, 5, 4, 10]),
            (['e1', 'e2', 'e3', 'e4', 'e5', 'e6', 'e7', 'e8', 'e9', 'e10', 'e11', 'e12', 'e13'],
             [5, 4, 3, 2, 1, 6, 7, 8, 9, 10, 42, 15, 17]),
    )
)
@pytest.mark.parametrize(
    ('hex_lbl', 'evn_lbl', 'x_lbl', 'y_lbl'),
    (
            pytest.param('hx', 'evn', 'x', 'y', id='short labels'),
            pytest.param('hex number', 'event serial', 'x location', 'y location', id='full labels'),
    )
)
def test_flashpoint_add_coord_cols(test_utils, evn_ls, hx_ls, hex_lbl, evn_lbl, x_lbl, y_lbl):
    fail_msg_ls = []
    func = test_utils.get_cdf_func()

    # build a map df from the add_coord_cols_map_dict
    add_coord_cols_map_dict = {'HexNumber': list(add_coords_loc_dict.keys()),
                               'XCoord': [],
                               'YCoord': []}
    for hex_num in add_coord_cols_map_dict['HexNumber']:
        add_coord_cols_map_dict['XCoord'].append(add_coords_loc_dict[hex_num][0])
        add_coord_cols_map_dict['YCoord'].append(add_coords_loc_dict[hex_num][1])
    map_df = pd.DataFrame(data=add_coord_cols_map_dict)

    # build an event df from the evn_ls and hx_ls
    add_coord_cols_in_dict = {evn_lbl: evn_ls,
                              hex_lbl: hx_ls}
    event_df = pd.DataFrame(data=add_coord_cols_in_dict)

    # get the out df using the function
    out_df = func.flashpoint_add_coord_cols(event_df=event_df, hex_col=hex_lbl,
                                            map_df=map_df,
                                            x_lbl=x_lbl, y_lbl=y_lbl)

    # build an exp df by taking the evn ls, hx ls and getting the x and y from the add_coords_loc_dict
    x_exp_ls = []
    y_exp_ls = []
    for hex_num in hx_ls:
        x_exp_ls.append(add_coords_loc_dict[hex_num][0])
        y_exp_ls.append(add_coords_loc_dict[hex_num][1])
    add_coord_cols_exp_dict = {evn_lbl: evn_ls,
                               x_lbl: x_exp_ls,
                               y_lbl: y_exp_ls}
    exp_df = pd.DataFrame(data=add_coord_cols_exp_dict)

    # check the out_df and exp_df are equal
    if not out_df.equals(exp_df):
        fail_msg_ls.append(f"flashpoint_add_coord_cols returned dataframe different to expected, differences: "
                           f"\n {test_utils.get_dataframe_diff(df_exp=exp_df, df_act=out_df)}")

    test_utils.check_fail_ls(fail_msg_ls)


@pytest.mark.parametrize(
    ('kia_str_ls', 'fall_str_ls', 'exp_count_ls'),
    (
            (['r12'], ['g10'], [2]),
            (['r12, r12, g3'], ['r12'], [4]),
            (['r12', 'g15, g18,r15,l56'], ['g15,g16 ,g17', 'r15'], [4, 5]),
            (['r12, r15,g16,f54', 'd3,h5'], ['h16,c34, j17', 'r2 ,r1'], [7, 4]),
    )
)
@pytest.mark.parametrize(
    ('kia_col_lbl', 'fall_col_lbl'),
    (
            pytest.param('kia', 'fall', id='short labels)'),
            pytest.param('kia list', 'fall list', id='ful labels'),
    )
)
def test_flashpoint_kills_from_lists(test_utils, kia_col_lbl, fall_col_lbl, kia_str_ls, fall_str_ls, exp_count_ls):
    fail_msg_ls = []
    func = test_utils.get_cdf_func()

    kills_from_lists_test_dict = {kia_col_lbl: kia_str_ls,
                                  fall_col_lbl: fall_str_ls}
    input_df = pd.DataFrame(data=kills_from_lists_test_dict)

    out_df = func.flashpoint_kills_from_lists(input_df=input_df, kia_col_lbl=kia_col_lbl, fall_col_lbl=fall_col_lbl)
    act_count_ls = out_df['Kills'].to_list()

    for idx, exp_count in enumerate(exp_count_ls):
        act_count = act_count_ls[idx]
        kia_list = kills_from_lists_test_dict[kia_col_lbl][idx]
        fall_list = kills_from_lists_test_dict[fall_col_lbl][idx]
        if exp_count != act_count:
            fail_msg_ls.append(f"flashpoint_kills_from_lists returned {act_count} in Kills column"
                               f" with kia list {kia_list} and fall list {fall_list} ({exp_count} expected)")

    test_utils.check_fail_ls(fail_msg_ls)


@pytest.mark.parametrize(
    ('detail_key_ls', 'detail_val_cols', 'exp_ls'),
    (
            (['key a'], [['k a-val1', 'k a-val2', 'k a-val3']],
             ['{"key_a":"k_a-val1"}',
              '{"key_a":"k_a-val2"}',
              '{"key_a":"k_a-val3"}']),
            (['key a', 'key b'], [['k a val1', 'k a val2'],
                                  ['k b val1', 'k b val2']],
             ['{"key_a":"k_a_val1", "key_b":"k_b_val1"}',
              '{"key_a":"k_a_val2", "key_b":"k_b_val2"}']),
            (['key a', 'key b', 'key c'], [['a val1'], ['b val1'], ['c val1']],
             ['{"key_a":"a_val1", "key_b":"b_val1", "key_c":"c_val1"}']),

    )
)
def test_encode_event_detail_list(test_utils, detail_key_ls, detail_val_cols, exp_ls):
    fail_msg_ls = []
    func = test_utils.get_cdf_func()

    detail_val_cols_ls = []
    for ls in list(detail_val_cols):
        detail_val_cols_ls.append(ls)

    out_ls = func.encode_event_detail_list(*detail_val_cols_ls, detail_keys=detail_key_ls)
    if out_ls != exp_ls:
        fail_msg_ls.append(f"encode_event_detail_list returned {out_ls} with key list {detail_key_ls} and "
                           f"value lists {detail_val_cols_ls} but expected {exp_ls}")

    test_utils.check_fail_ls(fail_msg_ls)


@pytest.mark.parametrize(
    ('detail_key_ls', 'detail_val_ls', 'exp_str'),
    (
            (['key_a'], ['val_a'], '{"key_a":"val_a"}'),
            (['key a'], ['val a'], '{"key_a":"val_a"}'),
            (['k"e:,y a'], ['""va,l:a'], '{"k_e__y_a":"__va_l_a"}'),
            (['{key a}'], ['val{}a'], '{"_key_a_":"val__a"}'),
            (['key a', 'key b'], ['val a', 'val b'], '{"key_a":"val_a", "key_b":"val_b"}'),
            (['key a', 'key b'], ['val a'], '{"key_a":"val_a", "key_b":"no_val"}'),
            (['key a'], ['val a', 'val b'], '{"key_a":"val_a", "no_key":"val_b"}'),

    )
)
def test_encode_event_detail(test_utils, detail_key_ls, detail_val_ls, exp_str):
    fail_msg_ls = []
    func = test_utils.get_cdf_func()

    out_str = func.encode_event_detail(detail_key_ls=detail_key_ls, detail_val_ls=detail_val_ls)
    if out_str != exp_str:
        fail_msg_ls.append(f"encode event detail returned {out_str} "
                           f"for key_ls {detail_key_ls} and val_ls {detail_val_ls} but {exp_str} was expected")

    test_utils.check_fail_ls(fail_msg_ls)


@pytest.mark.parametrize(
    ('input_str', 'exp_bool'),
    (
            ('1', True),
            ('0', False),
            ('y', False),
            ('', False),
            (None, False),
    )
)
def test_parse_config_bool(test_utils, input_str, exp_bool):
    fail_msg_ls = []
    func = test_utils.get_cdf_func()

    out_bool = func.parse_config_bool(input_str)
    if out_bool != exp_bool:
        fail_msg_ls.append(f"parse_config_bool returned {out_bool} for {input_str} but {exp_bool} expected")

    test_utils.check_fail_ls(fail_msg_ls)


@pytest.mark.parametrize(
    ('input_str', 'exp_str'),
    (
            pytest.param(r'input/files\game1', r'input\files\game1', id='relative, mixed fwd and bck slash'),
            pytest.param(r'Input/game1', r'Input\game1', id='relative, fwd slash'),
            pytest.param(r'./Input/game1', r'Input\game1', id='relative, ./ notation'),
            pytest.param(r'../Output', r'..\Output', id='relative, ../ notation'),
            pytest.param(r'/Input/game', r'\Input\game', id='absolute, leading slash'),
            pytest.param(r'C:/files/InputData', r'C:\files\InputData', id='absolute, drive letter'),
            pytest.param(r'C://\\\files//\/\/InputData', r'C:\files\InputData', id='absolute, multiple mixed slashes'),
            pytest.param(r'Input///\\//Files', r'Input\Files', id='relative, multiple mixed slashes'),
            pytest.param(r'Input/Game1/', r'Input\Game1', id='relative, trailing fwd slash'),
            pytest.param(r'/Home/Files/InputData\\', r'\Home\Files\InputData', id='absolute, trailing bck slash'),
    )
)
def test_parse_config_location(test_utils, input_str, exp_str):
    """
    Note that this test is set up for Windows systems
    """
    fail_msg_ls = []
    func = test_utils.get_cdf_func()

    out_str = func.parse_config_location(input_loc=input_str)
    if out_str != exp_str:
        fail_msg_ls.append(f"parse_config_location returned {out_str} for {input_str} but {exp_str} expected")

    test_utils.check_fail_ls(fail_msg_ls)
