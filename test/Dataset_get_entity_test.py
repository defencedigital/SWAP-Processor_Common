import pytest

entity_lists = [['t-1'],
                ['t-1', 't-2'],
                ['one', 'two', 'three', 'four', 'five', 'six']]


@pytest.fixture(params=entity_lists)
def get_ent_ls(request):
    return request.param


@pytest.mark.parametrize(
    'export_import',
    (
            pytest.param(False, id=''),
            pytest.param(True, id='export-import entity data'),
    )
)
def test_get_num_entities(test_utils, export_import, get_ent_ls):
    """
    Create a dataset instance
    Add entities from ent_ls one at a time and check that get_num_entities returns the expected number
    Remove the entities one at a time and check that the get_num_entities function still returns the expected number
    The whole sequence is repeated to test adding entities that had previously been added and removed

    if the export_import parameter is True then the entity_export_dict and entity_import_dict functions will be tested
    """
    fail_msg_ls = []
    test_dataset = test_utils.make_dataset()

    ent_ls = get_ent_ls

    for i in range(0, 2):
        num_exp = 0
        for uid in ent_ls:
            test_dataset.add_entity(uid)
            num_exp += 1
            if export_import:
                test_utils.dataset_export_import_entities(dataset=test_dataset)
            num_act = test_dataset.get_num_entities()
            if num_act != num_exp:
                fail_msg_ls.append(f"get_num_entities returned {num_act} but {num_exp} expected")

        for uid in ent_ls:
            test_dataset.remove_entity(uid)
            num_exp -= 1
            if export_import:
                test_utils.dataset_export_import_entities(dataset=test_dataset)
            num_act = test_dataset.get_num_entities()
            if num_act != num_exp:
                fail_msg_ls.append(f"get_num_entities returned {num_act} but {num_exp} expected")

    test_utils.check_fail_ls(fail_msg_ls)


@pytest.mark.parametrize(
    'export_import',
    (
            pytest.param(False, id=''),
            pytest.param(True, id='export-import entity data'),
    )
)
def test_get_uid_ls(test_utils, export_import, get_ent_ls):
    """
    Create a dataset instance
    Add entities one at a time from entity_ls and check that the list returned from get_uid_ls:
        - contains the uid of the added entity
        - is of the expected length

    remove entities one at a time and check that the list returned from get_uid_ls:
        - does not contain the uid of the removed entity
        - is of the expected length

    The whole sequence is repeated to test adding entities that had previously been added and removed

    if the export_import parameter is True then the entity_export_dict and entity_import_dict functions will be tested
    """
    fail_msg_ls = []
    test_dataset = test_utils.make_dataset()

    ent_ls = get_ent_ls

    for i in range(0, 2):
        num_exp = 0
        for uid in ent_ls:
            test_dataset.add_entity(uid)
            num_exp += 1
            if export_import:
                test_utils.dataset_export_import_entities(dataset=test_dataset)
            list_act = test_dataset.get_uid_ls()
            num_act = len(list_act)
            if num_act != num_exp:
                fail_msg_ls.append(f"length of list returned by get_uid_ls was {num_act} but expected {num_exp}")
            if uid not in list_act:
                fail_msg_ls.append(f"added uid {uid} not in list returned by get_uid_ls")

        for uid in ent_ls:
            test_dataset.remove_entity(uid)
            num_exp -= 1
            if export_import:
                test_utils.dataset_export_import_entities(dataset=test_dataset)
            list_act = test_dataset.get_uid_ls()
            num_act = len(list_act)
            if num_act != num_exp:
                fail_msg_ls.append(f"length of list returned by get_uid_ls was {len(test_dataset.get_uid_ls())} "
                                   f"but expected {num_exp}")
            if uid in list_act:
                fail_msg_ls.append(f"removed uid {uid} still present in list returned by get_uid_ls")

    test_utils.check_fail_ls(fail_msg_ls)


@pytest.mark.parametrize(
    'export_import',
    (
            pytest.param(False, id=''),
            pytest.param(True, id='export-import entity data'),
    )
)
def test_get_ent_idx(test_utils, export_import, get_ent_ls):
    """
    create a dataset instance
    add entities one at a time from ent_ls and check that the index returned by get_ent_idx for all uids is correct
    remove entities one at a time and check that the index returned by get_ent_ix for all uids (inc. None for removed
    uids) is correct.
    The whole sequence is repeated to test adding entities that had previously been added and removed

    if the export_import parameter is True then the entity_export_dict and entity_import_dict functions will be tested
    """
    fail_msg_ls = []
    test_dataset = test_utils.make_dataset()

    ent_ls = get_ent_ls

    for uid in ent_ls:
        test_dataset.add_entity(uid)
        if export_import:
            test_utils.dataset_export_import_entities(dataset=test_dataset)

        for search_uid in test_dataset.get_uid_ls():
            idx_exp = ent_ls.index(search_uid)
            idx_act = test_dataset.get_entity_index(search_uid)
            if idx_act != idx_exp:
                fail_msg_ls.append(f"get_entity_index returned index {idx_act} "
                                   f"for {search_uid} but index {idx_exp} expected")

    removed_count = 0
    for uid in ent_ls:
        test_dataset.remove_entity(uid)
        removed_count += 1
        if export_import:
            test_utils.dataset_export_import_entities(dataset=test_dataset)

        for search_uid in ent_ls:
            idx_exp = ent_ls.index(search_uid) - removed_count
            idx_act = test_dataset.get_entity_index(search_uid)
            if idx_exp < 0:
                if idx_act is not None:
                    fail_msg_ls.append(f"get_entity_index returned index {idx_act} "
                                       f"for removed entity {search_uid} (None expected)")
            else:
                if idx_act != idx_exp:
                    fail_msg_ls.append(f"get_entity_index returned index {idx_act} "
                                       f"for {search_uid} but index {idx_exp} expected")

    test_utils.check_fail_ls(fail_msg_ls)
