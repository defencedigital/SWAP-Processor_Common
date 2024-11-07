import pytest


@pytest.mark.parametrize(
    'export_import',
    (
            pytest.param(False, id=''),
            pytest.param(True, id='export-import entity data'),
    )
)
@pytest.mark.parametrize(
    'ent_uid',
    (
            't-1', 't-2', 't-3', 't-test', 1, 'one', '',
    )
)
def test_add_entity(test_utils, export_import, ent_uid):
    """
    For each ent_uid:
        Check the Dataset instance has no entities
        Try getting the index for the ent_uid, should return None as entity unknown
        Add an entity with that uid, confirm by getting index as not None (double check vs. get_num_entities)
        Add the entity uid again, confirm not added by getting length of entities array as 1
        Remove the entity, confirm by getting index for ent_uid as None
        Check that Dataset instance has no entities

    if the export_import parameter is True then the entity_export_dict and entity_import_dict functions will be tested
    on entity add and entity remove
    """
    fail_msg_ls = []
    test_dataset = test_utils.make_dataset(dataset_config={'output_location': 'Output/EntityAddTest'})

    if test_dataset.get_num_entities() != 0:
        fail_msg_ls.append(f"Dataset instance has pre-existing entities")

    if test_dataset.get_entity_index(ent_uid) is not None:
        fail_msg_ls.append(f"entity {ent_uid} recognised by Dataset instance before being added")

    test_dataset.add_entity(ent_uid)
    if export_import:
        test_utils.dataset_export_import_entities(dataset=test_dataset)

    if test_dataset.get_entity_index(ent_uid) is None or len(test_dataset.entities) == 0:
        fail_msg_ls.append(f"entity {ent_uid} was not added to Dataset instance")

    test_dataset.add_entity(ent_uid)
    if export_import:
        test_utils.dataset_export_import_entities(dataset=test_dataset)

    if len(test_dataset.entities) > 1:
        fail_msg_ls.append(f"duplicate entity with uid {ent_uid} added to dataset")

    test_dataset.remove_entity(ent_uid)
    if export_import:
        test_utils.dataset_export_import_entities(dataset=test_dataset)

    if test_dataset.get_entity_index(ent_uid) is not None:
        fail_msg_ls.append(f"entity {ent_uid} was not removed from Dataset instance")
    elif len(test_dataset.entities) != 0:
        fail_msg_ls.append(f"Dataset instance still has entities following removal of added entity")

    test_utils.check_fail_ls(fail_msg_ls)
