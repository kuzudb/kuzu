vPerson.csv
# A Non-Empty DB with 999 nodes, none of which have any unstructured properties. This ensures
# that the list headers for unstructured properties all have UINT32_T_MAX values. This gives
# a disk array that is non-empty and has predictable values in them, which is 0 because each list
# is empty and starts at csrOffset 0 (see BaseListHeaders::getSmallListHeader function as of
# July 26, 2022).
