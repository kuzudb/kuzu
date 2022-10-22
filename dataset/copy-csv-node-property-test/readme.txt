vPerson.csv
# The randomString structured property contains 1000 randomly generated strings with the following
# length x count pairs: 10x100, 4x10, 90x100, 40x100, 2x10, 780x80, 9x100,
# 2000x10, 13x100, 6x100, 83x100, 23x90, 41x100.
# Every 100th node's string properties are NULL (so v0, v100, v200, ..., v900)
# Except for 3 nodes, each node also contains 4 unstructured properties: (1) STRING: (strPropKey1, strPropVal1);
# (2) INT64: (int64PropKey1, 1); (3)  DOUBLE: (doublePropKey1, 1.0); and (4) BOOLEAN: (boolPropKey1, True).
# Exceptions are nodes with offsets 300, 400, and 500. These contain: 300x4, 400x4, and 500x4 unstructured properties:
# e.g., for node 300: (1) (strPropKey0, strPropVal0),  ..., (strPropKey299, strPropVal299); (2) (int64PropKey0, 0), ...
# (int64PropKey0, 299); (3) (doublePropKey0, 0.0), ..., (doublePropKey299, 299.0); (4) (boolPropKey1, False), ...,
# (boolPropKey1, False) (note that the exception vertices are False this time and the key indices start from 0 not 1).
# At least 1 unstructured property of the following nodes spans 2 pages: 77, 154, 231, 300, 310, 387, 400, 465, 500,
# 589, 666, 743, 821, 898, 975.
