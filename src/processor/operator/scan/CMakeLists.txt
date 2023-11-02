add_library(kuzu_processor_operator_scan
        OBJECT
        scan_multi_node_tables.cpp
        scan_node_table.cpp
        scan_rel_csr_columns.cpp
        scan_rel_regular_columns.cpp
        scan_table.cpp
        scan_multi_rel_tables.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_scan>
        PARENT_SCOPE)
