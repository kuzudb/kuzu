add_library(kuzu_processor_operator_scan
        OBJECT
        generic_scan_rel_tables.cpp
        scan_columns.cpp
        scan_node_table.cpp
        scan_rel_table.cpp
        scan_rel_table_columns.cpp
        scan_rel_table_lists.cpp
        )

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_scan>
        PARENT_SCOPE)
