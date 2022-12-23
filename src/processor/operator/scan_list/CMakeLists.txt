add_library(kuzu_processor_operator_scan_list
        OBJECT
        scan_rel_table_lists.cpp
        )

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_scan_list>
        PARENT_SCOPE)
