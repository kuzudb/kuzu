add_library(kuzu_processor_operator_scan_list
        OBJECT
        adj_list_extend.cpp
        )

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_scan_list>
        PARENT_SCOPE)
