add_library(kuzu_processor_operator_scan_column
        OBJECT
        adj_column_extend.cpp
        scan_column.cpp
        scan_column_property.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_scan_column>
        PARENT_SCOPE)
