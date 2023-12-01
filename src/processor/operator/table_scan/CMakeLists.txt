add_library(kuzu_processor_operator_table_scan
        OBJECT
        factorized_table_scan.cpp
        union_all_scan.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_table_scan>
        PARENT_SCOPE)
