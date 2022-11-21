add_library(kuzu_processor_operator_table_scan
        OBJECT
        factorized_table_scan.cpp
        union_all_scan.cpp)

#target_include_directories(operator_table_scan PUBLIC ../../../include)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_table_scan>
        PARENT_SCOPE)
