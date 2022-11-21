add_library(kuzu_processor_operator_aggregate
        OBJECT
        aggregate_hash_table.cpp
        base_aggregate.cpp
        base_aggregate_scan.cpp
        hash_aggregate.cpp
        hash_aggregate_scan.cpp
        simple_aggregate.cpp
        simple_aggregate_scan.cpp)

#target_include_directories(operator_aggregate PUBLIC ../../../include)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_aggregate>
        PARENT_SCOPE)
