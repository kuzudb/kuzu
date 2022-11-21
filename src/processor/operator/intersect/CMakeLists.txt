add_library(kuzu_processor_operator_intersect STATIC
        intersect.cpp
        intersect_build.cpp
        intersect_hash_table.cpp)

#target_include_directories(operator_intersect PUBLIC ../../../include)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_intersect>
        PARENT_SCOPE)
