add_library(kuzu_processor_operator_shortestpath
        OBJECT
        scan_bfs_level.cpp
        simple_recursive_join.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_shortestpath>
        PARENT_SCOPE)
