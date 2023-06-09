add_library(kuzu_processor_operator_ver_length_extend
        OBJECT
        frontier.cpp
        frontier_scanner.cpp
        recursive_join.cpp
        scan_frontier.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_ver_length_extend>
        PARENT_SCOPE)
