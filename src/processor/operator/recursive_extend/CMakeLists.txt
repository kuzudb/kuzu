add_library(kuzu_processor_operator_ver_length_extend
        OBJECT
        all_shortest_path_state.cpp
        bfs_scheduler.cpp
        bfs_state.cpp
        frontier.cpp
        frontier_scanner.cpp
        recursive_join.cpp
        path_property_probe.cpp
        scan_frontier.cpp
        variable_length_state.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_ver_length_extend>
        PARENT_SCOPE)
