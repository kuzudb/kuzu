add_library(kuzu_processor_operator_ver_length_extend
        OBJECT
        bfs_state.cpp
        recursive_join.cpp
        shortest_path_recursive_join.cpp
        variable_length_recursive_join.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_processor_operator_ver_length_extend>
        PARENT_SCOPE)
