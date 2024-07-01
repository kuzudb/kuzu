add_library(kuzu_function_algorithm
        OBJECT
        ife_morsel.cpp
        gds.cpp
        gds_task.cpp
        page_rank.cpp
        1T1S_parallel_shortest_path.cpp
        nT1S_parallel_shortest_path.cpp
        nTkS_parallel_shortest_path.cpp
        parallel_utils.cpp
        shortest_paths.cpp
        gds_utils.cpp
        weakly_connected_components.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_function_algorithm>
        PARENT_SCOPE)
