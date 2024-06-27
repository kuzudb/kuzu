add_library(kuzu_function_algorithm
        OBJECT
        gds.cpp
        gds_task.cpp
        page_rank.cpp
        shortest_paths.cpp
        gds_utils.cpp
        weakly_connected_components.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_function_algorithm>
        PARENT_SCOPE)
