add_library(kuzu_function_algorithm
        OBJECT
        gds.cpp
        gds_frontier.cpp
        gds_task.cpp
        page_rank.cpp
        rec_joins.cpp
        all_shortest_paths.cpp
        single_shortest_paths.cpp
        gds_utils.cpp
        output_writer.cpp
        weakly_connected_components.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_function_algorithm>
        PARENT_SCOPE)
