add_library(kuzu_function_algorithm
        OBJECT
        all_shortest_paths.cpp
        gds.cpp
        gds_frontier.cpp
        gds_task.cpp
        gds_utils.cpp
        k_core_decomposition.cpp
        output_writer.cpp
        page_rank.cpp
        rec_joins.cpp
        single_shortest_paths.cpp
        variable_length_path.cpp
        weakly_connected_components.cpp
        weighted_shortest_paths.cpp
        )

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_function_algorithm>
        PARENT_SCOPE)
