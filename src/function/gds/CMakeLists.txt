add_library(kuzu_function_algorithm
        OBJECT
        asp_destinations.cpp
        asp_paths.cpp
        awsp_paths.cpp
        bfs_graph.cpp
        gds.cpp
        gds_frontier.cpp
        gds_state.cpp
        gds_task.cpp
        gds_utils.cpp
        k_core_decomposition.cpp
        output_writer.cpp
        page_rank.cpp
        rec_joins.cpp
        ssp_destinations.cpp
        ssp_paths.cpp
        variable_length_path.cpp
        weakly_connected_components.cpp
        strongly_connected_components_kosaraju.cpp
        strongly_connected_components.cpp
        wsp_destinations.cpp
        wsp_paths.cpp
        )

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_function_algorithm>
        PARENT_SCOPE)
