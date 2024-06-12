add_library(kuzu_function_algorithm
        OBJECT
        ife_morsel.cpp
        page_rank.cpp
        parallel_shortest_path.cpp
        parallel_utils.cpp
        shortest_paths.cpp
        variable_length_paths.cpp
        weakly_connected_components.cpp
        )

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_function_algorithm>
        PARENT_SCOPE)
