add_subdirectory(graph)

add_library(kuzu_algorithm
        OBJECT
        graph.cpp
        on_disk_graph.cpp
        parallel_utils.cpp
        extend_frontier_task.cpp
        )

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_algorithm>
        PARENT_SCOPE)
