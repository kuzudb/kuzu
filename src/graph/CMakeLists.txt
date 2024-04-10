add_subdirectory(algorithm)

add_library(kuzu_graph
        OBJECT
        graph_function.cpp
        graph.cpp
        on_disk_graph.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_graph>
        PARENT_SCOPE)
