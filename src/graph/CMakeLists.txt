add_library(kuzu_graph
        OBJECT
        in_mem_graph.cpp
        on_disk_graph.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_graph>
        PARENT_SCOPE)
