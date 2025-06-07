add_library(kuzu_graph
        OBJECT
        graph.cpp
        graph_entry.cpp
        graph_entry_set.cpp
        on_disk_graph.cpp
        parsed_graph_entry.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_graph>
        PARENT_SCOPE)
