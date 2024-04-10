add_library(kuzu_graph_algorithm
        OBJECT
        demo.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_graph_algorithm>
        PARENT_SCOPE)
