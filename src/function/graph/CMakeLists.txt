add_library(kuzu_function_graph
        OBJECT
        create_graph_function.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_function_graph>
        PARENT_SCOPE)
